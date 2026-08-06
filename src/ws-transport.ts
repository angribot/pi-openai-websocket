/**
 * WebSocket transport for the OpenAI Responses API, expressed as a `fetch`
 * replacement.
 *
 * Pi-ai passes a per-request `fetch` to the OpenAI SDK. Handing it one that speaks
 * WebSocket replaces the transport without touching anything above it or mutating
 * process-global state.
 *
 * Everything above this layer (request body construction, retries, error
 * formatting, usage accounting, abort handling) stays pi-ai's, unmodified.
 *
 * Protocol: the current Responses WebSocket protocol used by Codex.
 * `wss://{base}/responses`, one `{"type":"response.create", ...body}` text frame
 * per request, unwrapped `response.*` events back, with the dated beta handshake.
 */

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

import { createHash } from "node:crypto";
import {
	applyContinuation,
	closeQuietly,
	responseIdFrom,
	type Continuation,
	type PooledSocket,
	type SocketPool,
} from "./continuation.ts";

export interface WsStats {
	/** Requests that attempted the WebSocket transport. */
	attempts: number;
	/** Successful handshakes. */
	connected: number;
	/** Requests that fell back to HTTP SSE. */
	sseFallbacks: number;
	/** Requests sent as a `previous_response_id` delta. */
	deltaRequests: number;
	/** Requests that sent the full input. */
	fullRequests: number;
	/** Handshakes avoided by reusing a pooled socket. */
	connectionsReused: number;
	/** Parameters dropped because the endpoint rejected them. */
	strippedParams: string[];
	/** Deltas the server refused because it no longer held the previous response. */
	staleContinuations: number;
	lastError?: string;
}

export function createStats(): WsStats {
	return {
		attempts: 0,
		connected: 0,
		sseFallbacks: 0,
		deltaRequests: 0,
		fullRequests: 0,
		connectionsReused: 0,
		strippedParams: [],
		staleContinuations: 0,
	};
}

/** One-line rendering, shared by `/ws-stats` and the smoke scripts. */
export function formatStats(stats: WsStats): string {
	return (
		`attempts=${stats.attempts} connected=${stats.connected} reused=${stats.connectionsReused} ` +
		`full=${stats.fullRequests} delta=${stats.deltaRequests} stale=${stats.staleContinuations} ` +
		`sseFallbacks=${stats.sseFallbacks}` +
		(stats.strippedParams.length ? ` stripped=${stats.strippedParams.join(",")}` : "") +
		(stats.lastError ? ` lastError="${stats.lastError}"` : "")
	);
}

export interface WsFetchOptions {
	/** Caller-provided fetch, used for pass-through requests and SSE fallback. */
	fallbackFetch: FetchLike;
	/** Handshake budget. 0 disables the timeout. */
	connectTimeoutMs: number;
	/** Per-frame idle budget. Undefined or 0 disables it. */
	idleTimeoutMs?: number;
	signal?: AbortSignal;
	stats: WsStats;
	/** Called once when a request falls back to HTTP SSE. */
	onFallback?: (reason: string) => void;
	/** Called when WebSocket transport fails, so later requests can prefer SSE. */
	onTransportUnavailable?: (failure: WsTransportUnavailable) => void;
	/**
	 * Named-session socket reuse. Without this every request opens and closes its own
	 * connection and always sends the full input.
	 */
	reuse?: {
		pool: SocketPool;
		sessionId: string;
		provider: string;
		model: string;
		/**
		 * Called when a response completes, with the continuation record held by the
		 * socket. The caller fills in `responseItems` from the finished assistant
		 * message and flips `complete`, enabling the next request to send a delta.
		 */
		onContinuation?: (continuation: Continuation) => void;
	};
}

export interface WsTransportUnavailable {
	phase: "before-stream-start" | "after-stream-start";
	reason: string;
}

/** Frames that end a response. */
const TERMINAL_TYPES = new Set(["response.completed", "response.incomplete", "response.failed"]);

/** Parameters rejected by a connection identity and request model. Lives for the process. */
const unsupportedParams = new Map<string, Set<string>>();

/** Ceiling on strip-and-retry rounds, so a misbehaving endpoint cannot spin. */
const MAX_STRIP_ROUNDS = 4;

/** Every parameter any endpoint has rejected. `stats` spans identities, so this must too. */
function allUnsupportedParams(): string[] {
	const all = new Set<string>();
	for (const set of unsupportedParams.values()) for (const name of set) all.add(name);
	return [...all].sort();
}

export function resetUnsupportedParams(): void {
	unsupportedParams.clear();
}

/** `Unsupported parameter: max_output_tokens` -> `max_output_tokens`. */
export function unsupportedParamFrom(frame: Record<string, unknown>): string | undefined {
	if (frame.type !== "error") return undefined;
	return /unsupported parameter:\s*'?([A-Za-z0-9_.]+)'?/i.exec(errorMessageOf(frame))?.[1];
}

/**
 * Whether the endpoint rejected the `previous_response_id` a delta was built on.
 * The response it referred to is gone, so the conversation has to be resent whole.
 */
export function isStaleContinuation(frame: Record<string, unknown>): boolean {
	if (frame.type !== "error") return false;
	if (errorCodeOf(frame) === "previous_response_not_found") return true;
	return /previous_response_not_found|previous response .*not found/i.test(errorMessageOf(frame));
}

function errorCodeOf(frame: Record<string, unknown>): unknown {
	return (frame.error as { code?: unknown } | undefined)?.code ?? frame.code;
}

function errorMessageOf(frame: Record<string, unknown>): string {
	const error = frame.error as { message?: unknown } | undefined;
	if (typeof error?.message === "string") return error.message;
	return typeof frame.message === "string" ? frame.message : "";
}

function isConnectionLimit(frame: Record<string, unknown>): boolean {
	return frame.type === "error" && errorCodeOf(frame) === "websocket_connection_limit_reached";
}

const BETA_HEADER = "OpenAI-Beta";
const BETA_HEADER_LOWER = BETA_HEADER.toLowerCase();
const BETA_VALUE = "responses_websockets=2026-02-06";
const WEBSOCKET_DROPPED_BODY_FIELDS = new Set(["stream"]);

/** Headers that describe an HTTP body and mean nothing to a WebSocket handshake. */
const DROPPED_HEADERS = new Set(["content-type", "content-length", "accept", "accept-encoding", "connection"]);

export function createWsFetch(options: WsFetchOptions): { fetch: FetchLike; sawRequest: () => boolean } {
	let sawRequest = false;
	let transportUnavailable = false;

	const wsFetch: FetchLike = async (input, init) => {
		const url = String(input instanceof Request ? input.url : input);
		const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();

		// Anything that is not the streaming Responses POST is none of our business.
		if (method !== "POST" || !new URL(url).pathname.endsWith("/responses") || typeof init?.body !== "string") {
			return options.fallbackFetch(input, init);
		}
		if (transportUnavailable) return options.fallbackFetch(input, init);

		sawRequest = true;

		const reportUnavailable = (phase: WsTransportUnavailable["phase"], reason: string) => {
			if (options.signal?.aborted) return;
			transportUnavailable = true;
			options.stats.lastError = reason;
			options.onTransportUnavailable?.({ phase, reason });
		};
		const fallback = (reason: string): Promise<Response> => {
			options.stats.sseFallbacks++;
			options.stats.lastError = reason;
			options.onFallback?.(reason);
			return options.fallbackFetch(input, init);
		};
		const fallbackUnavailable = (reason: string): Promise<Response> => {
			reportUnavailable("before-stream-start", reason);
			return fallback(reason);
		};

		let body: Record<string, unknown>;
		try {
			body = JSON.parse(init.body) as Record<string, unknown>;
		} catch (error) {
			return fallback(`unparseable request body: ${errorText(error)}`);
		}

		if (body.background === true) return options.fallbackFetch(input, init);

		options.stats.attempts++;
		const websocketBody = stripKeys(body, WEBSOCKET_DROPPED_BODY_FIELDS);

		const wsUrl = toWebSocketUrl(url);
		const headers = headerRecord(init.headers);
		const connectionIdentity = identityDigest(["connection", wsUrl, canonicalHeaderEntries(headers)]);
		const unsupportedKey = identityDigest(["unsupported", connectionIdentity, websocketBody.model]);
		const rejected = unsupportedParams.get(unsupportedKey) ?? new Set<string>();
		unsupportedParams.set(unsupportedKey, rejected);
		const reuse = options.reuse?.sessionId ? options.reuse : undefined;
		const poolKey = reuse
			? identityDigest([
					"pool",
					reuse.sessionId,
					reuse.provider,
					reuse.model,
					websocketBody.model,
					connectionIdentity,
				])
			: connectionIdentity;

		let entry: PooledSocket | undefined;
		/** Ends an attempt. `keep` pools the socket; otherwise it is closed. */
		const finish = (target: PooledSocket, keep: boolean) => {
			if (entry === target) entry = undefined;
			if (reuse) reuse.pool.release(target, keep);
			else closeQuietly(target.socket);
		};

		// Recovery budgets are independent, so one endpoint quirk cannot starve another.
		let stripRounds = 0;
		let retriedConnectionLimit = false;
		let forceFreshSocket = false;

		try {
			for (;;) {
				if (!entry) {
					entry = forceFreshSocket ? undefined : reuse?.pool.acquire(poolKey);
					forceFreshSocket = false;
					if (entry) {
						options.stats.connectionsReused++;
					} else {
						let socket: WebSocket;
						try {
							socket = await connect(wsUrl, headers, options);
						} catch (error) {
							return fallbackUnavailable(errorText(error));
						}
						options.stats.connected++;
						entry = reuse?.pool.add(poolKey, socket) ?? unpooledEntry(poolKey, socket);
					}
				}
				const attempt = entry;

				// The delta is computed against what this very socket last produced, never
				// against a session-wide record: continuation state is connection local.
				// Stripping happens first so the recorded body and the compared body agree.
				const prepared = stripKeys(websocketBody, rejected);
				const sent = applyContinuation(prepared, attempt.continuation);

				try {
					attempt.socket.send(JSON.stringify({ ...sent, type: "response.create" }));
				} catch (error) {
					finish(attempt, false);
					return fallbackUnavailable(`send failed: ${errorText(error)}`);
				}

				const frames = readFrames(attempt.socket, options, (event) => {
					const responseId = responseIdFrom(event);
					if (!responseId) {
						attempt.continuation = undefined;
						return;
					}
					// `prepared` is the full-input form of this request, which is what the next
					// request's input has to extend.
					const record: Continuation = { requestBody: prepared, responseId, responseItems: [], complete: false };
					attempt.continuation = record;
					reuse?.onContinuation?.(record);
				});
				let first: IteratorResult<Record<string, unknown>>;
				try {
					first = await frames.next();
				} catch (error) {
					finish(attempt, false);
					return fallbackUnavailable(errorText(error));
				}

				if (first.done) {
					finish(attempt, false);
					return fallbackUnavailable("stream closed before any event");
				}

				// A connection at its age limit is rejected before work starts. Retry once on
				// a newly opened socket, independently of the other recovery budgets.
				if (isConnectionLimit(first.value)) {
					await frames.return(undefined);
					finish(attempt, false);
					if (!retriedConnectionLimit) {
						retriedConnectionLimit = true;
						forceFreshSocket = true;
						continue;
					}
					const reason = errorMessageOf(first.value) || "websocket connection limit reached";
					return fallbackUnavailable(reason);
				}

				// Nothing has streamed yet, so the remaining recoveries can resend safely.

				// Some relays accept a narrower parameter set over WebSocket than over HTTP and
				// name the offending field. Drop it and retry.
				const unsupported = unsupportedParamFrom(first.value);
				if (unsupported && !rejected.has(unsupported) && stripRounds < MAX_STRIP_ROUNDS) {
					stripRounds++;
					rejected.add(unsupported);
					options.stats.strippedParams = allUnsupportedParams();
					await frames.return(undefined);
					finish(attempt, false);
					continue;
				}

				// The server no longer holds the response the delta chained onto. Forget the
				// continuation and resend the whole conversation on the same connection, once.
				if (sent.previous_response_id && isStaleContinuation(first.value)) {
					options.stats.staleContinuations++;
					attempt.continuation = undefined;
					await frames.return(undefined);
					continue;
				}

				// A wrapped error frame carrying an HTTP status is reported as an HTTP error, so
				// pi-ai's own retry and error formatting see exactly what they see over SSE.
				const httpError = asHttpError(first.value);
				if (httpError) {
					attempt.continuation = undefined;
					finish(attempt, false);
					return httpError;
				}

				// Counted only once the frame is known to be a real response.
				if (sent.previous_response_id) options.stats.deltaRequests++;
				else options.stats.fullRequests++;

				const encoder = new TextEncoder();
				const sse = async function* () {
					let complete = false;
					try {
						yield encoder.encode(sseFrame(first.value));
						for await (const frame of frames) yield encoder.encode(sseFrame(frame));
						complete = true;
					} catch (error) {
						reportUnavailable("after-stream-start", errorText(error));
						throw error;
					} finally {
						// An aborted or failed response leaves the server side in an unknown
						// state, so the socket is dropped rather than reused.
						if (!complete) attempt.continuation = undefined;
						finish(attempt, complete && !options.signal?.aborted);
					}
				};

				// The socket now belongs to the response body, which releases it when drained.
				entry = undefined;
				const responseBody = (
					ReadableStream as typeof ReadableStream & {
						from<T>(source: AsyncIterable<T>): ReadableStream<T>;
					}
				).from(sse());
				return new Response(responseBody, {
					status: 200,
					headers: { "content-type": "text/event-stream" },
				});
			}
		} finally {
			// Any path that leaves without handing the socket to a response body, including a
			// throw, must not leave it checked out of the pool.
			if (entry) finish(entry, false);
		}
	};

	return { fetch: wsFetch, sawRequest: () => sawRequest };
}

function unpooledEntry(key: string, socket: WebSocket): PooledSocket {
	return { key, socket, openedAt: Date.now(), lastUsedAt: Date.now(), busy: true };
}

function sseFrame(event: unknown): string {
	return `data: ${JSON.stringify(event)}\n\n`;
}

function stripKeys(body: Record<string, unknown>, keys: ReadonlySet<string>): Record<string, unknown> {
	if (keys.size === 0) return body;
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(body)) {
		if (!keys.has(key)) out[key] = value;
	}
	return out;
}

/** `https://host/v1/responses` -> `wss://host/v1/responses`. */
export function toWebSocketUrl(httpUrl: string): string {
	const url = new URL(httpUrl);
	if (url.protocol === "https:") url.protocol = "wss:";
	else if (url.protocol === "http:") url.protocol = "ws:";
	return url.toString();
}

/** Headers for the WebSocket handshake, including the fixed current protocol opt-in. */
export function headerRecord(headers: RequestInit["headers"]): Record<string, string> {
	const out = filterHeaders(headers, (key) => !DROPPED_HEADERS.has(key) && key !== BETA_HEADER_LOWER);
	out[BETA_HEADER] = BETA_VALUE;
	return out;
}

function filterHeaders(headers: RequestInit["headers"], keep: (lowerKey: string) => boolean): Record<string, string> {
	const out: Record<string, string> = {};
	const add = (key: string, value: string) => {
		if (keep(key.toLowerCase())) out[key] = value;
	};
	if (!headers) return out;
	if (headers instanceof Headers) headers.forEach((value, key) => add(key, value));
	else if (Array.isArray(headers)) for (const [key, value] of headers) add(key, value);
	else for (const [key, value] of Object.entries(headers)) add(key, String(value));
	return out;
}

function canonicalHeaderEntries(headers: Record<string, string>): [string, string][] {
	const canonical = new Map<string, string>();
	for (const [key, value] of Object.entries(headers)) canonical.set(key.toLowerCase(), value);
	return [...canonical.entries()].sort(([left], [right]) => left.localeCompare(right));
}

function identityDigest(parts: unknown[]): string {
	return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

async function connect(
	url: string,
	headers: Record<string, string>,
	options: Pick<WsFetchOptions, "connectTimeoutMs" | "signal">,
): Promise<WebSocket> {
	// `headers` is a Node extension to the WebSocket constructor; it is how auth
	// reaches the upgrade request.
	// ponytail: no proxy handling. Bun ignores proxy env vars for WebSocket and needs
	// a `proxy` option passed here; add it when someone runs this under Bun.
	const socket = new WebSocket(url, { headers } as unknown as string[]);
	socket.binaryType = "arraybuffer";

	return new Promise<WebSocket>((resolve, reject) => {
		const settle = (error?: Error) => {
			socket.removeEventListener("open", onOpen);
			socket.removeEventListener("error", onError);
			socket.removeEventListener("close", onClose);
			options.signal?.removeEventListener("abort", onAbort);
			if (timer) clearTimeout(timer);
			if (error) {
				closeQuietly(socket);
				reject(error);
			} else {
				resolve(socket);
			}
		};
		const onOpen = () => settle();
		const onError = () => settle(new Error("websocket handshake failed"));
		const onClose = (event: CloseEvent) =>
			settle(new Error(`websocket closed during handshake (${event.code}${event.reason ? `: ${event.reason}` : ""})`));
		const onAbort = () => settle(new Error("Request was aborted"));

		const timer =
			options.connectTimeoutMs > 0
				? setTimeout(() => settle(new Error(`websocket connect timeout after ${options.connectTimeoutMs}ms`)), options.connectTimeoutMs)
				: undefined;

		if (options.signal?.aborted) {
			settle(new Error("Request was aborted"));
			return;
		}
		socket.addEventListener("open", onOpen);
		socket.addEventListener("error", onError);
		socket.addEventListener("close", onClose);
		options.signal?.addEventListener("abort", onAbort);
	});
}

/**
 * Yields parsed frames until a terminal `response.*` event. Throws if the socket
 * closes, errors, aborts, or goes idle first. `onTerminal` fires before the terminal
 * frame is yielded, so continuation state is recorded even if the consumer stops.
 */
async function* readFrames(
	socket: WebSocket,
	options: Pick<WsFetchOptions, "idleTimeoutMs" | "signal">,
	onTerminal: (frame: Record<string, unknown>) => void,
): AsyncGenerator<Record<string, unknown>> {
	const queue: Record<string, unknown>[] = [];
	let wake: (() => void) | null = null;
	let done = false;
	let failure: Error | null = null;
	let sawTerminal = false;

	const bump = () => {
		const resolve = wake;
		wake = null;
		resolve?.();
	};
	const fail = (error: Error) => {
		failure ??= error;
		done = true;
		bump();
	};

	const onMessage = (event: MessageEvent) => {
		const text = decode(event.data);
		if (text === null) {
			fail(new Error("unexpected binary websocket frame"));
			return;
		}
		let frame: Record<string, unknown>;
		try {
			frame = JSON.parse(text) as Record<string, unknown>;
		} catch (error) {
			fail(new Error(`invalid websocket JSON: ${errorText(error)}`));
			return;
		}
		if (frame.type === "codex.rate_limits") {
			bump();
			return;
		}
		if (typeof frame.type === "string" && TERMINAL_TYPES.has(frame.type)) {
			sawTerminal = true;
			done = true;
			onTerminal(frame);
		}
		queue.push(frame);
		bump();
	};
	const onError = () => fail(new Error("websocket error"));
	const onClose = (event: CloseEvent) => {
		if (sawTerminal) {
			done = true;
			bump();
			return;
		}
		fail(new Error(`websocket closed before a terminal response event (${event.code}${event.reason ? `: ${event.reason}` : ""})`));
	};
	const onAbort = () => fail(new Error("Request was aborted"));

	socket.addEventListener("message", onMessage);
	socket.addEventListener("error", onError);
	socket.addEventListener("close", onClose);
	options.signal?.addEventListener("abort", onAbort);

	try {
		for (;;) {
			if (queue.length > 0) {
				yield queue.shift()!;
				continue;
			}
			if (done) break;

			const idle = options.idleTimeoutMs;
			let timer: NodeJS.Timeout | undefined;
			await new Promise<void>((resolve) => {
				wake = resolve;
				if (idle && idle > 0) {
					timer = setTimeout(() => {
						fail(new Error(`websocket idle timeout after ${idle}ms`));
					}, idle);
				}
			}).finally(() => {
				if (timer) clearTimeout(timer);
			});
		}
		if (failure) throw failure;
		if (!sawTerminal) throw new Error("websocket stream ended before a terminal response event");
	} finally {
		socket.removeEventListener("message", onMessage);
		socket.removeEventListener("error", onError);
		socket.removeEventListener("close", onClose);
		options.signal?.removeEventListener("abort", onAbort);
	}
}

/**
 * Maps a wrapped `{"type":"error","status":429,...}` frame onto an HTTP error
 * response. Returns undefined for anything else, including bare error frames with
 * no status, which the normal event pipeline already handles.
 */
function asHttpError(frame: Record<string, unknown>): Response | undefined {
	if (frame.type !== "error" || typeof frame.status !== "number") return undefined;
	const headers = new Headers();
	if (frame.headers && typeof frame.headers === "object") {
		for (const [key, value] of Object.entries(frame.headers as Record<string, unknown>)) {
			if (typeof value === "string") headers.set(key, value);
		}
	}
	if (!headers.has("content-type")) headers.set("content-type", "application/json");
	const payload = frame.error ?? { message: frame.message ?? "websocket error" };
	return new Response(JSON.stringify({ error: payload }), { status: frame.status, headers });
}

/** Frame payload as text. `binaryType` is `arraybuffer`, so those are the only cases. */
function decode(data: unknown): string | null {
	if (typeof data === "string") return data;
	if (data instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(data));
	return null;
}

export function errorText(error: unknown): string {
	if (error instanceof Error) return error.message;
	if (typeof error === "string") return error;
	try {
		return JSON.stringify(error) ?? String(error);
	} catch {
		return String(error);
	}
}
