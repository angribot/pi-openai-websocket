/**
 * WebSocket transport for the OpenAI Responses API, expressed as a `fetch`
 * replacement.
 *
 * The OpenAI SDK captures `globalThis.fetch` when a client is constructed. pi-ai's
 * `openai-responses` api builds its client synchronously, before its first `await`,
 * so a caller can swap the global for the duration of that synchronous window and
 * hand the SDK a transport that speaks WebSocket instead of HTTP.
 *
 * Everything above this layer (request body construction, retries, error
 * formatting, usage accounting, abort handling) stays pi-ai's, unmodified.
 *
 * Protocol: OpenAI's documented WebSocket mode for the Responses API.
 * `wss://{base}/responses`, one `{"type":"response.create", ...body}` text frame
 * per request, unwrapped `response.*` events back. No beta header.
 */

export type { FetchLike } from "./fetch-hook.ts";

import { applyContinuation, responseIdFrom, type Continuation, type SocketPool } from "./continuation.ts";
import { MARKER_HEADER, type FetchLike } from "./fetch-hook.ts";

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
	};
}

export interface WsFetchOptions {
	/** The real fetch, used for the SSE fallback. */
	realFetch: FetchLike;
	/** Handshake budget. 0 disables the timeout. */
	connectTimeoutMs: number;
	/** Per-frame idle budget. Undefined or 0 disables it. */
	idleTimeoutMs?: number;
	signal?: AbortSignal;
	stats: WsStats;
	/** Called once when a request falls back to HTTP SSE. */
	onFallback?: (reason: string) => void;
	/**
	 * Rewrites the request body before it goes out, for `previous_response_id`
	 * continuation. Returns the body to send.
	 */
	transformBody?: (body: Record<string, unknown>) => Record<string, unknown>;
	/** Called with the terminal `response.*` event, for continuation bookkeeping. */
	onTerminal?: (event: Record<string, unknown>) => void;
	/**
	 * Scope for remembering parameters the endpoint rejects, normally the provider
	 * name. Relays that forward to a Codex-style backend accept a narrower parameter
	 * set over WebSocket than over HTTP, and they name the offending field in the
	 * error, so it can be dropped and the request retried.
	 */
	unsupportedScope?: string;
	/**
	 * Socket pool. Supplying one enables `previous_response_id` continuation, whose
	 * state is bound to the socket. Without a pool every request opens and closes its
	 * own connection and always sends the full input.
	 */
	pool?: SocketPool;
	/** Pool bucket, normally session + provider + model. */
	poolKey?: string;
	/**
	 * Called when a response completes, with the continuation record held by the
	 * socket. The caller fills in `responseItems` from the finished assistant message
	 * and flips `complete`, which is what allows the next request to send a delta.
	 */
	onContinuation?: (continuation: Continuation) => void;
}

/** Frames that end a response. */
const TERMINAL_TYPES = new Set(["response.completed", "response.incomplete", "response.failed"]);

/** Parameters a given endpoint has rejected, keyed by scope. Lives for the process. */
const unsupportedParams = new Map<string, Set<string>>();

/** Ceiling on strip-and-retry rounds, so a misbehaving endpoint cannot spin. */
const MAX_STRIP_ROUNDS = 4;

export function knownUnsupportedParams(scope: string): string[] {
	return [...(unsupportedParams.get(scope) ?? [])];
}

export function resetUnsupportedParams(): void {
	unsupportedParams.clear();
}

/** `Unsupported parameter: max_output_tokens` -> `max_output_tokens`. */
export function unsupportedParamFrom(frame: Record<string, unknown>): string | undefined {
	if (frame.type !== "error") return undefined;
	const error = frame.error as { message?: unknown } | undefined;
	const message = typeof error?.message === "string" ? error.message : typeof frame.message === "string" ? frame.message : "";
	return /unsupported parameter:\s*'?([A-Za-z0-9_.]+)'?/i.exec(message)?.[1];
}

/** Headers that describe an HTTP body or route this request, and mean nothing to a WebSocket handshake. */
const DROPPED_HEADERS = new Set([
	"content-type",
	"content-length",
	"accept",
	"accept-encoding",
	"connection",
	MARKER_HEADER,
]);

export function createWsFetch(options: WsFetchOptions): { fetch: FetchLike; used: () => boolean } {
	let used = false;

	const wsFetch: FetchLike = async (input, init) => {
		const url = String(input instanceof Request ? input.url : input);
		const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();

		// Anything that is not the streaming Responses POST is none of our business.
		if (method !== "POST" || !new URL(url).pathname.endsWith("/responses") || typeof init?.body !== "string") {
			return options.realFetch(input, init);
		}

		used = true;
		options.stats.attempts++;

		const fallback = (reason: string): Promise<Response> => {
			options.stats.sseFallbacks++;
			options.stats.lastError = reason;
			options.onFallback?.(reason);
			// The marker has to go before delegating. With overlapping requests the fetch we
			// captured may itself be a dispatcher, which would route the retry straight back
			// here and loop.
			return options.realFetch(input, { ...init, headers: withoutMarker(init.headers) });
		};

		let body: Record<string, unknown>;
		try {
			body = JSON.parse(init.body) as Record<string, unknown>;
		} catch (error) {
			return fallback(`unparseable request body: ${errorText(error)}`);
		}

		const wsUrl = toWebSocketUrl(url);
		const headers = headerRecord(init.headers);
		const scope = options.unsupportedScope ?? new URL(wsUrl).host;
		const rejected = unsupportedParams.get(scope) ?? new Set<string>();
		unsupportedParams.set(scope, rejected);
		const pool = options.pool;
		const poolKey = options.poolKey ?? scope;

		for (let round = 0; ; round++) {
			let entry = pool?.acquire(poolKey);
			if (entry) {
				options.stats.connectionsReused++;
			} else {
				let socket: WebSocket;
				try {
					socket = await connect(wsUrl, headers, options);
				} catch (error) {
					return fallback(errorText(error));
				}
				options.stats.connected++;
				entry = pool?.add(poolKey, socket) ?? { socket, openedAt: Date.now(), lastUsedAt: Date.now(), busy: true };
			}

			// The delta is computed against what this very socket last produced, never
			// against a session-wide record: continuation state is connection local.
			// Stripping happens first so the recorded body and the compared body agree.
			const prepared = stripKeys(options.transformBody ? options.transformBody(body) : body, rejected);
			const sent = applyContinuation(prepared, entry.continuation);
			const done = (keep: boolean) => {
				if (pool) pool.release(poolKey, entry!, keep);
				else closeQuietly(entry!.socket);
			};

			try {
				entry.socket.send(JSON.stringify({ type: "response.create", ...sent }));
			} catch (error) {
				done(false);
				return fallback(`send failed: ${errorText(error)}`);
			}

			const frames = readFrames(entry.socket, {
				...options,
				onTerminal: (event) => {
					const responseId = responseIdFrom(event);
					if (responseId) {
						// `prepared` is the full-input form of this request, which is what the
						// next request's input has to extend.
						const record: Continuation = { requestBody: prepared, responseId, responseItems: [], complete: false };
						entry!.continuation = record;
						options.onContinuation?.(record);
					} else {
						entry!.continuation = undefined;
					}
					options.onTerminal?.(event);
				},
			});
			const first = await frames.next().catch((error: unknown) => {
				// Nothing streamed yet, so retrying over HTTP is still safe.
				return { done: true as const, value: error instanceof Error ? error : new Error(errorText(error)) };
			});

			if (first.done) {
				done(false);
				const reason = first.value instanceof Error ? first.value.message : "stream closed before any event";
				return fallback(reason);
			}

			// Some relays accept a narrower parameter set over WebSocket than over HTTP and
			// name the offending field. Drop it and retry: nothing has streamed yet.
			const unsupported = unsupportedParamFrom(first.value);
			if (unsupported && !rejected.has(unsupported) && round < MAX_STRIP_ROUNDS) {
				rejected.add(unsupported);
				options.stats.strippedParams = [...rejected];
				done(false);
				continue;
			}

			if (sent.previous_response_id) options.stats.deltaRequests++;
			else options.stats.fullRequests++;

			// A wrapped error frame carrying an HTTP status is reported as an HTTP error, so
			// pi-ai's own retry and error formatting see exactly what they see over SSE.
			const httpError = asHttpError(first.value);
			if (httpError) {
				entry.continuation = undefined;
				done(false);
				return httpError;
			}

			const encoder = new TextEncoder();
			const sse = async function* () {
				let complete = false;
				try {
					yield encoder.encode(sseFrame(first.value));
					for await (const frame of frames) yield encoder.encode(sseFrame(frame));
					complete = true;
				} finally {
					// An aborted or failed response leaves the server side in an unknown
					// state, so the socket is dropped rather than reused.
					if (!complete) entry!.continuation = undefined;
					done(complete && !options.signal?.aborted);
				}
			};

			return new Response(ReadableStream.from(sse()), {
				status: 200,
				headers: { "content-type": "text/event-stream" },
			});
		}
	};

	return { fetch: wsFetch, used: () => used };
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

/** Headers to keep for the WebSocket handshake, dropping HTTP body and routing ones. */
export function headerRecord(headers: RequestInit["headers"]): Record<string, string> {
	return filterHeaders(headers, (key) => !DROPPED_HEADERS.has(key));
}

/** Every header except the dispatch marker, which must never leave this process. */
export function withoutMarker(headers: RequestInit["headers"]): Record<string, string> {
	return filterHeaders(headers, (key) => key !== MARKER_HEADER);
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

async function connect(
	url: string,
	headers: Record<string, string>,
	options: Pick<WsFetchOptions, "connectTimeoutMs" | "signal">,
): Promise<WebSocket> {
	// `headers` is a Node extension to the WebSocket constructor; it is how auth
	// reaches the upgrade request.
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
 * closes, errors, aborts, or goes idle first.
 */
async function* readFrames(
	socket: WebSocket,
	options: Pick<WsFetchOptions, "idleTimeoutMs" | "signal" | "onTerminal">,
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
		void decode(event.data).then(
			(text) => {
				if (text === null) return;
				let frame: Record<string, unknown>;
				try {
					frame = JSON.parse(text) as Record<string, unknown>;
				} catch (error) {
					fail(new Error(`invalid websocket JSON: ${errorText(error)}`));
					return;
				}
				if (typeof frame.type === "string" && TERMINAL_TYPES.has(frame.type)) {
					sawTerminal = true;
					done = true;
					options.onTerminal?.(frame);
				}
				queue.push(frame);
				bump();
			},
			(error: unknown) => fail(new Error(`undecodable websocket frame: ${errorText(error)}`)),
		);
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

async function decode(data: unknown): Promise<string | null> {
	if (typeof data === "string") return data;
	if (data instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(data));
	if (ArrayBuffer.isView(data)) {
		const view = data as ArrayBufferView;
		return new TextDecoder().decode(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
	}
	if (data && typeof data === "object" && "arrayBuffer" in data) {
		const buffer = await (data as Blob).arrayBuffer();
		return new TextDecoder().decode(new Uint8Array(buffer));
	}
	return null;
}

function closeQuietly(socket: WebSocket): void {
	try {
		if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) socket.close();
	} catch {
		// Closing is best effort; a socket that refuses to close is already dead.
	}
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
