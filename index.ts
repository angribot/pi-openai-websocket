/**
 * pi-openai-websocket
 *
 * Gives third-party `openai-responses` providers the WebSocket transport that pi
 * otherwise only offers through the proprietary `openai-codex-responses` api.
 *
 * The transport is swapped underneath pi-ai's own `openai-responses`
 * implementation, so request construction, retries, error formatting, usage
 * accounting and abort handling stay pi-ai's. See src/ws-transport.ts.
 *
 * Configure in ~/.pi/agent/settings.json:
 *
 *   { "openaiWebsocket": { "providers": ["my-provider"] } }
 *
 * Transport choice follows pi's built-in `transport` setting.
 */

import {
	openAIResponsesApi,
	registerSessionResourceCleanup,
	type Api,
	type AssistantMessage,
	type AssistantMessageEventStream,
	type Context,
	type Model,
	type SimpleStreamOptions,
} from "@earendil-works/pi-ai/compat";
import { SettingsManager, getAgentDir } from "@earendil-works/pi-coding-agent";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { SocketPool, serverItems, type Continuation } from "./src/continuation.ts";
import { installFetchHandler, MARKER_HEADER } from "./src/fetch-hook.ts";
import { createStats, createWsFetch, errorText, type FetchLike, type WsStats } from "./src/ws-transport.ts";

/**
 * pi's extension loader aliases the pi-ai root to the compat entrypoint and maps no
 * `./api/*` subpaths, so the api implementation is reached through compat's lazy
 * accessor rather than imported directly. `/compat` is spelled out because the bare
 * root resolves elsewhere outside the loader.
 */
const responsesApi = openAIResponsesApi();

/** Matches pi's own default when the setting is unset. */
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;

interface WsSettings {
	openaiWebsocket?: {
		providers?: string[];
	};
}

type TransportSetting = "sse" | "websocket" | "websocket-cached" | "auto";

export default function extension(pi: ExtensionAPI): void {
	const stats = createStats();
	const pool = new SocketPool();
	let uiContext: ExtensionContext | undefined;
	let warnedFallback = false;

	const settings = readSettings();
	const providers = settings.providers;

	const warnFallback = (reason: string) => {
		if (warnedFallback) return;
		warnedFallback = true;
		const message = `WebSocket transport unavailable (${reason}); using HTTP SSE.`;
		if (uiContext?.hasUI) uiContext.ui.notify(`[pi-openai-websocket] ${message}`, "warning");
		else console.warn(`[pi-openai-websocket] ${message}`);
	};

	for (const provider of providers) {
		pi.registerProvider(provider, {
			api: "openai-responses",
			streamSimple: (model, context, options) =>
				streamOverWebSocket(model, context, options, { stats, settings, warnFallback, pool }),
		});
	}

	pi.on("session_start", (_event, ctx) => {
		uiContext = ctx;
	});

	pi.on("session_shutdown", () => {
		pool.closeAll();
	});

	// Modes without a session teardown still need the sockets gone, or the process
	// lingers on a live connection.
	if (providers.length > 0) registerSessionResourceCleanup(() => pool.closeAll());

	pi.registerCommand("ws-stats", {
		description: "OpenAI Responses WebSocket transport stats",
		handler: async (_args, ctx) => {
			const line = providers.length
				? `providers=${providers.join(",")} attempts=${stats.attempts} connected=${stats.connected} ` +
					`reused=${stats.connectionsReused} open=${pool.size} full=${stats.fullRequests} ` +
					`delta=${stats.deltaRequests} sseFallbacks=${stats.sseFallbacks}` +
					(stats.strippedParams.length ? ` stripped=${stats.strippedParams.join(",")}` : "") +
					(stats.lastError ? ` lastError="${stats.lastError}"` : "")
				: "no providers configured; set openaiWebsocket.providers in settings.json";
			if (ctx.hasUI) ctx.ui.notify(line, "info");
			else console.warn(line);
		},
	});
}

export interface ResolvedSettings {
	providers: string[];
	transport: TransportSetting;
	connectTimeoutMs: number;
}

function readSettings(): ResolvedSettings {
	try {
		const manager = SettingsManager.create(process.cwd(), getAgentDir());
		const global = manager.getGlobalSettings() as WsSettings;
		return {
			providers: global.openaiWebsocket?.providers ?? [],
			transport: (manager.getTransport() as TransportSetting) ?? "auto",
			connectTimeoutMs: manager.getWebSocketConnectTimeoutMs() ?? DEFAULT_CONNECT_TIMEOUT_MS,
		};
	} catch (error) {
		console.warn(`[pi-openai-websocket] could not read settings: ${errorText(error)}`);
		return { providers: [], transport: "auto", connectTimeoutMs: DEFAULT_CONNECT_TIMEOUT_MS };
	}
}

export interface StreamDeps {
	stats: WsStats;
	settings: ResolvedSettings;
	warnFallback: (reason: string) => void;
	pool: SocketPool;
}

/**
 * Runs pi-ai's `openai-responses` stream with the WebSocket transport installed.
 *
 * pi-ai reaches its api implementations lazily, so the SDK client is built after an
 * `await` and the hook has to stay installed for the whole request. Dispatch is
 * opt-in through a marker header, so nothing else in the process is affected.
 */
export function streamOverWebSocket(
	model: Model<Api>,
	context: Context,
	options: SimpleStreamOptions | undefined,
	deps: StreamDeps,
): AssistantMessageEventStream {
	// `transport` is absent on non-agent calls such as compaction, so fall back to
	// the configured value rather than assuming "auto".
	const transport = (options?.transport as TransportSetting | undefined) ?? deps.settings.transport;
	if (transport === "sse") return responsesApi.streamSimple(model, context, options);

	// Continuation needs a socket that survives between turns, so "websocket" without
	// caching gets a fresh connection each time and always sends the full input.
	const pooled = transport !== "websocket";
	let pending: Continuation | undefined;

	const { fetch: wsFetch, used } = createWsFetch({
		realFetch: globalThis.fetch as FetchLike,
		connectTimeoutMs: options?.websocketConnectTimeoutMs ?? deps.settings.connectTimeoutMs,
		idleTimeoutMs: options?.timeoutMs,
		signal: options?.signal,
		stats: deps.stats,
		unsupportedScope: model.provider,
		onFallback: deps.warnFallback,
		...(pooled
			? {
					pool: deps.pool,
					poolKey: `${options?.sessionId ?? "no-session"}:${model.provider}:${model.id}`,
					onContinuation: (record) => {
						pending = record;
					},
				}
			: {}),
	});

	const { marker, release } = installFetchHandler(wsFetch);
	let stream: AssistantMessageEventStream;
	try {
		stream = responsesApi.streamSimple(model, context, {
			...options,
			headers: { ...options?.headers, [MARKER_HEADER]: marker },
		});
	} catch (error) {
		release();
		throw error;
	}

	void stream.result().then(async (message) => {
		release();

		// Dispatch is keyed on a header pi-ai carries into the request. If it ever stops
		// arriving, the request goes out over HTTP instead, so say so.
		if (!used()) {
			deps.warnFallback("transport hook was not reached");
			return;
		}
		if (!pending) return;
		if (message.stopReason === "aborted" || message.stopReason === "error") {
			pending.complete = false;
			return;
		}

		const items = await responseItemsFor(model, context, message, options);
		if (items) {
			pending.responseItems = serverItems(items);
			pending.complete = true;
		}
	});

	return stream;
}

/** Thrown to stop the probe below before it reaches the network. */
class PayloadCaptured extends Error {}

/**
 * The items the next request's input will carry for `message`.
 *
 * The baseline for a `previous_response_id` delta has to be byte-identical to what
 * the next request sends, so it is produced by pi-ai's own conversion rather than
 * reimplemented here or taken from the server's echo, which relays may report empty.
 *
 * pi-ai does not export the conversion, but it runs inside request construction, and
 * `onPayload` sees the finished body. Throwing from there stops the request before
 * any client call, so this costs one body build and no network.
 */
async function responseItemsFor(
	model: Model<Api>,
	context: Context,
	message: AssistantMessage,
	options: SimpleStreamOptions | undefined,
): Promise<unknown[] | undefined> {
	let captured: unknown[] | undefined;
	try {
		const probe = responsesApi.streamSimple(
			model,
			{ messages: [message], tools: context.tools },
			{
				...options,
				signal: undefined,
				onPayload: (payload: unknown) => {
					const input = (payload as { input?: unknown }).input;
					if (Array.isArray(input)) captured = input;
					throw new PayloadCaptured();
				},
			},
		);
		await probe.result();
	} catch {
		// Without a trustworthy baseline the next request sends the full input.
		return undefined;
	}
	return captured;
}
