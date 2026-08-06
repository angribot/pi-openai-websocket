import assert from "node:assert/strict";
import test from "node:test";
import {
	createStats,
	createWsFetch,
	formatStats,
	headerRecord,
	isStaleContinuation,
	resetUnsupportedParams,
	toWebSocketUrl,
	unsupportedParamFrom,
	type WsFetchOptions,
} from "./ws-transport.ts";
import { SocketPool } from "./continuation.ts";

/**
 * Minimal stand-in for the global WebSocket. Scripted frames are delivered after
 * open, so a test can describe a whole server turn as a list of events.
 */
class FakeWebSocket extends EventTarget {
	static OPEN = 1;
	static CONNECTING = 0;
	static instances: FakeWebSocket[] = [];

	readyState = 1;
	binaryType = "arraybuffer";
	sent: string[] = [];
	closed = false;
	readonly url: string;
	readonly headers: Record<string, string>;

	constructor(url: string, options?: { headers?: Record<string, string> }) {
		super();
		this.url = url;
		this.headers = options?.headers ?? {};
		FakeWebSocket.instances.push(this);
		queueMicrotask(() => {
			if (script.failHandshake) {
				this.dispatchEvent(new Event("error"));
				return;
			}
			this.dispatchEvent(new Event("open"));
		});
	}

	send(data: string): void {
		this.sent.push(data);
		// Each connection gets its own scripted turn, so strip-and-retry rounds can be
		// given different answers.
		const frames = script.turns.length > 0 ? (script.turns.shift() ?? []) : script.frames;
		queueMicrotask(async () => {
			for (const frame of frames) {
				this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(frame) }));
				await Promise.resolve();
			}
			if (script.closeWithoutTerminal) {
				this.readyState = 3;
				this.dispatchEvent(new CloseEvent("close", { code: 1006, reason: "boom" }));
			}
		});
	}

	close(): void {
		this.closed = true;
		this.readyState = 3;
	}
}

const script: {
	frames: unknown[];
	turns: unknown[][];
	failHandshake: boolean;
	closeWithoutTerminal: boolean;
} = { frames: [], turns: [], failHandshake: false, closeWithoutTerminal: false };

function useFakeSocket(frames: unknown[], opts: { failHandshake?: boolean; closeWithoutTerminal?: boolean } = {}) {
	script.frames = frames;
	script.turns = [];
	script.failHandshake = opts.failHandshake ?? false;
	script.closeWithoutTerminal = opts.closeWithoutTerminal ?? false;
	FakeWebSocket.instances = [];
	resetUnsupportedParams();
	(globalThis as { WebSocket: unknown }).WebSocket = FakeWebSocket;
}

/** Scripts one frame list per connection, in order. */
function useFakeSocketTurns(turns: unknown[][]) {
	useFakeSocket([]);
	script.turns = turns;
}

const COMPLETED = { type: "response.completed", response: { id: "resp_1", output: [] } };

function requestInit(body: unknown, headers: Record<string, string> = { authorization: "Bearer secret" }): RequestInit {
	return { method: "POST", body: JSON.stringify(body), headers: { ...headers, "content-type": "application/json" } };
}

async function readSse(response: Response): Promise<unknown[]> {
	const text = await response.text();
	return text
		.split("\n\n")
		.filter((chunk) => chunk.startsWith("data: "))
		.map((chunk) => JSON.parse(chunk.slice("data: ".length)));
}

function noFallback(): FetchLike {
	return () => {
		throw new Error("fell back to HTTP unexpectedly");
	};
}
type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
type ReuseOptions = NonNullable<WsFetchOptions["reuse"]>;

function reuseOptions(
	pool: SocketPool,
	overrides: Partial<Omit<ReuseOptions, "pool">> = {},
): ReuseOptions {
	return {
		pool,
		sessionId: "session-a",
		provider: "test-provider",
		model: "m",
		...overrides,
	};
}

interface CompleteRequestOptions {
	url?: string;
	body?: Record<string, unknown>;
	headers?: Record<string, string>;
	reuse?: ReuseOptions;
	stats?: ReturnType<typeof createStats>;
}

async function completeRequest(options: CompleteRequestOptions): Promise<void> {
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: options.stats ?? createStats(),
		reuse: options.reuse,
	});
	const response = await wsFetch(
		options.url ?? "https://api.example.com/v1/responses",
		requestInit(options.body ?? { model: "m", input: [] }, options.headers),
	);
	assert.equal(response.status, 200);
	await response.text();
}

test("url and header handling", () => {
	assert.equal(toWebSocketUrl("https://api.example.com/v1/responses"), "wss://api.example.com/v1/responses");
	assert.equal(toWebSocketUrl("http://127.0.0.1:8080/v1/responses"), "ws://127.0.0.1:8080/v1/responses");

	const headers = headerRecord({ authorization: "Bearer x", "content-type": "application/json", "x-keep": "1" });
	assert.deepEqual(headers, {
		authorization: "Bearer x",
		"x-keep": "1",
		"OpenAI-Beta": "responses_websockets=2026-02-06",
	});
	assert.deepEqual(headerRecord({ "openai-beta": "older" }), {
		"OpenAI-Beta": "responses_websockets=2026-02-06",
	});
});

test("the HTTP fallback preserves the caller request", async () => {
	useFakeSocket([], { failHandshake: true });
	let delegatedHeaders: Record<string, string> | undefined;
	let delegatedBody: BodyInit | null | undefined;
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async (_input, init) => {
			delegatedHeaders = init?.headers as Record<string, string>;
			delegatedBody = init?.body;
			return new Response("sse-body");
		},
		connectTimeoutMs: 1000,
		stats: createStats(),
	});

	const response = await wsFetch(
		"https://api.example.com/v1/responses",
		requestInit(
			{ model: "m", stream: true },
			{ authorization: "Bearer secret", "OpenAI-Beta": "caller-value" },
		),
	);

	assert.equal(await response.text(), "sse-body");
	assert.deepEqual(JSON.parse(String(delegatedBody)), { model: "m", stream: true });
	assert.equal(delegatedHeaders?.authorization, "Bearer secret");
	assert.equal(delegatedHeaders?.["content-type"], "application/json");
	assert.equal(delegatedHeaders?.["OpenAI-Beta"], "caller-value");
});

test("sends one response.create frame and re-emits events as SSE", async () => {
	useFakeSocket([{ type: "response.created", response: { id: "resp_1" } }, COMPLETED]);
	const stats = createStats();
	const { fetch: wsFetch, sawRequest } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats });

	const response = await wsFetch(
		"https://api.example.com/v1/responses",
		requestInit({ model: "m", input: [], stream: true, background: false, type: "caller-value" }),
	);

	assert.equal(response.status, 200);
	assert.equal(response.headers.get("content-type"), "text/event-stream");
	assert.equal(sawRequest(), true);

	const socket = FakeWebSocket.instances[0]!;
	assert.equal(socket.url, "wss://api.example.com/v1/responses");
	assert.equal(socket.headers.authorization, "Bearer secret");
	assert.equal(socket.headers["OpenAI-Beta"], "responses_websockets=2026-02-06");
	assert.equal(socket.headers["content-type"], undefined, "HTTP body headers must not reach the handshake");

	// HTTP transport fields are omitted, and the transport owns the envelope type.
	assert.deepEqual(JSON.parse(socket.sent[0]!), {
		model: "m",
		input: [],
		background: false,
		type: "response.create",
	});

	assert.deepEqual(await readSse(response), [{ type: "response.created", response: { id: "resp_1" } }, COMPLETED]);
	assert.equal(stats.attempts, 1);
	assert.equal(stats.connected, 1);
	assert.equal(stats.fullRequests, 1);
	assert.equal(stats.deltaRequests, 0);
});

test("background requests bypass WebSocket without changing the HTTP request", async () => {
	useFakeSocket([COMPLETED]);
	const body = { model: "m", input: [], stream: true, background: true };
	let delegatedBody: BodyInit | null | undefined;
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async (_input, init) => {
			delegatedBody = init?.body;
			return new Response("background-body");
		},
		connectTimeoutMs: 1000,
		stats,
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit(body));

	assert.equal(await response.text(), "background-body");
	assert.deepEqual(JSON.parse(String(delegatedBody)), body);
	assert.equal(FakeWebSocket.instances.length, 0);
	assert.equal(stats.attempts, 0);
	assert.equal(stats.sseFallbacks, 0);
});

test("continuation state is recorded from the terminal event", async () => {
	useFakeSocket([COMPLETED]);
	const records: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		reuse: reuseOptions(new SocketPool(), { onContinuation: (record) => records.push(record) }),
	});
	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m", input: [] }));
	await response.text();

	assert.deepEqual(records, [
		{ requestBody: { model: "m", input: [] }, responseId: "resp_1", responseItems: [], complete: false },
	]);
});

test("header order and casing share one opaque connection identity", async () => {
	useFakeSocket([COMPLETED]);
	const pool = new SocketPool();
	const stats = createStats();
	const reuse = reuseOptions(pool, { sessionId: "private-session" });

	await completeRequest({
		headers: { Authorization: "Bearer private-credential", "X-Route": "private-route" },
		reuse,
		stats,
	});
	await completeRequest({
		headers: { "x-route": "private-route", authorization: "Bearer private-credential" },
		reuse,
		stats,
	});

	assert.equal(FakeWebSocket.instances.length, 1);
	assert.equal(stats.connectionsReused, 1);
	const entries = (pool as unknown as { entries: Map<string, unknown> }).entries;
	const keys = [...entries.keys()];
	assert.equal(keys.length, 1);
	assert.match(keys[0]!, /^[a-f0-9]{64}$/, "pool keys expose only an opaque SHA-256 digest");
	for (const secret of ["private-session", "private-credential", "private-route", "api.example.com"]) {
		assert.equal(keys[0]!.includes(secret), false);
		assert.equal(JSON.stringify(stats).includes(secret), false);
		assert.equal(formatStats(stats).includes(secret), false);
	}
	pool.closeAll();
});

test("endpoint changes do not share pooled sockets", async () => {
	useFakeSocket([COMPLETED]);
	const pool = new SocketPool();
	const reuse = reuseOptions(pool);

	await completeRequest({ url: "https://first.example/v1/responses", reuse });
	await completeRequest({ url: "https://second.example/v1/responses", reuse });

	assert.equal(FakeWebSocket.instances.length, 2);
	pool.closeAll();
});

test("authentication and routing header changes do not share pooled sockets", async (t) => {
	for (const scenario of [
		{
			name: "credential",
			first: { authorization: "Bearer first", "x-route": "route" },
			second: { authorization: "Bearer second", "x-route": "route" },
		},
		{
			name: "route",
			first: { authorization: "Bearer secret", "x-route": "first" },
			second: { authorization: "Bearer secret", "x-route": "second" },
		},
	]) {
		await t.test(scenario.name, async () => {
			useFakeSocket([COMPLETED]);
			const pool = new SocketPool();
			const reuse = reuseOptions(pool);

			await completeRequest({ headers: scenario.first, reuse });
			await completeRequest({ headers: scenario.second, reuse });

			assert.equal(FakeWebSocket.instances.length, 2);
			pool.closeAll();
		});
	}
});

test("model and session changes do not share pooled sockets", async (t) => {
	for (const scenario of [
		{
			name: "request model",
			firstReuse: {},
			secondReuse: {},
			firstBody: { model: "model-a", input: [] },
			secondBody: { model: "model-b", input: [] },
		},
		{
			name: "session",
			firstReuse: { sessionId: "session-a" },
			secondReuse: { sessionId: "session-b" },
			firstBody: { model: "m", input: [] },
			secondBody: { model: "m", input: [] },
		},
	]) {
		await t.test(scenario.name, async () => {
			useFakeSocket([COMPLETED]);
			const pool = new SocketPool();

			await completeRequest({ body: scenario.firstBody, reuse: reuseOptions(pool, scenario.firstReuse) });
			await completeRequest({ body: scenario.secondBody, reuse: reuseOptions(pool, scenario.secondReuse) });

			assert.equal(FakeWebSocket.instances.length, 2);
			pool.closeAll();
		});
	}
});

test("a wrapped error frame becomes an HTTP error response", async () => {
	useFakeSocket([
		{
			type: "error",
			status: 429,
			error: { message: "slow down", type: "rate_limit_error" },
			headers: { "retry-after-ms": "1200" },
		},
	]);
	const stats = createStats();
	const unavailable: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
		onTransportUnavailable: (failure) => unavailable.push(failure),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(response.ok, false);
	assert.equal(response.status, 429);
	assert.equal(response.headers.get("retry-after-ms"), "1200");
	assert.deepEqual(await response.json(), { error: { message: "slow down", type: "rate_limit_error" } });
	assert.equal(stats.sseFallbacks, 0, "an HTTP error is a real answer, not a transport failure");
	assert.deepEqual(unavailable, []);
});

test("Codex rate-limit events stay out of the Responses event stream", async () => {
	useFakeSocket([
		{ type: "codex.rate_limits", rate_limits: { primary: { used_percent: 10 } } },
		{ type: "response.created", response: { id: "resp_1" } },
		COMPLETED,
	]);
	const { fetch: wsFetch } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats() });

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.deepEqual(await readSse(response), [{ type: "response.created", response: { id: "resp_1" } }, COMPLETED]);
});

test("an error frame without a status stays in the event stream", async () => {
	useFakeSocket([{ type: "error", code: "invalid_request", message: "bad tool" }, COMPLETED]);
	const { fetch: wsFetch } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats() });

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(response.status, 200, "the shared event pipeline already turns this into an error");
	assert.deepEqual((await readSse(response))[0], { type: "error", code: "invalid_request", message: "bad tool" });
});

test("handshake failure falls back to HTTP and reports transport unavailability", async () => {
	useFakeSocket([], { failHandshake: true });
	const stats = createStats();
	const reasons: string[] = [];
	const unavailable: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async () => new Response("sse-body", { status: 200 }),
		connectTimeoutMs: 1000,
		stats,
		onFallback: (reason) => reasons.push(reason),
		onTransportUnavailable: (failure) => unavailable.push(failure),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(await response.text(), "sse-body");
	assert.equal(stats.sseFallbacks, 1);
	assert.equal(stats.connected, 0);
	assert.match(reasons[0]!, /handshake failed/);
	assert.deepEqual(unavailable, [{ phase: "before-stream-start", reason: "websocket handshake failed" }]);
});

test("a close before any event falls back to HTTP", async () => {
	useFakeSocket([], { closeWithoutTerminal: true });
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async () => new Response("sse-body"),
		connectTimeoutMs: 1000,
		stats,
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(await response.text(), "sse-body");
	assert.equal(stats.sseFallbacks, 1);
});

test("a close after streaming started reports unavailability and surfaces as a stream error", async () => {
	useFakeSocket([{ type: "response.created", response: { id: "resp_1" } }], { closeWithoutTerminal: true });
	const unavailable: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		onTransportUnavailable: (failure) => unavailable.push(failure),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	await assert.rejects(() => response.text(), /closed before a terminal response event/);
	assert.deepEqual(unavailable, [
		{ phase: "after-stream-start", reason: "websocket closed before a terminal response event (1006: boom)" },
	]);
});

test("abort before connect does not report transport unavailability", async () => {
	useFakeSocket([COMPLETED]);
	const controller = new AbortController();
	controller.abort();
	const stats = createStats();
	const reasons: string[] = [];
	const unavailable: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async () => new Response("sse-body"),
		connectTimeoutMs: 1000,
		stats,
		signal: controller.signal,
		onFallback: (reason) => reasons.push(reason),
		onTransportUnavailable: (failure) => unavailable.push(failure),
	});

	await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));
	assert.match(reasons[0]!, /aborted/);
	assert.deepEqual(unavailable, []);
});

test("a malformed request falls back without reporting transport unavailability", async () => {
	useFakeSocket([COMPLETED]);
	const unavailable: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async () => new Response("sse-body"),
		connectTimeoutMs: 1000,
		stats: createStats(),
		onTransportUnavailable: (failure) => unavailable.push(failure),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", {
		method: "POST",
		body: "not-json",
	});

	assert.equal(await response.text(), "sse-body");
	assert.deepEqual(unavailable, []);
});

test("non-Responses requests are delegated unchanged", async () => {
	useFakeSocket([COMPLETED]);
	let delegatedInput: string | URL | Request | undefined;
	let delegatedInit: RequestInit | undefined;
	const { fetch: wsFetch, sawRequest } = createWsFetch({
		fallbackFetch: async (input, init) => {
			delegatedInput = input;
			delegatedInit = init;
			return new Response("models");
		},
		connectTimeoutMs: 1000,
		stats: createStats(),
	});
	const init = {
		method: "GET",
		headers: { authorization: "Bearer secret", "x-request-id": "request-1" },
	};

	await wsFetch("https://api.example.com/v1/models", init);

	assert.equal(delegatedInput, "https://api.example.com/v1/models");
	assert.equal(delegatedInit, init);
	assert.equal(sawRequest(), false, "only the streaming Responses POST is intercepted");
});

test("an HTTP error is not counted as a served request", async () => {
	useFakeSocket([{ type: "error", status: 500, error: { message: "boom" } }]);
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats });

	await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(stats.fullRequests, 0);
	assert.equal(stats.deltaRequests, 0);
});

test("a connection-limit rejection retries once on a fresh socket", async () => {
	const limit = {
		type: "error",
		status: 400,
		error: { code: "websocket_connection_limit_reached", message: "create a new connection" },
	};
	useFakeSocketTurns([[limit], [COMPLETED]]);
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m", stream: true }));

	assert.deepEqual(await readSse(response), [COMPLETED]);
	assert.equal(FakeWebSocket.instances.length, 2);
	assert.equal(FakeWebSocket.instances[0]!.closed, true);
	assert.deepEqual(JSON.parse(FakeWebSocket.instances[1]!.sent[0]!), { type: "response.create", model: "m" });
	assert.equal(stats.fullRequests, 1);
	assert.equal(stats.sseFallbacks, 0);
});

test("an exhausted connection-limit retry falls back and reports transport unavailability", async () => {
	const limit = {
		type: "error",
		status: 400,
		error: { code: "websocket_connection_limit_reached", message: "create a new connection" },
	};
	useFakeSocketTurns([[limit], [limit], [COMPLETED]]);
	const unavailable: unknown[] = [];
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: async () => new Response("sse-body"),
		connectTimeoutMs: 1000,
		stats,
		onTransportUnavailable: (failure) => unavailable.push(failure),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(await response.text(), "sse-body");
	assert.equal(FakeWebSocket.instances.length, 2, "the retry is bounded independently of other recoveries");
	assert.equal(stats.sseFallbacks, 1);
	assert.deepEqual(unavailable, [{ phase: "before-stream-start", reason: "create a new connection" }]);
});

test("a stale continuation retry does not consume the strip budget", async () => {
	// ADR 0005 bounds the two recoveries separately: the stale retry is allowed once,
	// stripping up to MAX_STRIP_ROUNDS.
	const stale = { type: "error", status: 400, error: { code: "previous_response_not_found" } };
	const reject = (param: string) => ({ type: "error", status: 400, error: { message: `Unsupported parameter: ${param}` } });
	useFakeSocketTurns([
		[COMPLETED],
		[stale],
		[reject("p1")],
		[reject("p2")],
		[reject("p3")],
		[reject("p4")],
		[COMPLETED],
	]);

	const pool = new SocketPool();
	const reuse = reuseOptions(pool, {
		onContinuation: (record) => {
			record.responseItems = [{ role: "assistant" }];
			record.complete = true;
		},
	});
	const firstBody = { model: "m", input: [{ role: "user" }] };
	const seed = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats(), reuse });
	await (await seed.fetch("https://api.example.com/v1/responses", requestInit(firstBody))).text();

	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats, reuse });
	const response = await wsFetch(
		"https://api.example.com/v1/responses",
		requestInit({ model: "m", input: [{ role: "user" }, { role: "assistant" }, { role: "user" }] }),
	);

	assert.equal(response.status, 200, "one stale retry plus four strips all fit");
	assert.deepEqual(await readSse(response), [COMPLETED]);
	assert.equal(stats.staleContinuations, 1);
	assert.deepEqual(stats.strippedParams, ["p1", "p2", "p3", "p4"]);
});

test("cancelling the response body releases the socket", async () => {
	// The consumer may stop mid-response. Cancelling the body unwinds the frame
	// generator, which is what returns the socket, so releasing must not depend on the
	// response being read to completion.
	useFakeSocket([{ type: "response.created", response: { id: "resp_1" } }, COMPLETED]);
	const pool = new SocketPool();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		reuse: reuseOptions(pool),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));
	const reader = response.body!.getReader();
	await reader.read();
	await reader.cancel();

	assert.equal(pool.size, 0, "an incomplete response leaves the server side unknown, so the socket goes");
});

test("a rejected previous_response_id resends the full input on the same socket", async () => {
	// The server no longer holds the response the delta chained onto. Nothing has
	// streamed, so resending whole is safe, and it must not surface as an error.
	const stale = { type: "error", status: 400, error: { code: "previous_response_not_found" } };
	useFakeSocketTurns([[COMPLETED], [stale], [COMPLETED]]);

	const pool = new SocketPool();
	const reuse = reuseOptions(pool, {
		onContinuation: (record) => {
			record.responseItems = [{ role: "assistant" }];
			record.complete = true;
		},
	});
	const firstBody = { model: "m", input: [{ role: "user" }] };
	const seed = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats(), reuse });
	await (await seed.fetch("https://api.example.com/v1/responses", requestInit(firstBody))).text();

	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats, reuse });
	const full = { model: "m", input: [{ role: "user" }, { role: "assistant" }, { role: "user" }] };
	const response = await wsFetch("https://api.example.com/v1/responses", requestInit(full));

	assert.equal(response.status, 200, "the recovery is invisible to the caller");
	assert.deepEqual(await readSse(response), [COMPLETED]);

	const sent = FakeWebSocket.instances[0]!;
	assert.equal(sent.sent.length, 3, "the seed and both attempts went out on the one socket");
	assert.equal(JSON.parse(sent.sent[1]!).previous_response_id, "resp_1", "first attempt was a delta");
	assert.deepEqual(JSON.parse(sent.sent[2]!), { type: "response.create", ...full }, "retry sent everything");
	assert.equal(stats.staleContinuations, 1);
	assert.equal(stats.fullRequests, 1);
	assert.equal(stats.deltaRequests, 0, "the abandoned delta is not counted as sent");
	assert.equal(stats.sseFallbacks, 0);
	assert.equal(stats.connected, 0, "no new handshake was needed");
});

test("a stale continuation is recognised in either shape", () => {
	assert.equal(
		isStaleContinuation({ type: "error", status: 400, error: { code: "previous_response_not_found" } }),
		true,
	);
	assert.equal(
		isStaleContinuation({ type: "error", status: 400, error: { message: "Previous response not found" } }),
		true,
	);
	assert.equal(isStaleContinuation({ type: "error", status: 429, error: { message: "slow down" } }), false);
	assert.equal(isStaleContinuation({ type: "response.completed" }), false);
});

test("a second stale rejection is surfaced instead of retried forever", async () => {
	const stale = { type: "error", status: 400, error: { code: "previous_response_not_found" } };
	useFakeSocketTurns([[COMPLETED], [stale], [stale]]);

	const pool = new SocketPool();
	const reuse = reuseOptions(pool, {
		onContinuation: (record) => {
			record.responseItems = [{ role: "assistant" }];
			record.complete = true;
		},
	});
	const seed = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats(), reuse });
	await (
		await seed.fetch(
			"https://api.example.com/v1/responses",
			requestInit({ model: "m", input: [{ role: "user" }] }),
		)
	).text();

	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({ fallbackFetch: noFallback(), connectTimeoutMs: 1000, stats, reuse });
	const response = await wsFetch(
		"https://api.example.com/v1/responses",
		requestInit({ model: "m", input: [{ role: "user" }, { role: "assistant" }, { role: "user" }] }),
	);

	// The retry carries no previous_response_id, so a repeat rejection is the server's
	// answer to a full request and belongs to the caller.
	assert.equal(response.status, 400);
	assert.equal(stats.staleContinuations, 1);
});

test("unsupported parameter names are read out of the error message", () => {
	assert.equal(
		unsupportedParamFrom({ type: "error", status: 400, error: { message: "Unsupported parameter: max_output_tokens" } }),
		"max_output_tokens",
	);
	assert.equal(unsupportedParamFrom({ type: "error", message: "Unsupported parameter: 'store'" }), "store");
	assert.equal(unsupportedParamFrom({ type: "error", status: 429, error: { message: "slow down" } }), undefined);
	assert.equal(unsupportedParamFrom({ type: "response.completed" }), undefined);
});

test("a rejected parameter is dropped and the request retried", async () => {
	// Relays that forward to a Codex-style backend reject max_output_tokens over
	// WebSocket while accepting it over HTTP.
	const reject = (param: string) => ({
		type: "error",
		status: 400,
		error: { message: `Unsupported parameter: ${param}`, type: "upstream_error" },
	});
	useFakeSocketTurns([[reject("max_output_tokens")], [reject("prompt_cache_options")], [COMPLETED]]);

	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
	});

	const response = await wsFetch(
		"https://api.example.com/v1/responses",
		requestInit({ model: "m", input: [], max_output_tokens: 128000, prompt_cache_options: { mode: "explicit" } }),
	);

	assert.equal(response.status, 200);
	assert.deepEqual(await readSse(response), [COMPLETED]);
	assert.equal(FakeWebSocket.instances.length, 3, "one handshake per strip round");

	const final = JSON.parse(FakeWebSocket.instances[2]!.sent[0]!);
	assert.deepEqual(final, { type: "response.create", model: "m", input: [] });
	assert.deepEqual(stats.strippedParams, ["max_output_tokens", "prompt_cache_options"]);
	assert.equal(stats.attempts, 1, "strip rounds are one logical request");
	assert.equal(stats.fullRequests, 1);
	assert.equal(stats.sseFallbacks, 0);
});

test("a rejected parameter stays dropped for later requests", async () => {
	const reject = { type: "error", status: 400, error: { message: "Unsupported parameter: max_output_tokens" } };
	useFakeSocketTurns([[reject], [COMPLETED], [COMPLETED]]);
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
	});
	const body = { model: "m", input: [], max_output_tokens: 4096 };

	await (await wsFetch("https://api.example.com/v1/responses", requestInit(body))).text();
	await (await wsFetch("https://api.example.com/v1/responses", requestInit(body))).text();

	assert.equal(FakeWebSocket.instances.length, 3, "the second request must not re-learn the rejection");
	assert.equal(JSON.parse(FakeWebSocket.instances[2]!.sent[0]!).max_output_tokens, undefined);
});

test("unsupported-parameter learning is isolated by connection identity and request model", async (t) => {
	const reject = { type: "error", status: 400, error: { message: "Unsupported parameter: max_output_tokens" } };
	const scenarios: Array<{
		name: string;
		first: Pick<CompleteRequestOptions, "url" | "body" | "headers">;
		second: Pick<CompleteRequestOptions, "url" | "body" | "headers">;
	}> = [
		{
			name: "endpoint",
			first: { url: "https://first.example/v1/responses" },
			second: { url: "https://second.example/v1/responses" },
		},
		{
			name: "credential",
			first: { headers: { authorization: "Bearer first" } },
			second: { headers: { authorization: "Bearer second" } },
		},
		{
			name: "route",
			first: { headers: { authorization: "Bearer secret", "x-route": "first" } },
			second: { headers: { authorization: "Bearer secret", "x-route": "second" } },
		},
		{
			name: "request model",
			first: { body: { model: "model-a", input: [], max_output_tokens: 4096 } },
			second: { body: { model: "model-b", input: [], max_output_tokens: 4096 } },
		},
	];

	for (const scenario of scenarios) {
		await t.test(scenario.name, async () => {
			useFakeSocketTurns([[reject], [COMPLETED], [reject], [COMPLETED]]);
			const defaultBody = { model: "m", input: [], max_output_tokens: 4096 };

			await completeRequest({ body: defaultBody, ...scenario.first });
			await completeRequest({ body: defaultBody, ...scenario.second });

			assert.equal(FakeWebSocket.instances.length, 4, "the second identity learns the rejection independently");
			const secondIdentityFirstAttempt = JSON.parse(FakeWebSocket.instances[2]!.sent[0]!);
			assert.equal(secondIdentityFirstAttempt.max_output_tokens, 4096);
		});
	}
});

test("a persistent rejection eventually stops retrying", async () => {
	// Each round names a new parameter, so the ceiling is what ends it.
	const turns = Array.from({ length: 8 }, (_unused, index) => [
		{ type: "error", status: 400, error: { message: `Unsupported parameter: p${index}` } },
	]);
	useFakeSocketTurns(turns);
	const { fetch: wsFetch } = createWsFetch({
		fallbackFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(response.status, 400, "the last rejection is surfaced as an HTTP error");
	assert.ok(FakeWebSocket.instances.length <= 5);
});
