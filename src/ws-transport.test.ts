import assert from "node:assert/strict";
import test from "node:test";
import {
	createStats,
	createWsFetch,
	headerRecord,
	isStaleContinuation,
	knownUnsupportedParams,
	resetUnsupportedParams,
	toWebSocketUrl,
	unsupportedParamFrom,
	withoutMarker,
} from "./ws-transport.ts";
import { SocketPool } from "./continuation.ts";
import { MARKER_HEADER } from "./fetch-hook.ts";

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

	assert.equal(headerRecord({ [MARKER_HEADER]: "m1" })[MARKER_HEADER], undefined, "the marker never goes upstream");
	assert.deepEqual(withoutMarker({ authorization: "Bearer x", [MARKER_HEADER]: "m1" }), { authorization: "Bearer x" });
	assert.deepEqual(withoutMarker({ "content-type": "application/json" }), { "content-type": "application/json" });
});

test("the HTTP fallback drops the marker so it cannot be routed back", async () => {
	// With overlapping requests the captured fetch may itself be a dispatcher; a
	// fallback still carrying the marker would return here and loop.
	useFakeSocket([], { failHandshake: true });
	let delegatedHeaders: Record<string, string> | undefined;
	let delegatedBody: BodyInit | null | undefined;
	const { fetch: wsFetch } = createWsFetch({
		realFetch: async (_input, init) => {
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
			{ authorization: "Bearer secret", "OpenAI-Beta": "caller-value", [MARKER_HEADER]: "ws-1" },
		),
	);

	assert.equal(await response.text(), "sse-body");
	assert.deepEqual(JSON.parse(String(delegatedBody)), { model: "m", stream: true });
	assert.equal(delegatedHeaders?.[MARKER_HEADER], undefined);
	assert.equal(delegatedHeaders?.authorization, "Bearer secret", "everything else survives");
	assert.equal(delegatedHeaders?.["content-type"], "application/json", "including the HTTP body headers");
});

test("sends one response.create frame and re-emits events as SSE", async () => {
	useFakeSocket([{ type: "response.created", response: { id: "resp_1" } }, COMPLETED]);
	const stats = createStats();
	const { fetch: wsFetch, sawRequest } = createWsFetch({ realFetch: noFallback(), connectTimeoutMs: 1000, stats });

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
		realFetch: async (_input, init) => {
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

test("the request is settled once the socket work ends", async () => {
	// The caller releases process-wide resources here, so it has to fire on every
	// path, not only when the event stream above resolves.
	useFakeSocket([COMPLETED]);
	let settled = 0;
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		onSettled: () => settled++,
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));
	assert.equal(settled, 0, "the body still owns the socket");
	await response.text();
	assert.equal(settled, 1);
});

test("a fallback settles the request too", async () => {
	useFakeSocket([], { failHandshake: true });
	let settled = 0;
	const { fetch: wsFetch } = createWsFetch({
		realFetch: async () => new Response("sse-body"),
		connectTimeoutMs: 1000,
		stats: createStats(),
		onSettled: () => settled++,
	});

	await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));
	assert.equal(settled, 1);
});

test("continuation state is recorded from the terminal event", async () => {
	useFakeSocket([COMPLETED]);
	const records: unknown[] = [];
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		pool: new SocketPool(),
		poolKey: "k",
		onContinuation: (record) => records.push(record),
	});
	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m", input: [] }));
	await response.text();

	assert.deepEqual(records, [
		{ requestBody: { model: "m", input: [] }, responseId: "resp_1", responseItems: [], complete: false },
	]);
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
		realFetch: noFallback(),
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
	const { fetch: wsFetch } = createWsFetch({ realFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats() });

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.deepEqual(await readSse(response), [{ type: "response.created", response: { id: "resp_1" } }, COMPLETED]);
});

test("an error frame without a status stays in the event stream", async () => {
	useFakeSocket([{ type: "error", code: "invalid_request", message: "bad tool" }, COMPLETED]);
	const { fetch: wsFetch } = createWsFetch({ realFetch: noFallback(), connectTimeoutMs: 1000, stats: createStats() });

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
		realFetch: async () => new Response("sse-body", { status: 200 }),
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
		realFetch: async () => new Response("sse-body"),
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
		realFetch: noFallback(),
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
		realFetch: async () => new Response("sse-body"),
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
		realFetch: async () => new Response("sse-body"),
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

test("non-Responses requests pass straight through", async () => {
	useFakeSocket([COMPLETED]);
	let delegated = false;
	const { fetch: wsFetch, sawRequest } = createWsFetch({
		realFetch: async () => {
			delegated = true;
			return new Response("models");
		},
		connectTimeoutMs: 1000,
		stats: createStats(),
	});

	await wsFetch("https://api.example.com/v1/models", { method: "GET" });
	assert.equal(delegated, true);
	assert.equal(sawRequest(), false, "only the streaming Responses POST is intercepted");
});

test("an unrelated request from the same client is delegated without its marker", async () => {
	// With overlapping requests the captured fetch is itself the dispatcher. A
	// still-marked pass-through would be routed back here and recurse forever.
	useFakeSocket([COMPLETED]);
	let delegatedHeaders: Record<string, string> | undefined;
	const { fetch: wsFetch } = createWsFetch({
		realFetch: async (_input, init) => {
			delegatedHeaders = init?.headers as Record<string, string>;
			return new Response("models");
		},
		connectTimeoutMs: 1000,
		stats: createStats(),
	});

	await wsFetch("https://api.example.com/v1/models", {
		method: "GET",
		headers: { authorization: "Bearer secret", [MARKER_HEADER]: "ws-1" },
	});

	assert.equal(delegatedHeaders?.[MARKER_HEADER], undefined);
	assert.equal(delegatedHeaders?.authorization, "Bearer secret");
});

test("an HTTP error does not settle the request, so pi-ai's retry still reaches the transport", async () => {
	// pi-ai retries 429 and 5xx by calling fetch again on the same client. Settling here
	// would deregister the marker and send that retry over HTTP with no warning.
	useFakeSocket([{ type: "error", status: 429, error: { message: "slow down" } }]);
	let settled = 0;
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		onSettled: () => settled++,
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(response.status, 429);
	assert.equal(settled, 0);
});

test("an HTTP error is not counted as a served request", async () => {
	useFakeSocket([{ type: "error", status: 500, error: { message: "boom" } }]);
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({ realFetch: noFallback(), connectTimeoutMs: 1000, stats });

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
		realFetch: noFallback(),
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
		realFetch: async () => new Response("sse-body"),
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
	useFakeSocketTurns([[stale], [reject("p1")], [reject("p2")], [reject("p3")], [reject("p4")], [COMPLETED]]);

	const pool = new SocketPool();
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
		pool,
		poolKey: "budget",
	});

	const seeded = pool.add("budget", new FakeWebSocket("wss://api.example.com/v1/responses") as unknown as WebSocket);
	seeded.continuation = {
		requestBody: { model: "m", input: [{ role: "user" }] },
		responseId: "resp_0",
		responseItems: [{ role: "assistant" }],
		complete: true,
	};
	pool.release(seeded, true);

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
	let settled = 0;
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		pool,
		poolKey: "cancelled",
		onSettled: () => settled++,
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));
	const reader = response.body!.getReader();
	await reader.read();
	assert.equal(settled, 0, "still streaming");

	await reader.cancel();

	assert.equal(settled, 1);
	assert.equal(pool.size, 0, "an incomplete response leaves the server side unknown, so the socket goes");
	assert.equal(pool.acquire("cancelled"), undefined);
});

test("a rejected previous_response_id resends the full input on the same socket", async () => {
	// The server no longer holds the response the delta chained onto. Nothing has
	// streamed, so resending whole is safe, and it must not surface as an error.
	const stale = { type: "error", status: 400, error: { code: "previous_response_not_found" } };
	useFakeSocketTurns([[stale], [COMPLETED]]);

	const pool = new SocketPool();
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
		pool,
		poolKey: "stale-key",
	});

	// Seed a socket that believes it can continue from resp_0.
	const seeded = pool.add("stale-key", new FakeWebSocket("wss://api.example.com/v1/responses") as unknown as WebSocket);
	seeded.continuation = {
		requestBody: { model: "m", input: [{ role: "user" }] },
		responseId: "resp_0",
		responseItems: [{ role: "assistant" }],
		complete: true,
	};
	pool.release(seeded, true);

	const full = { model: "m", input: [{ role: "user" }, { role: "assistant" }, { role: "user" }] };
	const response = await wsFetch("https://api.example.com/v1/responses", requestInit(full));

	assert.equal(response.status, 200, "the recovery is invisible to the caller");
	assert.deepEqual(await readSse(response), [COMPLETED]);

	const sent = seeded.socket as unknown as FakeWebSocket;
	assert.equal(sent.sent.length, 2, "both attempts went out on the one socket");
	assert.equal(JSON.parse(sent.sent[0]!).previous_response_id, "resp_0", "first attempt was a delta");
	assert.deepEqual(JSON.parse(sent.sent[1]!), { type: "response.create", ...full }, "retry sent everything");
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
	useFakeSocketTurns([[stale], [stale]]);

	const pool = new SocketPool();
	const stats = createStats();
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
		pool,
		poolKey: "stubborn-stale",
	});

	const seeded = pool.add(
		"stubborn-stale",
		new FakeWebSocket("wss://api.example.com/v1/responses") as unknown as WebSocket,
	);
	seeded.continuation = {
		requestBody: { model: "m", input: [{ role: "user" }] },
		responseId: "resp_0",
		responseItems: [{ role: "assistant" }],
		complete: true,
	};
	pool.release(seeded, true);

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
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
		unsupportedScope: "test-provider",
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
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats,
		unsupportedScope: "sticky-provider",
	});
	const body = { model: "m", input: [], max_output_tokens: 4096 };

	await (await wsFetch("https://api.example.com/v1/responses", requestInit(body))).text();
	await (await wsFetch("https://api.example.com/v1/responses", requestInit(body))).text();

	assert.equal(FakeWebSocket.instances.length, 3, "the second request must not re-learn the rejection");
	assert.equal(JSON.parse(FakeWebSocket.instances[2]!.sent[0]!).max_output_tokens, undefined);
	assert.deepEqual(knownUnsupportedParams("sticky-provider"), ["max_output_tokens"]);
});

test("a persistent rejection eventually stops retrying", async () => {
	// Each round names a new parameter, so the ceiling is what ends it.
	const turns = Array.from({ length: 8 }, (_unused, index) => [
		{ type: "error", status: 400, error: { message: `Unsupported parameter: p${index}` } },
	]);
	useFakeSocketTurns(turns);
	const { fetch: wsFetch } = createWsFetch({
		realFetch: noFallback(),
		connectTimeoutMs: 1000,
		stats: createStats(),
		unsupportedScope: "stubborn",
	});

	const response = await wsFetch("https://api.example.com/v1/responses", requestInit({ model: "m" }));

	assert.equal(response.status, 400, "the last rejection is surfaced as an HTTP error");
	assert.ok(FakeWebSocket.instances.length <= 5);
});
