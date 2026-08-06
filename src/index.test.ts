import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import type { Api, Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai/compat";
import { streamOverWebSocket } from "../index.ts";
import { SocketPool } from "./continuation.ts";
import { StickySseSessions } from "./session-fallback.ts";
import { createStats } from "./ws-transport.ts";

class FakeWebSocket extends EventTarget {
	static OPEN = 1;
	static CONNECTING = 0;
	static instances: FakeWebSocket[] = [];
	static failHandshakes = 0;
	static turns: unknown[][] = [];

	readyState = FakeWebSocket.OPEN;
	binaryType = "arraybuffer";
	sent: string[] = [];
	readonly url: string;
	readonly headers: Record<string, string>;

	constructor(url: string, options?: { headers?: Record<string, string> }) {
		super();
		this.url = url;
		this.headers = options?.headers ?? {};
		FakeWebSocket.instances.push(this);
		queueMicrotask(() => {
			if (FakeWebSocket.failHandshakes > 0) {
				FakeWebSocket.failHandshakes--;
				this.dispatchEvent(new Event("error"));
				return;
			}
			this.dispatchEvent(new Event("open"));
		});
	}

	send(data: string): void {
		this.sent.push(data);
		const frames = FakeWebSocket.turns.shift() ?? [completedFrame()];
		queueMicrotask(() => {
			for (const frame of frames) {
				this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(frame) }));
			}
		});
	}

	close(): void {
		this.readyState = 3;
	}
}

function completedFrame(): unknown {
	return {
		type: "response.completed",
		response: {
			id: "resp_1",
			status: "completed",
			output: [],
			usage: {
				input_tokens: 1,
				output_tokens: 1,
				input_tokens_details: { cached_tokens: 0 },
				output_tokens_details: { reasoning_tokens: 0 },
			},
		},
	};
}

function useFakeWebSocket(
	t: TestContext,
	options: { failHandshakes?: number; turns?: unknown[][] } = {},
): void {
	const originalWebSocket = globalThis.WebSocket;
	t.after(() => {
		globalThis.WebSocket = originalWebSocket;
	});
	FakeWebSocket.instances = [];
	FakeWebSocket.failHandshakes = options.failHandshakes ?? 0;
	FakeWebSocket.turns = options.turns ?? [];
	(globalThis as { WebSocket: unknown }).WebSocket = FakeWebSocket;
}

function sseResponse(frame: unknown): Response {
	return new Response(`data: ${JSON.stringify(frame)}\n\n`, {
		headers: { "content-type": "text/event-stream" },
	});
}

const model: Model<Api> = {
	id: "test-model",
	name: "Test Model",
	api: "openai-responses",
	provider: "test-provider",
	baseUrl: "https://api.example.com/v1",
	reasoning: false,
	input: ["text"],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: 10_000,
	maxTokens: 1_000,
};

const context: Context = {
	messages: [{ role: "user", content: "hello", timestamp: 1 }],
};

function streamDeps(
	selectedModel: Model<Api> = model,
	transport: "auto" | "sse" | "websocket" | "websocket-cached" = "websocket",
) {
	return {
		stats: createStats(),
		settings: { providers: [selectedModel.provider], transport, connectTimeoutMs: 1_000 },
		warnFallback: () => {},
		pool: new SocketPool(),
		stickySseSessions: new StickySseSessions(),
	};
}

function runStream(options: SimpleStreamOptions, selectedModel: Model<Api> = model) {
	return streamOverWebSocket(selectedModel, context, options, streamDeps(selectedModel));
}

test("requests without a session never reuse a cached WebSocket", async (t) => {
	useFakeWebSocket(t);
	const deps = streamDeps(model, "websocket-cached");
	t.after(() => deps.pool.closeAll());

	await streamOverWebSocket(model, context, { apiKey: "secret", transport: "websocket-cached" }, deps).result();
	await streamOverWebSocket(model, context, { apiKey: "secret", transport: "websocket-cached" }, deps).result();

	assert.equal(FakeWebSocket.instances.length, 2);
	for (const socket of FakeWebSocket.instances) {
		assert.equal(JSON.parse(socket.sent[0]!).previous_response_id, undefined);
	}
});

test("explicit websocket transport opens a fresh connection for a named session", async (t) => {
	useFakeWebSocket(t);
	const deps = streamDeps(model, "websocket");
	t.after(() => deps.pool.closeAll());
	const options = { apiKey: "secret", transport: "websocket" as const, sessionId: "session-a" };

	await streamOverWebSocket(model, context, options, deps).result();
	await streamOverWebSocket(model, context, options, deps).result();

	assert.equal(FakeWebSocket.instances.length, 2);
	for (const socket of FakeWebSocket.instances) {
		assert.equal(JSON.parse(socket.sent[0]!).previous_response_id, undefined);
	}
});

test("equivalent cached requests in one named session reuse a WebSocket", async (t) => {
	useFakeWebSocket(t);
	const deps = streamDeps(model, "websocket-cached");
	t.after(() => deps.pool.closeAll());
	const options = { apiKey: "secret", transport: "websocket-cached" as const, sessionId: "session-a" };

	await streamOverWebSocket(model, context, options, deps).result();
	await streamOverWebSocket(model, context, options, deps).result();

	assert.equal(FakeWebSocket.instances.length, 1);
	assert.equal(deps.stats.connectionsReused, 1);
});

test("a credential change in one named session opens a new WebSocket", async (t) => {
	useFakeWebSocket(t);
	const deps = streamDeps(model, "websocket-cached");
	t.after(() => deps.pool.closeAll());
	const options = { transport: "websocket-cached" as const, sessionId: "session-a" };

	await streamOverWebSocket(model, context, { ...options, apiKey: "first-credential" }, deps).result();
	await streamOverWebSocket(model, context, { ...options, apiKey: "second-credential" }, deps).result();

	assert.equal(FakeWebSocket.instances.length, 2);
	assert.equal(FakeWebSocket.instances[0]!.headers.authorization, "Bearer first-credential");
	assert.equal(FakeWebSocket.instances[1]!.headers.authorization, "Bearer second-credential");
});

test("Pi 0.84 sampling parameters reach response.create with request values winning", async (t) => {
	useFakeWebSocket(t);
	const sampledModel: Model<Api> = {
		...model,
		samplingParams: { top_p: 0.9, top_k: 40, model_extension: { mode: "model" } },
	};

	const message = await runStream(
		{
			apiKey: "secret",
			transport: "websocket",
			samplingParams: { top_p: 0.5, min_p: 0.1, request_extension: ["kept"] },
		},
		sampledModel,
	).result();

	assert.equal(message.stopReason, "stop", message.errorMessage);
	const sent = JSON.parse(FakeWebSocket.instances[0]!.sent[0]!) as Record<string, unknown>;
	assert.equal(sent.top_p, 0.5, "request sampling parameters override model defaults");
	assert.equal(sent.top_k, 40);
	assert.equal(sent.min_p, 0.1);
	assert.deepEqual(sent.model_extension, { mode: "model" });
	assert.deepEqual(sent.request_extension, ["kept"], "arbitrary sampling keys stay intact");
});

test("Pi 0.84 removes nullable provider headers before the resolved handshake", async (t) => {
	useFakeWebSocket(t);
	const resolvedModel: Model<Api> = {
		...model,
		baseUrl: "https://resolved.example/v9",
		headers: { "X-Provider-Default": "delete-me", "X-Provider-Keep": "kept" },
	};

	const message = await runStream(
		{
			apiKey: "resolved-credential",
			headers: { "x-provider-default": null, "X-Route": "account-a" },
			transport: "websocket",
		},
		resolvedModel,
	).result();

	assert.equal(message.stopReason, "stop", message.errorMessage);
	const socket = FakeWebSocket.instances[0]!;
	assert.equal(socket.url, "wss://resolved.example/v9/responses");
	assert.equal(socket.headers.authorization, "Bearer resolved-credential");
	assert.equal(socket.headers["x-provider-keep"], "kept");
	assert.equal(socket.headers["x-route"], "account-a");
	assert.equal(
		Object.keys(socket.headers).some((key) => key.toLowerCase() === "x-provider-default"),
		false,
		"the SDK applies null deletion before invoking the injected fetch",
	);
	assert.equal(Object.values(socket.headers).includes("null"), false);
});

test("injects the WebSocket transport without replacing the caller fetch", async (t) => {
	const originalFetch = globalThis.fetch;
	let callerFetches = 0;
	const callerFetch: typeof fetch = async () => {
		callerFetches++;
		throw new Error("caller fetch should only handle HTTP fallback");
	};
	useFakeWebSocket(t);

	const stream = runStream({ apiKey: "secret", fetch: callerFetch, transport: "websocket" });
	assert.equal(globalThis.fetch, originalFetch, "global fetch stays unchanged while the stream is active");

	const message = await stream.result();
	assert.equal(message.stopReason, "stop", message.errorMessage);
	assert.equal(callerFetches, 0);
	assert.equal(FakeWebSocket.instances.length, 1);
	assert.equal(globalThis.fetch, originalFetch);
});

test("uses global fetch for fallback when the caller does not supply one", async (t) => {
	const originalFetch = globalThis.fetch;
	t.after(() => {
		globalThis.fetch = originalFetch;
	});
	let globalFetches = 0;
	let delegatedInput: string | URL | Request | undefined;
	globalThis.fetch = async (input) => {
		globalFetches++;
		delegatedInput = input;
		return sseResponse(completedFrame());
	};
	useFakeWebSocket(t, { failHandshakes: 1 });

	const message = await runStream({ apiKey: "secret", transport: "websocket" }).result();

	assert.equal(message.stopReason, "stop", message.errorMessage);
	assert.equal(globalFetches, 1);
	assert.equal(String(delegatedInput), "https://api.example.com/v1/responses");
	assert.equal(FakeWebSocket.instances.length, 1);
});

test("keeps pi-ai retries on caller fetch after WebSocket becomes unavailable", async (t) => {
	let callerFetches = 0;
	const callerFetch: typeof fetch = async () => {
		callerFetches++;
		if (callerFetches === 1) {
			return new Response(JSON.stringify({ error: { message: "retry over HTTP" } }), {
				status: 429,
				headers: { "content-type": "application/json", "retry-after": "0" },
			});
		}
		return sseResponse(completedFrame());
	};
	useFakeWebSocket(t, { failHandshakes: 1 });

	const message = await runStream({
		apiKey: "secret",
		fetch: callerFetch,
		transport: "websocket",
		maxRetries: 1,
	}).result();

	assert.equal(message.stopReason, "stop", message.errorMessage);
	assert.equal(callerFetches, 2);
	assert.equal(FakeWebSocket.instances.length, 1);
});

test("keeps pi-ai retries on WebSocket after a provider HTTP error", async (t) => {
	let callerFetches = 0;
	const callerFetch: typeof fetch = async () => {
		callerFetches++;
		throw new Error("provider errors must not trigger HTTP fallback");
	};
	useFakeWebSocket(t, {
		turns: [
			[
				{
					type: "error",
					status: 429,
					headers: { "retry-after": "0" },
					error: { message: "retry over WebSocket" },
				},
			],
			[completedFrame()],
		],
	});

	const message = await runStream({
		apiKey: "secret",
		fetch: callerFetch,
		transport: "websocket",
		maxRetries: 1,
	}).result();

	assert.equal(message.stopReason, "stop", message.errorMessage);
	assert.equal(callerFetches, 0);
	assert.equal(FakeWebSocket.instances.length, 2);
});
