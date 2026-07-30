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

	constructor(_url: string, _options?: unknown) {
		super();
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

function runStream(options: SimpleStreamOptions) {
	return streamOverWebSocket(model, context, options, {
		stats: createStats(),
		settings: { providers: [model.provider], transport: "websocket", connectTimeoutMs: 1_000 },
		warnFallback: () => {},
		pool: new SocketPool(),
		stickySseSessions: new StickySseSessions(),
	});
}

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
