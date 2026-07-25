import assert from "node:assert/strict";
import test from "node:test";
import {
	applyContinuation,
	responseIdFrom,
	serverItems,
	SocketPool,
	type Continuation,
	type PooledSocket,
} from "./continuation.ts";

const user = (text: string) => ({ role: "user", content: [{ type: "input_text", text }] });
const assistant = (text: string) => ({ type: "message", role: "assistant", content: [{ type: "output_text", text }] });

function baseBody(input: unknown[]): Record<string, unknown> {
	return { model: "m", input, stream: true, store: false };
}

function continuation(overrides: Partial<Continuation> = {}): Continuation {
	return {
		requestBody: baseBody([user("one")]),
		responseId: "resp_1",
		responseItems: [assistant("ok")],
		complete: true,
		...overrides,
	};
}

test("a strict extension of the previous turn becomes a delta", () => {
	const body = baseBody([user("one"), assistant("ok"), user("two")]);

	const sent = applyContinuation(body, continuation());

	assert.equal(sent.previous_response_id, "resp_1");
	assert.deepEqual(sent.input, [user("two")], "only the items the server has not seen");
	assert.equal(sent.model, "m", "other fields are untouched");
});

test("multiple new items all go in the delta", () => {
	const body = baseBody([user("one"), assistant("ok"), user("two"), user("three")]);
	const sent = applyContinuation(body, continuation());
	assert.deepEqual(sent.input, [user("two"), user("three")]);
});

test("unfinished baseline sends the full input", () => {
	// The items are derived from the finished assistant message, which lands after the
	// socket returns to the pool. Acting early would resend what the server holds.
	const body = baseBody([user("one"), assistant("ok"), user("two")]);

	const sent = applyContinuation(body, continuation({ complete: false, responseItems: [] }));

	assert.equal(sent.previous_response_id, undefined);
	assert.deepEqual(sent.input, body.input);
});

test("no continuation sends the full input", () => {
	const body = baseBody([user("one")]);
	assert.deepEqual(applyContinuation(body, undefined), body);
});

test("an edited history sends the full input", () => {
	const body = baseBody([user("EDITED"), assistant("ok"), user("two")]);
	const sent = applyContinuation(body, continuation());
	assert.equal(sent.previous_response_id, undefined);
	assert.deepEqual(sent.input, body.input);
});

test("a dropped server item sends the full input", () => {
	// The assistant turn the server produced is missing, so the histories diverge.
	const body = baseBody([user("one"), user("two")]);
	const sent = applyContinuation(body, continuation());
	assert.equal(sent.previous_response_id, undefined);
});

test("a changed non-input field sends the full input", () => {
	const body = { ...baseBody([user("one"), assistant("ok"), user("two")]), temperature: 0.5 };
	const sent = applyContinuation(body, continuation());
	assert.equal(sent.previous_response_id, undefined, "the server would answer under different settings");
});

test("no new input sends the full input", () => {
	// Nothing to add means there is nothing to ask for; an empty delta would be a
	// request the server cannot answer.
	const body = baseBody([user("one"), assistant("ok")]);
	const sent = applyContinuation(body, continuation());
	assert.equal(sent.previous_response_id, undefined);
});

test("a shorter input sends the full input", () => {
	const body = baseBody([user("one")]);
	const sent = applyContinuation(body, continuation());
	assert.equal(sent.previous_response_id, undefined);
});

test("tool outputs are excluded from the baseline", () => {
	// Tool results come from the client, so they arrive as new input next turn.
	const items = serverItems([
		{ type: "function_call", name: "read", call_id: "c1" },
		{ type: "function_call_output", call_id: "c1", output: "text" },
		{ type: "custom_tool_call_output", call_id: "c2", output: "text" },
	]);
	assert.deepEqual(items, [{ type: "function_call", name: "read", call_id: "c1" }]);
});

test("a tool call turn deltas to just the tool output", () => {
	const call = { type: "function_call", name: "read", call_id: "c1", arguments: "{}" };
	const output = { type: "function_call_output", call_id: "c1", output: "file text" };
	const body = baseBody([user("read a file"), call, output]);

	const sent = applyContinuation(
		body,
		continuation({ requestBody: baseBody([user("read a file")]), responseItems: [call] }),
	);

	assert.deepEqual(sent.input, [output]);
});

test("response id is read from the terminal event", () => {
	assert.equal(responseIdFrom({ type: "response.completed", response: { id: "resp_9" } }), "resp_9");
	assert.equal(responseIdFrom({ type: "response.completed", response: {} }), undefined);
	assert.equal(responseIdFrom({ type: "response.completed" }), undefined);
});

// --- pool -------------------------------------------------------------------

class StubSocket extends EventTarget {
	readyState = 1;
	closed = false;
	close(): void {
		this.closed = true;
		this.readyState = 3;
	}
}

const stub = () => new StubSocket() as unknown as WebSocket;

test("a released socket is handed back on the next request", () => {
	const pool = new SocketPool();
	const entry = pool.add("key", stub());
	pool.release(entry, true);

	const reused = pool.acquire("key");
	assert.equal(reused, entry);
	assert.equal(reused?.busy, true, "an acquired socket is marked busy");
	assert.equal(pool.size, 1);
});

test("a busy socket is never handed out twice", () => {
	// Interleaved frames on one socket would corrupt both responses.
	const pool = new SocketPool();
	pool.add("key", stub());
	assert.equal(pool.acquire("key"), undefined);
});

test("a socket released without keep is closed and dropped", () => {
	const pool = new SocketPool();
	const socket = stub();
	const entry = pool.add("key", socket);
	pool.release(entry, false);

	assert.equal((socket as unknown as StubSocket).closed, true);
	assert.equal(pool.size, 0);
	assert.equal(pool.acquire("key"), undefined);
});

test("an expired socket is dropped rather than reused", () => {
	const pool = new SocketPool();
	const socket = stub();
	const entry = pool.add("key", socket, 0);
	pool.release(entry, true, 0);

	// Past the 55 minute age cap, ahead of the server's own 60 minute limit.
	assert.equal(pool.acquire("key", 56 * 60 * 1000), undefined);
	assert.equal((socket as unknown as StubSocket).closed, true);
});

test("an idle socket is dropped rather than reused", () => {
	const pool = new SocketPool();
	const entry = pool.add("key", stub(), 0);
	pool.release(entry, true, 0);
	assert.equal(pool.acquire("key", 6 * 60 * 1000), undefined);
});

test("a closed socket is not reused", () => {
	const pool = new SocketPool();
	const socket = stub();
	const entry = pool.add("key", socket);
	pool.release(entry, true);
	(socket as unknown as StubSocket).readyState = 3;

	assert.equal(pool.acquire("key"), undefined);
	assert.equal(pool.size, 0);
});

test("keys do not share sockets", () => {
	const pool = new SocketPool();
	const entry = pool.add("a", stub());
	pool.release(entry, true);
	assert.equal(pool.acquire("b"), undefined);
	assert.equal(pool.acquire("a"), entry);
});

test("closeAll closes every socket", () => {
	const pool = new SocketPool();
	const first = stub();
	const second = stub();
	const a = pool.add("a", first);
	pool.release(a, true);
	pool.add("b", second);

	pool.closeAll();

	assert.equal((first as unknown as StubSocket).closed, true);
	assert.equal((second as unknown as StubSocket).closed, true);
	assert.equal(pool.size, 0);
});

test("the sweep drops sockets nobody will ask for again", () => {
	// acquire and release are the usual expiry checks, but a session can sit idle for
	// longer than the TTL without either running.
	const pool = new SocketPool(0);
	const idle = stub();
	const fresh = stub();
	const a = pool.add("k", idle, 0);
	pool.release(a, true, 0);
	const b = pool.add("k", fresh, 6 * 60 * 1000);
	pool.release(b, true, 6 * 60 * 1000);

	assert.equal(pool.sweep(6 * 60 * 1000), 1, "only the one past its idle limit");
	assert.equal((idle as unknown as StubSocket).closed, true);
	assert.equal((fresh as unknown as StubSocket).closed, false);
	assert.equal(pool.size, 1);
});

test("the sweep reclaims a busy socket only once it is past the age limit", () => {
	// A response body can be abandoned without being read or cancelled, which leaves its
	// socket checked out for good. Idleness cannot decide this, because a long streaming
	// turn is legitimately busy for minutes; age can, because the server cuts the
	// connection at 60 minutes anyway.
	const pool = new SocketPool(0);
	const abandoned = stub();
	pool.add("k", abandoned, 0);

	assert.equal(pool.sweep(30 * 60 * 1000), 0, "still plausibly streaming");
	assert.equal(pool.size, 1);

	assert.equal(pool.sweep(56 * 60 * 1000), 1);
	assert.equal((abandoned as unknown as StubSocket).closed, true);
	assert.equal(pool.size, 0);
});

test("releasing a swept socket is a no-op", () => {
	// The sweep can drop a socket whose stream then unwinds and releases it.
	const pool = new SocketPool(0);
	const entry = pool.add("k", stub(), 0);
	pool.sweep(56 * 60 * 1000);
	assert.equal(pool.size, 0);

	pool.release(entry, true, 56 * 60 * 1000);
	assert.equal(pool.size, 0);
});

test("the sweep timer only runs while the pool holds something", () => {
	const hasTimer = (pool: SocketPool) => (pool as unknown as { sweepTimer?: unknown }).sweepTimer !== undefined;
	const pool = new SocketPool(1);
	const entry = pool.add("k", stub());
	assert.equal(hasTimer(pool), true);

	pool.release(entry, false);
	assert.equal(hasTimer(pool), false, "an empty pool keeps no timer");

	pool.add("k", stub());
	assert.equal(hasTimer(pool), true, "and starts again when refilled");
	pool.closeAll();
	assert.equal(hasTimer(pool), false);
});

test("continuation follows the socket, not the key", () => {
	// One measured relay ignores previous_response_id and answers from whatever that
	// connection produced last, so state must never be shared across sockets.
	const pool = new SocketPool();
	const first = pool.add("key", stub());
	first.continuation = continuation();
	pool.release(first, true);

	const second = pool.add("key", stub());
	assert.equal(second.continuation, undefined);

	const entries: PooledSocket[] = [first, second];
	assert.notEqual(entries[0]?.continuation, entries[1]?.continuation);
});
