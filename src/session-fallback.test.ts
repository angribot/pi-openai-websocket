import assert from "node:assert/strict";
import test from "node:test";
import { StickySseSessions, shouldUseSse } from "./session-fallback.ts";

test("sticky SSE only marks named sessions", () => {
	const sessions = new StickySseSessions();

	sessions.markSseOnly(undefined);
	assert.equal(sessions.isSseOnly(undefined), false);

	sessions.markSseOnly("session-a");
	assert.equal(sessions.isSseOnly("session-a"), true);
	assert.equal(sessions.isSseOnly("session-b"), false);
});

test("SSE routing combines the transport preference with sticky session state", () => {
	const sessions = new StickySseSessions();
	sessions.markSseOnly("session-a");

	assert.equal(shouldUseSse("sse", "session-b", sessions), true);
	assert.equal(shouldUseSse("auto", "session-a", sessions), true);
	assert.equal(shouldUseSse("websocket", "session-b", sessions), false);
	assert.equal(shouldUseSse("websocket-cached", undefined, sessions), false);
});

test("session cleanup clears one sticky session or all sessions", () => {
	const sessions = new StickySseSessions();
	sessions.markSseOnly("session-a");
	sessions.markSseOnly("session-b");

	sessions.clear("session-a");
	assert.equal(sessions.isSseOnly("session-a"), false);
	assert.equal(sessions.isSseOnly("session-b"), true);

	sessions.clear();
	assert.equal(sessions.isSseOnly("session-b"), false);
});
