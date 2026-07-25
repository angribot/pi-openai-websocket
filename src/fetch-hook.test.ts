import assert from "node:assert/strict";
import test from "node:test";
import { hookIsClean, installFetchHandler, MARKER_HEADER, readHeader } from "./fetch-hook.ts";

const original = globalThis.fetch;

function restore(): void {
	globalThis.fetch = original;
}

test("header lookup is case insensitive across every headers shape", () => {
	assert.equal(readHeader({ "X-Pi-OpenAI-WebSocket": "m1" }, MARKER_HEADER), "m1");
	assert.equal(readHeader(new Headers({ [MARKER_HEADER]: "m2" }), MARKER_HEADER), "m2");
	assert.equal(readHeader([["X-PI-OPENAI-WEBSOCKET", "m3"]], MARKER_HEADER), "m3");
	assert.equal(readHeader(undefined, MARKER_HEADER), undefined);
	assert.equal(readHeader({ other: "x" }, MARKER_HEADER), undefined);
});

test("a marked request reaches its handler and the global is restored", async () => {
	assert.equal(hookIsClean(), true);
	let seen = 0;
	const { marker, release } = installFetchHandler(async () => {
		seen++;
		return new Response("handled");
	});

	assert.notEqual(globalThis.fetch, original, "the hook is installed while a request is live");
	const response = await globalThis.fetch("https://example.com/v1/responses", {
		method: "POST",
		headers: { [MARKER_HEADER]: marker },
	});

	assert.equal(await response.text(), "handled");
	assert.equal(seen, 1);

	release();
	assert.equal(globalThis.fetch, original);
	assert.equal(hookIsClean(), true);
	restore();
});

test("an unmarked request is not touched", async () => {
	let handled = false;
	let delegated = false;
	globalThis.fetch = (async () => {
		delegated = true;
		return new Response("real");
	}) as typeof globalThis.fetch;
	const realDuringTest = globalThis.fetch;

	const { release } = installFetchHandler(async () => {
		handled = true;
		return new Response("handled");
	});

	const response = await globalThis.fetch("https://other.example.com/v1/models");

	assert.equal(await response.text(), "real");
	assert.equal(delegated, true);
	assert.equal(handled, false, "other providers must be unaffected");

	release();
	assert.equal(globalThis.fetch, realDuringTest);
	restore();
});

test("a request marked for another handler is not stolen", async () => {
	const first = installFetchHandler(async () => new Response("first"));
	const second = installFetchHandler(async () => new Response("second"));

	const a = await globalThis.fetch("https://example.com/", { headers: { [MARKER_HEADER]: first.marker } });
	const b = await globalThis.fetch("https://example.com/", { headers: { [MARKER_HEADER]: second.marker } });

	assert.equal(await a.text(), "first");
	assert.equal(await b.text(), "second");

	first.release();
	second.release();
	assert.equal(hookIsClean(), true);
	restore();
});

test("overlapping requests restore the global only once the last one finishes", async () => {
	const first = installFetchHandler(async () => new Response("first"));
	const second = installFetchHandler(async () => new Response("second"));
	const hooked = globalThis.fetch;

	first.release();
	assert.equal(globalThis.fetch, hooked, "the second request is still live");

	second.release();
	assert.equal(globalThis.fetch, original);
	assert.equal(hookIsClean(), true);
	restore();
});

test("a released marker stops being dispatched", async () => {
	let delegated = false;
	globalThis.fetch = (async () => {
		delegated = true;
		return new Response("real");
	}) as typeof globalThis.fetch;

	const { marker, release } = installFetchHandler(async () => new Response("handled"));
	const outer = installFetchHandler(async () => new Response("outer"));
	release();

	const response = await globalThis.fetch("https://example.com/", { headers: { [MARKER_HEADER]: marker } });
	assert.equal(await response.text(), "real");
	assert.equal(delegated, true);

	outer.release();
	restore();
});

test("release is idempotent", () => {
	const { release } = installFetchHandler(async () => new Response("x"));
	release();
	release();
	assert.equal(hookIsClean(), true, "a double release must not unbalance the counter");
	restore();
});

test("the marker is read from a Request object too", async () => {
	const { marker, release } = installFetchHandler(async () => new Response("handled"));
	const request = new Request("https://example.com/", { headers: { [MARKER_HEADER]: marker } });

	const response = await globalThis.fetch(request);
	assert.equal(await response.text(), "handled");

	release();
	restore();
});
