/**
 * Installs a `globalThis.fetch` dispatcher that routes only marked requests to a
 * per-request handler and leaves everything else untouched.
 *
 * pi-ai reaches its api implementations through a lazy loader, so the client the
 * OpenAI SDK builds is constructed after an `await`. A transport swap therefore
 * cannot rely on a synchronous window; the hook has to stay installed for the whole
 * request.
 *
 * That is safe because dispatch is opt-in. Each request carries a marker header, and
 * anything without a recognised marker is handed to the fetch that was in place
 * before the hook went in. Other providers, other extensions, and pi's own HTTP
 * traffic are unaffected. Nested installs are reference counted, so the original
 * `fetch` is restored once the last request finishes.
 */

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

/** Request header naming the handler a request belongs to. Never sent upstream. */
export const MARKER_HEADER = "x-pi-openai-websocket";

const handlers = new Map<string, FetchLike>();
let savedFetch: typeof globalThis.fetch | undefined;
let depth = 0;
let counter = 0;

/**
 * Registers `handler` and returns its marker plus a release function. The caller puts
 * the marker in the request headers and calls release when the request is done.
 */
export function installFetchHandler(handler: FetchLike): { marker: string; release: () => void } {
	const marker = `ws-${++counter}`;
	handlers.set(marker, handler);

	if (depth++ === 0) {
		const real = globalThis.fetch as FetchLike;
		savedFetch = globalThis.fetch;
		globalThis.fetch = ((input: string | URL | Request, init?: RequestInit) => {
			const target = markerOf(input, init);
			const chosen = target ? handlers.get(target) : undefined;
			return chosen ? chosen(input, init) : real(input, init);
		}) as typeof globalThis.fetch;
	}

	let released = false;
	return {
		marker,
		release: () => {
			if (released) return;
			released = true;
			handlers.delete(marker);
			if (--depth === 0 && savedFetch) {
				globalThis.fetch = savedFetch;
				savedFetch = undefined;
			}
		},
	};
}

function markerOf(input: string | URL | Request, init?: RequestInit): string | undefined {
	const fromInit = readHeader(init?.headers, MARKER_HEADER);
	if (fromInit) return fromInit;
	return input instanceof Request ? (input.headers.get(MARKER_HEADER) ?? undefined) : undefined;
}

export function readHeader(headers: RequestInit["headers"], name: string): string | undefined {
	if (!headers) return undefined;
	const wanted = name.toLowerCase();
	if (headers instanceof Headers) return headers.get(name) ?? undefined;
	if (Array.isArray(headers)) {
		for (const [key, value] of headers) if (key.toLowerCase() === wanted) return value;
		return undefined;
	}
	for (const [key, value] of Object.entries(headers)) if (key.toLowerCase() === wanted) return String(value);
	return undefined;
}

/** Test helper: true when no hook is installed and nothing is registered. */
export function hookIsClean(): boolean {
	return depth === 0 && handlers.size === 0 && savedFetch === undefined;
}
