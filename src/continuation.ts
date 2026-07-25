/**
 * `previous_response_id` continuation, and the socket pool it requires.
 *
 * The Responses WebSocket protocol keeps a connection-local, single-entry cache of
 * the previous response, so a follow-up request can send only the new input items
 * and reference the previous response by id. That saves re-uploading the whole
 * conversation on every turn.
 *
 * Two properties make this safe:
 *
 *  1. Continuation state is bound to a socket, never to a session or model key.
 *     Some relays ignore `previous_response_id` and simply reuse whatever that
 *     connection produced last, so a delta is only sound when the state we recorded
 *     is that connection's most recent response. Measured against one such relay:
 *     chaining to an older id still answered from the newest state, and an id from
 *     another connection was not recognised at all.
 *  2. A socket carries one request at a time. A busy socket is never handed out; a
 *     second concurrent request gets its own connection. Interleaved frames on one
 *     socket would corrupt both responses.
 *
 * When anything is uncertain, the full input is sent. That is always correct, just
 * more expensive.
 */

export interface Continuation {
	/** The full-input request that produced `responseId`, for field comparison. */
	requestBody: Record<string, unknown>;
	responseId: string;
	/** Items the server produced, in the shape the next request's input will use. */
	responseItems: unknown[];
	/**
	 * Whether `responseItems` is final. They are derived from the finished assistant
	 * message, which lands slightly after the socket goes back to the pool, and an
	 * incomplete baseline would put items the server already holds into the delta.
	 */
	complete: boolean;
}

export interface PooledSocket {
	/** Pool bucket this socket belongs to, so releasing it needs no second argument. */
	key: string;
	socket: WebSocket;
	/** Wall clock at handshake, for the connection age cap. */
	openedAt: number;
	lastUsedAt: number;
	busy: boolean;
	continuation?: Continuation;
}

/** Idle sockets are dropped after this long. */
export const IDLE_TTL_MS = 5 * 60 * 1000;
/** OpenAI documents a 60 minute server cap; stay clear of it. */
export const MAX_AGE_MS = 55 * 60 * 1000;

/**
 * Sockets keyed by pool key. Several sockets can share a key when requests overlap.
 */
export class SocketPool {
	private readonly entries = new Map<string, PooledSocket[]>();

	/** An open, idle, non-expired socket for `key`, marked busy. */
	acquire(key: string, now = Date.now()): PooledSocket | undefined {
		const bucket = this.entries.get(key);
		if (!bucket) return undefined;

		for (let index = bucket.length - 1; index >= 0; index--) {
			const entry = bucket[index]!;
			if (isUnusable(entry, now)) {
				closeQuietly(entry.socket);
				bucket.splice(index, 1);
				continue;
			}
			if (entry.busy) continue;
			entry.busy = true;
			entry.lastUsedAt = now;
			return entry;
		}
		if (bucket.length === 0) this.entries.delete(key);
		return undefined;
	}

	/** Registers a freshly opened socket, already marked busy. */
	add(key: string, socket: WebSocket, now = Date.now()): PooledSocket {
		const entry: PooledSocket = { key, socket, openedAt: now, lastUsedAt: now, busy: true };
		const bucket = this.entries.get(key);
		if (bucket) bucket.push(entry);
		else this.entries.set(key, [entry]);
		return entry;
	}

	/** Returns a socket to the pool, or discards it. */
	release(entry: PooledSocket, keep: boolean, now = Date.now()): void {
		entry.busy = false;
		entry.lastUsedAt = now;
		if (keep && !isUnusable(entry, now)) return;

		closeQuietly(entry.socket);
		const bucket = this.entries.get(entry.key);
		if (!bucket) return;
		const index = bucket.indexOf(entry);
		if (index >= 0) bucket.splice(index, 1);
		if (bucket.length === 0) this.entries.delete(entry.key);
	}

	closeAll(): void {
		for (const bucket of this.entries.values()) {
			for (const entry of bucket) closeQuietly(entry.socket);
		}
		this.entries.clear();
	}

	get size(): number {
		let total = 0;
		for (const bucket of this.entries.values()) total += bucket.length;
		return total;
	}
}

function isUnusable(entry: PooledSocket, now: number): boolean {
	return (
		entry.socket.readyState !== WebSocket.OPEN ||
		now - entry.openedAt > MAX_AGE_MS ||
		(!entry.busy && now - entry.lastUsedAt > IDLE_TTL_MS)
	);
}

/** Closing is best effort; a socket that refuses to close is already gone. */
export function closeQuietly(socket: WebSocket): void {
	try {
		if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) socket.close();
	} catch {
		// Nothing to do: the socket is unusable either way.
	}
}

/**
 * Rewrites `body` as a delta against `continuation`, or returns it unchanged when
 * the conversation is not a strict continuation of what that socket last saw.
 *
 * A delta is only sound when every field other than the input is unchanged and the
 * new input begins with exactly the previous input plus the items the server
 * produced. Anything else, including a reordered or edited history, means the server
 * side no longer matches and the full input has to go.
 */
export function applyContinuation(
	body: Record<string, unknown>,
	continuation: Continuation | undefined,
): Record<string, unknown> {
	if (!continuation?.complete) return body;

	const delta = inputDelta(body, continuation);
	if (!delta) return body;

	return { ...body, previous_response_id: continuation.responseId, input: delta };
}

function inputDelta(body: Record<string, unknown>, continuation: Continuation): unknown[] | undefined {
	if (!sameExceptInput(body, continuation.requestBody)) return undefined;

	const input = asArray(body.input);
	const baseline = [...asArray(continuation.requestBody.input), ...continuation.responseItems];
	if (input.length <= baseline.length) return undefined;
	if (!jsonEqual(input.slice(0, baseline.length), baseline)) return undefined;

	return input.slice(baseline.length);
}

function sameExceptInput(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
	return jsonEqual(withoutInput(a), withoutInput(b));
}

function withoutInput(body: Record<string, unknown>): Record<string, unknown> {
	const { input: _input, previous_response_id: _previous, ...rest } = body;
	return rest;
}

function asArray(value: unknown): unknown[] {
	return Array.isArray(value) ? value : [];
}

function jsonEqual(a: unknown, b: unknown): boolean {
	return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * Extracts the response id from a terminal event.
 *
 * The event's own `output` is deliberately not used as the baseline: some relays
 * report it empty, and even when present it need not byte-match the items the
 * client will send for that same turn. The baseline is derived from the finished
 * assistant message instead, through the same conversion the next request uses.
 */
export function responseIdFrom(event: Record<string, unknown>): string | undefined {
	const response = event.response as { id?: unknown } | undefined;
	return typeof response?.id === "string" ? response.id : undefined;
}

/** Drops client-provided tool outputs, which arrive as new input next turn. */
export function serverItems(items: readonly unknown[]): unknown[] {
	return items.filter((item) => {
		const type = (item as { type?: unknown }).type;
		return type !== "function_call_output" && type !== "custom_tool_call_output";
	});
}
