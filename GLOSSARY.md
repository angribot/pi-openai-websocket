# OpenAI WebSocket Transport

A pi extension that carries the plain `openai-responses` api over the Codex WebSocket transport for
opted-in providers, falling back to HTTP when that transport is unavailable.

## Language

### Transport

**Transport Swap**:
Replacing the HTTP transport under pi-ai's `openai-responses` api rather than reimplementing the api. A
substitute `fetch` that speaks WebSocket changes the transport and nothing else: request construction,
retries, error formatting, usage accounting and abort handling remain pi-ai's.
_Avoid_: shim, patch

**Injected Fetch**:
The request-local `fetch` pi-ai passes to the OpenAI SDK. It routes Responses POSTs over WebSocket and
delegates unrelated requests and HTTP fallback to the caller's original fetch. Never replaces
`globalThis.fetch`.
_Avoid_: global fetch, patched fetch

**Fallback Fetch**:
`options.fetch` when the caller supplied one, otherwise `globalThis.fetch`. WebSocket failures delegate the
original request to it unchanged.
_Avoid_: error fetch, backup transport

**SSE Fallback**:
Completing a request over ordinary HTTP streaming after the WebSocket transport failed before anything
streamed. A failure after streaming started remains an error for that turn and moves later requests in the
session to HTTP; API errors and user aborts do not.
_Avoid_: downgrade, retry fallback

### Connection identity

**Connection Identity**:
The endpoint and effective handshake metadata that identify the remote account and route a WebSocket
belongs to. Reusable connection state never crosses connection identities.
_Avoid_: connection key, session

**Sweep**:
The pool's periodic pass for sockets neither `acquire` nor `release` will look at: a session idle past the
TTL, or a response body abandoned without being read or cancelled, whose socket would stay checked out. A
busy socket is dropped only once it is past the age limit.
_Avoid_: cleanup, reaper

### Continuation

**Continuation**:
The state that lets the next request in a named session send only new input items and reference the
previous response by `previous_response_id`. Held on a pooled socket, never shared between sockets.
_Avoid_: session state, cache

**Baseline**:
The previous input plus the previous response's items: what a new input must begin with for a delta to be
sound. Derived from the finished assistant message through pi-ai's own conversion, not from the server's
echo of its own output.
_Avoid_: history, prefix

**Full Request**:
A request carrying the whole conversation rather than only the items the server has not seen. Always
correct, and what any request sends when a delta would not be sound.
_Avoid_: complete request, non-delta

**Delta**:
A request carrying `previous_response_id` and only the input items the server has not seen. Sent only when
the non-input fields are byte-identical, the new input extends the baseline, and those items are final.
_Avoid_: incremental request, partial request

**Stale Continuation**:
The endpoint rejecting the `previous_response_id` a delta chained onto, usually
`previous_response_not_found`. The continuation is forgotten and the conversation resent whole on the same
socket, once.
_Avoid_: expired continuation, invalid continuation

### Recovery

**Strip-and-Retry**:
Dropping a request parameter the endpoint named as unsupported and resending. Rejections are remembered
only for the same connection identity and request model.
_Avoid_: retry without parameter, downgrade

**Terminal Event**:
`response.completed`, `response.incomplete` or `response.failed`. Ends a response but not the socket. A
close or EOF before one is an error.
_Avoid_: completion event, done event
