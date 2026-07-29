# Context

A pi extension that carries the plain `openai-responses` api over the current Codex WebSocket transport
for opted-in providers, with HTTP/SSE fallback when that transport is unavailable.

## Glossary

**Transport swap** — replacing the HTTP transport under pi-ai's `openai-responses` api rather than
reimplementing the api. The OpenAI SDK takes `fetch` from `globalThis` at client construction, so a
substitute `fetch` that speaks WebSocket changes the transport and nothing else. Avoid "shim" and "patch"
for this; both understate that request construction, retries, error formatting, usage accounting and abort
handling remain pi-ai's.

**Marker header** — `x-pi-openai-websocket`, carried on a request so the installed dispatcher knows the
request belongs to this extension. Requests without a recognised marker go to the fetch that was in place
beforehand. This is what makes a process-global mutation safe: it is inert for everything else. The marker
is stripped before the handshake and before any HTTP fallback, and never reaches a provider.

**Dispatcher** — the `fetch` installed on `globalThis`. Routes by marker, reference counted so the
original is restored when the last live request finishes. One per process, not one per request.

**Settled** — the point where a request can no longer issue another `fetch`, so resources scoped to it may
be released. Later than the socket being returned to the pool: an error pi-ai may retry keeps the request
unsettled, because that retry has to reach this transport rather than going out over HTTP. Distinct from
the event stream above resolving, which may be later or never.

**Sweep** — the pool's periodic pass for sockets neither `acquire` nor `release` will look at: a session
idle past the TTL, or a response body abandoned without being read or cancelled, whose socket would stay
checked out. A busy socket is dropped only once it is past the age limit, since a long streaming turn is
legitimately busy for minutes. The timer starts with the first socket, stops when the pool empties, and is
unreferenced so it never holds the process open.

**Continuation** — the state that lets the next request send only new input items and reference the
previous response by `previous_response_id`: the previous full-input request body, its response id, and the
items that response produced. Held on a pooled socket, never in a map keyed by session or model, because
some relays ignore the id and answer from whatever that connection produced last.

**Delta** — a request carrying `previous_response_id` and only the input items the server has not seen.
The opposite is a **full request**, which carries the whole conversation. A delta is only sent when the
non-input fields are byte-identical, the new input extends the previous input plus that response's items,
and those items are final. Anything else sends full, which is always correct.

**Baseline** — the previous input plus the previous response's items, i.e. what a new input must begin with
for a delta to be sound. Derived from the finished assistant message through pi-ai's own conversion, not
from the server's echo of its own output, which relays may report empty.

**Stale continuation** — the endpoint rejecting the `previous_response_id` a delta chained onto, usually
`previous_response_not_found`. The continuation is forgotten and the conversation resent whole on the same
socket, once.

**Strip-and-retry** — dropping a request parameter the endpoint named as unsupported and resending. Relays
forwarding to a Codex-style backend accept a narrower parameter set over WebSocket than over HTTP.
Rejections are remembered per provider for the rest of the process.

**Terminal event** — `response.completed`, `response.incomplete` or `response.failed`. Ends a response but
not the socket. A close or EOF before one is an error.

**SSE fallback** — completing a request over ordinary HTTP streaming after the WebSocket transport failed
before anything streamed. The named session then stays on HTTP. A failure after streaming started remains
an error for that turn and moves later requests in the session to HTTP; API errors and user aborts do not.

## Decisions

See `docs/adr/`.
