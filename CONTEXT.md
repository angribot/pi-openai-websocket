# Context

A pi extension that carries the OpenAI Responses API over WebSocket for any provider, so third-party
endpoints do not need the proprietary `openai-codex-responses` api.

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

**Settled** — the point where a request's socket work is over, whichever way it ended. Distinct from the
event stream above resolving, which may be later or never. Resources that must not outlive a request are
released on settle.

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
before anything streamed. A failure after streaming started is an error instead, because resending is no
longer safe.

## Decisions

See `docs/adr/`.
