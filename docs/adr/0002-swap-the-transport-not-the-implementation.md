# ADR 0002: Swap the transport under pi-ai, do not reimplement the api

Status: accepted
Date: 2026-07-25

## Context

An extension supplying `streamSimple` has to produce the same events pi-ai's `openai-responses` produces.
Two ways to get there were measured against pi-ai 0.82.0.

**A, reimplement.** Rebuild the request body and the error formatting on top of the exported helpers.
`buildParams` is private, and the package export map has no `./utils/*` entry, so `splitDeferredTools`
(35 lines) and the error-body normalisers (108 lines) are unreachable too. Around 270 lines duplicated,
every one of them free to drift from pi-ai on any release, with the divergence invisible until a request
behaves subtly differently.

**B, swap the transport.** The OpenAI SDK takes `fetch` from `globalThis` when a client is constructed
(`client.js:149`). Give it one that speaks WebSocket, and everything above the transport stays pi-ai's.
Around 60 lines, nothing duplicated.

B first looked like it could be scoped to a synchronous window: pi-ai constructs its client at
`openai-responses.js:98`, before its first `await` at `:100`, so patch, call, restore would be invisible
to any other task. That is true of the api module in isolation and false of the path an extension actually
takes. pi's extension loader aliases the pi-ai root to the **compat** entrypoint and maps no `./api/*`
subpaths, so the implementation is only reachable through `openAIResponsesApi()`, whose `lazyApi` wrapper
routes every call through `lazyStream(model, async () => (await load()).streamSimple(...))`. Client
construction therefore always happens after an `await`, and the synchronous window never covers it.
Observed directly: the hook was never reached and every request went out over HTTP.

## Decision

B, with the hook installed for the whole request instead of a synchronous window.

Dispatch is opt-in. Each request carries a marker header, `x-pi-openai-websocket`, passed through
`options.headers` into the SDK's default headers. The installed `fetch` routes only recognised markers to
their handler and hands everything else to the fetch that was in place before it. Installs are reference
counted, so the original is restored when the last request finishes, and the marker is stripped before the
WebSocket handshake.

Frames are re-emitted as `text/event-stream` bytes so the SDK's own decoder parses them. Request
construction, retries, error formatting, `onResponse`, abort handling and usage accounting stay pi-ai's,
untouched.

## Consequences

Zero duplicated logic, so nothing drifts. Behaviour over WebSocket is the HTTP behaviour by construction,
not by resemblance.

No dependency on statement ordering inside pi-ai. The remaining assumption is far weaker: that
`options.headers` reaches the SDK's request headers. If it ever stops doing so, no marker arrives, the
request goes out over HTTP, and the extension warns rather than failing.

A process-global mutation held across awaits is only acceptable because it is inert for anything
unmarked. Concurrent requests from other providers, other extensions and pi itself take the pre-existing
fetch unchanged, which is asserted by tests rather than assumed.

Re-encoding parsed frames back into SSE text so the SDK can parse them again is redundant work. It is also
what keeps the event pipeline shared rather than reimplemented, and the cost is invisible next to the
network.

The same loader constraint rules out importing `convertResponsesMessages` for the continuation baseline.
It is obtained instead by letting pi-ai build a request body and capturing it from `onPayload`, throwing
from there to stop before the network. See ADR 0004.

Nothing here needs changes to pi.
