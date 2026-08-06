# ADR 0008: Isolate reusable WebSocket state by request identity

Status: accepted
Date: 2026-08-06
Partially supersedes: ADR 0004 (pooling eligibility), ADR 0005 (learning scope), ADR 0007 (minimum version)

## Context

Pi 0.84 fixes its built-in Codex transport so cached WebSocket sessions cannot cross account credentials.
This extension had the same risk: its pool bucket used only session, provider and model, although Pi can
resolve a different credential, endpoint or routing header for each request. Reusing the old socket would
silently send a request through the previous account or route. Provider-wide unsupported-parameter
learning could likewise carry one route's capabilities into another.

Calls without a session ID pose a separate ambiguity: there is no logical conversation boundary within
which connection-local continuation state can be reused safely. Maximising reuse in either case is less
important than preventing account or context crossover.

## Decision

Require pi-ai and pi-coding-agent 0.84.0 or newer. Continue to let Pi resolve credentials, endpoints,
headers, sampling parameters and request bodies before the injected fetch sees the request.

Only named sessions using `auto` or `websocket-cached` may pool sockets. Reuse requires the same session,
provider and model plus the same connection identity derived from the final WebSocket endpoint and
effective handshake headers. Store that identity and the resulting pool key only as opaque digests, never
as raw credentials, endpoints or header values. Explicit `websocket` requests, requests without a session
ID and any uncertain identity use a fresh socket and send full input.

Keep continuation state bound to its socket. Scope learned unsupported parameters to the connection
identity and final request model rather than to a provider.

## Consequences

Credential, endpoint, route, model or session changes sacrifice a possible handshake reuse instead of
risking cross-account or cross-context state. Equivalent named-session requests still reuse sockets and
can send deltas. Aggregate stats remain useful without exposing identity material, and each route relearns
its own unsupported fields after process start.
