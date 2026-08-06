# ADR 0005: Recover from endpoint rejections instead of surfacing them

Status: accepted; per-provider learning scope superseded by ADR 0008
Date: 2026-07-25

## Context

Two rejections were measured against a live relay, both arriving as the first frame of a response, before
anything had streamed.

**A narrower parameter set over WebSocket than over HTTP.** The relay answers
`{"type":"error","status":400,"error":{"message":"Unsupported parameter: max_output_tokens","type":"upstream_error"}}`
and the same for `prompt_cache_options`, while accepting both over HTTP on the same base URL. It forwards to
a Codex-style backend whose request type has no such fields. `max_output_tokens` is not optional in
practice: pi-ai's `buildBaseOptions` fills it from `model.maxTokens` on every request, so without handling
this the transport fails on the first real turn.

**A rejected `previous_response_id`.** OpenAI documents `previous_response_not_found` for an id the
connection no longer holds. A response can be evicted by a failed turn, and against OpenAI itself an
expired id is a normal occurrence, not a defect.

Both leave the socket usable and nothing emitted, so resending is safe. Surfacing either one puts a
provider implementation detail in front of the user as a failed turn.

## Decision

Recover from both, in the same retry loop, and only while no event has been emitted.

**Unsupported parameter:** read the field name out of the error message, drop it, resend. Remember the
rejection per provider for the rest of the process, so the cost is one extra round trip per parameter per
process rather than per request. `MAX_STRIP_ROUNDS = 4` bounds it.

**Stale continuation:** forget the socket's continuation state and resend the whole conversation on the same
connection. Once: the retry carries no `previous_response_id`, so a repeat rejection is the server's answer
to a full request and belongs to the caller.

Both are counted (`strippedParams`, `staleContinuations`) and visible in `/ws-stats`.

## Consequences

The transport works against relays whose WebSocket ingress is stricter than their HTTP ingress, without the
user having to discover a field name from a 400.

Dropping a parameter silently changes semantics. For `max_output_tokens` the value pi-ai sends is the
model's own maximum, so dropping it costs nothing observable; a caller-set cap would be silently ignored
instead, which is why the drop is counted and reported rather than hidden. Adaptive discovery was preferred
over a static configured list because the endpoint already names the field, and a list makes the user do
the discovery.

Learning is per process and never persisted, so a restart pays one extra round trip. Fine, and the
alternative is a cache to invalidate when an endpoint changes.

Recovery is confined to "before the first event". After that, retrying would duplicate output, so an error
is the only honest answer.
