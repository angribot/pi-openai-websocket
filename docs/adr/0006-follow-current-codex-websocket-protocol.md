# ADR 0006: Follow the current Codex Responses WebSocket protocol

Status: accepted
Date: 2026-07-29
Supersedes: ADR 0001

## Context

ADR 0001 separated OpenAI's public Responses WebSocket protocol from the then-experimental Codex v2
protocol. That boundary no longer describes the upstream direction. OpenAI now documents
`previous_response_id`, incremental input and optional `generate: false` warmup on WebSocket. Codex removed
its v1 `response.append` path, removed the `responses_websockets` and `responses_websockets_v2` feature
selection, and uses one `response.create` protocol whenever a provider declares WebSocket support.

Codex still sends `OpenAI-Beta: responses_websockets=2026-02-06` on every WebSocket handshake, while the
public guide does not require it and official SDK examples vary. This extension values compatibility with
the current Codex implementation over maintaining two nearly identical WebSocket dialects. Providers
remain explicitly opted in, and HTTP/SSE remains the compatibility path when that handshake is unavailable.

Some Codex behavior belongs to the Codex product rather than the transport. `client_metadata`,
`x-codex-turn-state` and rate-limit presentation have no consumer in pi's plain `openai-responses` path.
Startup prewarm also does not fit the transport-swap boundary: the exact final request body is only visible
when pi-ai is about to send it, too late for warmup to hide latency.

## Decision

Use one current Responses WebSocket protocol for every opted-in provider:

- send `OpenAI-Beta: responses_websockets=2026-02-06`, replacing any caller-supplied value;
- send top-level `response.create` frames and keep socket-bound `previous_response_id` continuation;
- omit the HTTP-only `stream` field from WebSocket frames, while preserving the original HTTP fallback body;
- route requests with `background: true` through HTTP because background mode is unsupported on WebSocket;
- consume `codex.rate_limits` as an out-of-band event rather than passing it to the Responses decoder;
- open a fresh socket and retry once after `websocket_connection_limit_reached`;
- after genuine WebSocket transport failure, use HTTP/SSE for later requests in the same named session.

Do not add protocol profiles or capability probing. Do not add generic WebSocket retries, startup prewarm,
Codex metadata, turn-state handling or rate-limit UI. API errors, malformed requests and user aborts do not
mark WebSocket unavailable. Calls without a session id do not share fallback state.

## Consequences

Configuration remains a provider allowlist. Upgrading changes the handshake for every listed provider,
but adds no settings or migration state. A relay that rejects the current Codex handshake falls back to
HTTP/SSE; the extension no longer promises WebSocket acceleration for every implementation of the public
subset.

The transport remains a transport swap under pi-ai. Request construction, event decoding, API retry,
usage accounting and error formatting stay shared with HTTP. WebSocket-only normalization is applied to a
separate body, so fallback preserves the original request.

Fallback state is session-local and process-local. The first transport failure warns and completes over
HTTP when it occurs before streaming; a failure after streaming starts remains an error for that turn, but
later turns use HTTP. Session cleanup removes the fallback state.
