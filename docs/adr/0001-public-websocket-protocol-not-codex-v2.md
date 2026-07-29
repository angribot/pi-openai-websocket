# ADR 0001: Speak the public Responses WebSocket protocol, not Codex v2

Status: superseded by ADR 0006
Date: 2026-07-25

## Context

pi already implements Responses-over-WebSocket, but only inside `openai-codex-responses`. That api is
tied to `chatgpt.com/backend-api/codex` and to ChatGPT account auth, so third-party providers cannot use
it. The obvious starting point was to copy what that code sends.

Reading `openai/codex` showed its wire format includes a beta opt-in header
`OpenAI-Beta: responses_websockets=2026-02-06`, a `client_metadata` map carrying what would otherwise be
HTTP headers, `x-codex-turn-state`, `codex.rate_limits` events, timing-metrics headers and a wrapped
error envelope.

OpenAI, however, documents WebSocket mode as a public feature of the Responses API: `wss://{base}/responses`,
plain `Authorization: Bearer`, one `{"type":"response.create", ...}` text frame per request, unwrapped
`response.*` events back, and **no beta header**. `openai-python`'s `client.responses.connect()` and
`openai-node`'s `resources/responses/ws.ts` send none. Azure documents the same protocol at its own URL
shape. The beta header and everything it unlocks is a Codex-specific extension, not part of the protocol.

A handshake probe against a third-party relay returned `101` with and without the beta header, and a live
turn streamed `response.created` → `response.in_progress` → `output_item.added` → `content_part.added` →
`output_text.delta` with the header absent.

## Decision

Implement the publicly documented protocol only. No `OpenAI-Beta` header, no `client_metadata`, no
`x-codex-*` headers, no `codex.rate_limits` handling, no `generate: false` prewarm.

## Consequences

The transport works against any endpoint implementing the documented protocol: OpenAI, Azure by URL
substitution, and the relay ecosystem that accepts `GET /v1/responses` upgrades. Codex-only signals are
unavailable, which costs nothing here because none of them feed pi's event pipeline.

Wrapped `{"type":"error","status":...}` frames are still handled, because relays forwarding to a
Codex-style backend emit them, and mapping them onto HTTP errors is what lets pi-ai's existing retry and
error formatting apply unchanged.
