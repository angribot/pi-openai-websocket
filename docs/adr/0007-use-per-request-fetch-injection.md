# ADR 0007: Use pi-ai's per-request fetch injection

Status: accepted; minimum-version decision superseded by ADR 0008
Date: 2026-07-30
Supersedes: ADR 0002

## Context

ADR 0002 chose a process-global `fetch` dispatcher because pi-ai 0.82 did not expose the OpenAI SDK's
`fetch` option through `SimpleStreamOptions`. Lazy api loading meant the dispatcher had to stay installed
across awaits, so marker headers and reference-counted cleanup kept unrelated requests out of it.

Pi-ai 0.83 adds per-request `fetch` injection and forwards it through `streamSimple` to the OpenAI SDK.
The extension can now replace transport at the request boundary directly. Callers may also supply a custom
fetch for proxies, instrumentation, tests or network policy, and HTTP fallback must preserve that choice.

## Decision

Pass the WebSocket fetch implementation to pi-ai through `SimpleStreamOptions.fetch`.

- Use the caller's `options.fetch`, or `globalThis.fetch` when absent, as the HTTP fallback.
- Delegate fallback requests unchanged.
- Keep transport-unavailable state inside the injected fetch so retries from the same pi-ai request remain
  on HTTP after a WebSocket failure.
- Do not replace `globalThis.fetch`, add routing headers or maintain process-global dispatcher state.
- Require pi-ai and pi-coding-agent 0.83.0 or newer.

Frames remain encoded as `text/event-stream` bytes so pi-ai's existing OpenAI Responses decoder still owns
request construction, retries, event parsing, errors, usage accounting and abort handling.

## Consequences

Transport state is request-local. Concurrent providers and extensions cannot interact through a shared
fetch dispatcher, and caller-supplied fetch behavior survives HTTP fallback.

The marker header, dispatcher lifecycle and settled callback are removed. Tests focus on injected-fetch
routing, fallback preservation and socket ownership.

Pi versions before 0.83.0 are no longer supported because they do not forward `SimpleStreamOptions.fetch`
to the OpenAI SDK.
