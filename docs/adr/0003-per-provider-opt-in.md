# ADR 0003: Opt in per provider, not per api

Status: accepted
Date: 2026-07-25

## Context

Three registration seams exist for reaching models configured with `"api": "openai-responses"`.

**Global api override.** `registerApiProvider({ api: "openai-responses", ... })` mutates a module-level
`Map` in pi-ai's compat layer. `Map.set` overwrites silently, and for providers configured in
`models.json` the composer does reach `getApiProvider(model.api)` on every stream call, so the override
takes effect. It takes effect for *every* provider using that api, including ones that cannot speak
WebSocket. It is also erased by `resetApiProviders()`, which `/reload` calls before extensions reload, and
`unregisterApiProviders(sourceId)` deletes the id outright rather than restoring the builtin.

**A new api id.** `openai-responses-ws` works, since `Api` is `KnownApi | (string & {})`. It forces users
to edit `models.json`, and models declared under a custom api no longer match pi's built-in `transport`
setting semantics.

**Per-provider registration.** `registerProvider(name, { api: "openai-responses", streamSimple })` merges
into an existing `models.json` provider without throwing, keeps its models and baseUrl, and sits above
the base provider in the composer's precedence, so it also works for builtin provider names. Both
`stream` and `streamSimple` route through it. It is re-applied automatically after `/reload`, because the
extension factory re-runs.

Endpoint support is not uniform enough to assume: of the providers surveyed, only OpenAI, Azure, xAI and
the Codex relay ecosystem serve this protocol at all, and no Chinese model vendor does.

## Decision

Per-provider registration, driven by a list the user maintains:

```json
{ "openaiWebsocket": { "providers": ["my-provider"] } }
```

Providers not listed are untouched. Transport selection continues to follow pi's own `transport` setting,
whose four values keep the meanings the Codex path already gave them.

## Consequences

A provider that cannot speak WebSocket is unaffected, and enabling one is a single line rather than a
config rewrite. `/settings → transport: sse` still disables the transport, because `options.transport` is
honoured.

Users have to name their providers, which is the point: this is opt-in for an endpoint capability that
cannot be detected without trying it.

pi has no per-extension settings namespace, so the list is read through the exported `SettingsManager`.
`transport` is absent from non-agent calls such as compaction, so the configured value is cached at
startup and used when the option is missing, rather than assuming `auto`.
