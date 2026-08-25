# pi-openai-websocket

WebSocket transport for the OpenAI Responses API in [pi](https://github.com/earendil-works/pi), for
opted-in third-party providers.

pi ships WebSocket support only inside `openai-codex-responses`, a proprietary api third-party providers
cannot use. This extension gives the plain `openai-responses` api the current Codex WebSocket transport
while retaining HTTP/SSE when an endpoint cannot use it. The core request and event shapes follow OpenAI's
[WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode).

## Install

Requires pi and pi-ai 0.84.0 or newer.

```
pi install git:github.com/angribot/pi-openai-websocket
```

Or clone into `~/.pi/agent/extensions/pi-openai-websocket/`.

## Configure

Name the providers that should use WebSocket in `~/.pi/agent/settings.json`:

```json
{
  "openaiWebsocket": { "providers": ["my-provider"] }
}
```

Each name must already exist in `~/.pi/agent/models.json` with `"api": "openai-responses"`. Nothing
happens for providers not listed.

Transport selection follows pi's own `transport` setting, so `/settings` still controls it:

| `transport` | Behaviour |
|---|---|
| `sse` | WebSocket disabled, unchanged HTTP streaming |
| `websocket` | WebSocket, one connection per request, full input every time |
| `websocket-cached` | Named sessions pool compatible connections and may send `previous_response_id` deltas; calls without a session use a fresh connection |
| `auto` (default) | Same as `websocket-cached` |

`websocketConnectTimeoutMs` bounds the handshake (default 15000, `0` disables). Stream idleness uses
pi's `httpIdleTimeoutMs`.

`/ws-stats` prints attempts, handshakes, reuse, delta vs full requests, fallbacks and any stripped
parameters.

## How it works

Pi-ai 0.84 passes a per-request `fetch` to the OpenAI SDK after resolving the request's endpoint,
credentials, headers and sampling parameters. The extension supplies a fetch that speaks WebSocket,
without changing `globalThis.fetch`. Other providers, other extensions and pi's own traffic are
untouched. If WebSocket is unavailable, the request is delegated unchanged to the fetch supplied by the
caller, or to `globalThis.fetch` when no custom fetch was supplied.

Everything above the transport stays pi-ai's: request construction, retries, error formatting, usage
accounting and abort handling. Frames are re-emitted as `text/event-stream` bytes so the SDK's own decoder
parses them, which is why the event pipeline is shared rather than reimplemented.

A pooled connection is reusable only within the same named session, provider and model, and only when the
final WebSocket endpoint and effective handshake headers still identify the same account and route. The
identity is stored as an opaque digest, so credentials and routing headers do not appear in pool keys or
stats. Credential, endpoint or routing changes safely open a fresh connection.

Every WebSocket handshake sends `OpenAI-Beta: responses_websockets=2026-02-06`. WebSocket frames omit the
HTTP-only `stream` field; the original body remains intact for HTTP fallback. Requests with
`background: true` go directly over HTTP. Codex rate-limit events are consumed out of band because the plain
Responses decoder has no consumer for them.

If the handshake fails, or the socket closes before any event, the request falls back to HTTP with one
warning. That named session then stays on HTTP. A failure after streaming has started surfaces as a normal
stream error and moves later requests in the session to HTTP; API errors and user aborts do not. Calls
without a session id do not share fallback state.

## Relay quirks

Some relays forward to a Codex-style backend that accepts a narrower parameter set over WebSocket than
over HTTP, and answer with `Unsupported parameter: <name>`. The named parameter is dropped, the request
is retried on a fresh socket, and the rejection is remembered only for that connection identity and
request model. Observed on one relay: `max_output_tokens` and `prompt_cache_options`.

If the endpoint rejects the `previous_response_id` a delta chained onto, usually
`previous_response_not_found`, the continuation is forgotten and the conversation is resent whole on the
same socket, once. If a connection reaches its server limit, a fresh socket is opened and tried once.
These recoveries only happen before any response event has streamed. Parameter and stale-continuation
recoveries are counted in `/ws-stats`. See ADR 0005 and ADR 0006.

## Continuation safety

`previous_response_id` lets a follow-up request send only the new input items. A delta only goes out
when:

- every field other than the input is byte-identical to the previous request,
- the new input begins with exactly the previous input plus the items the previous response produced,
- and those items are known to be final.

Continuation state is bound to the socket, never to a session or model key. One measured relay ignores
`previous_response_id` and simply reuses whatever that connection produced last: chaining to an older id
still answered from the newest state, and an id from another connection was not recognised at all. Socket
bound state is correct under both that behaviour and OpenAI's documented one. A socket carries one
request at a time; concurrent requests get their own connections.

The baseline is derived from the finished assistant message through the same conversion the next request
uses, not from the server's echo of its own output, because relays may report that empty. pi's extension
loader does not expose that conversion to extensions, so it is obtained by letting pi-ai build a request
body and capturing it from `onPayload`, which costs one body build and no network.

Only named sessions using `auto` or `websocket-cached` can reuse a socket and send a delta. Calls without a
session ID and calls using explicit `websocket` always open a fresh connection and send the full input.
Anything uncertain also sends the full input, which is always correct and merely more expensive.

## Not covered

- `azure-openai-responses`. Same protocol, different URL shape and Entra auth.
- `generate: false` prewarm.
- Codex product state: `client_metadata`, `x-codex-turn-state` and rate-limit presentation.
- Bun's proxy handling. Written for Node.
- Socket cleanup on `SIGINT` under `--print`, where pi does not run session teardown.

## Development

```
npm install                                   # installs the exact pinned development dependencies
npm run lint                                  # oxlint
npm test                                      # unit tests, no credentials
npm run typecheck                             # TypeScript contract check
node src/smoke.ts <provider> <model>          # one real turn
node src/smoke-continuation.ts <provider>     # three real turns, reports delta vs full
```

The smoke scripts read `models.json` and `auth.json` from the pi agent directory and never print
credentials.
