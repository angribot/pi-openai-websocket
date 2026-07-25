# pi-openai-websocket

WebSocket transport for the OpenAI Responses API in [pi](https://github.com/badlogic/pi-mono), for any
third-party provider.

pi ships WebSocket support only inside `openai-codex-responses`, a proprietary api third-party providers
cannot use. This extension gives the plain `openai-responses` api the same transport, using OpenAI's
publicly documented [WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode).

## Install

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
| `websocket-cached` | WebSocket, pooled connections, `previous_response_id` deltas |
| `auto` (default) | same as `websocket-cached` |

`websocketConnectTimeoutMs` bounds the handshake (default 15000, `0` disables). Stream idleness uses
pi's `httpIdleTimeoutMs`.

`/ws-stats` prints attempts, handshakes, reuse, delta vs full requests, fallbacks and any stripped
parameters.

## How it works

The OpenAI SDK takes `fetch` from `globalThis` when a client is constructed. The extension installs one
that speaks WebSocket, and dispatch is opt-in: each request carries a marker header, and anything without
a recognised marker is handed to the fetch that was already there. Other providers, other extensions and
pi's own traffic are untouched. Installs are reference counted, so the original is restored once the last
request finishes.

Everything above the transport stays pi-ai's: request construction, retries, error formatting, usage
accounting and abort handling. Frames are re-emitted as `text/event-stream` bytes so the SDK's own decoder
parses them, which is why the event pipeline is shared rather than reimplemented.

If the handshake fails, or the socket closes before any event, the request falls back to HTTP with one
warning. A failure after streaming has started surfaces as a normal stream error, because retrying is no
longer safe.

## Relay quirks

Some relays forward to a Codex-style backend that accepts a narrower parameter set over WebSocket than
over HTTP, and answer with `Unsupported parameter: <name>`. The named parameter is dropped, the request
is retried on a fresh socket, and the rejection is remembered for the rest of the process. Observed on
one relay: `max_output_tokens` and `prompt_cache_options`.

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

Anything uncertain sends the full input, which is always correct and merely more expensive.

## Not covered

- `azure-openai-responses`. Same protocol, different URL shape and Entra auth.
- `generate: false` prewarm.
- Codex v2 extensions: the `OpenAI-Beta` header, `client_metadata`, `x-codex-turn-state`,
  `codex.rate_limits`.
- Bun's proxy handling. Written for Node.
- Socket cleanup on `SIGINT` under `--print`, where pi does not run session teardown.

## Development

```
npm test                                      # unit tests, no credentials
node src/smoke.ts <provider> <model>          # one real turn
node src/smoke-continuation.ts <provider>     # three real turns, reports delta vs full
```

The smoke scripts read `models.json` and `auth.json` from the pi agent directory and never print
credentials.
