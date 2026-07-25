# ADR 0004: Bind continuation state to the socket

Status: accepted
Date: 2026-07-25

## Context

`previous_response_id` lets a follow-up request send only the new input items instead of the whole
conversation. OpenAI documents the mechanism as a connection-local, single-entry cache, and reports
`previous_response_not_found` for an id it does not hold.

Two measurements changed the design.

**A relay does not honour the id.** Three turns were sent on one connection: A established a fact, B
chained to A and added a second fact, C chained to **A** again and asked about the fact only B
introduced. C answered from B's state. A fourth turn, on a fresh connection, referencing an id from the
first connection, did not recognise it. So that endpoint treats `previous_response_id` as "whatever this
connection produced last" and validates nothing.

The consequence is specific: if continuation state is keyed by session and model, and anything else
advances the connection in between, a delta splices onto the wrong context, with no error. pi does issue
other requests inside a session, compaction among them.

**The server's echo of its own output is not usable as the baseline.** The prefix check needs the items
the server contributed, in the shape the *next* request's input will carry. The same relay reports
`response.completed` with `output: []` while returning all 30-odd other fields of the response object.
Even where the echo is populated, it need not byte-match the client's own conversion of that turn.

## Decision

Continuation state lives on the pooled socket object, never in a map keyed by session, provider or model.
A delta is sent only when:

1. every field other than the input is byte-identical to the previous request on that socket,
2. the new input begins with exactly the previous input plus that response's items,
3. and those items are marked final.

The baseline items are derived from the finished assistant message through pi-ai’s own conversion,
reached by capturing a request body from `onPayload` and throwing before the network, not from the
server’s echo. They are filled in after the stream resolves, which is later than the socket’s return to
the pool, so an explicit `complete` flag gates the delta: an unfinalised baseline sends the full input.

A socket carries one request at a time. A busy socket is never handed out; a concurrent request opens its
own connection. Sockets are dropped after 5 minutes idle or 55 minutes of age, ahead of the documented
60 minute server cap. Those limits are enforced when a socket is taken or returned, and by a sweep timer
for the cases where neither happens: an idle session, or a response body abandoned without being read or
cancelled, whose socket would otherwise stay checked out until the session ends. A busy socket is reclaimed
only on age, never on idleness, because a long streaming turn is legitimately busy for minutes.

A rejected `previous_response_id` is recovered from rather than surfaced: see ADR 0005.

`transport: "websocket"` skips the pool entirely and always sends the full input. `websocket-cached` and
`auto` use it.

## Consequences

Correct under both the documented semantics and the relay's looser ones, because "this connection's most
recent response" and "the id we recorded" are then the same thing.

Every uncertainty degrades to sending the full input, which is correct and merely more expensive. The
failure mode is a lost optimisation, never a wrong answer, which is the right way round for something that
would otherwise corrupt context silently.

Pooling is a correctness requirement here, not a performance tweak, so the busy flag and the age caps
cannot be dropped as an optimisation later.

Measured on three turns against the relay: turn 1 full, turns 2 and 3 delta on a reused socket, with the
fact from turn 1 still answerable on turn 3.
