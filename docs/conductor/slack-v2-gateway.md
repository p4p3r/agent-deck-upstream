# Slack v2 channel gateway: source contract

This document defines the durable gateway and its source-only Slack Socket Mode /
`chat.postMessage` boundary for a future conductor channel. It does not replace
the current bridge or configure a live Slack connection. `internal/slacknetwork`
can supply authenticated Socket Mode frames and top-level posts; the agent
driver separately claims turns and commits results.

## Two routing modes

Each conversation binds exactly one external channel and one conductor. A
conversation also has an explicit allowlist of external sender IDs. An event is
eligible only when its channel matches that binding and its sender is allowed.
Adapters must reject bot-authored events before intake to avoid reply loops. The
gateway treats external IDs as opaque values and never infers authorization from
message content.

| Mode | Eligible inbound event | Destination |
| --- | --- | --- |
| `channel_stream` | A top-level message in the dedicated channel | The conversation's single stream; assistant output is top-level with no thread ID |
| `thread_segments` | A mention in the shared channel outside an owned thread | A new segment, opened at a turn boundary |
| `thread_segments` | A message in the active segment's owned thread | That segment, even without a mention |

An unmentioned top-level shared-channel message has no destination. A message in
an unknown thread has no destination. A superseded thread cannot silently become
active again: intake returns a rejection with the active thread pointer, which a
later adapter can render as a short redirect reply. The pointer is an external
thread ID, not a copy of a message body.

## Turn and segment ownership

The ledger owns monotonically increasing turn numbers per conversation. At most
one turn is active per conversation. Intake records eligible inbound events once
by external event ID and makes them available for that conversation's next turn.
The reconciliation worker persists a stable local attempt ID before submitting
to the agent. The agent's authoritative turn ID is stored at the acceptance
callback. Repeating acceptance or completion with the same IDs is safe after a
restart; different IDs cannot take over an active turn.

For `thread_segments`, the first mention establishes an open segment and its
owned reply thread. A later mention while a turn is active establishes a pending
segment. Earlier accepted messages in the old segment remain ahead of it. Once a
segment is pending, new messages in the old thread are rejected with a pointer to
the pending thread, making that queue finite. At the first turn boundary after
the old queue drains, the pending segment becomes open and the former segment
becomes superseded. A message in the pending segment's thread is recorded for
its future turn; it never enters a turn in the old segment. Further mentions
while a segment is pending are rejected with its pointer, so retries cannot
create extra segments.

The gateway stores inbound message bodies because the eventual driver needs to
read them. That stored content is data, not an authorization input. Public errors,
diagnostics, and normal status results must identify records by opaque IDs and
states, without echoing message bodies or credentials. The database itself needs
the same local file protection as other conductor state.

## Durable boundary

The conversation, segment, event, turn, acceptance, and outbound records live in
one SQLite database. State transitions that assign a segment, reserve a turn,
accept ownership, finish a turn, or enqueue outbound work commit atomically. A
transport acknowledgment is not a database acknowledgment: the adapter should
acknowledge an external event only after the intake transaction commits.

Outbound items are an outbox, with a stable item ID and a delivery state. A
driver's turn completion and its outbound items commit together. Before posting,
the Slack boundary atomically changes `pending` to `sending` with a unique attempt
ID. Only the creator of that attempt may call the sender. A restart or concurrent
drain changes old `sending` work to `uncertain`, never back to `pending`; it cannot
automatically resend. An exact late success for the same attempt may still move
`sending` or `uncertain` to `delivered`. Delivery confirmation requires the bound
channel and a nonempty provider message ID; two items cannot claim one provider ID
in a conversation. The old direct `MarkDelivered` method cannot confirm an
unprepared item. Ambiguous post results and call errors leave an item uncertain
for external reconciliation. This is at-most-one automatic post attempt, not a
claim of exactly-once delivery or proof of provider receipt after a lost response.

On restart, the ledger retains unfinished turns and pending outbound work. The
conversation binds one immutable, private agent thread and a last completed
external turn cursor. A bound agent thread cannot be reused by another
conversation, and an external turn ID cannot be reused by another ledger turn
in the same conversation. Before an external start, the worker stores an attempt ID
and that cursor in the same transaction that claims the ledger turn. Only the
worker that created the attempt may submit it. A crash after opening a thread
but before binding it can leave an unused external thread; the bound winner is
the only thread used for subsequent turns.

For recovery, the driver reads the complete stored turn history for that bound
thread. An accepted attempt matches its exact external turn ID. A prepared
attempt with no recorded external ID may match exactly one turn after its saved
baseline cursor. This inference requires exclusive ownership of the private
agent thread; a driver unable to guarantee complete, authoritative history and
exclusive ownership must report uncertainty. Zero or multiple post-baseline
turns, a missing accepted ID, and failed or interrupted terminal status require
reconciliation. A prepared attempt is never submitted again just because no
external turn is visible. An in-progress accepted turn remains active for a
later inspection. Completion, outbound enqueue, and cursor advance commit in
one transaction. Schema version 3 fails closed on older gateway files; migration
of deployed data is outside this unreleased source slice.

## Slack boundary obligations and limits

`internal/slackgateway` handles only dedicated-channel `channel_stream`. It
accepts bounded, already-authenticated Socket Mode `events_api` envelopes and
ordinary, top-level text message events. A Socket Mode frame requires a payload
object and boolean `accepts_response_payload` (`false` for `events_api`). It
requires one coherent workspace identity
across all represented team fields, rejects duplicate JSON keys, and compares
opaque team, channel, and user IDs against explicit configuration. Bot/self,
subtype, thread, wrong-team/channel/user, unsupported events, and messages with
attachments or files are filtered; attached content is not silently discarded.
Valid filtered events are acknowledged once; malformed/oversized envelopes and
storage failures are not. For eligible events, the caller's acknowledgement
callback runs only after `Ingest` commits, so a failed ack followed by Slack
redelivery deduplicates on `event_id`. The boundary never starts an agent turn.

The outbound sender interface accepts channel and text but no thread parameter;
the future `chat.postMessage` implementation must omit `thread_ts`. It accepts
success only with `ok`, the exact bound channel, and a nonempty opaque `ts`.
Replies over 40,000 Unicode characters are marked uncertain without posting,
because Slack may truncate longer `text` values; they need explicit handling.
`internal/slacknetwork` owns a one-connection Socket Mode client and a sender
with separate app-level and bot tokens. It calls only the fixed
`apps.connections.open` and `chat.postMessage` methods, never follows HTTP
redirects, and never returns provider bodies, token-bearing ticket URLs, or
raw network errors. A connection ticket must use `wss` on `wss.slack.com` or
a single `wss-*` label under `slack.com`, with the documented `/link/` path,
nonempty `ticket`, and no unsafe port, userinfo, or fragment. The session
requires Slack's `hello`, bounds text frames, hands event envelopes to the
gateway's commit-before-ack callback, and writes its exact acknowledgment at
most once. It exits with a typed disconnect/reconnect result; it does not
automatically retry or make a second connection. The sender posts only JSON
`channel` and `text` and requires an exact-channel, nonempty-`ts` success.
Configuration plumbing, `thread_segments` transport, deployment, and legacy
bridge migration remain out of scope. The agent driver preserves turn ordering
and uses a fallible acceptance callback; on callback failure it stops with an
uncertain result.

Slack wire-field behavior follows the official [Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode/),
[Events API](https://docs.slack.dev/apis/events-api/), and
[`chat.postMessage`](https://docs.slack.dev/reference/methods/chat.postMessage/) documentation.

## Codex app-server client boundary

`internal/codexappserver` provides a stdio client for a future agent driver. It
launches `codex app-server --listen stdio://` using discrete argv entries and
performs `initialize`/`initialized` before a thread request. A new thread uses
`thread/start`; an existing one uses `thread/resume`. The returned `thread.id`
and `turn/start` response's `turn.id` are authoritative. A future adapter must
use `thread/read` with `includeTurns` to inspect stored turns without resuming;
it must record the turn ID at a fallible acceptance callback before relying on
`turn/completed`. It must
not derive them from Slack IDs, local counters, or the process ID.

One client serializes turns on one connection. A completed `agentMessage` item is
the source of final reply text; `turn/completed` supplies the terminal status.
Protocol and process errors contain classifications only, not prompt text,
server error messages, stderr, or raw events. Canceling an operation kills and
reaps the child. `internal/channelreconcile/codexdriver` implements the
provider-neutral driver with a separate, owned app-server subprocess for each
operation. It reads complete history with `thread/read includeTurns`, rejects
partial item views or ambiguous IDs, and closes the subprocess if acceptance
persistence fails. The recovery worker still owns ledger transitions; this
source adapter does not enqueue or deliver Slack replies itself.
