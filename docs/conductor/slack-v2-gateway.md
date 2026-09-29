# Slack v2 channel gateway: source contract

This document defines a provider-neutral routing and persistence foundation for a
future Slack conductor channel. It does not replace the current bridge or connect
to Slack. A transport adapter will translate Slack event IDs, channel IDs, thread
IDs, mentions, and message text into the gateway's inputs; an agent driver will
claim turns and commit results. Neither adapter is part of this slice.

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
The agent driver must explicitly accept a turn using a stable acceptance ID.
Repeating the same acceptance or completion operation must be safe after an
adapter retry or process restart; a different acceptance cannot take over an
active turn.

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
driver's turn completion and its outbound items must commit together. The
transport later sends each item and marks it delivered; retry after a crash may
send an item again, so the future transport needs an idempotency or reconciliation
strategy for the external API. This slice promises durable, deduplicated enqueue,
not exactly-once network delivery.

On restart, the ledger retains unfinished turns and pending outbound work. It
must not assume an unfinished turn failed merely because a process died. A
driver must inspect its acceptance ID and reconcile the prior attempt before
starting another turn. Schema mismatch must fail closed rather than discard a
database containing unrecoverable conversation state.

## Adapter obligations and limits

The later Slack adapter will post `channel_stream` output with no thread ID and
route `thread_segments` output to its segment's root thread. It will filter
bot/self events, bind sender allowlists, and render superseded thread pointers.
It must deduplicate Slack retry deliveries by Slack event ID before invoking a
conductor. The later agent driver will supply stable acceptance IDs, preserve
turn ordering, and write outbound replies through the ledger. This contract
leaves network connection, tokens, the Codex driver, configuration,
migration from the existing bridge, and live deployment for separate work.

## Codex app-server client boundary

`internal/codexappserver` provides a stdio client for a future agent driver. It
launches `codex app-server --listen stdio://` using discrete argv entries and
performs `initialize`/`initialized` before a thread request. A new thread uses
`thread/start`; an existing one uses `thread/resume`. The returned `thread.id`
and `turn/start` response's `turn.id` are authoritative. The driver must record
these IDs at the acceptance callback before relying on `turn/completed`; it must
not derive them from Slack IDs, local counters, or the process ID.

One client serializes turns on one connection. A completed `agentMessage` item is
the source of final reply text; `turn/completed` supplies the terminal status.
Protocol and process errors contain classifications only, not prompt text,
server error messages, stderr, or raw events. Canceling an operation kills and
reaps the child, so a later driver must reconcile any accepted turn using its
durable ledger state before starting another attempt. The client itself does not
perform that reconciliation or enqueue Slack replies.
