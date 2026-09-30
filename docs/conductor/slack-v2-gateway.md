# Slack v2 row gateway

Slack v2 is a durable channel transport for one existing Agent Deck row. Agent
Deck remains the only owner of that row's native agent conversation. The Slack
runtime does not create, resume, inspect, or start native agent threads.

## Configuration

A named conductor selects the backend and binds it to one immutable row, one
Slack channel, and an explicit sender allowlist:

```toml
[conductors.example]
backend = "slack-v2"

[conductors.example.slack_v2]
app_token = "${SLACK_APP_TOKEN}"
bot_token = "$SLACK_BOT_TOKEN"
channel_id = "$SLACK_CHANNEL"
allowed_user_ids = ["${SLACK_ALLOWED_USER}"]
row_instance_id = "$AGENT_DECK_ROW_ID"
row_binding_token = "$AGENT_DECK_ROW_BINDING"
```

The token, channel, sender, row ID, and binding values may be literals or exact
environment references. Partial expansion, recursion, missing values,
whitespace-padded values, and duplicate senders fail closed. Secrets are loaded
only after the per-conductor singleton lock is acquired.

Run the selected backend with:

```text
agent-deck [-p profile] conductor slack-v2 run <name>
```

There are no create, resume, executable, working-directory, model, or thread
options. The credential-free runtime manifest is version 2 and binds the exact
profile, immutable row ID and row-binding token, Slack identity and channel,
and allowed senders. A changed binding requires an explicit new configuration;
it is never inferred from a title or newer output.

## Durable row operation

For every eligible Slack message, the gateway commits ingress before Socket
Mode acknowledgement and assigns one durable attempt ID. The row driver passes
the message body only on standard input and invokes these public commands:

```text
agent-deck [-p profile] session send <immutable-row-id> --queue \
  --idempotency-key <gateway-attempt-id> \
  --expected-row-binding <opaque-token> --message-file - --json

agent-deck [-p profile] session send-status <send-id> --json
```

Loss of the submit response is handled by repeating the first command with the
same attempt ID and body. Once a send ID is committed, recovery uses only
`send-status`. The driver accepts bounded, strict JSON and rejects duplicate or
unknown fields, malformed values, identity changes, and a completion generation
that differs from the persisted accepted generation.

The gateway persists the send ID, monotonic operation state, accepted Codex
session and generation, and terminal error. Only `completed` may supply an
assistant outbox body. `refused`, `binding_changed`, `expired`, `indeterminate`,
and `result_unavailable` finish the turn with one body-free status receipt. The
Slack delivery boundary renders that receipt as fixed text; it never treats it
as an assistant reply or retries an uncertain row transport.

The adapter never uses `session output`, pane capture, session titles, rollout
or transcript files, private queue or marker state, or a later response.

## Ledger and migration

The channel ledger schema is version 4. It retains durable ingress, event
deduplication, sender authorization, FIFO ordering, one active turn, outbox
state, and egress attempt identity. Conversation records also bind the immutable
row and row-binding token; turn records retain the row operation and accepted
generation.

An older version 3 ledger is never reinterpreted as row-operation state. The
only automatic transition is deterministic archive-and-create for a pristine
ledger with no inbound events, turns, outbox records, cursor, sequence, or
uncertainty evidence. The original database is atomically renamed to the fixed
`.v3-archive` path before a new v4 database is created. Existing archive bytes,
unknown schemas, sidecars, or any recorded work cause refusal.

## Slack boundary

Socket Mode accepts only bounded `events_api` envelopes for the configured team,
channel, and human allowlist. Bot/self events, subtypes, files, wrong identities,
and channel-stream thread replies are filtered. Eligible events are acknowledged
only after the SQLite transaction commits; redelivery deduplicates by provider
event ID. Disconnect and reconnect do not create another row operation.

Outbound work is committed with turn completion. Before posting, `pending`
becomes `sending` with a durable attempt ID. A confirmed exact provider result
becomes `delivered`; a crash or ambiguous outcome becomes `uncertain` and is
never automatically posted again. Status receipts follow the same delivery
rules as assistant replies.

The runtime owns one Socket Mode connection, one FIFO work pump, and one
conductor lock. It does not install a service, change a provider manifest, stop
the legacy bridge, or alter the running Agent Deck row. Deployment and rollback
remain separate operator actions.
