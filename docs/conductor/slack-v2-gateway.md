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
app_id = "$SLACK_APP_ID"
team_id = "$SLACK_TEAM_ID"
channel_id = "$SLACK_CHANNEL"
allowed_user_ids = ["${SLACK_ALLOWED_USER}"]
row_instance_id = "$AGENT_DECK_ROW_ID"
row_binding_token = "$AGENT_DECK_ROW_BINDING"
# Optional, whole hours. Omitted values use 24, 168, and 2160.
retention_delivered_hours = 24
retention_uncertain_hours = 168
retention_metadata_hours = 2160
# Optional. Omit to create no health/control listener.
control_socket = "/run/user/1000/agent-deck/slack-v2-control.sock"
```

The token, app, team, channel, sender, row ID, and binding values may be literals or exact
environment references. Partial expansion, recursion, missing values,
whitespace-padded values, and duplicate senders fail closed. Secrets are loaded
only after the per-conductor singleton lock is acquired.
Retention overrides must be nonnegative whole hours and resolve in the order
delivered ≤ uncertain ≤ metadata; zero selects that field's default.

Set `SLACK_DECK_SPOOL_KEY` to canonical padded standard base64 encoding of
exactly 32 high-entropy random bytes, unique to this box and separate from
Slack tokens. Missing, malformed, or wrong keys fail closed. Keep the key
available across restarts and migration; losing it makes accepted content and
exact delivery resolvers unreadable. Rotate it only with a separate planned
data transition. The spool uses AES-256-GCM, a 96-bit random nonce per record,
domain-separated keys, and authenticated record identity. Limit one root key
to at most 2^20 record encryptions before rotation. At that limit the random
nonce collision probability is below 2^-57 per domain; the bound is an
operational limit, not an automatic key rotation mechanism.

Run the selected backend with:

```text
agent-deck [-p profile] conductor slack-v2 run <name>
```

There are no create, resume, executable, working-directory, model, or thread
options. The runtime manifest is version 3 and stores keyed opaque aliases of
the profile, immutable row ID and binding token, Slack app, team, bot, channel,
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

The gateway persists the send ID, monotonic operation state, aliases of the
accepted Codex session and generation, and a fixed terminal error. Only `completed` may supply an
assistant outbox body. `refused`, `binding_changed`, `expired`, `indeterminate`,
and `result_unavailable` finish the turn with one body-free status receipt. The
Slack delivery boundary renders that receipt as fixed text; it never treats it
as an assistant reply or retries an uncertain row transport.

The adapter never uses `session output`, pane capture, session titles, rollout
or transcript files, private queue or marker state, or a later response.

## Ledger and migration

The channel ledger schema is version 5. It retains durable ingress, event
deduplication, sender authorization, FIFO ordering, one active turn, outbox
state, and egress attempt identity. It stores keyed opaque aliases for external
IDs and bindings. Accepted request bodies, reply bodies, exact channel bindings,
and confirmed provider message IDs live in separate owner-only authenticated
encrypted records. Neither the ledger nor the manifest contains exact values.

An existing body-bearing v4 ledger and version 2 manifest migrate under the
conductor singleton lock. Migration first copies the original database, WAL,
and SHM byte for byte into `migration-v4-evidence/`, stages a complete v5 ledger
and encrypted spool, then retires the live old database files into that
owner-only evidence directory and activates v5. It moves the old manifest into
the archive only after v5 activation, then writes the aliased v3 manifest.
Fixed stage markers allow restart after each durable boundary. Accepted, active, completed,
delivery-pending, and uncertain ownership is retained; unsupported legacy
routing state fails closed before activation. **The raw v4 evidence directory
is the explicit migration exception to the no-plaintext-artifact rule.** It is
not a convenience backup: retain it for review, include it deliberately in
privacy scans, and never publish it. Migration does not delete old evidence.

The runner calls bounded `Store.Prune` at startup and then at least hourly;
large backlogs continue in bounded batches. Each pass checks sealed records
against ledger references under the write lock and removes unreferenced
records left by an interrupted insert. Defaults
are 24 hours for encrypted content and exact delivered-message resolvers after
confirmed delivery, seven days for terminal uncertain content, and 90 days
for ordinary delivered terminal metadata. A caller may set positive durations
that keep metadata at least as long as uncertain content and uncertain content
at least as long as delivered content.
Queued, active, in-progress, and delivery-pending work is never expired.
Uncertain ledger ownership and state remain after ciphertext expiry, so an
ambiguous post cannot become sendable again. Expiry is first recorded in SQL,
then files are removed, then SQL records completed deletion; another call
finishes an interrupted operation.

An older version 3 ledger is never reinterpreted as row-operation state. The
only automatic transition is deterministic archive-and-create for a pristine
ledger with no inbound events, turns, outbox records, cursor, sequence, or
uncertainty evidence. The original database is atomically renamed to the fixed
`.v3-archive` path before a new v5 database is created. Existing archive bytes,
unknown schemas, sidecars, or any recorded work cause refusal.

## Slack boundary

Socket Mode accepts only bounded `events_api` envelopes for the exact configured
app, team, private channel, and human allowlist. Bot/self events, subtypes,
files, attachments, wrong identities, and channel-stream thread replies are
filtered. The inclusive 32 KiB message limit is measured over decoded UTF-8
text before durable acceptance. Eligible events are acknowledged
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

Runner failures expose fixed, body-free causes. Configuration, protocol,
identity, and link-safety failures exit 78; unavailable, timeout, and unknown
observations exit 75 for a bounded external retry policy. Cancellation exits
0. No provider response, body, token, or exact ID enters the cause string.

## Optional health/control socket

`control_socket` enables a Linux owner-only Unix socket; it is absent by
default. The absolute path must have a nonsymlink owner-owned `0700` parent.
The socket and its namespace lock are `0600`. Startup rejects unsafe existing
objects and active listeners. It replaces a same-owner stale socket only while
holding the namespace lock and removes its socket at shutdown only if the path
still names the inode it created. Every accepted peer must have the same uid as
reported by `SO_PEERCRED`.

One connection is a strict newline-delimited JSON v1 exchange:

1. `{"version":1,"type":"hello","nonce":"<32 lower hex>"}`
2. one `pong` with the same nonce and exactly the objects below
3. `{"version":1,"type":"close","nonce":"<same nonce>"}`
4. `{"version":1,"type":"closed","nonce":"<same nonce>"}`

Frames are strict UTF-8 and at most 16 KiB. Missing, duplicate, unknown, early,
or extra fields/frames and version, type, or nonce mismatches close the
connection without a response. Accept, read, write, and status work are
bounded. The control path only samples memory and a read-only body-free ledger
projection; it cannot start a row operation, retry delivery, mutate the ledger,
or restart a process.

The `pong` contains exactly `version`, `type`, `nonce`, `identity`, `runner`,
`pump`, `backlog`, `egress`, and `conductor`. Identity contains the SHA-256 of
the exact running Linux image (`/proc/self/exe`), the effective configuration
digest described below, and the existing keyed row-binding alias. No exact
provider identifier is returned. Runner is `running` with a null exit code or
`exited` with exit code 0–255, plus terminal and degraded booleans.

Backlog is `pending` while accepted ingress, an active turn, or undelivered or
uncertain outbox ownership remains; its age is measured from the oldest
accepted ledger timestamp. Otherwise it is `empty`. Conductor is `working`
only while the ledger has an active turn, with age from that turn's accepted
timestamp; otherwise it is `idle`. Egress is `uncertain` for a durable
uncertain send, `unknown` while a send is in flight or the bounded projection
cannot complete, and otherwise `clear`.

Pump evidence is in memory. A known empty backlog reports `idle`. With pending
work, a successful pump pass is `fresh` below 10 seconds old, `stale` from 10
seconds, and `stalled` from 30 seconds. Missing evidence or a failed projection
is `unknown`. Age values are non-null only for pending/working and
fresh/stale/stalled states. All ages are whole seconds clamped to
315,360,000.

When the runner exits retryably with code 75, the process keeps the control
socket and read-only store available for at most 90 seconds or one completed
handshake that observed a fully recoverable exit-75 state, whichever comes
first. Terminal, degraded,
and unknown observations are never converted to recoverable health. Shutdown
joins the sole provider socket and pump before closing the control socket,
driver, store, and conductor lease.

The configuration digest is SHA-256 over the ASCII prefix
`agent-deck/slack-v2/effective-config/v1` followed by a NUL byte and compact Go
JSON for these fields in order: version 1; conversation and conductor aliases;
profile; exact row ID and binding; exact app, team, and channel IDs; sorted
allowed users; app and bot tokens; the 32-byte spool key (standard padded
base64 under Go JSON); effective delivered, uncertain, and metadata retention
seconds; and the absolute control socket. The digest exposes none of those
values. Omitted retention values are normalized to 86,400, 604,800, and
7,776,000 seconds before hashing.
