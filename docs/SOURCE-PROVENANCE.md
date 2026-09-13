# Source provenance and reproducible local build

This branch is a source replacement for a locally installed Agent Deck binary
and conductor bridge. Runtime artifacts were used only as behavioral evidence;
they are not the source of record.

## Official base

The base is the official `asheshgoplani/agent-deck` v1.16.8 release:

| Identity | Value |
|---|---|
| Official repository | `https://github.com/asheshgoplani/agent-deck` |
| Tag | `v1.16.8` |
| Annotated tag object | `b4b032fba2127294af14186eec43999eb16d5699` |
| Peeled commit | `9c884abdba47def96b2b811c87978def95ef79cf` |
| Commit tree | `c3b51ccbbaa79f179ca8b000420d1fa20cb369e9` |
| Commit timestamp | `2026-09-13T16:57:54+02:00` (`1789311474`) |
| Deterministic `git archive` SHA-256 | `9d697081cee16c3cffa1b6efbe7c2ea8f0865ed5f214b8a86f86de31eb10a2bd` |

The archive digest above is for:

```sh
git archive --format=tar --prefix=agent-deck-v1.16.8/ \
  9c884abdba47def96b2b811c87978def95ef79cf | sha256sum
```

An independent `git ls-remote --tags` against the official URL returned the
same tag object and peeled commit. The annotated tag has no cryptographic
signature, so this establishes agreement with the official public ref, not a
signed-tag chain of custody.

The machine-readable record at
`scripts/testdata/phase1-official-provenance.json` pins the commit, tree, and
deterministic archive recipe/digest. Its ordinary offline Go test reconstructs
only those local Git facts; it does not pretend to repeat a network or
cryptographic release verification.

In a shallow checkout that does not contain the pinned commit object, the
ordinary test still validates the record and explicitly skips archive replay.
Fetch `refs/tags/v1.16.8` from the official repository before running the
explicit `git archive` command above; the local provenance verification command
and expected digest are unchanged.

Separately, the root incident audit independently downloaded the official
Linux amd64 v1.16.8 asset on 2026-09-13 and found both its bytes and the official
`checksums.txt` entry equal SHA-256
`26470bf567efcf456c1b514d7478d31bbbb0afedd46a129a97d51a3c2fc95028`.
The release was published at `2026-09-13T15:15:13Z` and was still the latest
release at that audit's recheck. The downloaded
`checksums.txt` itself had SHA-256
`7c678625c465fe818d94a0871bb95329425e1c8e3d02baa6015c7495e9e7b77d`.
It also successfully enforced repository, source digest, tag ref, and signer
workflow policy with:

```sh
gh attestation verify agent-deck_1.16.8_linux_amd64.tar.gz \
  --repo asheshgoplani/agent-deck \
  --source-digest 9c884abdba47def96b2b811c87978def95ef79cf \
  --source-ref refs/tags/v1.16.8 \
  --signer-workflow asheshgoplani/agent-deck/.github/workflows/release.yml
```

The verified predicate type was `https://slsa.dev/provenance/v1`, and its
subject named that exact asset and digest. This is recorded external audit
evidence, not a capability claimed by the offline fixture. The release commit's
GitHub verified signature likewise does not make the annotated tag a signed tag.

## Runtime evidence and disposition

The inspected local artifacts reported:

| Artifact | Observed identity |
|---|---|
| `~/.local/bin/agent-deck` | `Agent Deck v1.15.0`; SHA-256 `6b7eb3cff804f433a533a12a6215cc08d310bbc01f3d817c7717710ac24e73ab` |
| installed `conductor/bridge.py` | SHA-256 `12df097d7a9fbee9ed4ba9ac481d8ac1070c756e9f271d4aa1b5215bd8d85c25` |

The binary's Go build info says module version `(devel)` and contains no VCS
revision. Its bytes therefore cannot prove a source commit. Symbols and strings
were used only to corroborate the required behaviors, which are implemented and
tested in this repository.

The installed bridge differs from both official v1.15.0 and the v1.16.8 base in
four unified-diff hunks containing six logical edits:

The comparison base for v1.15.0 was annotated tag object
`172637c377bbda7a8493567a804166f5b339308e`, peeled to commit
`bf50689893053c6dd33a29b21e12eb36e251d94b`. The relevant upstream bridge file
is byte-identical from that commit through v1.16.8 (SHA-256
`be1466b4f02bb4a36e1ce7078e1e121902c1be09009a7a8dbf80ea12188a2358`),
so the same four hunks apply to both comparisons.

| Live edit | Source disposition |
|---|---|
| Resolve Slack `channel_id` through `_resolve_secret` | Retained with behavioral config tests |
| Resolve every Slack `allowed_user_ids` entry without filtering missing values | Retained; an unresolved configured allowlist stays non-empty and denies all real users |
| Select the conductor runtime from `meta.json` | Retained for all upstream-supported conductor agents: Claude, Codex, and Hermes |
| Document the Codex freshness-timeout form | Retained beside the classifier |
| Treat `Codex output freshness timeout` as a delivered, still-running turn | Retained with async-reply tests |
| Use the runtime selector when recreating a missing conductor | Retained with command-argument tests |

The v1.16.8 base already supplies prerequisites that this patch deliberately
reuses:

| Upstream behavior | Exact source | Existing upstream regression coverage |
|---|---|---|
| One canonical embedded bridge source | `internal/session/conductor_bridge_embed.go` (`go:embed conductor_bridge.py`) | `internal/session/conductor_bridge_embed_test.go`; `conductor/tests/conftest.py` loads that same file |
| Account/runtime-aware Codex home resolution | `internal/session/instance.go`: `codexHomeForCommand`, `getCodexHomeDir` | `internal/session/issue1922_codex_account_home_test.go`; `internal/session/issue1929_codex_account_resume_test.go` |
| Codex subagent rollouts cannot usurp the user-thread binding | `internal/session/codex_subagent_gate.go` | `internal/session/codex_subagent_gate_test.go` |
| Supported conductor runtime definitions and legacy Claude default | `internal/session/conductor.go`: `conductorAgentSpecs`, `ConductorMeta.GetAgent` | `internal/session/conductor_test.go`: `TestSetupConductorWithAgent_Codex`, `TestLoadConductorMeta_EmptyAgentDefaultsToClaude`; `internal/session/hermes_conductor_test.go` |
| Automatic install/restart suppression for Go tests, CI, script/test markers, and non-terminal TUI paths | `internal/update/suppress.go`: `AutoUpdateSuppressed`, `TUIAutoUpdateSuppressed`; call sites in `cmd/agent-deck/main.go` and `internal/ui/update_auto.go` | `internal/update/suppress_test.go`; `cmd/agent-deck/auto_update_suppress_test.go`; `internal/ui/update_auto_test.go` |
| Model-readable Agent Deck session identity, persisted opt-out, and harness command wiring | `internal/session/identity_injection.go`, `identity_injection_persist.go`; persistence in `storage.go`; launch wiring in `instance.go`, `cross_harness_switch.go`, and `cmd/agent-deck/main.go` | `internal/session/identity_injection_test.go` |

The identity injection supplies an Agent Deck instance/session context to the
model and exports `AGENTDECK_IDENTITY_FILE`; for Codex it adds a
`developer_instructions` override. It does not discover, bind, or prove
ownership of `CODEX_SESSION_ID`, and therefore does not replace the exact
rollout/session correlation protections below.

Structured Codex final-answer extraction, Codex freshness correlation, the
full-timeout fresh-output wait, and status-line-shaped Codex busy recognition
were not present at the base. They are source changes in this branch, with
regression tests adjacent to the affected packages.

Structured Codex reads also require a bound authoritative session ID and a
local, non-sandboxed instance. They never satisfy an SSH or sandbox response
from a coincidentally matching rollout in the controller's local Codex home,
and they do not guess ownership by selecting the newest local rollout.

The fail-closed correlation has an intentional compatibility cost: legacy
unphased Codex assistant records are not authoritative final answers, and a
local session without a bound `CODEX_SESSION_ID` cannot fall back to terminal
prose for `session send --wait`. The command first gives its own pane a chance
to supply the ID, then reloads profile state within the same absolute timeout;
if neither can establish ownership, it returns an actionable error. This can
reject output from older Codex formats, but it cannot report another or stale
turn as the reply to the new send.

The same correlation boundary applies to `session output`: missing, remote,
sandboxed, ambiguous, or colliding Codex rollout identity returns a structured
error and never degrades to uncorrelated terminal prose.

Slack thread affinity and a control ledger are intentionally excluded from this
provenance workstream.

## Reproducible build

After the local source changes are committed, build only from a clean checkout:

```sh
git status --porcelain=v1 -uall
git rev-parse HEAD
make build-reproducible
./build/agent-deck version
go version -m ./build/agent-deck
sha256sum ./build/agent-deck
make verify-reproducible-build
```

`make build-reproducible` refuses a dirty tree, derives version and source
revision from `HEAD`, uses the commit timestamp as `SOURCE_DATE_EPOCH`, pins
`GOTOOLCHAIN=go1.25.13` from `go.mod`, disables CGO and implicit VCS stamping,
removes host paths and the Go build ID, and resolves modules read-only against
`go.sum`. The compiled CSS and all other Go embed inputs are committed bytes in
that exact source tree; Tailwind authoring inputs are not consulted by the Go
build. Run `make css-verify` when changing those authoring inputs.

The embedded display version uses SemVer build metadata, for example
`1.16.8+p4p3r.9c884abdba47`, so updater comparison treats it as the base release
rather than as a pre-release older than v1.16.8. The full `SourceCommit` remains
the authoritative exact source identity.

`make verify-reproducible-build` extracts the same commit into two independent
source directories, gives each build a distinct `GOCACHE` and `GOTMPDIR`, and
requires byte equality.

Plain `go build` is not a provenance build. In particular, automatic Go VCS
metadata can describe an enclosing checkout rather than a managed nested
worktree, and a dirty checkout can only stamp a base revision plus a modified
bit rather than a commit that identifies the source bytes. It also omits this
project's linker-injected `SourceCommit`. Only the clean-tree
`build-reproducible` and `verify-reproducible-build` targets define the supported
local provenance contract.

The first `agent-deck version` line remains `Agent Deck v<version>` for existing
parsers. Reproducible and release builds add a second `Source commit:` line from
the linker-injected exact commit. Release builds inject the same value through
GoReleaser's `.FullCommit` metadata.

Because this candidate is intentionally left uncommitted for orchestration
review, its final source commit does not exist yet and the clean-tree build gate
must fail until that single commit is created.
