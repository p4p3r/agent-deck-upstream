package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

func TestAcceptanceOnlyReturnsAtTaskStartedBeforeCompletion(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	path := filepath.Join(home, "codex", "sessions", "2026", "09", "24", "rollout-test-thread-accept.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-old"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst := &session.Instance{ID: "instance-accept", Tool: "codex", CodexSessionID: "thread-accept"}
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	if err := guard.Prepare(inst.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := guard.RecordTransportOutcome(deliverySubmitted, time.Now()); err != nil {
		t.Fatal(err)
	}

	oldTimeout, oldInterval := codexAcceptedTurnPollTimeout, codexAcceptedTurnPollInterval
	codexAcceptedTurnPollTimeout = time.Second
	codexAcceptedTurnPollInterval = time.Millisecond
	defer func() {
		codexAcceptedTurnPollTimeout = oldTimeout
		codexAcceptedTurnPollInterval = oldInterval
	}()

	writeDone := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		writeDone <- appendCodexTurnStart(path, "turn-new")
	}()
	result := acceptedTurnOnlyVerdict(inst, deliverySubmitted, time.Now(), guard.fence, guard)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !result.Success || result.Acceptance != acceptanceOnlyAccepted || result.AcceptedTurn == nil {
		t.Fatalf("acceptance-only verdict = %#v", result)
	}
	if result.AcceptedTurn.TurnGeneration != "thread-accept:turn-new" {
		t.Fatalf("turn generation = %q", result.AcceptedTurn.TurnGeneration)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "task_complete") {
		t.Fatal("fixture completed before the acceptance-only result")
	}
	if _, err := session.ReconcileCodexSubmissionMarker(inst.ID, inst.CodexSessionID, result.AcceptedTurn.TurnGeneration); err != nil {
		t.Fatalf("accepted verdict did not durably resolve its marker: %v", err)
	}
}

func TestAcceptanceOnlyPromotesDeliveredOnlyAfterExactTurnStart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	path := filepath.Join(home, "codex", "sessions", "2026", "09", "24", "rollout-test-thread-delivered.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-old"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst := &session.Instance{ID: "instance-delivered", Tool: "codex", CodexSessionID: "thread-delivered"}
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	if err := guard.Prepare(inst.ID, time.Now()); err != nil {
		t.Fatal(err)
	}

	const message = "DELIVERED_WITHOUT_TRANSPORT_SUBMISSION_SIGNAL"
	mock := &mockSendRetryTarget{
		statuses: []string{"waiting", "waiting", "waiting"},
		panes:    []string{"codex> ", "codex> " + message, "codex> " + message, "codex> " + message},
	}
	tuning := testGuardTuning(sendRetryOptions{maxRetries: 2, checkDelay: 0, verifyDelivery: true})
	sendResult, err := performSend(inst, mock, message, false, tuning, "tmux", false, nil, nil, nil)
	if err != nil {
		t.Fatalf("performSend: %v", err)
	}
	if sendResult.delivery != deliveryDelivered {
		t.Fatalf("performSend delivery = %q, want %q", sendResult.delivery, deliveryDelivered)
	}
	if err := guard.RecordTransportOutcome(sendResult.delivery, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := appendCodexTurnStart(path, "turn-new"); err != nil {
		t.Fatal(err)
	}

	result := acceptedTurnOnlyVerdict(inst, sendResult.delivery, time.Now(), guard.fence, guard)
	if !result.Success || result.Acceptance != acceptanceOnlyAccepted || result.AcceptedTurn == nil {
		t.Fatalf("acceptance-only verdict = %#v", result)
	}
	if result.Delivery != deliverySubmitted || result.Submitted == nil || !*result.Submitted {
		t.Fatalf("promoted result is not canonical submitted acceptance: %#v", result)
	}
	if result.AcceptedTurn.TurnGeneration != "thread-delivered:turn-new" {
		t.Fatalf("turn generation = %q", result.AcceptedTurn.TurnGeneration)
	}
	if _, err := session.ReconcileCodexSubmissionMarker(inst.ID, inst.CodexSessionID, result.AcceptedTurn.TurnGeneration); err != nil {
		t.Fatalf("accepted delivered verdict did not clear uncertainty marker: %v", err)
	}
}

func TestAcceptanceOnlyDoesNotPromoteOtherTransportOutcomes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))

	tests := []struct {
		name     string
		delivery string
		retained bool
	}{
		{name: "no evidence", delivery: deliveryNoEvidence, retained: true},
		{name: "typed not submitted", delivery: deliveryTypedNotSubmitted, retained: true},
		{name: "menu open", delivery: deliveryMenuOpen, retained: true},
		{name: "pane gone", delivery: deliveryPaneGone, retained: true},
		{name: "line too long", delivery: deliveryLineTooLong, retained: false},
		{name: "queued", delivery: deliveryQueued, retained: true},
		{name: "queued socket", delivery: deliveryQueuedSocket, retained: true},
		{name: "socket write failed", delivery: deliverySocketWriteFailed, retained: true},
	}
	for n, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessionID := fmt.Sprintf("thread-negative-%d", n)
			path := filepath.Join(home, "codex", "sessions", "2026", "09", "24", "rollout-test-"+sessionID+".jsonl")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-old"}}`+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			inst := &session.Instance{ID: "instance-" + sessionID, Tool: "codex", CodexSessionID: sessionID}
			guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := guard.Prepare(inst.ID, time.Now()); err != nil {
				guard.Release()
				t.Fatal(err)
			}
			if err := guard.RecordTransportOutcome(tt.delivery, time.Now()); err != nil {
				guard.Release()
				t.Fatal(err)
			}
			if err := appendCodexTurnStart(path, "turn-new"); err != nil {
				guard.Release()
				t.Fatal(err)
			}

			result := acceptedTurnOnlyVerdict(inst, tt.delivery, time.Now(), guard.fence, guard)
			guard.Release()
			if result.Success || result.Acceptance != acceptanceOnlyIndeterminate || result.Delivery != tt.delivery {
				t.Fatalf("acceptance-only verdict = %#v", result)
			}
			_, reconcileErr := session.ReconcileCodexSubmissionMarker(inst.ID, sessionID, guard.fence.priorTurnGeneration)
			if tt.retained && reconcileErr == nil {
				t.Fatal("ambiguous transport outcome lost its uncertainty marker")
			}
			if !tt.retained && reconcileErr != nil {
				t.Fatalf("definitive non-delivery retained a marker: %v", reconcileErr)
			}
			if tt.retained {
				if _, err := session.ReconcileCodexSubmissionMarker(inst.ID, sessionID, sessionID+":turn-new"); err != nil {
					t.Fatalf("cleanup retained marker: %v", err)
				}
			}
		})
	}
}

func TestAcceptanceOnlyDeliveredRequiresRecordedExactProof(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))

	oldTimeout, oldInterval := codexAcceptedTurnPollTimeout, codexAcceptedTurnPollInterval
	codexAcceptedTurnPollTimeout = 5 * time.Millisecond
	codexAcceptedTurnPollInterval = time.Millisecond
	defer func() {
		codexAcceptedTurnPollTimeout = oldTimeout
		codexAcceptedTurnPollInterval = oldInterval
	}()

	t.Run("unchanged generation", func(t *testing.T) {
		inst, guard, _ := acceptanceOnlyTestGuard(t, home, "thread-delivered-unchanged")
		defer guard.Release()
		if err := guard.RecordTransportOutcome(deliveryDelivered, time.Now()); err != nil {
			t.Fatal(err)
		}
		result := acceptedTurnOnlyVerdict(inst, deliveryDelivered, time.Now(), guard.fence, guard)
		if result.Success || result.Acceptance != acceptanceOnlyIndeterminate {
			t.Fatalf("unchanged generation verdict = %#v", result)
		}
		if _, err := session.ReconcileCodexSubmissionMarker(inst.ID, inst.CodexSessionID, guard.fence.priorTurnGeneration); err == nil {
			t.Fatal("unchanged generation cleared the uncertainty marker")
		}
	})

	t.Run("transport outcome not recorded", func(t *testing.T) {
		inst, guard, path := acceptanceOnlyTestGuard(t, home, "thread-delivered-prepared")
		defer guard.Release()
		if err := appendCodexTurnStart(path, "turn-new"); err != nil {
			t.Fatal(err)
		}
		result := acceptedTurnOnlyVerdict(inst, deliveryDelivered, time.Now(), guard.fence, guard)
		if result.Success || result.Acceptance != acceptanceOnlyIndeterminate {
			t.Fatalf("prepared-only verdict = %#v", result)
		}
	})

	t.Run("changed session identity", func(t *testing.T) {
		inst, guard, _ := acceptanceOnlyTestGuard(t, home, "thread-delivered-original")
		defer guard.Release()
		if err := guard.RecordTransportOutcome(deliveryDelivered, time.Now()); err != nil {
			t.Fatal(err)
		}
		otherSessionID := "thread-delivered-unrelated"
		otherPath := filepath.Join(home, "codex", "sessions", "2026", "09", "24", "rollout-test-"+otherSessionID+".jsonl")
		if err := os.WriteFile(otherPath, []byte(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-new"}}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		inst.CodexSessionID = otherSessionID
		result := acceptedTurnOnlyVerdict(inst, deliveryDelivered, time.Now(), guard.fence, guard)
		if result.Success || result.Acceptance != acceptanceOnlyIndeterminate {
			t.Fatalf("unrelated session verdict = %#v", result)
		}
	})
}

func acceptanceOnlyTestGuard(t *testing.T, home, sessionID string) (*session.Instance, *codexAcceptanceGuard, string) {
	t.Helper()
	path := filepath.Join(home, "codex", "sessions", "2026", "09", "24", "rollout-test-"+sessionID+".jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-old"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst := &session.Instance{ID: "instance-" + sessionID, Tool: "codex", CodexSessionID: sessionID}
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Prepare(inst.ID, time.Now()); err != nil {
		guard.Release()
		t.Fatal(err)
	}
	return inst, guard, path
}

func TestAcceptanceOnlyResultIsBoundedAndBodyFree(t *testing.T) {
	const sensitive = "SENSITIVE prompt /private/worktree terminal-response"
	receipt := &codexAcceptedTurnReceipt{
		ReceiptID:      "11111111-2222-4333-8444-555555555555",
		InstanceID:     "instance-123",
		CodexSessionID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		TurnGeneration: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee:99999999-8888-4777-8666-555555555555",
		AcceptedAt:     "2026-09-24T12:00:00.123456789Z",
	}
	result, err := newAcceptanceOnlySuccessResult(receipt)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := marshalAcceptanceOnlyResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > acceptanceOnlyResultMaxBytes {
		t.Fatalf("result length = %d, max = %d", len(raw), acceptanceOnlyResultMaxBytes)
	}
	if strings.Contains(string(raw), sensitive) || strings.Contains(string(raw), "message") ||
		strings.Contains(string(raw), "content") || strings.Contains(string(raw), "title") ||
		strings.Contains(string(raw), "cwd") || strings.Contains(string(raw), "path") ||
		strings.Contains(string(raw), "transport") || strings.Contains(string(raw), "error") {
		t.Fatalf("acceptance-only result crossed the body/privacy boundary: %s", raw)
	}

	var top map[string]any
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	wantTop := []string{"acceptance", "accepted_turn", "accepted_turn_kind", "delivery", "instance_id", "schema_version", "submitted", "success"}
	if got := sortedMapKeys(top); !reflect.DeepEqual(got, wantTop) {
		t.Fatalf("top-level keys = %v, want exact allowlist %v", got, wantTop)
	}
	accepted, ok := top["accepted_turn"].(map[string]any)
	if !ok {
		t.Fatalf("accepted_turn = %#v", top["accepted_turn"])
	}
	wantReceipt := []string{"accepted_at", "codex_session_id", "instance_id", "receipt_id", "turn_generation"}
	if got := sortedMapKeys(accepted); !reflect.DeepEqual(got, wantReceipt) {
		t.Fatalf("accepted-turn keys = %v, want exact allowlist %v", got, wantReceipt)
	}

	for name, mutate := range map[string]func(*codexAcceptedTurnReceipt){
		"oversized": func(r *codexAcceptedTurnReceipt) {
			r.TurnGeneration = r.CodexSessionID + ":" + strings.Repeat("a", acceptanceOnlyOpaqueIDMaxBytes+1)
		},
		"wrong session prefix": func(r *codexAcceptedTurnReceipt) { r.TurnGeneration = "other:turn" },
		"path-like identity":   func(r *codexAcceptedTurnReceipt) { r.CodexSessionID = "../private" },
		"invalid timestamp":    func(r *codexAcceptedTurnReceipt) { r.AcceptedAt = "not-a-time" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := *receipt
			mutate(&invalid)
			if _, err := newAcceptanceOnlySuccessResult(&invalid); err == nil {
				t.Fatal("invalid accepted-turn identity crossed the result boundary")
			}
		})
	}
}

func TestAcceptanceOnlyTimeoutIsIndeterminateAndRetainsMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	path := filepath.Join(home, "codex", "sessions", "2026", "09", "24", "rollout-test-thread-timeout.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-only"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst := &session.Instance{ID: "instance-timeout", Tool: "codex", CodexSessionID: "thread-timeout"}
	guard, err := acquireCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Prepare(inst.ID, time.Now()); err != nil {
		guard.Release()
		t.Fatal(err)
	}
	if err := guard.RecordTransportOutcome(deliverySubmitted, time.Now()); err != nil {
		guard.Release()
		t.Fatal(err)
	}

	oldTimeout, oldInterval := codexAcceptedTurnPollTimeout, codexAcceptedTurnPollInterval
	codexAcceptedTurnPollTimeout = 5 * time.Millisecond
	codexAcceptedTurnPollInterval = time.Millisecond
	defer func() {
		codexAcceptedTurnPollTimeout = oldTimeout
		codexAcceptedTurnPollInterval = oldInterval
	}()
	result := acceptedTurnOnlyVerdict(inst, deliverySubmitted, time.Now(), guard.fence, guard)
	guard.Release()
	if result.Success || result.Acceptance != acceptanceOnlyIndeterminate || result.Code != acceptanceOnlyCodeIndeterminate {
		t.Fatalf("timeout verdict = %#v", result)
	}
	if result.Delivery != deliverySubmitted || result.Submitted == nil || !*result.Submitted {
		t.Fatalf("timeout lost bounded submission classification: %#v", result)
	}
	if _, err := session.ReconcileCodexSubmissionMarker(inst.ID, inst.CodexSessionID, guard.fence.priorTurnGeneration); err == nil {
		t.Fatal("timeout cleared the ambiguity marker and made an unsafe resend possible")
	}
}

func TestAcceptanceOnlyRefusesUnsupportedTargetsBeforeGuard(t *testing.T) {
	tests := []struct {
		name string
		inst *session.Instance
		code string
	}{
		{name: "unsupported", inst: &session.Instance{Tool: "claude"}, code: acceptanceOnlyCodeUnsupported},
		{name: "remote", inst: &session.Instance{Tool: "codex", SSHHost: "remote"}, code: acceptanceOnlyCodeUnavailable},
		{name: "sandbox", inst: &session.Instance{Tool: "codex", Sandbox: &session.SandboxConfig{Enabled: true}}, code: acceptanceOnlyCodeUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := acceptanceOnlyPreconditionCode(tt.inst); got != tt.code {
				t.Fatalf("precondition code = %q, want %q", got, tt.code)
			}
		})
	}
}

func TestAcceptanceOnlyCLIRefusesUnsupportedBeforeDelivery(t *testing.T) {
	const helper = "AGENT_DECK_ACCEPTANCE_ONLY_REFUSAL"
	if target := os.Getenv(helper); target != "" {
		profile := "acceptance_only_refusal_" + target
		inst := session.NewInstanceWithTool("private title", t.TempDir(), "codex")
		switch target {
		case "unsupported":
			inst.Tool = "claude"
		case "remote":
			inst.SSHHost = "private.example"
		case "sandbox":
			inst.Sandbox = &session.SandboxConfig{Enabled: true}
		}
		storage, err := session.NewStorageWithProfile(profile)
		if err != nil {
			t.Fatal(err)
		}
		if err := storage.SaveWithGroups([]*session.Instance{inst}, nil); err != nil {
			t.Fatal(err)
		}
		if err := storage.Close(); err != nil {
			t.Fatal(err)
		}
		handleSessionSend(profile, []string{inst.ID, "SENSITIVE prompt body", "--acceptance-only"})
		os.Exit(99)
	}

	for _, tt := range []struct {
		target, code string
	}{
		{target: "unsupported", code: acceptanceOnlyCodeUnsupported},
		{target: "remote", code: acceptanceOnlyCodeUnavailable},
		{target: "sandbox", code: acceptanceOnlyCodeUnavailable},
	} {
		t.Run(tt.target, func(t *testing.T) {
			home := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestAcceptanceOnlyCLIRefusesUnsupportedBeforeDelivery$")
			cmd.Env = append(os.Environ(), helper+"="+tt.target, "HOME="+home,
				"XDG_CONFIG_HOME="+filepath.Join(home, "config"),
				"XDG_DATA_HOME="+filepath.Join(home, "data"),
				"XDG_CACHE_HOME="+filepath.Join(home, "cache"),
				"CODEX_HOME="+filepath.Join(home, "codex"))
			raw, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("exit = %v, want 1; output: %s", err, raw)
			}
			if strings.Contains(string(raw), "SENSITIVE") || strings.Contains(string(raw), "private") {
				t.Fatalf("pre-send refusal leaked private input: %s", raw)
			}
			if len(raw) > acceptanceOnlyResultMaxBytes {
				t.Fatalf("refusal length = %d, max = %d", len(raw), acceptanceOnlyResultMaxBytes)
			}
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatalf("decode refusal fields: %v; output: %s", err, raw)
			}
			if got, want := sortedMapKeys(fields), []string{"acceptance", "code", "schema_version", "success"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("refusal keys = %v, want exact allowlist %v", got, want)
			}
			var result acceptanceOnlyResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("decode refusal: %v; output: %s", err, raw)
			}
			if result.Success || result.Acceptance != acceptanceOnlyNotAccepted || result.Code != tt.code {
				t.Fatalf("refusal = %#v", result)
			}
		})
	}
}

func TestAcceptanceOnlyMainDiagnosticsStayInsideSafeResultBoundary(t *testing.T) {
	t.Run("flag parse error", func(t *testing.T) {
		home := t.TempDir()
		stdout, stderr, code := runAgentDeck(t, home,
			"session", "send", "missing", "SENSITIVE prompt body",
			"--acceptance-only=PRIVATE_INVALID_VALUE")
		assertAcceptanceOnlyProcessFailure(
			t, stdout, stderr, code, acceptanceOnlyCodeInvalidOptions,
			"PRIVATE_INVALID_VALUE", "SENSITIVE prompt body",
		)
	})

	t.Run("store root divergence warning", func(t *testing.T) {
		home, _, strayDB := strayXDGFixture(t)
		stdout, stderr, code := runAgentDeck(t, home,
			"session", "send", "ch_support_test-seed-a", "SENSITIVE prompt body",
			"--acceptance-only")
		assertAcceptanceOnlyProcessFailure(
			t, stdout, stderr, code, acceptanceOnlyCodeUnsupported,
			filepath.Dir(filepath.Dir(strayDB)), "SENSITIVE prompt body", "legacy-a",
		)
	})

	t.Run("inferred profile fallback warning", func(t *testing.T) {
		home := t.TempDir()
		seedStoreCLI(t, filepath.Join(home, ".local", "share", "agent-deck"), "default", 1)
		privateConfigDir := filepath.Join(home, "PRIVATE-profile-root", ".claude-PRIVATE")
		stdout, stderr, code := runAgentDeckEnv(t, home, "", []string{
			"AGENTDECK_PROFILE=",
			"CLAUDE_CONFIG_DIR=" + privateConfigDir,
		}, "session", "send", "default-seed-a", "SENSITIVE prompt body", "--acceptance-only")
		assertAcceptanceOnlyProcessFailure(
			t, stdout, stderr, code, acceptanceOnlyCodeUnsupported,
			privateConfigDir, "PRIVATE", "SENSITIVE prompt body", "legacy-a",
		)
	})
}

func TestAcceptanceOnlyQueueBoundaryRejectsIncompatibleOptions(t *testing.T) {
	const (
		body    = "synthetic acceptance request delta-61"
		key     = "synthetic-acceptance-key-delta-61"
		content = "synthetic completed result delta-61"
	)
	for _, tt := range []struct {
		name      string
		flags     []string
		completed bool
		fileInput bool
	}{
		{name: "ordinary queue", flags: []string{"--queue"}},
		{name: "ordinary JSON queue", flags: []string{"--queue", "--json"}},
		{name: "correlated queue", flags: []string{"--queue", "--json", "--idempotency-key", key, "--expected-row-binding", "BINDING"}},
		{name: "completed correlated retry", flags: []string{"--queue", "--json", "--idempotency-key", key, "--expected-row-binding", "BINDING"}, completed: true},
		{name: "queued unreadable input", flags: []string{"--queue"}, fileInput: true},
		{name: "correlated unreadable input", flags: []string{"--queue", "--json", "--idempotency-key", key, "--expected-row-binding", "BINDING"}, fileInput: true},
		{name: "idempotency key alone", flags: []string{"--idempotency-key", key}},
		{name: "row binding alone", flags: []string{"--expected-row-binding", "BINDING"}},
		{name: "queue worker", flags: []string{"--queue-worker"}},
		{name: "correlated worker operation", flags: []string{"--correlated-operation", key}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
			t.Setenv("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
			t.Setenv("XDG_CACHE_HOME", filepath.Join(home, ".cache"))
			t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
			inst := session.NewInstanceWithTool("synthetic acceptance row", home, "codex")
			storage, err := session.NewStorageWithProfile("ch_support_test")
			if err != nil {
				t.Fatal(err)
			}
			if err := storage.SaveWithGroups([]*session.Instance{inst}, nil); err != nil {
				t.Fatal(err)
			}
			queueDir := sendQueueDir(storage)
			if err := storage.Close(); err != nil {
				t.Fatal(err)
			}
			binding := session.RowBindingToken(inst)
			if tt.completed {
				now := time.Now().UTC()
				record, created, err := sendqueue.EnqueueCorrelated(queueDir, sendqueue.CorrelatedRequest{
					SessionID: inst.ID, IdempotencyKey: key, RowBindingToken: binding, Message: body, Now: now,
				})
				if err != nil || !created {
					t.Fatalf("seed correlated request: created=%v err=%v", created, err)
				}
				for _, state := range []string{sendqueue.OperationPreparing, sendqueue.OperationAccepted, sendqueue.OperationCompleted} {
					if _, err := sendqueue.Update(queueDir, record.SendID, now, func(current *sendqueue.Record) {
						current.OperationState = state
						if state == sendqueue.OperationAccepted {
							current.AcceptedTurn = &sendqueue.AcceptedTurn{
								ReceiptID: "synthetic-receipt", InstanceID: inst.ID, CodexSessionID: "synthetic-thread",
								TurnGeneration: "synthetic-thread:turn-one", AcceptedAt: now.Format(time.RFC3339Nano),
							}
						}
						if state == sendqueue.OperationCompleted {
							current.Completion = &sendqueue.Completion{TurnGeneration: current.AcceptedTurn.TurnGeneration}
							current.Content = content
						}
					}); err != nil {
						t.Fatal(err)
					}
				}
				loaded, err := sendqueue.Load(queueDir, record.SendID)
				if err != nil || loaded.OperationState != sendqueue.OperationCompleted || loaded.Content != content {
					t.Fatalf("completed fixture unavailable: record=%+v err=%v", loaded, err)
				}
			}
			before := acceptanceOnlyQueueSnapshot(t, queueDir)
			if tt.completed && len(before) == 0 {
				t.Fatal("completed queue snapshot is empty")
			}

			binDir := t.TempDir()
			effects := filepath.Join(home, "synthetic-effects")
			shim := "#!/bin/sh\nprintf '%s\\n' \"$0 $*\" >> \"$ACCEPTANCE_ONLY_TEST_EFFECTS\"\nexit 1\n"
			for _, name := range []string{"tmux", "codex", "claude", "gemini", "agent-deck"} {
				if err := os.WriteFile(filepath.Join(binDir, name), []byte(shim), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"session", "send", inst.ID, body, "--acceptance-only"}
			missingInput := filepath.Join(home, "synthetic-missing-input")
			if tt.fileInput {
				args = []string{"session", "send", inst.ID, "--message-file", missingInput, "--acceptance-only"}
			}
			for _, value := range tt.flags {
				if value == "BINDING" {
					value = binding
				}
				args = append(args, value)
			}
			stdout, stderr, code := runAgentDeckEnv(t, home, "", []string{
				"PATH=" + binDir, "CODEX_HOME=" + filepath.Join(home, "codex"), "ACCEPTANCE_ONLY_TEST_EFFECTS=" + effects,
			}, args...)
			assertAcceptanceOnlyProcessFailure(t, stdout, stderr, code, acceptanceOnlyCodeInvalidOptions,
				body, key, binding, content, inst.ID, inst.Title, home, missingInput)
			want := "{\"schema_version\":1,\"success\":false,\"acceptance\":\"not_accepted\",\"code\":\"INVALID_OPTIONS\"}\n"
			if stdout != want {
				t.Fatalf("stdout = %q, want exactly one failure envelope %q", stdout, want)
			}
			if after := acceptanceOnlyQueueSnapshot(t, queueDir); !reflect.DeepEqual(after, before) {
				t.Fatal("rejected acceptance-only request changed durable queue or worker state")
			}
			if _, err := os.Stat(effects); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected acceptance-only request invoked a transport tool: %v", err)
			}
		})
	}
}

func TestAcceptanceOnlyQueueBoundaryKeepsJSONSynchronous(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	inst := session.NewInstanceWithTool("synthetic acceptance JSON row", home, "codex")
	storage, err := session.NewStorageWithProfile("ch_support_test")
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.SaveWithGroups([]*session.Instance{inst}, nil); err != nil {
		t.Fatal(err)
	}
	queueDir := sendQueueDir(storage)
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runAgentDeckEnv(t, home, "", []string{"PATH=" + t.TempDir()}, "session", "send", inst.ID,
		"synthetic acceptance JSON request", "--acceptance-only", "--json")
	assertAcceptanceOnlyProcessFailure(t, stdout, stderr, code, acceptanceOnlyCodeTargetUnavailable,
		"synthetic acceptance JSON request", home)
	if len(acceptanceOnlyQueueSnapshot(t, queueDir)) != 0 {
		t.Fatal("acceptance-only JSON request created durable queue state")
	}
}

func TestAcceptanceOnlyQueueBoundaryHelpDescribesIncompatibilities(t *testing.T) {
	stdout, stderr, code := runAgentDeck(t, t.TempDir(), "session", "send", "--help")
	if code != 0 || stderr != "" {
		t.Fatalf("send help: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	for _, want := range []string{
		"Incompatible with --wait, --stream, --no-wait, --draft, -q, --queue, and --queue-worker.",
		"Also incompatible with --idempotency-key, --expected-row-binding, and --correlated-operation.",
		"--json is optional and does not queue an acceptance-only send.",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("acceptance-only help omitted %q", want)
		}
	}
}

func acceptanceOnlyQueueSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrNotExist) && path == dir {
			return nil
		}
		if err != nil {
			return err
		}
		files[path] = info.Mode().String() + info.ModTime().String()
		if !info.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[path] += string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func assertAcceptanceOnlyProcessFailure(
	t *testing.T,
	stdout, stderr string,
	exitCode int,
	wantCode string,
	forbidden ...string,
) {
	t.Helper()
	if exitCode != 1 {
		t.Fatalf("exit = %d, want 1; stdout: %s\nstderr: %s", exitCode, stdout, stderr)
	}
	if stderr != "" {
		t.Fatalf("acceptance-only stderr must be empty, got: %q", stderr)
	}
	if len(stdout) > acceptanceOnlyResultMaxBytes {
		t.Fatalf("result length = %d, max = %d", len(stdout), acceptanceOnlyResultMaxBytes)
	}
	for _, value := range forbidden {
		if strings.Contains(stdout, value) || strings.Contains(stderr, value) {
			t.Fatalf("acceptance-only diagnostics leaked %q:\nstdout: %s\nstderr: %s", value, stdout, stderr)
		}
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(stdout), &fields); err != nil {
		t.Fatalf("decode acceptance-only failure: %v; stdout: %s", err, stdout)
	}
	wantKeys := []string{"acceptance", "code", "schema_version", "success"}
	if got := sortedMapKeys(fields); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("failure keys = %v, want exact allowlist %v", got, wantKeys)
	}
	var result acceptanceOnlyResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode acceptance-only result: %v", err)
	}
	if result.Success || result.Acceptance != acceptanceOnlyNotAccepted || result.Code != wantCode {
		t.Fatalf("failure = %#v, want code %q", result, wantCode)
	}
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
