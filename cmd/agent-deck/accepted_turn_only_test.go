package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
