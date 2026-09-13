package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/session"
)

const phase1FreshCodexID = "039c9ffa-c9d6-7be1-9e1c-527080e68953"

func phase1CodexRolloutPath(t *testing.T, home string) string {
	t.Helper()
	dir := filepath.Join(home, "sessions", "2026", "09", "13")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create Codex sessions dir: %v", err)
	}
	return filepath.Join(dir, "rollout-2026-09-13T12-00-00-"+phase1FreshCodexID+".jsonl")
}

func phase1CodexFinal(timestamp, text string) string {
	return `{"timestamp":"` + timestamp + `","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"` + text + `"}]}}` + "\n"
}

func TestWaitForFreshOutputCodexWaitsForLateCorrelatedFinal(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	path := phase1CodexRolloutPath(t, home)
	if err := os.WriteFile(
		path,
		[]byte(phase1CodexFinal("2026-09-13T11:59:00Z", "STALE")),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	setFastFreshOutputConfig(t, 900*time.Millisecond)
	inst := &session.Instance{
		ID:             "codex-late-output",
		Tool:           "codex",
		CodexSessionID: phase1FreshCodexID,
	}
	sentAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	writeDone := make(chan error, 1)
	go func() {
		time.Sleep(350 * time.Millisecond)
		writeDone <- os.WriteFile(
			path,
			[]byte(
				phase1CodexFinal("2026-09-13T11:59:00Z", "STALE")+
					phase1CodexFinal("2026-09-13T12:00:01Z", "FRESH LATE ANSWER"),
			),
			0o600,
		)
	}()

	started := time.Now()
	got, err := waitForFreshOutput(inst, sentAt, nil, 0)
	elapsed := time.Since(started)
	if writeErr := <-writeDone; writeErr != nil {
		t.Fatalf("append late Codex answer: %v", writeErr)
	}
	if err != nil {
		t.Fatalf("waitForFreshOutput() error: %v", err)
	}
	if elapsed < 300*time.Millisecond {
		t.Fatalf("returned in %v before the correlated final_answer was written", elapsed)
	}
	if got.Content != "FRESH LATE ANSWER" || got.SessionID != phase1FreshCodexID {
		t.Fatalf("returned wrong Codex turn: %+v", got)
	}
}

func TestWaitForFreshOutputCodexTimeoutNeverReturnsStaleTurn(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	path := phase1CodexRolloutPath(t, home)
	if err := os.WriteFile(
		path,
		[]byte(phase1CodexFinal("2026-09-13T11:59:00Z", "STALE")),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	setFastFreshOutputConfig(t, 250*time.Millisecond)
	inst := &session.Instance{
		ID:             "codex-stale-output",
		Tool:           "codex",
		CodexSessionID: phase1FreshCodexID,
	}
	got, err := waitForFreshOutput(
		inst,
		time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC),
		nil,
		0,
	)
	if err == nil {
		t.Fatalf("stale Codex turn reported as this send's reply: %+v", got)
	}
	if got != nil {
		t.Fatalf("Codex freshness timeout returned stale response: %+v", got)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "codex output freshness timeout") {
		t.Fatalf("timeout error %q is not recognizable by the conductor bridge", err)
	}
}

func TestCodexSendWaitUsesOneFullCLITimeoutBudget(t *testing.T) {
	// This is the same release-safety gate consumed by agent-cli-updates.nix.
	// The behavioral tests above prove the waiter uses its whole budget; this
	// source wiring assertion prevents the CLI from silently feeding it the old
	// fixed five-second fallback instead of the operator's --timeout value.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	source, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "session_cmd.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"waitDeadline = time.Now().Add(*timeout)",
		"freshWait = *timeout",
		`remainingWaitBudget(waitDeadline, "Codex final-answer correlation")`,
		`remainingWaitBudget(waitDeadline, "Codex final-answer correlation retry")`,
		"retryCodexFreshOutputWithinDeadline(",
	} {
		if !strings.Contains(string(source), required) {
			t.Fatalf("Codex wait budget wiring is missing %q", required)
		}
	}
}

func TestRetryCodexFreshOutputUsesRemainingBudgetWhenReloadCannotImproveTarget(t *testing.T) {
	original := &session.Instance{
		ID:             "codex-original",
		Tool:           "codex",
		CodexSessionID: phase1FreshCodexID,
	}
	originalPeers := []*session.Instance{original}
	sentAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name              string
		load              func(string) (*session.Storage, []*session.Instance, []*session.GroupData, error)
		resolveResult     *session.Instance
		wantResolveCalled bool
	}{
		{
			name: "reload failure",
			load: func(string) (*session.Storage, []*session.Instance, []*session.GroupData, error) {
				return nil, nil, nil, errors.New("synthetic reload failure")
			},
		},
		{
			name: "nil resolution",
			load: func(string) (*session.Storage, []*session.Instance, []*session.GroupData, error) {
				return nil, []*session.Instance{{ID: "unrelated", Tool: "codex"}}, nil, nil
			},
			wantResolveCalled: true,
		},
		{
			name: "reload resolves non-Codex target",
			load: func(string) (*session.Storage, []*session.Instance, []*session.GroupData, error) {
				return nil, []*session.Instance{{ID: "claude-reloaded", Tool: "claude"}}, nil, nil
			},
			resolveResult:     &session.Instance{ID: "claude-reloaded", Tool: "claude"},
			wantResolveCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolveCalled := false
			waitCalled := false
			// The helper is entered only after the capped first attempt. A
			// long synthetic remainder proves that this second attempt is not
			// itself capped to five seconds, without making the test sleep.
			deadline := time.Now().Add(30 * time.Second)
			got, err := retryCodexFreshOutputWithinDeadline(
				"isolated", "codex-original", original, sentAt, originalPeers, deadline,
				codexFreshOutputRetryDeps{
					load: tt.load,
					resolve: func(string, []*session.Instance) (*session.Instance, string, string) {
						resolveCalled = true
						return tt.resolveResult, "not found", ErrCodeNotFound
					},
					wait: func(inst *session.Instance, gotSentAt time.Time, peers []*session.Instance, retryWait time.Duration) (*session.ResponseOutput, error) {
						waitCalled = true
						if inst != original || len(peers) != 1 || peers[0] != original {
							t.Fatalf("retry did not preserve original snapshot: inst=%p peers=%v", inst, peers)
						}
						if !gotSentAt.Equal(sentAt) {
							t.Fatalf("sentAt changed across retry: got %s want %s", gotSentAt, sentAt)
						}
						if retryWait <= 5*time.Second || retryWait > 30*time.Second {
							t.Fatalf("retry budget = %s, want the remaining caller budget without a new cap", retryWait)
						}
						return &session.ResponseOutput{Content: "FRESH AFTER INITIAL CAP", SessionID: phase1FreshCodexID}, nil
					},
				},
			)
			if err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if !waitCalled || resolveCalled != tt.wantResolveCalled {
				t.Fatalf("dependency calls: wait=%v resolve=%v, want wait=true resolve=%v", waitCalled, resolveCalled, tt.wantResolveCalled)
			}
			if got.Content != "FRESH AFTER INITIAL CAP" || got.SessionID != phase1FreshCodexID {
				t.Fatalf("late correlated response was not returned: %+v", got)
			}
		})
	}
}

func TestRetryCodexFreshOutputDoesNotStartAfterAbsoluteDeadline(t *testing.T) {
	original := &session.Instance{ID: "codex-expired", Tool: "codex", CodexSessionID: phase1FreshCodexID}
	called := false
	got, err := retryCodexFreshOutputWithinDeadline(
		"isolated", original.ID, original, time.Now(), []*session.Instance{original},
		time.Now().Add(-time.Millisecond),
		codexFreshOutputRetryDeps{
			load: func(string) (*session.Storage, []*session.Instance, []*session.GroupData, error) {
				called = true
				return nil, nil, nil, nil
			},
			resolve: func(string, []*session.Instance) (*session.Instance, string, string) {
				called = true
				return original, "", ""
			},
			wait: func(*session.Instance, time.Time, []*session.Instance, time.Duration) (*session.ResponseOutput, error) {
				called = true
				return &session.ResponseOutput{Content: "must not be returned"}, nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "overall --timeout budget exhausted") {
		t.Fatalf("expired aggregate deadline did not fail clearly: got %+v, %v", got, err)
	}
	if got != nil || called {
		t.Fatalf("expired deadline performed retry work: got %+v called=%v", got, called)
	}
}

func TestRemainingWaitBudgetNeverRestartsDeadline(t *testing.T) {
	deadline := time.Now().Add(200 * time.Millisecond)
	first, err := remainingWaitBudget(deadline, "first")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	second, err := remainingWaitBudget(deadline, "retry")
	if err != nil {
		t.Fatal(err)
	}
	if second >= first || second > 180*time.Millisecond {
		t.Fatalf("retry budget restarted: first=%s second=%s", first, second)
	}
}
