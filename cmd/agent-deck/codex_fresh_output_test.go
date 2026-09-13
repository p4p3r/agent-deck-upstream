package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/session"
)

func codexFinalAnswerRecord(timestamp, text string) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":%q}]}}`+"\n", timestamp, text)
}

func TestWaitForFreshOutputCodex(t *testing.T) {
	for _, scenario := range []string{"fresh", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			home := t.TempDir()
			codexHome := filepath.Join(home, ".codex")
			t.Setenv("HOME", home)
			t.Setenv("CODEX_HOME", codexHome)
			session.ClearUserConfigCache()
			t.Cleanup(session.ClearUserConfigCache)

			sessionID := "019f1234-5678-7abc-8def-0123456789ab"
			dir := filepath.Join(codexHome, "sessions", "2026", "09", "13")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "rollout-2026-09-13T12-00-00-"+sessionID+".jsonl")
			if err := os.WriteFile(path, []byte(codexFinalAnswerRecord("2026-09-13T11:59:00Z", "old answer")), 0o600); err != nil {
				t.Fatal(err)
			}

			setFastFreshOutputConfig(t, 400*time.Millisecond)
			sentAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
			if scenario == "fresh" {
				done := make(chan error, 1)
				go func() {
					time.Sleep(100 * time.Millisecond)
					f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
					if err != nil {
						done <- err
						return
					}
					_, err = f.WriteString(codexFinalAnswerRecord(sentAt.Add(time.Second).Format(time.RFC3339Nano), "fresh answer"))
					closeErr := f.Close()
					if err == nil {
						err = closeErr
					}
					done <- err
				}()
				t.Cleanup(func() {
					if err := <-done; err != nil {
						t.Error(err)
					}
				})
			}

			inst := &session.Instance{ID: "codex-fresh", Tool: "codex", CodexSessionID: sessionID}
			got, err := waitForFreshOutput(inst, sentAt, nil, 0)
			if scenario == "fresh" {
				if err != nil {
					t.Fatal(err)
				}
				if got.Content != "fresh answer" {
					t.Fatalf("got stale answer: %+v", got)
				}
			} else if err == nil || got != nil || !strings.Contains(err.Error(), "Codex output freshness timeout") {
				t.Fatalf("missing fresh reply must fail, got %+v, %v", got, err)
			}
		})
	}
}

func TestWaitForFreshOutputCodexHonorsExplicitWait(t *testing.T) {
	home := t.TempDir()
	codexHome := filepath.Join(home, ".codex")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	session.ClearUserConfigCache()
	t.Cleanup(session.ClearUserConfigCache)

	sessionID := "019f9999-8888-7777-8666-555544443333"
	dir := filepath.Join(codexHome, "sessions", "2026", "09", "13")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "rollout-2026-09-13T12-00-00-"+sessionID+".jsonl")
	if err := os.WriteFile(path, []byte(codexFinalAnswerRecord("2026-09-13T11:59:00Z", "old answer")), 0o600); err != nil {
		t.Fatal(err)
	}

	wait := 120 * time.Millisecond
	started := time.Now()
	got, err := waitForFreshOutput(
		&session.Instance{ID: "codex-explicit-wait", Tool: "codex", CodexSessionID: sessionID},
		time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC), nil, wait,
	)
	elapsed := time.Since(started)
	if err == nil || got != nil || !strings.Contains(err.Error(), "(120ms)") {
		t.Fatalf("explicit freshness wait not reported: got %+v, %v", got, err)
	}
	if elapsed < wait || elapsed > time.Second {
		t.Fatalf("explicit wait elapsed %s, want >= %s and < 1s", elapsed, wait)
	}
}

func TestWaitForFreshOutputCodexRefusesLocalSessionIDCollision(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	sessionID := "019f9999-8888-7777-8666-555544443333"
	inst := &session.Instance{ID: "codex-a", Tool: "codex", CodexSessionID: sessionID}
	peer := &session.Instance{ID: "codex-b", Tool: "codex", CodexSessionID: sessionID, Status: session.StatusRunning}

	got, err := waitForFreshOutput(inst, time.Now(), []*session.Instance{inst, peer}, time.Second)
	if err == nil || got != nil || !strings.Contains(err.Error(), "colliding Codex rollout") {
		t.Fatalf("colliding local Codex IDs must fail closed, got %+v, %v", got, err)
	}
}

func TestWaitForFreshOutputCodexWithoutIDFailsPromptly(t *testing.T) {
	started := time.Now()
	got, err := waitForFreshOutput(&session.Instance{ID: "codex-no-id", Tool: "codex"}, time.Now(), nil, time.Minute)
	if err == nil || got != nil || !strings.Contains(err.Error(), "authoritative Codex session ID is unavailable") {
		t.Fatalf("missing Codex correlation must be actionable, got %+v, %v", got, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("missing Codex ID consumed the wait budget: %s", elapsed)
	}
}
