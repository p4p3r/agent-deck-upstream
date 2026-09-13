package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/tmux"
)

func writeCodexOutputRollout(t *testing.T, codexHome, sessionID string, events ...map[string]any) {
	t.Helper()
	dir := filepath.Join(codexHome, "sessions", "2026", "09", "13")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(line)
		body.WriteByte('\n')
	}
	path := filepath.Join(dir, "rollout-2026-09-13T12-00-00-"+sessionID+".jsonl")
	if err := os.WriteFile(path, []byte(body.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

func codexAssistantEvent(timestamp, phase string, content ...map[string]any) map[string]any {
	return map[string]any{
		"timestamp": timestamp,
		"type":      "response_item",
		"payload": map[string]any{
			"type":    "message",
			"role":    "assistant",
			"phase":   phase,
			"content": content,
		},
	}
}

func TestGetLastResponseCodexUsesStructuredFinalAnswer(t *testing.T) {
	home := t.TempDir()
	codexHome := filepath.Join(home, ".codex")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	ClearUserConfigCache()
	t.Cleanup(ClearUserConfigCache)

	sessionID := "019f1111-2222-7333-8444-555566667777"
	writeCodexOutputRollout(t, codexHome, sessionID,
		map[string]any{"timestamp": "2026-09-13T12:00:00Z", "type": "session_meta", "payload": map[string]any{"id": sessionID}},
		codexAssistantEvent("2026-09-13T12:00:01Z", "commentary",
			map[string]any{"type": "output_text", "text": "intermediate analysis"}),
		codexAssistantEvent("2026-09-13T12:00:02.123Z", "final_answer",
			map[string]any{"type": "output_text", "text": "first paragraph"},
			map[string]any{"type": "tool_call", "text": "ignored"},
			map[string]any{"type": "output_text", "text": "second paragraph"}),
		// A later commentary record must not replace the correlated final answer.
		codexAssistantEvent("2026-09-13T12:00:03Z", "commentary",
			map[string]any{"type": "output_text", "text": "later scratch text"}),
	)

	inst := &Instance{Tool: "codex", CodexSessionID: sessionID}
	got, err := inst.GetLastResponse()
	if err != nil {
		t.Fatal(err)
	}
	if got.Tool != "codex" || got.Role != "assistant" {
		t.Fatalf("unexpected response identity: %+v", got)
	}
	if got.Content != "first paragraph\nsecond paragraph" {
		t.Fatalf("structured content = %q", got.Content)
	}
	if got.Timestamp != "2026-09-13T12:00:02.123Z" || got.SessionID != sessionID {
		t.Fatalf("correlation metadata missing: %+v", got)
	}
}

func TestGetLastResponseCodexRejectsNonFinalPhases(t *testing.T) {
	home := t.TempDir()
	codexHome := filepath.Join(home, ".codex")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	ClearUserConfigCache()
	t.Cleanup(ClearUserConfigCache)

	sessionID := "019faaaa-bbbb-7ccc-8ddd-eeeeeeeeeeee"
	writeCodexOutputRollout(t, codexHome, sessionID,
		map[string]any{"timestamp": "2026-09-13T12:00:00Z", "type": "session_meta", "payload": map[string]any{"id": sessionID}},
		codexAssistantEvent("2026-09-13T12:00:01Z", "commentary",
			map[string]any{"type": "output_text", "text": "not a final reply"}),
	)

	_, err := (&Instance{Tool: "codex", CodexSessionID: sessionID}).GetLastResponse()
	if err == nil || !strings.Contains(err.Error(), "no final assistant response") {
		t.Fatalf("non-final output must fail closed, got %v", err)
	}
}

func TestGetLastResponseCodexRejectsLegacyUnphasedAssistantMessage(t *testing.T) {
	home := t.TempDir()
	codexHome := filepath.Join(home, ".codex")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	ClearUserConfigCache()
	t.Cleanup(ClearUserConfigCache)

	sessionID := "019f0000-1111-7222-8333-444444444444"
	writeCodexOutputRollout(t, codexHome, sessionID,
		map[string]any{"timestamp": "2026-09-13T12:00:00Z", "type": "session_meta", "payload": map[string]any{"id": sessionID}},
		codexAssistantEvent("2026-09-13T12:00:01Z", "",
			map[string]any{"type": "output_text", "text": "legacy reply"}),
	)

	got, err := (&Instance{Tool: "codex", CodexSessionID: sessionID}).GetLastResponse()
	if err == nil || got != nil || !strings.Contains(err.Error(), "no final assistant response") {
		t.Fatalf("unphased Codex output must fail closed, got %+v, %v", got, err)
	}
}

func TestCodexSessionIDCollisionIsScopedToLiveLocalRolloutOwners(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	sid := "019f0000-1111-7222-8333-444444444444"
	local := &Instance{ID: "local-a", Tool: "codex", CodexSessionID: sid}
	peer := &Instance{ID: "local-b", Tool: "codex", CodexSessionID: sid, Status: StatusRunning}
	if !local.CodexSessionIDCollidesWith([]*Instance{local, peer}) {
		t.Fatal("two live local Codex instances sharing one rollout identity were not detected")
	}
	if got, err := local.GetLastResponseBestEffortChecked([]*Instance{local, peer}); err == nil || got != nil || !strings.Contains(err.Error(), "colliding Codex rollout") {
		t.Fatalf("collision-aware output read did not fail closed: got %+v, %v", got, err)
	}
	peer.Status = StatusStopped
	if local.CodexSessionIDCollidesWith([]*Instance{local, peer}) {
		t.Fatal("stopped Codex peer was treated as a live collision")
	}
	peer.Status = StatusRunning
	peer.SSHHost = "remote.example"
	if local.CodexSessionIDCollidesWith([]*Instance{local, peer}) {
		t.Fatal("remote Codex peer was treated as an owner of a local rollout")
	}
}

func codexTerminalCaptureTrap(t *testing.T, paneSessionID string) (*tmux.Session, string) {
	t.Helper()
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "terminal-capture-invoked")
	script := `#!/bin/sh
case "$*" in
  *show-environment*CODEX_SESSION_ID*)
    if [ -n "$FAKE_CODEX_SESSION_ID" ]; then
      printf 'CODEX_SESSION_ID=%s\n' "$FAKE_CODEX_SESSION_ID"
      exit 0
    fi
    exit 1
    ;;
  *capture-pane*)
    : > "$CODEX_CAPTURE_SENTINEL"
    printf '%s\n' 'STALE TERMINAL ANSWER'
    exit 0
    ;;
esac
exit 1
`
	if err := os.WriteFile(filepath.Join(dir, "tmux"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	tmuxTmp := filepath.Join(dir, "tmux-tmp")
	if err := os.MkdirAll(tmuxTmp, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMUX_TMPDIR", tmuxTmp)
	t.Setenv("CODEX_CAPTURE_SENTINEL", sentinel)
	t.Setenv("FAKE_CODEX_SESSION_ID", paneSessionID)
	return &tmux.Session{Name: "codex-output-trap", SocketName: "codex-output-trap"}, sentinel
}

func assertCodexTerminalCaptureNotInvoked(t *testing.T, sentinel string) {
	t.Helper()
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("Codex correlation refusal reached terminal capture; stat error=%v", err)
	}
}

func TestGetLastResponseBestEffortCodexNeverFallsBackToTerminalProse(t *testing.T) {
	sessionID := "019f1234-5678-7abc-8def-0123456789ab"
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, inst *Instance) []*Instance
	}{
		{name: "missing ID"},
		{name: "SSH", setup: func(_ *testing.T, inst *Instance) []*Instance {
			inst.CodexSessionID = sessionID
			inst.SSHHost = "remote.example"
			return nil
		}},
		{name: "sandbox", setup: func(_ *testing.T, inst *Instance) []*Instance {
			inst.CodexSessionID = sessionID
			inst.Sandbox = &SandboxConfig{Enabled: true}
			return nil
		}},
		{name: "duplicate exact rollout", setup: func(t *testing.T, inst *Instance) []*Instance {
			inst.CodexSessionID = sessionID
			writeCodexOutputRollout(t, os.Getenv("CODEX_HOME"), sessionID,
				codexAssistantEvent("2026-09-13T12:00:01Z", "final_answer",
					map[string]any{"type": "output_text", "text": "structured answer"}))
			otherDir := filepath.Join(os.Getenv("CODEX_HOME"), "sessions", "2026", "09", "14")
			if err := os.MkdirAll(otherDir, 0o700); err != nil {
				t.Fatal(err)
			}
			duplicate := filepath.Join(otherDir, "rollout-2026-09-14T12-00-00-"+sessionID+".jsonl")
			if err := os.WriteFile(duplicate, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			return nil
		}},
		{name: "live collision", setup: func(_ *testing.T, inst *Instance) []*Instance {
			inst.CodexSessionID = sessionID
			inst.Status = StatusRunning
			peer := &Instance{ID: "peer", Tool: "codex", CodexSessionID: sessionID, Status: StatusRunning}
			return []*Instance{inst, peer}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("CODEX_HOME", filepath.Join(home, ".codex"))
			ClearUserConfigCache()
			t.Cleanup(ClearUserConfigCache)
			tmuxSession, sentinel := codexTerminalCaptureTrap(t, "")
			inst := &Instance{ID: "subject", Tool: "codex", tmuxSession: tmuxSession}
			var peers []*Instance
			if tc.setup != nil {
				peers = tc.setup(t, inst)
			}

			var got *ResponseOutput
			var err error
			if peers != nil {
				got, err = inst.GetLastResponseBestEffortChecked(peers)
			} else {
				got, err = inst.GetLastResponseBestEffort()
			}
			if err == nil || got != nil {
				t.Fatalf("uncorrelated Codex output did not fail closed: got=%+v err=%v", got, err)
			}
			if strings.Contains(err.Error(), "STALE TERMINAL ANSWER") {
				t.Fatalf("terminal prose escaped through correlation error: %v", err)
			}
			assertCodexTerminalCaptureNotInvoked(t, sentinel)
		})
	}
}

func TestGetLastResponseBestEffortCodexRecoversOwnPaneSessionID(t *testing.T) {
	home := t.TempDir()
	codexHome := filepath.Join(home, ".codex")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	ClearUserConfigCache()
	t.Cleanup(ClearUserConfigCache)

	sessionID := "019f9876-5432-7abc-8def-0123456789ab"
	writeCodexOutputRollout(t, codexHome, sessionID,
		codexAssistantEvent("2026-09-13T12:00:01Z", "final_answer",
			map[string]any{"type": "output_text", "text": "OWN STRUCTURED ANSWER"}))
	tmuxSession, sentinel := codexTerminalCaptureTrap(t, sessionID)
	inst := &Instance{ID: "own-pane", Tool: "codex", tmuxSession: tmuxSession}

	got, err := inst.GetLastResponseBestEffort()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Content != "OWN STRUCTURED ANSWER" || got.SessionID != sessionID {
		t.Fatalf("own-pane structured recovery = %+v, want session %s", got, sessionID)
	}
	if inst.CodexSessionID != sessionID {
		t.Fatalf("own-pane ID was not bound: got %q want %q", inst.CodexSessionID, sessionID)
	}
	assertCodexTerminalCaptureNotInvoked(t, sentinel)
}
