package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const phase1CodexSessionID = "019c9ffa-c9d6-7be1-9e1c-527080e68951"

func writePhase1CodexRollout(t *testing.T, home, sessionID, body string) string {
	t.Helper()
	dir := filepath.Join(home, "sessions", "2026", "09", "13")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create Codex sessions dir: %v", err)
	}
	path := filepath.Join(dir, "rollout-2026-09-13T12-00-00-"+sessionID+".jsonl")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatalf("write Codex rollout: %v", err)
	}
	return path
}

func TestCodexStructuredOutputSelectsLatestFinalAnswer(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	writePhase1CodexRollout(t, home, phase1CodexSessionID, `
{"timestamp":"2026-09-13T12:00:00.000Z","type":"session_meta","payload":{"id":"019c9ffa-c9d6-7be1-9e1c-527080e68951","cwd":"/tmp/project"}}
{"timestamp":"2026-09-13T12:00:01.000Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"private progress must not leak"}]}}
{"timestamp":"2026-09-13T12:00:02.000Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"first final"}]}}
{"timestamp":"2026-09-13T12:00:03.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{}"}}
{"timestamp":"2026-09-13T12:00:04.123Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"latest final line one"},{"type":"output_text","text":"line two"}]}}
{"timestamp":"2026-09-13T12:00:05.000Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"later non-final"}]}}
{"timestamp":`)

	got, err := (&Instance{
		ID:             "codex-structured",
		Tool:           "codex",
		CodexSessionID: phase1CodexSessionID,
	}).GetLastResponse()
	if err != nil {
		t.Fatalf("GetLastResponse() error: %v", err)
	}
	if got.Tool != "codex" || got.Role != "assistant" {
		t.Fatalf("response identity = %+v, want Codex assistant", got)
	}
	if got.Content != "latest final line one\nline two" {
		t.Fatalf("Content = %q, want only the latest final_answer text", got.Content)
	}
	if got.Timestamp != "2026-09-13T12:00:04.123Z" {
		t.Fatalf("Timestamp = %q, want structured event timestamp", got.Timestamp)
	}
	if got.SessionID != phase1CodexSessionID {
		t.Fatalf("SessionID = %q, want exact correlated ID %q", got.SessionID, phase1CodexSessionID)
	}
}

func TestCodexStructuredOutputRequiresExactLocalCorrelation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	otherID := "029c9ffa-c9d6-7be1-9e1c-527080e68952"
	writePhase1CodexRollout(t, home, phase1CodexSessionID, `
{"timestamp":"2026-09-13T12:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"RIGHT SESSION"}]}}`)
	writePhase1CodexRollout(t, home, otherID, `
{"timestamp":"2026-09-13T12:01:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"WRONG NEWER SESSION"}]}}`)

	local := &Instance{ID: "local", Tool: "codex", CodexSessionID: phase1CodexSessionID}
	got, err := local.GetLastResponse()
	if err != nil {
		t.Fatalf("exact local correlation failed: %v", err)
	}
	if got.Content != "RIGHT SESSION" || got.SessionID != phase1CodexSessionID {
		t.Fatalf("exact correlation selected wrong rollout: %+v", got)
	}

	for _, inst := range []*Instance{
		{ID: "missing-id", Tool: "codex"},
		{ID: "remote", Tool: "codex", CodexSessionID: phase1CodexSessionID, SSHHost: "remote.example"},
		{ID: "sandbox", Tool: "codex", CodexSessionID: phase1CodexSessionID, Sandbox: &SandboxConfig{Enabled: true}},
	} {
		if response, err := inst.GetLastResponse(); err == nil {
			t.Fatalf("%s Codex instance read an uncorrelated local rollout: %+v", inst.ID, response)
		}
	}
}
