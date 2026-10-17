package session

import (
	"os"
	"path/filepath"
	"testing"
)

// The 2026-10-17 stall: box 2 completed a turn at 06:44:08 and the completion
// was masked six seconds later, so the send waiting on it waited for ever.
// The history answers the same question whenever it is asked.
func TestCodexTurnCompletedReadsTheDurableHistory(t *testing.T) {
	dir := GetHooksDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	gen := "01a13457-5ae5-7050-af97-d55bb4bccf54:01a14896-589d-79c1-844b-f6c8127e95a5"
	lines := `{"at":"2026-10-17T06:39:35Z","event":"agent-turn-start","status":"running","codex_started_generation":"` + gen + `"}
{"at":"2026-10-17T06:44:08Z","event":"agent-turn-complete","status":"waiting","codex_started_generation":"` + gen + `","codex_completed_generation":"` + gen + `"}
{"at":"2026-10-17T06:44:14Z","event":"agent-turn-start","status":"running","codex_started_generation":"other:gen"}
`
	if err := os.WriteFile(filepath.Join(dir, "row-1.events.jsonl"), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	if !CodexTurnCompleted("row-1", gen) {
		t.Fatal("a completion recorded in the history must stay provable after it is masked")
	}
	if CodexTurnCompleted("row-1", "other:gen") {
		t.Fatal("a generation that only started is not completed")
	}
	if CodexTurnCompleted("row-1", "never:seen") {
		t.Fatal("an unknown generation is not completed")
	}
	if CodexTurnCompleted("", gen) || CodexTurnCompleted("row-1", "") {
		t.Fatal("empty identifiers prove nothing")
	}
	if CodexTurnCompleted("row-absent", gen) {
		t.Fatal("a missing history proves nothing")
	}
}
