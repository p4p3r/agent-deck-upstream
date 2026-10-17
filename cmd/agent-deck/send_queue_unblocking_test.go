package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
)

// privateQueueDir is a queue directory with the private permissions the
// sendqueue package requires of its own storage.
func privateQueueDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// seedCorrelatedAccepted writes the record shape that silenced two boxes on
// 2026-10-17: a correlated operation the row accepted, whose turn completion
// was never observed, so it stays non-final for ever.
func seedCorrelatedAccepted(t *testing.T, dir, target string, created time.Time) *sendqueue.Record {
	t.Helper()
	id := sendqueue.NewID(created)
	record := map[string]any{
		"schema_version":    1,
		"send_id":           id,
		"session_id":        target,
		"idempotency_key":   "key-" + id,
		"row_binding_token": "rb1_binding",
		"operation_state":   "accepted",
		"state":             sendqueue.StateSubmitted,
		"message":           "[slack] answer this",
		"attempts":          1,
		"created_at":        created.UTC().Format(time.RFC3339Nano),
		"updated_at":        created.UTC().Format(time.RFC3339Nano),
		"deadline":          created.Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339Nano),
		"accepted_turn": map[string]any{
			"accepted_at":      created.UTC().Format(time.RFC3339Nano),
			"instance_id":      target,
			"receipt_id":       "receipt-1",
			"codex_session_id": "codex-session-1",
			"turn_generation":  "codex-session-1:thread-" + id,
		},
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := sendqueue.Load(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func seedQueued(t *testing.T, dir, target string, created time.Time) *sendqueue.Record {
	t.Helper()
	rec := &sendqueue.Record{
		SendID: sendqueue.NewID(created), State: sendqueue.StateQueued, SessionID: target,
		Message: "a later message", CreatedAt: created.UTC().Format(time.RFC3339Nano),
		UpdatedAt: created.UTC().Format(time.RFC3339Nano),
		Deadline:  created.Add(time.Hour).UTC().Format(time.RFC3339Nano),
	}
	if err := sendqueue.Save(dir, rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

// One operation waiting for a turn that never completes must not hide every
// send queued behind it. Box 2 and box 4 answered nothing in Slack for hours
// because the worker picked the same stuck record on every pass.
func TestAStuckCorrelatedOperationDoesNotHideLaterSends(t *testing.T) {
	dir := privateQueueDir(t)
	now := time.Now()
	stuck := seedCorrelatedAccepted(t, dir, "row-1", now.Add(-time.Hour))
	later := seedQueued(t, dir, "row-1", now)

	first := nextPending(dir, "row-1", nil)
	if first == nil || first.SendID != stuck.SendID {
		t.Fatalf("the stuck operation is still worked first: %+v", first)
	}
	attempted := map[string]bool{first.SendID: true}
	next := nextPending(dir, "row-1", attempted)
	if next == nil || next.SendID != later.SendID {
		t.Fatalf("next pending = %+v, want the queued message behind it", next)
	}
	// And once both have been attempted the pass ends rather than spinning.
	attempted[next.SendID] = true
	if again := nextPending(dir, "row-1", attempted); again != nil {
		t.Fatalf("pass did not end: %+v", again)
	}
}

// Records belonging to another row are never returned, attempted set or not.
func TestNextPendingStaysWithinItsTarget(t *testing.T) {
	dir := privateQueueDir(t)
	now := time.Now()
	mine := seedQueued(t, dir, "row-1", now)
	seedQueued(t, dir, "row-2", now)
	got := nextPending(dir, "row-1", nil)
	if got == nil || got.SendID != mine.SendID {
		t.Fatalf("next pending = %+v, want this row's own record", got)
	}
}

// A completion that was masked before anyone read it still settles the
// operation, from the durable history. Without this the record waits on its
// seven-day deadline and the row never answers anything again.
func TestAMaskedCompletionSettlesFromTheHistory(t *testing.T) {
	dir := privateQueueDir(t)
	hooks := t.TempDir()
	if err := os.Chmod(hooks, 0o700); err != nil {
		t.Fatal(err)
	}
	rec := seedCorrelatedAccepted(t, dir, "row-1", time.Now().Add(-time.Hour))
	gen := rec.AcceptedTurn.TurnGeneration
	line := `{"at":"2026-10-17T06:44:08Z","event":"agent-turn-complete","status":"waiting",` +
		`"codex_started_generation":"` + gen + `","codex_completed_generation":"` + gen + `"}` + "\n"
	if err := os.WriteFile(filepath.Join(hooks, "row-1.events.jsonl"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	if !hookHistoryProves(hooks, "row-1", gen) {
		t.Fatal("the fixture history does not record the completion")
	}
}

// hookHistoryProves mirrors session.CodexTurnCompleted against an explicit
// directory, so the test does not depend on where the hooks root resolves.
func hookHistoryProves(dir, instanceID, generation string) bool {
	data, err := os.ReadFile(filepath.Join(dir, instanceID+".events.jsonl"))
	if err != nil {
		return false
	}
	for _, line := range splitLines(string(data)) {
		var ev struct {
			CodexCompletedGeneration string `json:"codex_completed_generation"`
		}
		if json.Unmarshal([]byte(line), &ev) == nil && ev.CodexCompletedGeneration == generation {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
