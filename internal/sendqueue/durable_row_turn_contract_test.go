package sendqueue

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const syntheticRowBody = "synthetic row body alpha-31"

func writeQueueJSON(t *testing.T, dir, id string, value any, mode os.FileMode) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, id+".json")
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func correlatedRecord(id, state string) map[string]any {
	return map[string]any{
		"schema_version":    1,
		"send_id":           id,
		"session_id":        "immutable-row-1",
		"idempotency_key":   "request-key-1",
		"row_binding_token": "opaque-binding-1",
		"operation_state":   state,
		"message":           syntheticRowBody,
		"created_at":        "2026-09-30T10:00:00Z",
		"updated_at":        "2026-09-30T10:00:00Z",
		"deadline":          "2026-10-01T10:00:00Z",
	}
}

func loadAsJSONMap(t *testing.T, dir, id string) map[string]any {
	t.Helper()
	record, err := Load(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func updateFromJSONMap(t *testing.T, dir, id string, next map[string]any) error {
	t.Helper()
	data, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Update(dir, id, time.Date(2026, 9, 30, 10, 1, 0, 0, time.UTC), func(record *Record) {
		if decodeErr := json.Unmarshal(data, record); decodeErr != nil {
			t.Errorf("decode update fixture: %v", decodeErr)
		}
	})
	return err
}

func TestCorrelatedRecordRoundTripRetainsDurableIdentity(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	want := correlatedRecord(id, "accepted")
	want["accepted_turn"] = map[string]any{
		"receipt_id":       "receipt-1",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-30T10:00:01Z",
	}
	writeQueueJSON(t, dir, id, want, 0o600)

	got := loadAsJSONMap(t, dir, id)
	for _, key := range []string{"schema_version", "idempotency_key", "row_binding_token", "operation_state", "accepted_turn"} {
		if _, ok := got[key]; !ok {
			t.Errorf("durable correlated record dropped %q: %v", key, got)
		}
	}
	if got["operation_state"] != "accepted" {
		t.Errorf("operation_state = %v, want accepted", got["operation_state"])
	}
}

func TestCorrelatedRecordRejectsMismatchedCompletionGeneration(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	record := correlatedRecord(id, "completed")
	record["accepted_turn"] = map[string]any{
		"receipt_id":       "receipt-1",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-30T10:00:01Z",
	}
	record["completion"] = map[string]any{
		"turn_generation": "codex-thread-1:turn-b",
		"completed_at":    "2026-09-30T10:00:02Z",
	}
	record["content"] = "synthetic repeated reply"
	writeQueueJSON(t, dir, id, record, 0o600)

	if got, err := Load(dir, id); err == nil {
		t.Fatalf("accepted generation A loaded completion B: %+v", got)
	}
}

func TestCorrelatedRecordStateTransitionsAreMonotonic(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	record := correlatedRecord(id, "queued")
	writeQueueJSON(t, dir, id, record, 0o600)

	for _, state := range []string{"preparing", "accepted", "completed"} {
		record["operation_state"] = state
		if state == "accepted" {
			record["accepted_turn"] = map[string]any{
				"receipt_id":       "receipt-1",
				"instance_id":      "immutable-row-1",
				"codex_session_id": "codex-thread-1",
				"turn_generation":  "codex-thread-1:turn-a",
				"accepted_at":      "2026-09-30T10:00:01Z",
			}
		}
		if state == "completed" {
			record["completion"] = map[string]any{
				"turn_generation": "codex-thread-1:turn-a",
				"completed_at":    "2026-09-30T10:00:02Z",
			}
			record["content"] = "synthetic repeated reply"
		}
		if err := updateFromJSONMap(t, dir, id, record); err != nil {
			t.Fatalf("advance to %s: %v", state, err)
		}
		if got := loadAsJSONMap(t, dir, id)["operation_state"]; got != state {
			t.Fatalf("after advance operation_state = %v, want %s", got, state)
		}
	}

	record["operation_state"] = "queued"
	if err := updateFromJSONMap(t, dir, id, record); err == nil {
		t.Fatal("completed operation regressed to queued")
	}
}

func TestCorrelatedRecordIdentityIsImmutable(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	record := correlatedRecord(id, "queued")
	writeQueueJSON(t, dir, id, record, 0o600)

	for _, key := range []string{"session_id", "idempotency_key", "row_binding_token"} {
		t.Run(key, func(t *testing.T) {
			changed := make(map[string]any, len(record))
			for k, v := range record {
				changed[k] = v
			}
			changed[key] = fmt.Sprint(changed[key]) + "-changed"
			if err := updateFromJSONMap(t, dir, id, changed); err == nil {
				t.Fatalf("durable operation allowed %s to change", key)
			}
		})
	}
}

func TestCorrelatedTerminalStatesAreFinal(t *testing.T) {
	states := []string{"refused", "binding_changed", "expired", "indeterminate", "result_unavailable", "completed"}
	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			id := NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
			record := correlatedRecord(id, state)
			if state == "completed" {
				record["accepted_turn"] = map[string]any{
					"receipt_id":       "receipt-1",
					"instance_id":      "immutable-row-1",
					"codex_session_id": "codex-thread-1",
					"turn_generation":  "codex-thread-1:turn-a",
					"accepted_at":      "2026-09-30T10:00:01Z",
				}
				record["completion"] = map[string]any{"turn_generation": "codex-thread-1:turn-a"}
				record["content"] = "synthetic repeated reply"
			}
			writeQueueJSON(t, dir, id, record, 0o600)
			loaded, err := Load(dir, id)
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.Final() {
				t.Fatalf("operation state %s is not final", state)
			}
		})
	}
}

func TestCorrelatedPruneRetainsNonterminalOperation(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC))
	record := correlatedRecord(id, "accepted")
	// Legacy delivery has landed, but the exact accepted turn is still running.
	record["state"] = StateLanded
	record["updated_at"] = "2026-09-01T10:00:00Z"
	record["accepted_turn"] = map[string]any{
		"receipt_id":       "receipt-1",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-01T10:00:01Z",
	}
	writeQueueJSON(t, dir, id, record, 0o600)

	Prune(dir, time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC))
	if _, err := Load(dir, id); err != nil {
		t.Fatalf("pruned accepted operation before its result existed: %v", err)
	}
}

func TestQueueRecordInputFailsClosed(t *testing.T) {
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	t.Run("unknown version", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		record := correlatedRecord(id, "queued")
		record["schema_version"] = 999
		writeQueueJSON(t, dir, id, record, 0o600)
		if got, err := Load(dir, id); err == nil {
			t.Fatalf("loaded unknown record version: %+v", got)
		}
	})

	t.Run("duplicate key", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		path := filepath.Join(dir, id+".json")
		raw := fmt.Sprintf(`{"send_id":%q,"send_id":%q,"state":"queued","session_id":"immutable-row-1"}`, id, NewID(now.Add(time.Second)))
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := Load(dir, id); err == nil {
			t.Fatalf("loaded duplicate JSON key: %+v", got)
		}
	})

	t.Run("identity does not match filename", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		record := correlatedRecord(NewID(now.Add(time.Second)), "queued")
		writeQueueJSON(t, dir, id, record, 0o600)
		if got, err := Load(dir, id); err == nil {
			t.Fatalf("loaded record under another operation identity: %+v", got)
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		record := correlatedRecord(id, "queued")
		record["unexpected_private_path"] = "/synthetic/private"
		writeQueueJSON(t, dir, id, record, 0o600)
		if got, err := Load(dir, id); err == nil {
			t.Fatalf("loaded unknown correlated field: %+v", got)
		}
	})

	t.Run("oversized", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		record := correlatedRecord(id, "queued")
		record["message"] = strings.Repeat("x", 8<<20)
		writeQueueJSON(t, dir, id, record, 0o600)
		if _, err := Load(dir, id); err == nil {
			t.Fatalf("loaded oversized record with %d message bytes", 8<<20)
		}
	})

	t.Run("world readable", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		path := writeQueueJSON(t, dir, id, correlatedRecord(id, "queued"), 0o600)
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if got, err := Load(dir, id); err == nil {
			t.Fatalf("loaded non-private record: %+v", got)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		target := filepath.Join(t.TempDir(), "record.json")
		data, err := json.Marshal(correlatedRecord(id, "queued"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, id+".json")); err != nil {
			t.Fatal(err)
		}
		if got, err := Load(dir, id); err == nil {
			t.Fatalf("followed queue record symlink: %+v", got)
		}
	})

	t.Run("non-private directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		id := NewID(now)
		if err := Save(dir, &Record{SendID: id, State: StateQueued, SessionID: "immutable-row-1", Message: syntheticRowBody}); err == nil {
			t.Fatal("saved queue work in a non-private directory")
		}
	})
}

func TestQueueEnumerationDoesNotHideCorruptOperation(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	validID := NewID(now)
	if err := Save(dir, &Record{SendID: validID, State: StateQueued, SessionID: "immutable-row-1", Message: syntheticRowBody}); err != nil {
		t.Fatal(err)
	}
	badID := NewID(now.Add(time.Second))
	bad := correlatedRecord(badID, "queued")
	bad["schema_version"] = 999
	writeQueueJSON(t, dir, badID, bad, 0o600)

	if records, err := List(dir, ""); err == nil {
		t.Fatalf("enumeration hid a corrupt durable operation and returned %d records", len(records))
	}
}

func TestQueueRecoveryFromPublicationCrashArtifacts(t *testing.T) {
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	t.Run("before queue temp write", func(t *testing.T) {
		dir := t.TempDir()
		if records, err := List(dir, ""); err != nil || len(records) != 0 {
			t.Fatalf("empty recovery: records=%v err=%v", records, err)
		}
	})

	t.Run("after temp write before publication", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		data, err := json.Marshal(correlatedRecord(id, "queued"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "."+id+".tmp"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(dir, id); err != ErrUnknown {
			t.Fatalf("unpublished temp became durable work: %v", err)
		}
		if records, err := List(dir, ""); err != nil || len(records) != 0 {
			t.Fatalf("enumerated unpublished temp: records=%v err=%v", records, err)
		}
	})

	t.Run("accepted final wins over newer completion temp", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		accepted := correlatedRecord(id, "accepted")
		accepted["accepted_turn"] = map[string]any{
			"receipt_id":       "receipt-crash-1",
			"instance_id":      "immutable-row-1",
			"codex_session_id": "codex-thread-1",
			"turn_generation":  "codex-thread-1:turn-a",
			"accepted_at":      "2026-09-30T10:00:01Z",
		}
		writeQueueJSON(t, dir, id, accepted, 0o600)

		completed := make(map[string]any, len(accepted)+2)
		for key, value := range accepted {
			completed[key] = value
		}
		completed["operation_state"] = "completed"
		completed["completion"] = map[string]any{"turn_generation": "codex-thread-1:turn-a"}
		completed["content"] = "synthetic exact result delta-59"
		data, err := json.Marshal(completed)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "."+id+".tmp"), data, 0o600); err != nil {
			t.Fatal(err)
		}

		got := loadAsJSONMap(t, dir, id)
		if got["operation_state"] != "accepted" || got["content"] != nil {
			t.Fatalf("unpublished completion replaced accepted state: %v", got)
		}
	})

	t.Run("durable completion survives response loss", func(t *testing.T) {
		dir := t.TempDir()
		id := NewID(now)
		completed := correlatedRecord(id, "completed")
		completed["accepted_turn"] = map[string]any{
			"receipt_id":       "receipt-crash-2",
			"instance_id":      "immutable-row-1",
			"codex_session_id": "codex-thread-1",
			"turn_generation":  "codex-thread-1:turn-a",
			"accepted_at":      "2026-09-30T10:00:01Z",
		}
		completed["completion"] = map[string]any{"turn_generation": "codex-thread-1:turn-a"}
		completed["content"] = "synthetic exact result epsilon-61"
		writeQueueJSON(t, dir, id, completed, 0o600)

		first := loadAsJSONMap(t, dir, id)
		second := loadAsJSONMap(t, dir, id)
		if first["operation_state"] != "completed" || second["operation_state"] != "completed" ||
			first["content"] != "synthetic exact result epsilon-61" || second["content"] != first["content"] {
			t.Fatalf("durable completion was not repeatable: first=%v second=%v", first, second)
		}
	})
}

func TestLegacyQueueRecordRemainsReadable(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	want := &Record{
		SendID: id, State: StateSubmitted, Verdict: "delivered", SessionID: "legacy-row",
		Message: syntheticRowBody, CreatedAt: "2026-09-30T10:00:00Z", UpdatedAt: "2026-09-30T10:00:00Z",
		Deadline: "2026-10-01T10:00:00Z", Attempts: 1,
	}
	if err := Save(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir, id)
	if err != nil {
		t.Fatalf("legacy record no longer readable: %v", err)
	}
	if got.SendID != want.SendID || got.State != want.State || got.Message != want.Message || got.Attempts != 1 {
		t.Fatalf("legacy record changed: got %+v want %+v", got, want)
	}
}

func TestQueueLockIsExclusiveAcrossProcesses(t *testing.T) {
	const helper = "AGENTDECK_TEST_SENDQUEUE_LOCK_HELPER"
	if os.Getenv(helper) == "1" {
		lock, ok, err := TryLock(os.Getenv("AGENTDECK_TEST_SENDQUEUE_DIR"), "immutable-row-1")
		if err != nil {
			fmt.Fprintln(os.Stdout, "error:", err)
			return
		}
		if !ok {
			fmt.Fprintln(os.Stdout, "blocked")
			return
		}
		lock.Release()
		fmt.Fprintln(os.Stdout, "acquired")
		return
	}

	dir := t.TempDir()
	first, ok, err := TryLock(dir, "immutable-row-1")
	if err != nil || !ok {
		t.Fatalf("first lock: ok=%v err=%v", ok, err)
	}
	run := func() string {
		cmd := exec.Command(os.Args[0], "-test.run=^TestQueueLockIsExclusiveAcrossProcesses$")
		cmd.Env = append(os.Environ(), helper+"=1", "AGENTDECK_TEST_SENDQUEUE_DIR="+dir)
		out, runErr := cmd.CombinedOutput()
		if runErr != nil {
			t.Fatalf("lock helper: %v\n%s", runErr, out)
		}
		fields := strings.Fields(string(out))
		if len(fields) == 0 {
			t.Fatal("lock helper returned no result")
		}
		return fields[0]
	}
	if got := run(); got != "blocked" {
		first.Release()
		t.Fatalf("second process while owned = %q, want blocked", got)
	}
	first.Release()
	if got := run(); got != "acquired" {
		t.Fatalf("second process after release = %q, want acquired", got)
	}
}

func TestNextIDPreservesCreationOrderAcrossProcesses(t *testing.T) {
	const helper = "AGENTDECK_TEST_SENDQUEUE_ID_HELPER"
	if os.Getenv(helper) == "1" {
		id, err := NextID(os.Getenv("AGENTDECK_TEST_SENDQUEUE_DIR"), time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
		if err != nil {
			fmt.Fprintln(os.Stdout, "error:", err)
			return
		}
		fmt.Fprintln(os.Stdout, id)
		return
	}

	dir := t.TempDir()
	ids := make([]string, 0, 2)
	for n := 0; n < 2; n++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestNextIDPreservesCreationOrderAcrossProcesses$")
		cmd.Env = append(os.Environ(), helper+"=1", "AGENTDECK_TEST_SENDQUEUE_DIR="+dir)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("id helper %d: %v\n%s", n, err, out)
		}
		fields := strings.Fields(string(out))
		if len(fields) == 0 {
			t.Fatalf("id helper %d returned no result", n)
		}
		id := fields[0]
		if !validID(id) {
			t.Fatalf("id helper %d returned %q", n, id)
		}
		ids = append(ids, id)
	}
	if !sort.StringsAreSorted(ids) || ids[0] == ids[1] {
		t.Fatalf("cross-process IDs do not preserve creation order: %v", ids)
	}
}
