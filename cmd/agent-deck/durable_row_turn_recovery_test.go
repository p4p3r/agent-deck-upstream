package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
)

func seedCorrelatedPreparing(t *testing.T, dir string) *sendqueue.Record {
	t.Helper()
	id := sendqueue.NewID(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	record := map[string]any{
		"schema_version":    1,
		"send_id":           id,
		"session_id":        "immutable-row-1",
		"idempotency_key":   "request-key-recovery-1",
		"row_binding_token": "opaque-binding-1",
		"operation_state":   "preparing",
		"state":             sendqueue.StateTyping,
		"message":           syntheticCorrelatedBody,
		"attempts":          1,
		"child_pid":         -1,
		"created_at":        "2026-09-30T10:00:00Z",
		"updated_at":        "2026-09-30T10:00:00Z",
		"deadline":          "2026-10-01T10:00:00Z",
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

func durableRecordMap(t *testing.T, dir, id string) map[string]any {
	t.Helper()
	record, err := sendqueue.Load(dir, id)
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

func correlatedRecordSetter(t *testing.T, dir string, record *sendqueue.Record) func(func(*sendqueue.Record)) error {
	t.Helper()
	return func(update func(*sendqueue.Record)) error {
		got, err := sendqueue.Update(dir, record.SendID, time.Now(), update)
		if err == nil {
			*record = *got
		}
		return err
	}
}

func TestCorrelatedRecoveryClassifiesTransportWithoutRetry(t *testing.T) {
	acceptedTurn := map[string]any{
		"receipt_id":       "receipt-recovery-1",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-30T10:00:01Z",
	}
	tests := []struct {
		name        string
		result      map[string]any
		code        int
		haveResult  bool
		wantState   string
		retrySafe   bool
		wantReceipt bool
	}{
		{name: "child vanished after possible write", haveResult: false, wantState: "indeterminate"},
		{name: "typed not submitted", result: map[string]any{"success": false, "delivery": deliveryTypedNotSubmitted}, code: 1, haveResult: true, wantState: "indeterminate"},
		{name: "pane vanished", result: map[string]any{"success": false, "delivery": deliveryPaneGone}, code: 1, haveResult: true, wantState: "indeterminate"},
		{name: "no evidence", result: map[string]any{"success": false, "delivery": deliveryNoEvidence}, code: 1, haveResult: true, wantState: "indeterminate"},
		{name: "socket partial write", result: map[string]any{"success": false, "delivery": deliverySocketWriteFailed}, code: 1, haveResult: true, wantState: "indeterminate"},
		{name: "target busy", result: map[string]any{"success": false, "delivery": deliveryTargetBusy}, code: 1, haveResult: true, wantState: "refused", retrySafe: true},
		{name: "composer blocked", result: map[string]any{"success": false, "delivery": deliveryComposerBlocked}, code: 1, haveResult: true, wantState: "refused", retrySafe: true},
		{name: "line too long", result: map[string]any{"success": false, "delivery": deliveryLineTooLong}, code: 1, haveResult: true, wantState: "refused", retrySafe: true},
		{name: "exact accepted generation", result: map[string]any{"success": true, "delivery": deliverySubmitted, "submitted": true, "accepted_turn": acceptedTurn}, haveResult: true, wantState: "accepted", wantReceipt: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			record := seedCorrelatedPreparing(t, dir)
			applyChildResult(record, tt.result, tt.code, tt.haveResult, correlatedRecordSetter(t, dir, record))
			got := durableRecordMap(t, dir, record.SendID)
			if got["operation_state"] != tt.wantState {
				t.Fatalf("operation_state = %v, want %s; record=%v", got["operation_state"], tt.wantState, got)
			}
			if got["attempts"] != float64(1) {
				t.Fatalf("recovery changed transport attempts: %v", got)
			}
			if tt.wantState == "refused" || tt.wantState == "indeterminate" {
				if got["retry_safe"] != tt.retrySafe {
					t.Fatalf("retry_safe = %v, want %v; record=%v", got["retry_safe"], tt.retrySafe, got)
				}
			}
			_, hasReceipt := got["accepted_turn"]
			if hasReceipt != tt.wantReceipt {
				t.Fatalf("accepted_turn presence = %v, want %v; record=%v", hasReceipt, tt.wantReceipt, got)
			}
		})
	}
}

func TestCorrelatedRecoveryNeverSubstitutesLaterGeneration(t *testing.T) {
	dir := t.TempDir()
	record := seedCorrelatedPreparing(t, dir)
	accepted := map[string]any{
		"receipt_id":       "receipt-recovery-2",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-30T10:00:01Z",
	}
	applyChildResult(record, map[string]any{
		"success": true, "delivery": deliverySubmitted, "submitted": true, "accepted_turn": accepted,
	}, 0, true, correlatedRecordSetter(t, dir, record))

	// A later identical reply exists, but it belongs to another generation.
	got := durableRecordMap(t, dir, record.SendID)
	got["operation_state"] = "completed"
	got["completion"] = map[string]any{"turn_generation": "codex-thread-1:turn-b"}
	got["content"] = "synthetic repeated reply"
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sendqueue.Update(dir, record.SendID, time.Now(), func(current *sendqueue.Record) {
		_ = json.Unmarshal(data, current)
	}); err == nil {
		t.Fatal("later generation completed an operation accepted for generation A")
	}

	durable := durableRecordMap(t, dir, record.SendID)
	if durable["operation_state"] != "accepted" {
		t.Fatalf("rejected later completion changed durable state: %v", durable)
	}
}

func TestCorrelatedStatusProjectionIsBodyFreeUntilCompleted(t *testing.T) {
	dir := t.TempDir()
	record := seedCorrelatedPreparing(t, dir)
	accepted := map[string]any{
		"receipt_id":       "receipt-projection-1",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-30T10:00:01Z",
	}
	applyChildResult(record, map[string]any{
		"success": true, "delivery": deliverySubmitted, "submitted": true, "accepted_turn": accepted,
	}, 0, true, correlatedRecordSetter(t, dir, record))

	status := recordFields(record)
	if status["operation_state"] != "accepted" || status["accepted_turn"] == nil {
		t.Fatalf("accepted status lost exact receipt: %v", status)
	}
	for _, forbidden := range []string{"message", "images", "content", "completion", "transcript_path", "child_pid"} {
		if _, leaked := status[forbidden]; leaked {
			t.Errorf("accepted status exposed %q: %v", forbidden, status)
		}
	}
}

func TestCorrelatedCompletedProjectionContainsOnlyExactResult(t *testing.T) {
	dir := t.TempDir()
	record := seedCorrelatedPreparing(t, dir)
	accepted := map[string]any{
		"receipt_id":       "receipt-projection-2",
		"instance_id":      "immutable-row-1",
		"codex_session_id": "codex-thread-1",
		"turn_generation":  "codex-thread-1:turn-a",
		"accepted_at":      "2026-09-30T10:00:01Z",
	}
	completed := durableRecordMap(t, dir, record.SendID)
	completed["operation_state"] = "completed"
	completed["accepted_turn"] = accepted
	completed["completion"] = map[string]any{
		"turn_generation": "codex-thread-1:turn-a",
		"completed_at":    "2026-09-30T10:00:02Z",
	}
	completed["content"] = "synthetic exact result gamma-53"
	data, err := json.Marshal(completed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sendqueue.Update(dir, record.SendID, time.Now(), func(current *sendqueue.Record) {
		_ = json.Unmarshal(data, current)
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := sendqueue.Load(dir, record.SendID)
	if err != nil {
		t.Fatal(err)
	}
	status := recordFields(loaded)
	if status["operation_state"] != "completed" || status["content"] != "synthetic exact result gamma-53" {
		t.Fatalf("completed status lost exact result: %v", status)
	}
	completion, _ := status["completion"].(map[string]any)
	if completion["turn_generation"] != "codex-thread-1:turn-a" {
		t.Fatalf("completed status generation mismatch: %v", status)
	}
	for _, forbidden := range []string{"message", "images", "transcript_path", "child_pid"} {
		if _, leaked := status[forbidden]; leaked {
			t.Errorf("completed status exposed %q: %v", forbidden, status)
		}
	}
}
