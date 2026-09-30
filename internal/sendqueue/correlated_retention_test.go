package sendqueue

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCorrelatedRetentionKeepsUncertainWorkAndTombstonesPrunedMetadata(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	request := func(key string, created time.Time) CorrelatedRequest {
		return CorrelatedRequest{
			SessionID: "immutable-row-retention", IdempotencyKey: key,
			RowBindingToken: "opaque-binding-retention", Message: "synthetic retention body",
			Now: created,
		}
	}

	t.Run("uncertain body scrubs but operation remains", func(t *testing.T) {
		dir := t.TempDir()
		record, _, err := EnqueueCorrelated(dir, request("retention-uncertain", now.Add(-8*24*time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Update(dir, record.SendID, now.Add(-8*24*time.Hour), func(current *Record) {
			current.OperationState = OperationPreparing
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := Update(dir, record.SendID, now.Add(-8*24*time.Hour), func(current *Record) {
			current.OperationState = OperationIndeterminate
			retrySafe := false
			current.RetrySafe = &retrySafe
		}); err != nil {
			t.Fatal(err)
		}
		PruneWithRetention(dir, now, DefaultRetentionPolicy())
		got, err := Load(dir, record.SendID)
		if err != nil {
			t.Fatal(err)
		}
		if got.OperationState != OperationIndeterminate || got.Message != "" || got.BodyScrubbedAt == "" {
			t.Fatalf("uncertain operation retention = %+v", got)
		}
	})

	t.Run("accepted body scrubs but active metadata remains", func(t *testing.T) {
		dir := t.TempDir()
		record, _, err := EnqueueCorrelated(dir, request("retention-active", now.Add(-100*24*time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Update(dir, record.SendID, now.Add(-100*24*time.Hour), func(current *Record) {
			current.OperationState = OperationPreparing
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := Update(dir, record.SendID, now.Add(-100*24*time.Hour), func(current *Record) {
			current.OperationState = OperationAccepted
			current.AcceptedTurn = &AcceptedTurn{
				ReceiptID: "receipt-retention", InstanceID: current.SessionID,
				CodexSessionID: "codex-retention", TurnGeneration: "codex-retention:turn-a",
				AcceptedAt: now.Add(-100 * 24 * time.Hour).Format(time.RFC3339Nano),
			}
		}); err != nil {
			t.Fatal(err)
		}
		PruneWithRetention(dir, now, DefaultRetentionPolicy())
		got, err := Load(dir, record.SendID)
		if err != nil || got.Message != "" || got.BodyScrubbedAt == "" || got.OperationState != OperationAccepted {
			t.Fatalf("active accepted retention = record=%+v err=%v", got, err)
		}
	})

	t.Run("terminal metadata leaves a no-resurrection tombstone", func(t *testing.T) {
		dir := t.TempDir()
		req := request("retention-terminal", now.Add(-100*24*time.Hour))
		record, _, err := EnqueueCorrelated(dir, req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Update(dir, record.SendID, now.Add(-100*24*time.Hour), func(current *Record) {
			current.OperationState = OperationPreparing
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := Update(dir, record.SendID, now.Add(-100*24*time.Hour), func(current *Record) {
			current.OperationState = OperationRefused
			retrySafe := true
			current.RetrySafe = &retrySafe
		}); err != nil {
			t.Fatal(err)
		}
		PruneWithRetention(dir, now, DefaultRetentionPolicy())
		if _, err := Load(dir, record.SendID); !errors.Is(err, ErrUnknown) {
			t.Fatalf("terminal metadata was not pruned: %v", err)
		}
		retried, created, err := EnqueueCorrelated(dir, req)
		if err != nil || created || retried.SendID != record.SendID || retried.OperationState != OperationExpired {
			t.Fatalf("tombstone retry = record=%+v created=%v err=%v", retried, created, err)
		}
	})
}

func TestCorrelatedEnumerationRejectsRecordShapedDirectory(t *testing.T) {
	dir := t.TempDir()
	id := NewID(time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC))
	if err := os.Mkdir(filepath.Join(dir, id+".json"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := List(dir, ""); err == nil {
		t.Fatal("enumeration hid a directory masquerading as an operation record")
	}
}
