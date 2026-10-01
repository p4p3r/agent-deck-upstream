package session

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
)

func TestCorrelatedSubmissionMarkerTerminalOwnerReconcilesOnlyWithNewGeneration(t *testing.T) {
	isolateCodexSubmissionMarkers(t)
	queueDir := t.TempDir()
	now := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	request := sendqueue.CorrelatedRequest{
		SessionID:       "immutable-row-1",
		IdempotencyKey:  "terminal-marker-request",
		RowBindingToken: "opaque-row-binding",
		Message:         "synthetic terminal marker body",
		Now:             now,
	}
	record, created, err := sendqueue.EnqueueCorrelated(queueDir, request)
	if err != nil || !created {
		t.Fatalf("enqueue correlated operation: created=%v err=%v", created, err)
	}
	if _, err := sendqueue.Update(queueDir, record.SendID, now.Add(time.Second), func(current *sendqueue.Record) {
		current.OperationState = sendqueue.OperationPreparing
		current.CodexSessionID = "codex-thread-1"
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sendqueue.Update(queueDir, record.SendID, now.Add(2*time.Second), func(current *sendqueue.Record) {
		current.OperationState = sendqueue.OperationIndeterminate
		current.OperationError = sendqueue.OperationIndeterminate
		retrySafe := false
		current.RetrySafe = &retrySafe
	}); err != nil {
		t.Fatal(err)
	}

	marker, err := PrepareCorrelatedCodexSubmissionMarker(
		request.SessionID, "codex-thread-1", "codex-thread-1:turn-a", record.SendID, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.MarkTransportAmbiguous(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	terminalOwner := func(operationID string) bool {
		owned, err := sendqueue.Load(queueDir, operationID)
		return err == nil && owned.SessionID == request.SessionID && owned.CodexSessionID == "codex-thread-1" &&
			owned.Final() && owned.AcceptedTurn == nil
	}

	if accepted, err := ReconcileCodexSubmissionMarkerWithTerminalOwner(
		request.SessionID, "codex-thread-1", "codex-thread-1:turn-a", terminalOwner,
	); err == nil || accepted != "" {
		t.Fatalf("unchanged generation resolved terminal marker: accepted=%q err=%v", accepted, err)
	}
	path, err := codexSubmissionMarkerPath("codex-thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("unchanged generation removed terminal marker: %v", err)
	}

	accepted, err := ReconcileCodexSubmissionMarkerWithTerminalOwner(
		request.SessionID, "codex-thread-1", "codex-thread-1:turn-b", terminalOwner,
	)
	if err != nil || accepted != "codex-thread-1:turn-b" {
		t.Fatalf("newer generation did not reconcile terminal marker: accepted=%q err=%v", accepted, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reconciled terminal marker remains: %v", err)
	}

	retried, created, err := sendqueue.EnqueueCorrelated(queueDir, request)
	if err != nil || created || retried.SendID != record.SendID || retried.OperationState != sendqueue.OperationIndeterminate {
		t.Fatalf("terminal retry changed operation: record=%+v created=%v err=%v", retried, created, err)
	}
	conflict := request
	conflict.Message = "different synthetic body"
	if _, _, err := sendqueue.EnqueueCorrelated(queueDir, conflict); !errors.Is(err, sendqueue.ErrIdempotencyConflict) {
		t.Fatalf("changed request did not preserve global idempotency conflict: %v", err)
	}

	nextID := sendqueue.NewID(now.Add(3 * time.Second))
	next, err := PrepareCorrelatedCodexSubmissionMarker(
		request.SessionID, "codex-thread-1", "codex-thread-1:turn-b", nextID, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatalf("later operation remained fenced: %v", err)
	}
	if accepted, err := ReconcileCodexSubmissionMarkerWithTerminalOwner(
		request.SessionID, "codex-thread-1", "codex-thread-1:turn-c", func(string) bool { return false },
	); err == nil || accepted != "" {
		t.Fatalf("nonterminal owner was reconciled: accepted=%q err=%v", accepted, err)
	}
	if err := ClearCodexSubmissionMarker(next); err != nil {
		t.Fatal(err)
	}
}
