package main

import (
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
)

func TestCorrelatedIdenticalRetrySurvivesRowRemoval(t *testing.T) {
	home := t.TempDir()
	id := addStoppedRow(t, home, "removed-retry-row", "codex")
	binding := rowBindingOrPlaceholder(t, home, id)
	first, stdout, stderr, code := submitCorrelated(t, home, id, binding, "removed-retry-key", syntheticCorrelatedBody)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code != 0 || first["send_id"] == nil {
		t.Fatalf("seed correlated submit: exit=%d response=%v stdout=%q stderr=%q", code, first, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	if stdout, stderr, code := runAgentDeck(t, home, "session", "remove", id, "--force", "--json"); code != 0 {
		t.Fatalf("remove stopped row: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	second, stdout, stderr, code := submitCorrelated(t, home, id, binding, "removed-retry-key", syntheticCorrelatedBody)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code != 0 || second["send_id"] != first["send_id"] {
		t.Fatalf("identical retry after row removal: exit=%d first=%v second=%v stdout=%q stderr=%q", code, first, second, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
}

func TestCorrelatedRecoveryRejectsAcceptedReceiptFromAnotherRow(t *testing.T) {
	dir := t.TempDir()
	record := seedCorrelatedPreparing(t, dir)
	applyChildResult(record, map[string]any{
		"success": true,
		"accepted_turn": map[string]any{
			"receipt_id":       "foreign-receipt",
			"instance_id":      "another-immutable-row",
			"codex_session_id": "codex-thread-foreign",
			"turn_generation":  "codex-thread-foreign:turn-a",
			"accepted_at":      "2026-09-30T10:00:01Z",
		},
	}, 0, true, correlatedRecordSetter(t, dir, record))
	got, err := sendqueue.Load(dir, record.SendID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OperationState != sendqueue.OperationIndeterminate || got.AcceptedTurn != nil {
		t.Fatalf("foreign accepted receipt became operation evidence: %+v", got)
	}
}
