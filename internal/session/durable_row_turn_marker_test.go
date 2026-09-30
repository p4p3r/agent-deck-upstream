package session

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func writeCorrelatedMarkerFixture(t *testing.T, operationID string) string {
	t.Helper()
	path, err := codexSubmissionMarkerPath("codex-thread-1")
	if err != nil {
		t.Fatal(err)
	}
	marker := map[string]any{
		"version":               2,
		"instance_id":           "immutable-row-1",
		"codex_session_id":      "codex-thread-1",
		"operation_id":          operationID,
		"attempt_id":            "00112233445566778899aabbccddeeff",
		"prior_turn_generation": "codex-thread-1:turn-before",
		"phase":                 "prepared",
		"created_at":            "2026-09-30T10:00:00Z",
		"updated_at":            "2026-09-30T10:00:00Z",
	}
	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func markerJSONMap(t *testing.T, marker *CodexSubmissionMarker) map[string]any {
	t.Helper()
	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCorrelatedSubmissionMarkerRetainsOperationOwnership(t *testing.T) {
	isolateCodexSubmissionMarkers(t)
	const operationID = "01K6E4W7R00000000000000000"
	writeCorrelatedMarkerFixture(t, operationID)

	marker, found, err := readCodexSubmissionMarker("codex-thread-1")
	if err != nil || !found {
		t.Fatalf("read correlated marker: found=%v err=%v", found, err)
	}
	got := markerJSONMap(t, marker)
	if got["operation_id"] != operationID {
		t.Fatalf("operation ownership lost: %v", got)
	}
	for _, value := range got {
		if strings.Contains(strings.ToLower(strings.TrimSpace(toMarkerString(value))), "synthetic row body") {
			t.Fatalf("marker leaked a message body: %v", got)
		}
	}
}

func TestCorrelatedSubmissionMarkerOwnershipCannotChange(t *testing.T) {
	isolateCodexSubmissionMarkers(t)
	writeCorrelatedMarkerFixture(t, "01K6E4W7R00000000000000000")

	marker, found, err := readCodexSubmissionMarker("codex-thread-1")
	if err != nil || !found {
		t.Fatalf("read correlated marker: found=%v err=%v", found, err)
	}
	changed := markerJSONMap(t, marker)
	changed["operation_id"] = "01K6E4W7R00000000000000001"
	data, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	var replacement CodexSubmissionMarker
	if err := json.Unmarshal(data, &replacement); err != nil {
		t.Fatal(err)
	}
	if err := writeCodexSubmissionMarker(&replacement, false); err == nil {
		t.Fatal("submission marker operation ownership changed in place")
	}
	if err := ClearCodexSubmissionMarker(&replacement); err == nil {
		t.Fatal("marker with the wrong operation ownership cleared the durable fence")
	}
}

func TestLegacySubmissionMarkerRemainsConservativeAndReadable(t *testing.T) {
	isolateCodexSubmissionMarkers(t)
	marker, err := PrepareCodexSubmissionMarker(
		"legacy-row", "legacy-thread", "legacy-thread:turn-before",
		time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err := readCodexSubmissionMarker(marker.CodexSessionID)
	if err != nil || !found {
		t.Fatalf("legacy marker is unreadable: found=%v err=%v", found, err)
	}
	got := markerJSONMap(t, loaded)
	if got["version"] != float64(1) {
		t.Fatalf("legacy marker version changed: %v", got)
	}
	if _, claimed := got["operation_id"]; claimed {
		t.Fatalf("legacy marker was silently assigned to a correlated operation: %v", got)
	}
	if accepted, err := ReconcileCodexSubmissionMarker(
		"legacy-row", "legacy-thread", "legacy-thread:turn-after",
	); err != nil || accepted != "legacy-thread:turn-after" {
		t.Fatalf("ordinary legacy reconciliation changed: accepted=%q err=%v", accepted, err)
	}
}

func toMarkerString(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}
