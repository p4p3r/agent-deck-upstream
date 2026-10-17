package session

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// maxHookHistoryBytes bounds the read of one instance's event history. The
// writer keeps the last 200 lines of ~200 bytes, so this covers the file whole
// and refuses anything that has grown beyond reason.
const maxHookHistoryBytes = 1 << 20

// hookHistoryEvent is the part of a history line this package reads back.
type hookHistoryEvent struct {
	Event                    string `json:"event"`
	CodexCompletedGeneration string `json:"codex_completed_generation"`
}

// HookHistoryPath is the durable event history for one instance.
func HookHistoryPath(instanceID string) string {
	return filepath.Join(GetHooksDir(), filepath.Base(instanceID)+".events.jsonl")
}

// CodexTurnCompleted reports whether this instance's history records the given
// Codex turn generation as completed.
//
// The status file holds only the last event, and a later "running" sample
// masks a completion so it cannot be reused for a different turn. That is
// right for reuse and wrong for evidence: a completion nobody read during the
// seconds it was visible simply vanished, and the send waiting on it waited
// for ever. On 2026-10-17 box 2 completed a turn at 06:44:08, the completion
// was masked at 06:44:14, and its Slack reply was never posted -- the gateway
// held that turn active until a human intervened.
//
// The history is append-only and generation-keyed, so asking it is idempotent
// and a late reader gets the same answer as an early one.
func CodexTurnCompleted(instanceID, generation string) bool {
	if strings.TrimSpace(instanceID) == "" || strings.TrimSpace(generation) == "" {
		return false
	}
	f, err := os.Open(HookHistoryPath(instanceID))
	if err != nil {
		return false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxHookHistoryBytes {
		return false
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		var ev hookHistoryEvent
		if json.Unmarshal(scanner.Bytes(), &ev) != nil {
			continue
		}
		if ev.CodexCompletedGeneration == generation {
			return true
		}
	}
	return false
}
