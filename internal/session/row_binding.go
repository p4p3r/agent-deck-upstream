package session

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/asheshgoplani/agent-deck/internal/statedb"
)

// RowBindingToken returns an opaque digest of the persisted row and its
// harness/native-conversation binding. A loaded instance stays bound to the
// exact storage snapshot it came from, so caller-local status refresh cannot
// change the token. A newly constructed instance falls back to its current
// fields until it has a storage snapshot.
func RowBindingToken(instance *Instance) string {
	if instance == nil {
		return ""
	}
	if snapshot := instance.storageSnapshot; snapshot != nil && snapshot.stored != nil {
		return digestRowBindingFields(persistedRowBindingFields(snapshot.stored))
	}
	return digestRowBindingFields(currentRowBindingFields(instance))
}

func currentRowBindingFields(instance *Instance) []string {
	tmuxName := ""
	if tmuxSession := instance.GetTmuxSession(); tmuxSession != nil {
		tmuxName = tmuxSession.Name
	}
	return []string{
		"row-binding-v1", instance.ID, instance.Tool, instance.Command, instance.Wrapper,
		instance.Account, instance.LastStartedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), tmuxName,
		instance.ClaudeSessionID, instance.CodexSessionID, instance.GenericSessionID,
		instance.GenericSessionTool, instance.GenericSessionCommand, instance.GenericSessionLocation,
		instance.GeminiSessionID, instance.OpenCodeSessionID, instance.PiSessionID, instance.PiSessionPath,
		instance.CopilotSessionID,
	}
}

func persistedRowBindingFields(row *statedb.InstanceRow) []string {
	var toolData struct {
		ClaudeSessionID        string `json:"claude_session_id"`
		CodexSessionID         string `json:"codex_session_id"`
		GenericSessionID       string `json:"generic_session_id"`
		GenericSessionTool     string `json:"generic_session_tool"`
		GenericSessionCommand  string `json:"generic_session_command"`
		GenericSessionLocation string `json:"generic_session_location"`
		GeminiSessionID        string `json:"gemini_session_id"`
		OpenCodeSessionID      string `json:"opencode_session_id"`
		PiSessionID            string `json:"pi_session_id"`
		PiSessionPath          string `json:"pi_session_path"`
		CopilotSessionID       string `json:"copilot_session_id"`
	}
	_ = json.Unmarshal(row.ToolData, &toolData)
	return []string{
		"row-binding-v1", row.ID, row.Tool, row.Command, row.Wrapper,
		row.Account, ReadLastStartedAtFromToolData(row.ToolData).UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), row.TmuxSession,
		toolData.ClaudeSessionID, toolData.CodexSessionID, toolData.GenericSessionID,
		toolData.GenericSessionTool, toolData.GenericSessionCommand, toolData.GenericSessionLocation,
		toolData.GeminiSessionID, toolData.OpenCodeSessionID, toolData.PiSessionID, toolData.PiSessionPath,
		toolData.CopilotSessionID,
	}
}

func digestRowBindingFields(fields []string) string {
	hash := sha256.New()
	for _, field := range fields {
		fmt.Fprintf(hash, "%d:", len(field))
		_, _ = io.WriteString(hash, field)
	}
	return "rb1_" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}
