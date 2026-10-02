package session

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

// RowBindingToken returns an opaque stable digest of the immutable row and
// its current harness/native-conversation binding. A restart, harness switch,
// or native conversation replacement changes the token without exposing any
// underlying identity.
func RowBindingToken(instance *Instance) string {
	if instance == nil {
		return ""
	}
	tmuxName := ""
	if tmuxSession := instance.GetTmuxSession(); tmuxSession != nil {
		tmuxName = tmuxSession.Name
	}
	fields := []string{
		"row-binding-v1", instance.ID, instance.Tool, instance.Command, instance.Wrapper,
		instance.Account, instance.LastStartedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), tmuxName,
		instance.ClaudeSessionID, instance.CodexSessionID, instance.GenericSessionID,
		instance.GenericSessionTool, instance.GenericSessionCommand, instance.GenericSessionLocation,
		instance.GeminiSessionID, instance.OpenCodeSessionID, instance.PiSessionID, instance.PiSessionPath,
		instance.CopilotSessionID,
	}
	hash := sha256.New()
	for _, field := range fields {
		fmt.Fprintf(hash, "%d:", len(field))
		_, _ = io.WriteString(hash, field)
	}
	return "rb1_" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}
