package session

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// getCodexLastResponse reads the rollout owned by this instance. Codex's
// terminal is a TUI, so pane scraping cannot reliably distinguish a final
// answer from commentary, tool output, or a still-running status line.
func (i *Instance) getCodexLastResponse() (*ResponseOutput, error) {
	// SSH and sandbox sessions do not write into this process's local Codex
	// home. Even when an identifier happens to collide, a local rollout cannot
	// establish ownership of their remote conversation.
	if i.IsSSH() || i.IsSandboxed() {
		return nil, fmt.Errorf("structured Codex output is unavailable for remote or sandboxed sessions")
	}
	if i.CodexSessionID == "" && i.tmuxSession != nil {
		if sessionID, err := i.tmuxSession.GetEnvironment("CODEX_SESSION_ID"); err == nil {
			i.CodexSessionID = strings.TrimSpace(sessionID)
		}
	}
	if i.CodexSessionID == "" {
		return nil, fmt.Errorf("authoritative Codex session ID is not available")
	}

	if err := validateExactSessionID(i.CodexSessionID); err != nil {
		return nil, fmt.Errorf("invalid Codex session ID: %w", err)
	}
	paths, err := exactCodexRolloutMatches(i.CodexSessionID, i.getCodexHomeDir())
	if err != nil {
		return nil, err
	}
	path, err := uniqueRegularArtifact(paths, "rollout for "+i.CodexSessionID)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open Codex rollout: %w", err)
	}
	defer f.Close()

	return parseCodexRollout(f, i.CodexSessionID)
}

type codexRolloutEvent struct {
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Payload   struct {
		Type    string `json:"type"`
		Role    string `json:"role"`
		Phase   string `json:"phase"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"payload"`
}

// parseCodexRollout returns the latest complete assistant answer. Recent Codex
// versions label response messages by phase; only final_answer is an
// authoritative completed turn. Legacy unphased messages cannot establish
// completion and are deliberately rejected rather than misattributed.
func parseCodexRollout(r io.Reader, sessionID string) (*ResponseOutput, error) {
	scanner := bufio.NewScanner(r)
	// Rollout records can include large tool results and instruction payloads.
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	var last *ResponseOutput
	for scanner.Scan() {
		var event codexRolloutEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}
		if event.Type != "response_item" || event.Payload.Type != "message" || event.Payload.Role != "assistant" {
			continue
		}
		if event.Payload.Phase != "final_answer" {
			continue
		}

		var parts []string
		for _, content := range event.Payload.Content {
			if content.Type == "output_text" && content.Text != "" {
				parts = append(parts, content.Text)
			}
		}
		if len(parts) == 0 {
			continue
		}
		last = &ResponseOutput{
			Tool:      "codex",
			Role:      "assistant",
			Content:   strings.Join(parts, "\n"),
			Timestamp: event.Timestamp,
			SessionID: sessionID,
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read Codex rollout: %w", err)
	}
	if last == nil {
		return nil, fmt.Errorf("no final assistant response found in Codex rollout")
	}
	return last, nil
}
