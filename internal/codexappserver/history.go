package codexappserver

import (
	"bytes"
	"context"
	"encoding/json"
)

// History is the complete stored thread history returned by thread/read.
// Turns retain the server's chronological order and authoritative IDs.
type History struct {
	ThreadID string
	Turns    []HistoryTurn
}

type HistoryTurn struct {
	ID     string
	Status string
	Reply  string
}

// ReadThread inspects stored turns without resuming or subscribing to a thread.
// Partial item views and structurally ambiguous responses fail closed.
func (c *Client) ReadThread(ctx context.Context, id string) (History, error) {
	if id == "" {
		return History{}, &Error{Invalid, "thread/read"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	raw, err := c.request(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": true}, nil)
	if err != nil {
		_ = c.Close()
		return History{}, err
	}
	h, err := decodeHistory(raw, id)
	if err != nil {
		_ = c.Close()
	}
	return h, err
}

func decodeHistory(raw json.RawMessage, requestedID string) (History, error) {
	bad := &Error{Protocol, "thread/read"}
	var response struct {
		Thread struct {
			ID     string `json:"id"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
			Turns json.RawMessage `json:"turns"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID != requestedID || !isArray(response.Thread.Turns) {
		return History{}, bad
	}
	switch response.Thread.Status.Type {
	case "notLoaded", "idle", "active", "systemError":
	default:
		return History{}, bad
	}
	var turns []json.RawMessage
	if json.Unmarshal(response.Thread.Turns, &turns) != nil {
		return History{}, bad
	}
	h := History{ThreadID: response.Thread.ID, Turns: make([]HistoryTurn, 0, len(turns))}
	seen := make(map[string]bool, len(turns))
	for i, rawTurn := range turns {
		var turn struct {
			ID        string          `json:"id"`
			Status    string          `json:"status"`
			ItemsView json.RawMessage `json:"itemsView"`
			Items     json.RawMessage `json:"items"`
		}
		if json.Unmarshal(rawTurn, &turn) != nil || turn.ID == "" || seen[turn.ID] || !isArray(turn.Items) || len(turn.ItemsView) != 0 && string(bytes.TrimSpace(turn.ItemsView)) != `"full"` {
			return History{}, bad
		}
		seen[turn.ID] = true
		switch turn.Status {
		case "completed", "failed", "interrupted":
		case "inProgress":
			if i != len(turns)-1 {
				return History{}, bad
			}
		default:
			return History{}, bad
		}
		var items []json.RawMessage
		if json.Unmarshal(turn.Items, &items) != nil {
			return History{}, bad
		}
		messages := make([]Message, 0)
		itemIDs := make(map[string]bool, len(items))
		for _, rawItem := range items {
			var item struct {
				Type  string  `json:"type"`
				ID    string  `json:"id"`
				Text  *string `json:"text"`
				Phase string  `json:"phase"`
			}
			if json.Unmarshal(rawItem, &item) != nil || item.Type == "" || item.ID == "" || itemIDs[item.ID] {
				return History{}, bad
			}
			itemIDs[item.ID] = true
			if item.Type == "agentMessage" {
				if item.Text == nil {
					return History{}, bad
				}
				switch item.Phase {
				case "", "commentary", "final_answer":
				default:
					return History{}, bad
				}
				messages = append(messages, Message{ID: item.ID, Text: *item.Text, Phase: item.Phase})
			}
		}
		h.Turns = append(h.Turns, HistoryTurn{ID: turn.ID, Status: turn.Status, Reply: finalText(messages)})
	}
	lastInProgress := len(h.Turns) > 0 && h.Turns[len(h.Turns)-1].Status == "inProgress"
	if (response.Thread.Status.Type == "active") != lastInProgress {
		return History{}, bad
	}
	return h, nil
}

func isArray(raw json.RawMessage) bool {
	b := bytes.TrimSpace(raw)
	return len(b) != 0 && b[0] == '['
}
