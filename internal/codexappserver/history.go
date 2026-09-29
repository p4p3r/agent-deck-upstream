package codexappserver

import (
	"bytes"
	"context"
	"encoding/json"
)

const historyTurnPageSize = 32
const historyItemPageSize = 8

// History is the complete stored thread history. Turns retain the server's
// chronological order and authoritative IDs/statuses. Reply is deliberately
// not loaded; recovery requests it only for a matched completed candidate.
type History struct {
	ThreadID string
	Turns    []HistoryTurn
}

type HistoryTurn struct {
	ID     string
	Status string
	Reply  string
}

// ReadThread pages turn metadata without loading items. A server without turn
// pagination fails closed; full-history thread/read is not a fallback.
func (c *Client) ReadThread(ctx context.Context, id string) (History, error) {
	if id == "" {
		return History{}, &Error{Invalid, "thread/turns/list"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	h, err := c.readThread(ctx, id)
	if err != nil {
		_ = c.Close()
	}
	return h, err
}

func (c *Client) readThread(ctx context.Context, id string) (History, error) {
	const op = "thread/turns/list"
	bad := &Error{Protocol, op}
	h := History{ThreadID: id}
	seenCursors := make(map[string]bool)
	seenTurns := make(map[string]bool)
	cursor := ""
	seenInProgress := false
	for {
		params := map[string]any{"threadId": id, "limit": historyTurnPageSize, "sortDirection": "asc", "itemsView": "notLoaded"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.request(ctx, op, params, nil)
		if err != nil {
			return History{}, err
		}
		data, next, err := decodeHistoryPage(raw, op)
		if err != nil || len(data) > historyTurnPageSize || next != "" && len(data) == 0 {
			return History{}, bad
		}
		for _, entry := range data {
			var turn struct {
				ID        string          `json:"id"`
				Status    string          `json:"status"`
				ItemsView string          `json:"itemsView"`
				Items     json.RawMessage `json:"items"`
			}
			if json.Unmarshal(entry, &turn) != nil || turn.ID == "" || seenTurns[turn.ID] || seenInProgress || turn.ItemsView != "notLoaded" || !isEmptyArray(turn.Items) {
				return History{}, bad
			}
			switch turn.Status {
			case "completed", "failed", "interrupted":
			case "inProgress":
				seenInProgress = true
			default:
				return History{}, bad
			}
			seenTurns[turn.ID] = true
			h.Turns = append(h.Turns, HistoryTurn{ID: turn.ID, Status: turn.Status})
		}
		if next == "" {
			break
		}
		if seenInProgress || seenCursors[next] {
			return History{}, bad
		}
		seenCursors[next] = true
		cursor = next
	}
	return h, nil
}

// ReadTurnReply hydrates only one known turn. A legacy thread store that cannot
// page items is still usable for metadata checks, but a completed recovery
// candidate cannot be confirmed without this authoritative reply read.
func (c *Client) ReadTurnReply(ctx context.Context, threadID, turnID string) (string, error) {
	if threadID == "" || turnID == "" {
		return "", &Error{Invalid, "thread/items/list"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	reply, err := c.readTurnReply(ctx, threadID, turnID)
	if err != nil {
		_ = c.Close()
	}
	return reply, err
}

func (c *Client) readTurnReply(ctx context.Context, threadID, turnID string) (string, error) {
	const op = "thread/items/list"
	bad := &Error{Protocol, op}
	seenCursors := make(map[string]bool)
	seenItems := make(map[string]bool)
	cursor, reply := "", ""
	hasFinal := false
	for {
		params := map[string]any{"threadId": threadID, "turnId": turnID, "limit": historyItemPageSize, "sortDirection": "asc"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.request(ctx, op, params, nil)
		if err != nil {
			return "", err
		}
		data, next, err := decodeHistoryPage(raw, op)
		if err != nil || len(data) > historyItemPageSize || next != "" && len(data) == 0 {
			return "", bad
		}
		for _, entry := range data {
			var record struct {
				TurnID string          `json:"turnId"`
				Item   json.RawMessage `json:"item"`
			}
			var item struct {
				Type  string          `json:"type"`
				ID    string          `json:"id"`
				Text  *string         `json:"text"`
				Phase json.RawMessage `json:"phase"`
			}
			if json.Unmarshal(entry, &record) != nil || record.TurnID != turnID || json.Unmarshal(record.Item, &item) != nil || item.Type == "" || item.ID == "" || seenItems[item.ID] {
				return "", bad
			}
			seenItems[item.ID] = true
			if item.Type != "agentMessage" {
				continue
			}
			if item.Text == nil {
				return "", bad
			}
			phase := ""
			if len(item.Phase) != 0 && !bytes.Equal(bytes.TrimSpace(item.Phase), []byte("null")) && json.Unmarshal(item.Phase, &phase) != nil {
				return "", bad
			}
			switch phase {
			case "":
				if !hasFinal {
					reply = *item.Text
				}
			case "commentary":
			case "final_answer":
				reply, hasFinal = *item.Text, true
			default:
				return "", bad
			}
		}
		if next == "" {
			return reply, nil
		}
		if seenCursors[next] {
			return "", bad
		}
		seenCursors[next] = true
		cursor = next
	}
}

// decodeHistoryPage requires the complete pagination envelope. Missing fields,
// null data, invalid cursors, and partial pages are not equivalent to an empty
// history, because treating them as such could replay an uncertain submission.
func decodeHistoryPage(raw json.RawMessage, op string) ([]json.RawMessage, string, error) {
	bad := &Error{Protocol, op}
	var page struct {
		Data            json.RawMessage `json:"data"`
		NextCursor      json.RawMessage `json:"nextCursor"`
		BackwardsCursor json.RawMessage `json:"backwardsCursor"`
	}
	if json.Unmarshal(raw, &page) != nil || !isArray(page.Data) || len(page.NextCursor) == 0 || len(page.BackwardsCursor) == 0 {
		return nil, "", bad
	}
	var data []json.RawMessage
	if json.Unmarshal(page.Data, &data) != nil {
		return nil, "", bad
	}
	next, ok := nullableCursor(page.NextCursor)
	if !ok {
		return nil, "", bad
	}
	if _, ok := nullableCursor(page.BackwardsCursor); !ok {
		return nil, "", bad
	}
	return data, next, nil
}

func nullableCursor(raw json.RawMessage) (string, bool) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", true
	}
	var cursor string
	if json.Unmarshal(raw, &cursor) != nil || cursor == "" {
		return "", false
	}
	return cursor, true
}

func isArray(raw json.RawMessage) bool {
	b := bytes.TrimSpace(raw)
	return len(b) != 0 && b[0] == '['
}

func isEmptyArray(raw json.RawMessage) bool {
	if !isArray(raw) {
		return false
	}
	var values []json.RawMessage
	return json.Unmarshal(raw, &values) == nil && len(values) == 0
}
