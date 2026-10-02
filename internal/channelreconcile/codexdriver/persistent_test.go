package codexdriver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/codexappserver"
	"github.com/stretchr/testify/require"
)

// Called by the existing TestMain fake after initialize/initialized. It keeps
// serving the same stdio connection across paginated inspection and turn/start.
func persistentFakeServer(r *bufio.Reader, w *json.Encoder) {
	if path := os.Getenv("AGENT_DECK_PERSISTENT_START_LOG"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			os.Exit(30)
		}
		_, _ = f.WriteString("started\n")
		_ = f.Close()
	}
	opened := false
	completed := false
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(line, &req) != nil {
			os.Exit(31)
		}
		switch req.Method {
		case "thread/start", "thread/resume":
			if opened {
				os.Exit(32)
			}
			opened = true
			if req.Method == "thread/resume" && !strings.Contains(string(req.Params), `"threadId":"thr_123"`) {
				os.Exit(33)
			}
			_ = w.Encode(map[string]any{"id": req.ID, "result": map[string]any{"thread": map[string]string{"id": "thr_123"}}})
		case "thread/turns/list":
			if !opened || !strings.Contains(string(req.Params), `"itemsView":"notLoaded"`) {
				os.Exit(34)
			}
			turns := []any{}
			if completed {
				turns = append(turns, map[string]any{"id": "turn_1", "status": "completed", "itemsView": "notLoaded", "items": []any{}})
			}
			_ = w.Encode(map[string]any{"id": req.ID, "result": map[string]any{"data": turns, "nextCursor": nil, "backwardsCursor": "back"}})
		case "thread/items/list":
			if !opened || !completed || !strings.Contains(string(req.Params), `"turnId":"turn_1"`) {
				os.Exit(34)
			}
			_ = w.Encode(map[string]any{"id": req.ID, "result": map[string]any{"data": []any{map[string]any{
				"turnId": "turn_1", "item": map[string]string{"type": "agentMessage", "id": "message_1", "phase": "final_answer", "text": "reply"},
			}}, "nextCursor": nil, "backwardsCursor": "back"}})
		case "turn/start":
			if !opened || completed {
				os.Exit(35)
			}
			_ = w.Encode(map[string]any{"id": req.ID, "result": map[string]any{"turn": map[string]string{"id": "turn_1"}}})
			_ = w.Encode(map[string]any{"method": "item/completed", "params": map[string]any{
				"threadId": "thr_123", "turnId": "turn_1",
				"item": map[string]string{"type": "agentMessage", "id": "message_1", "phase": "final_answer", "text": "reply"},
			}})
			_ = w.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": "thr_123", "turn": map[string]string{"id": "turn_1", "status": "completed"},
			}})
			completed = true
		default:
			os.Exit(36)
		}
	}
}

func TestPersistentUsesOneInitializedProcessAndOneThread(t *testing.T) {
	t.Setenv("AGENT_DECK_CODEXDRIVER_FAKE", "1")
	t.Setenv("AGENT_DECK_CODEXDRIVER_MODE", "persistent")
	log := filepath.Join(t.TempDir(), "starts")
	t.Setenv("AGENT_DECK_PERSISTENT_START_LOG", log)
	d := NewPersistent(Config{Executable: os.Args[0], CWD: t.TempDir()})
	ctx := context.Background()
	id, err := d.OpenThread(ctx)
	require.NoError(t, err)
	require.Equal(t, "thr_123", id)
	require.NoError(t, d.ResumeThread(ctx, id)) // already open: no second protocol request
	turns, err := d.InspectThread(ctx, id)
	require.NoError(t, err)
	require.Empty(t, turns)
	accepted := ""
	turn, err := d.StartTurn(ctx, id, "prompt", func(id string) error { accepted = id; return nil })
	require.NoError(t, err)
	require.Equal(t, "turn_1", accepted)
	require.Equal(t, "completed", turn.Status)
	require.Equal(t, "reply", turn.Reply)
	turns, err = d.InspectThread(ctx, id)
	require.NoError(t, err)
	require.Len(t, turns, 1)
	require.Equal(t, turn.ID, turns[0].ID)
	require.Equal(t, turn.Status, turns[0].Status)
	require.Empty(t, turns[0].Reply)
	reply, err := d.ReplyForTurn(ctx, id, turn.ID)
	require.NoError(t, err)
	require.Equal(t, turn.Reply, reply)
	require.NoError(t, d.Close())
	require.NoError(t, d.Close())
	starts, err := os.ReadFile(log)
	require.NoError(t, err)
	require.Equal(t, "started\n", string(starts))
}

func TestPersistentAcceptanceFailureClosesProcess(t *testing.T) {
	t.Setenv("AGENT_DECK_CODEXDRIVER_FAKE", "1")
	t.Setenv("AGENT_DECK_CODEXDRIVER_MODE", "persistent")
	pidPath := filepath.Join(t.TempDir(), "pid")
	t.Setenv("AGENT_DECK_CODEXDRIVER_PID_PATH", pidPath)
	d := NewPersistent(Config{Executable: os.Args[0]})
	id, err := d.OpenThread(context.Background())
	require.NoError(t, err)
	_, err = d.StartTurn(context.Background(), id, "private prompt", func(string) error { return errors.New("private failure") })
	checkKind(t, err, codexappserver.AcceptanceFailed)
	require.NotContains(t, err.Error(), "private")
	require.NoError(t, d.Close())
	assertReaped(t, pidPath)
}
