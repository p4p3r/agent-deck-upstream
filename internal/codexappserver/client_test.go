package codexappserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("AGENT_DECK_APP_SERVER_FAKE") == "1" {
		fakeServer(os.Getenv("AGENT_DECK_APP_SERVER_MODE"))
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func fakeServer(mode string) {
	if mode == "descendant_hold" {
		time.Sleep(15 * time.Second)
		return
	}
	args := os.Args[len(os.Args)-3:]
	if strings.Join(args, " ") != "app-server --listen stdio://" {
		os.Exit(2)
	}
	r := bufio.NewReader(os.Stdin)
	w := json.NewEncoder(os.Stdout)
	expect := func(method string) map[string]json.RawMessage {
		line, err := r.ReadBytes('\n')
		if err != nil {
			os.Exit(3)
		}
		var req map[string]json.RawMessage
		if json.Unmarshal(line, &req) != nil || string(req["method"]) != fmt.Sprintf("%q", method) {
			os.Exit(4)
		}
		return req
	}
	init := expect("initialize")
	var metadata struct {
		ClientInfo struct {
			Name string `json:"name"`
		} `json:"clientInfo"`
		Capabilities struct {
			ExperimentalAPI    bool `json:"experimentalApi"`
			RequestAttestation bool `json:"requestAttestation"`
		} `json:"capabilities"`
	}
	_ = json.Unmarshal(init["params"], &metadata)
	if metadata.ClientInfo.Name != "agent_deck" || !metadata.Capabilities.ExperimentalAPI || metadata.Capabilities.RequestAttestation {
		os.Exit(5)
	}
	_ = w.Encode(map[string]any{"id": json.RawMessage(init["id"]), "result": map[string]any{"userAgent": "test"}})
	expect("initialized")
	if strings.HasPrefix(mode, "history_") {
		historyFakeServer(r, w, mode)
		return
	}
	method := "thread/start"
	if mode == "resume" {
		method = "thread/resume"
	}
	thread := expect(method)
	if method == "thread/start" && strings.Contains(string(thread["params"]), "model") {
		os.Exit(6)
	}
	if method == "thread/resume" && (!strings.Contains(string(thread["params"]), `"threadId":"thr_123"`) || !strings.Contains(string(thread["params"]), `"excludeTurns":true`)) {
		os.Exit(7)
	}
	if mode == "server_error" {
		_, _ = os.Stderr.WriteString(strings.Repeat("sensitive-stderr", 2000))
		_ = w.Encode(map[string]any{"id": json.RawMessage(thread["id"]), "error": map[string]any{"code": -32000, "message": "sensitive-server-message"}})
		return
	}
	_ = w.Encode(map[string]any{"id": json.RawMessage(thread["id"]), "result": map[string]any{"thread": map[string]string{"id": "thr_123"}}})
	turn := expect("turn/start")
	if !strings.Contains(string(turn["params"]), `"threadId":"thr_123"`) {
		os.Exit(8)
	}
	if mode == "descendant" || mode == "descendant_stderr" {
		child := exec.Command(os.Args[0], "app-server", "--listen", "stdio://")
		child.Env = append(os.Environ(), "AGENT_DECK_APP_SERVER_MODE=descendant_hold")
		if mode == "descendant" {
			child.Stdout = os.Stdout // Hold the app-server stdout pipe open after its exit.
		} else {
			child.Stderr = os.Stderr // Hold the app-server stderr pipe open after its exit.
		}
		if child.Start() != nil {
			os.Exit(10)
		}
		path := os.Getenv("AGENT_DECK_APP_SERVER_DESCENDANT_PID_PATH")
		if os.WriteFile(path, []byte(strconv.Itoa(child.Process.Pid)), 0600) != nil {
			os.Exit(11)
		}
		_ = w.Encode(map[string]any{"id": json.RawMessage(turn["id"]), "result": map[string]any{"turn": map[string]string{"id": "turn_456", "status": "inProgress"}}})
		return
	}
	if mode == "exit" {
		os.Exit(9)
	}
	if mode == "malformed" {
		fmt.Fprintln(os.Stdout, "not-json")
		return
	}
	if mode == "oversized" {
		fmt.Fprintln(os.Stdout, strings.Repeat("x", maxFrame+1))
		return
	}
	if mode == "mismatch" {
		_ = w.Encode(map[string]any{"id": 999, "result": map[string]any{}})
		return
	}
	_ = w.Encode(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "thr_123", "turn": map[string]string{"id": "turn_456", "status": "inProgress"}}})
	_ = w.Encode(map[string]any{"id": json.RawMessage(turn["id"]), "result": map[string]any{"turn": map[string]string{"id": "turn_456", "status": "inProgress"}}})
	if mode == "cancel" {
		_, _ = r.ReadBytes('\n')
		return
	}
	_ = w.Encode(map[string]any{"method": "future/notification", "params": map[string]any{"private": "ignored"}})
	_ = w.Encode(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thr_123", "turnId": "turn_456", "item": map[string]string{"type": "agentMessage", "id": "msg_1", "phase": "commentary", "text": "Working"}}})
	_ = w.Encode(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thr_123", "turnId": "turn_456", "item": map[string]string{"type": "agentMessage", "id": "msg_2", "phase": "final_answer", "text": "Done"}}})
	status := "completed"
	if mode == "failed" {
		status = "failed"
	}
	if mode == "interrupted" {
		status = "interrupted"
	}
	_ = w.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thr_123", "turn": map[string]any{"id": "turn_456", "status": status, "error": map[string]string{"message": "sensitive-turn-error"}}}})
	// Keep the connection alive for a subsequent turn; Close owns termination.
	_, _ = r.ReadBytes('\n')
}

func historyFakeServer(r *bufio.Reader, w *json.Encoder, mode string) {
	turn := func(id, status string) map[string]any {
		return map[string]any{"id": id, "status": status, "itemsView": "notLoaded", "items": []any{}}
	}
	turns := []any{turn("turn_1", "completed"), turn("turn_2", "failed"), turn("turn_3", "interrupted")}
	if mode == "history_opaque_ids" {
		turns = []any{turn("z-first", "completed"), turn("m-second", "failed"), turn("a-third", "interrupted")}
	}
	if mode == "history_terminal" {
		turns = append(turns, turn("turn_4", "inProgress"))
	}
	if mode == "history_aggregate" {
		turns = make([]any, 140)
		for i := range turns {
			entry := turn(fmt.Sprintf("turn_%04d", i), "completed")
			entry["padding"] = strings.Repeat("x", 9<<10) // aggregate history exceeds a 1 MiB frame
			turns[i] = entry
		}
	}
	message := func(id, text, phase string) map[string]any {
		return map[string]any{"turnId": "turn_3", "item": map[string]any{"type": "agentMessage", "id": id, "text": text, "phase": phase}}
	}
	latest := "turn_3"
	if mode == "history_terminal" {
		latest = "turn_4"
	}
	if mode == "history_aggregate" {
		latest = "turn_0139"
	}
	if mode == "history_opaque_ids" {
		latest = "a-third"
	}
	items := []any{message("msg_1", "fallback", ""), message("msg_2", "progress", "commentary"), message("msg_3", "final reply", "final_answer")}
	for _, entry := range items {
		entry.(map[string]any)["turnId"] = latest
	}
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				ThreadID      string `json:"threadId"`
				TurnID        string `json:"turnId"`
				Cursor        string `json:"cursor"`
				Limit         int    `json:"limit"`
				SortDirection string `json:"sortDirection"`
				ItemsView     string `json:"itemsView"`
			} `json:"params"`
		}
		if json.Unmarshal(line, &req) != nil || req.Params.ThreadID != "thr_123" || req.Params.SortDirection != "asc" {
			os.Exit(30)
		}
		if req.Method == "thread/turns/list" {
			if req.Params.ItemsView != "notLoaded" || req.Params.TurnID != "" || req.Params.Limit != historyTurnPageSize {
				os.Exit(31)
			}
			if mode == "history_unsupported" {
				_ = w.Encode(map[string]any{"id": req.ID, "error": map[string]any{"code": -32601, "message": "private error"}})
				continue
			}
			start := 0
			if req.Params.Cursor != "" {
				if _, err := fmt.Sscanf(req.Params.Cursor, "page-%d", &start); err != nil {
					os.Exit(32)
				}
			}
			end := start + 2
			if mode == "history_aggregate" {
				end = start + historyTurnPageSize
			}
			if end > len(turns) {
				end = len(turns)
			}
			if start > len(turns) {
				os.Exit(33)
			}
			data := append([]any(nil), turns[start:end]...)
			if data == nil {
				data = []any{}
			}
			var next any
			if end < len(turns) {
				next = fmt.Sprintf("page-%d", end)
			}
			switch mode {
			case "history_turn_cursor_loop":
				next = "page-0"
			case "history_turn_duplicate":
				if start > 0 {
					data = []any{turn("turn_2", "completed")}
				}
			case "history_turn_empty_id":
				data = []any{turn("", "completed")}
				next = nil
			case "history_turn_unknown_status":
				data = []any{turn("turn_1", "unknown")}
				next = nil
			case "history_turn_inprogress_early":
				data = []any{turn("turn_1", "inProgress")}
				next = "page-1"
			case "history_turn_bad_view":
				data[0].(map[string]any)["itemsView"] = "summary"
			}
			result := map[string]any{"data": data, "nextCursor": next, "backwardsCursor": "previous"}
			if mode == "history_turn_malformed" {
				delete(result, "nextCursor")
			}
			_ = w.Encode(map[string]any{"id": req.ID, "result": result})
			continue
		}
		if req.Method != "thread/items/list" || req.Params.TurnID != latest || req.Params.ItemsView != "" || req.Params.Limit != historyItemPageSize {
			os.Exit(34)
		}
		if mode == "history_item_unsupported" {
			_ = w.Encode(map[string]any{"id": req.ID, "error": map[string]any{"code": -32601, "message": "private error"}})
			continue
		}
		data := []any{items[0], items[1]}
		var next any = "item-2"
		if req.Params.Cursor != "" {
			if req.Params.Cursor != "item-2" {
				os.Exit(35)
			}
			data, next = []any{items[2]}, nil
		}
		switch mode {
		case "history_item_cursor_loop":
			next = "item-2"
		case "history_item_duplicate":
			if req.Params.Cursor != "" {
				data = []any{items[0]}
			}
		case "history_item_mismatch":
			data[0].(map[string]any)["turnId"] = "other-turn"
		case "history_item_missing_text":
			delete(data[0].(map[string]any)["item"].(map[string]any), "text")
		case "history_item_unknown_phase":
			data[0].(map[string]any)["item"].(map[string]any)["phase"] = "unknown"
		case "history_item_missing_phase":
			data = []any{message("msg_optional", "unphased reply", "")}
			data[0].(map[string]any)["turnId"] = latest
			delete(data[0].(map[string]any)["item"].(map[string]any), "phase")
			next = nil
		case "history_item_null_phase":
			data = []any{message("msg_optional", "unphased reply", "")}
			data[0].(map[string]any)["turnId"] = latest
			data[0].(map[string]any)["item"].(map[string]any)["phase"] = nil
			next = nil
		}
		result := map[string]any{"data": data, "nextCursor": next, "backwardsCursor": "previous"}
		if mode == "history_item_malformed" {
			delete(result, "data")
		}
		_ = w.Encode(map[string]any{"id": req.ID, "result": result})
	}
}

func startFake(t *testing.T, mode string) *Client {
	t.Helper()
	t.Setenv("AGENT_DECK_APP_SERVER_FAKE", "1")
	t.Setenv("AGENT_DECK_APP_SERVER_MODE", mode)
	c, err := Start(context.Background(), Config{Executable: os.Args[0]})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func checkKind(t *testing.T, err error, kind Kind) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Kind != kind {
		t.Fatalf("error = %v, want %s", err, kind)
	}
}

func TestNewThreadTurnAndFinalMessage(t *testing.T) {
	c := startFake(t, "happy")
	id, err := c.StartThread(context.Background())
	if err != nil || id != "thr_123" {
		t.Fatalf("StartThread = %q, %v", id, err)
	}
	var acceptedThread, acceptedTurn string
	r, err := c.RunTurn(context.Background(), "private prompt", func(thread, turn string) error { acceptedThread, acceptedTurn = thread, turn; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if acceptedThread != "thr_123" || acceptedTurn != "turn_456" || r.ThreadID != acceptedThread || r.TurnID != acceptedTurn || r.Status != "completed" || r.Text != "Done" {
		t.Fatalf("result = %+v, accepted = %s/%s", r, acceptedThread, acceptedTurn)
	}
	if len(r.Messages) != 2 || r.Messages[0].Text != "Working" || r.Messages[1].Phase != "final_answer" {
		t.Fatalf("messages = %+v", r.Messages)
	}
}

func TestResumeThread(t *testing.T) {
	c := startFake(t, "resume")
	id, err := c.ResumeThread(context.Background(), "thr_123")
	if err != nil || id != "thr_123" {
		t.Fatalf("ResumeThread = %q, %v", id, err)
	}
	r, err := c.RunTurn(context.Background(), "continue", nil)
	if err != nil || r.Text != "Done" {
		t.Fatalf("RunTurn = %+v, %v", r, err)
	}
}

func TestReadThreadPagesMetadataAndReplyOnDemand(t *testing.T) {
	c := startFake(t, "history_pages")
	h, err := c.ReadThread(context.Background(), "thr_123")
	if err != nil {
		t.Fatal(err)
	}
	if h.ThreadID != "thr_123" || len(h.Turns) != 3 {
		t.Fatalf("history header = %+v", h)
	}
	for i, want := range []string{"completed", "failed", "interrupted"} {
		if h.Turns[i].ID != fmt.Sprintf("turn_%d", i+1) || h.Turns[i].Status != want {
			t.Fatalf("turn %d = %+v", i, h.Turns[i])
		}
	}
	for _, turn := range h.Turns {
		if turn.Reply != "" {
			t.Fatalf("metadata inspection hydrated reply: %+v", h.Turns)
		}
	}
	reply, err := c.ReadTurnReply(context.Background(), "thr_123", "turn_3")
	if err != nil || reply != "final reply" {
		t.Fatalf("ReadTurnReply = %q, %v", reply, err)
	}
}

func TestReadThreadTerminalStatuses(t *testing.T) {
	c := startFake(t, "history_terminal")
	h, err := c.ReadThread(context.Background(), "thr_123")
	if err != nil || len(h.Turns) != 4 || h.Turns[3].Status != "inProgress" || h.Turns[3].Reply != "" {
		t.Fatalf("terminal history = %+v, %v", h, err)
	}
}

func TestReadThreadPreservesOpaqueIDOrderAcrossPages(t *testing.T) {
	c := startFake(t, "history_opaque_ids")
	h, err := c.ReadThread(context.Background(), "thr_123")
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Turns) != 3 {
		t.Fatalf("turn count = %d", len(h.Turns))
	}
	for i, want := range []struct{ id, status string }{
		{"z-first", "completed"},
		{"m-second", "failed"},
		{"a-third", "interrupted"},
	} {
		if h.Turns[i].ID != want.id || h.Turns[i].Status != want.status {
			t.Fatalf("turn %d = %+v, want %+v", i, h.Turns[i], want)
		}
	}
}

func TestReadThreadAggregateHistoryAboveFrameLimit(t *testing.T) {
	c := startFake(t, "history_aggregate")
	h, err := c.ReadThread(context.Background(), "thr_123")
	if err != nil || len(h.Turns) != 140 || h.Turns[0].ID != "turn_0000" || h.Turns[139].ID != "turn_0139" || h.Turns[139].Reply != "" {
		t.Fatalf("aggregate history count = %d, error = %v", len(h.Turns), err)
	}
	// 140 x 9 KiB of ignored turn metadata exceeds the 1 MiB frame cap,
	// yet every individual page stays bounded and all IDs are retained.
	if 140*(9<<10) <= maxFrame {
		t.Fatal("fixture no longer exceeds frame limit")
	}
}

func TestReadThreadMetadataDoesNotRequireItemPagination(t *testing.T) {
	c := startFake(t, "history_item_unsupported")
	h, err := c.ReadThread(context.Background(), "thr_123")
	if err != nil || len(h.Turns) != 3 || h.Turns[2].ID != "turn_3" {
		t.Fatalf("metadata history = %+v, %v", h, err)
	}
	// Item pagination is unavailable only when a reply is actually requested.
	_, err = c.ReadTurnReply(context.Background(), "thr_123", "turn_3")
	checkKind(t, err, ServerError)
}

func TestReadTurnReplyAcceptsMissingOrNullPhase(t *testing.T) {
	for _, mode := range []string{"history_item_missing_phase", "history_item_null_phase"} {
		t.Run(mode, func(t *testing.T) {
			c := startFake(t, mode)
			reply, err := c.ReadTurnReply(context.Background(), "thr_123", "turn_3")
			if err != nil || reply != "unphased reply" {
				t.Fatalf("unphased reply = %q, %v", reply, err)
			}
		})
	}
}

func TestReadThreadRejectsAmbiguousPages(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		kind  Kind
		items bool
	}{
		{"history_unsupported", ServerError, false},
		{"history_turn_cursor_loop", Protocol, false},
		{"history_turn_duplicate", Protocol, false},
		{"history_turn_empty_id", Protocol, false},
		{"history_turn_unknown_status", Protocol, false},
		{"history_turn_inprogress_early", Protocol, false},
		{"history_turn_bad_view", Protocol, false},
		{"history_turn_malformed", Protocol, false},
		{"history_item_unsupported", ServerError, true},
		{"history_item_cursor_loop", Protocol, true},
		{"history_item_duplicate", Protocol, true},
		{"history_item_mismatch", Protocol, true},
		{"history_item_missing_text", Protocol, true},
		{"history_item_unknown_phase", Protocol, true},
		{"history_item_malformed", Protocol, true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			c := startFake(t, tc.mode)
			h, err := c.ReadThread(context.Background(), "thr_123")
			if tc.items {
				if err != nil || len(h.Turns) != 3 {
					t.Fatalf("metadata history = %+v, %v", h, err)
				}
				_, err = c.ReadTurnReply(context.Background(), "thr_123", "turn_3")
			}
			checkKind(t, err, tc.kind)
			if !tc.items && (h.ThreadID != "" || len(h.Turns) != 0) || strings.Contains(err.Error(), "private") {
				t.Fatalf("unsafe history = %+v, %v", h, err)
			}
			select {
			case <-c.done:
			default:
				t.Fatal("failed history did not reap app-server")
			}
		})
	}
}

func TestFailuresAndSensitiveData(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		kind     Kind
		onThread bool
	}{
		{"server_error", ServerError, true}, {"exit", ProcessExited, false},
		{"malformed", Protocol, false}, {"oversized", FrameTooLarge, false},
		{"mismatch", Protocol, false}, {"failed", TurnFailed, false}, {"interrupted", Interrupted, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			c := startFake(t, tc.mode)
			id, err := c.StartThread(context.Background())
			if !tc.onThread {
				if err != nil || id != "thr_123" {
					t.Fatalf("StartThread = %q, %v", id, err)
				}
				var r Result
				r, err = c.RunTurn(context.Background(), "sensitive-prompt-body", nil)
				if (tc.mode == "failed" || tc.mode == "interrupted") && (r.TurnID != "turn_456" || r.Status != tc.mode) {
					t.Fatalf("terminal turn = %+v", r)
				}
			}
			checkKind(t, err, tc.kind)
			for _, secret := range []string{"sensitive-prompt-body", "sensitive-server-message", "sensitive-stderr", "sensitive-turn-error"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error exposed %q", secret)
				}
			}
			if tc.mode == "server_error" {
				n, truncated := c.StderrSize()
				if n > maxStderr || !truncated {
					t.Fatalf("stderr capture = %d, %t", n, truncated)
				}
			}
		})
	}
}

func TestCancellationReapsChild(t *testing.T) {
	c := startFake(t, "cancel")
	if _, err := c.StartThread(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var accepted string
	r, err := c.RunTurn(ctx, "private prompt", func(_, turn string) error { accepted = turn; return nil })
	checkKind(t, err, Canceled)
	if r.TurnID != "turn_456" || accepted != r.TurnID {
		t.Fatalf("accepted ID lost on cancellation: %+v, %q", r, accepted)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("child was not reaped")
	}
}

func TestOutgoingFrameBound(t *testing.T) {
	c := startFake(t, "happy")
	if _, err := c.StartThread(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.RunTurn(context.Background(), strings.Repeat("p", maxFrame), nil)
	checkKind(t, err, FrameTooLarge)
	if strings.Contains(err.Error(), "pppp") {
		t.Fatal("error exposed prompt")
	}
}

func TestCloseDoesNotWaitForDescendantStdout(t *testing.T) {
	testCloseWithDescendant(t, "descendant")
}

func TestCloseDoesNotWaitForDescendantStderr(t *testing.T) {
	testCloseWithDescendant(t, "descendant_stderr")
}

func testCloseWithDescendant(t *testing.T, mode string) {
	t.Helper()
	pidPath := filepath.Join(t.TempDir(), "descendant.pid")
	t.Setenv("AGENT_DECK_APP_SERVER_DESCENDANT_PID_PATH", pidPath)
	c := startFake(t, mode)
	if _, err := c.StartThread(context.Background()); err != nil {
		t.Fatal(err)
	}
	accepted := make(chan string, 1)
	turnDone := make(chan struct{})
	go func() {
		defer close(turnDone)
		_, _ = c.RunTurn(context.Background(), "prompt", func(_, turn string) error { accepted <- turn; return nil })
	}()
	select {
	case id := <-accepted:
		if id != "turn_456" {
			t.Fatalf("turn ID = %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("turn was not accepted")
	}
	pidBytes, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidBytes))
	if err != nil {
		t.Fatal(err)
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Kill() })
	closeDone := make(chan struct{})
	go func() { _ = c.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Close waited for descendant-held pipe (%s)", mode)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("direct child was not reaped")
	}
	select {
	case <-turnDone:
	case <-time.After(2 * time.Second):
		t.Fatal("turn reader did not stop")
	}
}

func TestAcceptanceFailureStopsChild(t *testing.T) {
	c := startFake(t, "cancel") // Server accepts, then waits without completing.
	if _, err := c.StartThread(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.RunTurn(ctx, "private-prompt", func(_, id string) error {
		if id != "turn_456" {
			t.Fatalf("accepted ID = %q", id)
		}
		return errors.New("private-persistence-error")
	})
	checkKind(t, err, AcceptanceFailed)
	if r.TurnID != "turn_456" || strings.Contains(err.Error(), "private") {
		t.Fatalf("result = %+v, error = %v", r, err)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("child still running after callback failure")
	}
}
