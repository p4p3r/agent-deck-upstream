package codexdriver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/codexappserver"
)

func TestMain(m *testing.M) {
	if os.Getenv("AGENT_DECK_CODEXDRIVER_FAKE") == "1" {
		fakeServer(os.Getenv("AGENT_DECK_CODEXDRIVER_MODE"))
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func fakeServer(mode string) {
	if strings.Join(os.Args[len(os.Args)-3:], " ") != "app-server --listen stdio://" {
		os.Exit(2)
	}
	if path := os.Getenv("AGENT_DECK_CODEXDRIVER_PID_PATH"); path != "" {
		if os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0600) != nil {
			os.Exit(18)
		}
	}
	r := bufio.NewReader(os.Stdin)
	w := json.NewEncoder(os.Stdout)
	next := func() (map[string]json.RawMessage, bool) {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, false
		}
		var req map[string]json.RawMessage
		if json.Unmarshal(line, &req) != nil {
			os.Exit(3)
		}
		return req, true
	}
	method := func(req map[string]json.RawMessage) string {
		var name string
		if json.Unmarshal(req["method"], &name) != nil {
			os.Exit(4)
		}
		return name
	}
	init, ok := next()
	if !ok || method(init) != "initialize" {
		os.Exit(5)
	}
	_ = w.Encode(map[string]any{"id": json.RawMessage(init["id"]), "result": map[string]any{"userAgent": "fake"}})
	ack, ok := next()
	if !ok || method(ack) != "initialized" {
		os.Exit(6)
	}
	if mode == "persistent" {
		persistentFakeServer(r, w)
		return
	}
	req, ok := next()
	if !ok {
		os.Exit(7)
	}
	name := method(req)
	if name == "thread/items/list" {
		if !strings.Contains(string(req["params"]), `"threadId":"thr_123"`) || !strings.Contains(string(req["params"]), `"turnId":"turn_2"`) {
			os.Exit(20)
		}
		if mode == "item_unsupported" {
			_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "error": map[string]any{"code": -32601, "message": "private item-store error"}})
			return
		}
		_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "result": fakeItemsResponse(mode)})
		_, _ = r.ReadBytes('\n')
		return
	}
	if name == "thread/start" || name == "thread/resume" {
		if name == "thread/start" && mode != "open" {
			os.Exit(8)
		}
		if name == "thread/resume" && mode != "resume" && mode != "start" && mode != "start_failed" && mode != "start_interrupted" && mode != "callback_fail" && mode != "turn_block" {
			os.Exit(9)
		}
		var params struct{ ThreadID, CWD, Model string }
		if json.Unmarshal(req["params"], &params) != nil {
			os.Exit(10)
		}
		if name == "thread/resume" && params.ThreadID != "thr_123" {
			os.Exit(11)
		}
		if cwd := os.Getenv("AGENT_DECK_CODEXDRIVER_EXPECT_CWD"); cwd != "" {
			actual, _ := os.Getwd()
			if actual != cwd || params.CWD != cwd {
				os.Exit(12)
			}
		}
		if model := os.Getenv("AGENT_DECK_CODEXDRIVER_EXPECT_MODEL"); model != "" && params.Model != model {
			os.Exit(13)
		}
		_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "result": map[string]any{"thread": map[string]string{"id": "thr_123"}}})
		if name == "thread/start" || mode == "resume" {
			_, _ = r.ReadBytes('\n')
			return
		}
		req, ok = next()
		if !ok || method(req) != "turn/start" || !strings.Contains(string(req["params"]), `"threadId":"thr_123"`) {
			os.Exit(14)
		}
		if mode == "turn_block" {
			_, _ = r.ReadBytes('\n')
			return
		}
		_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "result": map[string]any{"turn": map[string]string{"id": "turn_456", "status": "inProgress"}}})
		if mode == "callback_fail" {
			_, _ = r.ReadBytes('\n')
			return
		}
		_ = w.Encode(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thr_123", "turnId": "turn_456", "item": map[string]string{"type": "agentMessage", "id": "msg_1", "phase": "final_answer", "text": "final reply"}}})
		status := "completed"
		if mode == "start_failed" {
			status = "failed"
		}
		if mode == "start_interrupted" {
			status = "interrupted"
		}
		_ = w.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thr_123", "turn": map[string]string{"id": "turn_456", "status": status}}})
		_, _ = r.ReadBytes('\n')
		return
	}
	if name != "thread/turns/list" {
		os.Exit(15)
	}
	var read struct {
		ThreadID      string `json:"threadId"`
		Limit         int    `json:"limit"`
		SortDirection string `json:"sortDirection"`
		ItemsView     string `json:"itemsView"`
	}
	if json.Unmarshal(req["params"], &read) != nil || read.ThreadID != "thr_123" || read.Limit != 32 || read.SortDirection != "asc" || read.ItemsView != "notLoaded" {
		os.Exit(16)
	}
	if mode == "inspect_block" {
		_ = os.WriteFile(os.Getenv("AGENT_DECK_CODEXDRIVER_READY"), []byte("ready"), 0600)
		_, _ = r.ReadBytes('\n')
		return
	}
	if mode == "process_exit" {
		os.Exit(17)
	}
	if mode == "server_error" {
		_, _ = os.Stderr.WriteString("private-stderr")
		_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "error": map[string]any{"code": -1, "message": "private-server-error"}})
		return
	}
	_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "result": fakeTurnsResponse(mode)})
	req, ok = next()
	if !ok { // Invalid turn pages close the connection before item hydration.
		return
	}
	if method(req) != "thread/items/list" || !strings.Contains(string(req["params"]), `"threadId":"thr_123"`) {
		os.Exit(19)
	}
	_ = w.Encode(map[string]any{"id": json.RawMessage(req["id"]), "result": fakeItemsResponse(mode)})
	_, _ = r.ReadBytes('\n')
}

func fakeTurnsResponse(mode string) map[string]any {
	turn := func(id, status string) map[string]any {
		return map[string]any{"id": id, "status": status, "itemsView": "notLoaded", "items": []any{}}
	}
	first := turn("turn_1", "completed")
	second := turn("turn_2", "completed")
	result := map[string]any{"data": []any{first, second}, "nextCursor": nil, "backwardsCursor": "back"}
	switch mode {
	case "statuses":
		result["data"] = []any{turn("turn_1", "failed"), turn("turn_2", "interrupted"), turn("turn_3", "inProgress")}
	case "missing_turns":
		delete(result, "data")
	case "null_turns":
		result["data"] = nil
	case "duplicate_turn":
		result["data"] = []any{first, first}
	case "summary_items":
		first["itemsView"] = "summary"
	case "null_items_view":
		first["itemsView"] = nil
	case "full_items_view":
		first["itemsView"] = "full"
	case "missing_items":
		delete(first, "items")
	case "bad_status":
		first["status"] = "unknown"
	case "out_of_order_active":
		first["status"] = "inProgress"
	case "nonchronological":
		result["data"] = []any{second, first}
	case "missing_cursor":
		delete(result, "nextCursor")
	}
	return result
}

func fakeItemsResponse(mode string) map[string]any {
	turnID := "turn_2"
	if mode == "statuses" {
		turnID = "turn_3"
	}
	item := func(id, text, phase string) map[string]any {
		return map[string]any{"turnId": turnID, "item": map[string]any{"type": "agentMessage", "id": id, "text": text, "phase": phase}}
	}
	data := []any{item("m1", "working", "commentary"), item("m2", "fallback two", "")}
	switch mode {
	case "duplicate_item":
		data = []any{item("m1", "one", ""), item("m1", "two", "final_answer")}
	case "missing_text":
		data = []any{map[string]any{"turnId": turnID, "item": map[string]any{"type": "agentMessage", "id": "m1", "phase": "final_answer"}}}
	case "mismatched_item_turn":
		data[0].(map[string]any)["turnId"] = "other_turn"
	}
	return map[string]any{"data": data, "nextCursor": nil, "backwardsCursor": "back"}
}

func fakeDriver(t *testing.T, mode string, cfg Config) *Driver {
	t.Helper()
	t.Setenv("AGENT_DECK_CODEXDRIVER_FAKE", "1")
	t.Setenv("AGENT_DECK_CODEXDRIVER_MODE", mode)
	cfg.Executable = os.Args[0]
	return New(cfg)
}

func checkKind(t *testing.T, err error, kind codexappserver.Kind) {
	t.Helper()
	var e *codexappserver.Error
	if !errors.As(err, &e) || e.Kind != kind {
		t.Fatalf("error = %v, want %s", err, kind)
	}
}

func assertReaped(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(b))
	if err != nil {
		t.Fatal(err)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return
	} // Some platforms cannot open an exited process.
	done := make(chan struct{}, 1)
	go func() { _, _ = p.Wait(); done <- struct{}{} }()
	select {
	case <-done:
	case <-time.After(time.Second):
		_ = p.Kill()
		t.Fatal("driver returned while fake process was still alive")
	}
}

func TestNewReadResumeAndStart(t *testing.T) {
	cwd := t.TempDir()
	pidPath := filepath.Join(t.TempDir(), "pid")
	t.Setenv("AGENT_DECK_CODEXDRIVER_PID_PATH", pidPath)
	t.Setenv("AGENT_DECK_CODEXDRIVER_EXPECT_CWD", cwd)
	t.Setenv("AGENT_DECK_CODEXDRIVER_EXPECT_MODEL", "fake-model")
	d := fakeDriver(t, "open", Config{CWD: cwd, Model: "fake-model"})
	id, err := d.OpenThread(context.Background())
	if err != nil || id != "thr_123" {
		t.Fatalf("OpenThread = %q, %v", id, err)
	}
	assertReaped(t, pidPath)
	t.Setenv("AGENT_DECK_CODEXDRIVER_MODE", "read")
	turns, err := d.InspectThread(context.Background(), id)
	if err != nil || len(turns) != 2 || turns[0].ID != "turn_1" || turns[0].Status != "completed" || turns[0].Reply != "" || turns[1].ID != "turn_2" || turns[1].Reply != "" {
		t.Fatalf("InspectThread = %+v, %v", turns, err)
	}
	assertReaped(t, pidPath)
	reply, err := d.ReplyForTurn(context.Background(), id, "turn_2")
	if err != nil || reply != "fallback two" {
		t.Fatalf("ReplyForTurn = %q, %v", reply, err)
	}
	assertReaped(t, pidPath)
	t.Setenv("AGENT_DECK_CODEXDRIVER_MODE", "resume")
	if err := d.ResumeThread(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	assertReaped(t, pidPath)
	t.Setenv("AGENT_DECK_CODEXDRIVER_MODE", "start")
	var accepted string
	ext, err := d.StartTurn(context.Background(), id, "private prompt", func(turn string) error { accepted = turn; return nil })
	if err != nil || accepted != "turn_456" || ext.ID != accepted || ext.Status != "completed" || ext.Reply != "final reply" {
		t.Fatalf("StartTurn = %+v, %v, accepted %q", ext, err, accepted)
	}
	assertReaped(t, pidPath)
}

func TestHistoryStatuses(t *testing.T) {
	d := fakeDriver(t, "statuses", Config{})
	turns, err := d.InspectThread(context.Background(), "thr_123")
	if err != nil || len(turns) != 3 {
		t.Fatalf("turns = %+v, %v", turns, err)
	}
	for i, want := range []string{"failed", "interrupted", "inProgress"} {
		if turns[i].Status != want || turns[i].ID != fmt.Sprintf("turn_%d", i+1) {
			t.Fatalf("turn %d = %+v", i, turns[i])
		}
	}
}

func TestHistoryUsesNotLoadedTurnMetadata(t *testing.T) {
	d := fakeDriver(t, "read", Config{})
	turns, err := d.InspectThread(context.Background(), "thr_123")
	if err != nil || len(turns) != 2 || turns[0].Reply != "" || turns[1].Reply != "" {
		t.Fatalf("turns = %+v, %v", turns, err)
	}
}

func TestStartTurnTerminalStatuses(t *testing.T) {
	for _, tc := range []struct{ mode, status string }{{"start_failed", "failed"}, {"start_interrupted", "interrupted"}} {
		t.Run(tc.status, func(t *testing.T) {
			d := fakeDriver(t, tc.mode, Config{})
			called := false
			ext, err := d.StartTurn(context.Background(), "thr_123", "prompt", func(id string) error { called = id == "turn_456"; return nil })
			if err != nil || !called || ext.ID != "turn_456" || ext.Status != tc.status || ext.Reply != "final reply" {
				t.Fatalf("turn = %+v, %v, accepted %t", ext, err, called)
			}
		})
	}
}

func TestRejectIncompleteOrMismatchedHistory(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		items bool
	}{
		{"missing_turns", false}, {"null_turns", false}, {"duplicate_turn", false},
		{"nonchronological", false}, {"summary_items", false}, {"null_items_view", false},
		{"full_items_view", false}, {"missing_items", false}, {"bad_status", false},
		{"out_of_order_active", false}, {"missing_cursor", false},
		{"duplicate_item", true}, {"missing_text", true}, {"mismatched_item_turn", true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			pidPath := filepath.Join(t.TempDir(), "pid")
			t.Setenv("AGENT_DECK_CODEXDRIVER_PID_PATH", pidPath)
			d := fakeDriver(t, tc.mode, Config{})
			turns, err := d.InspectThread(context.Background(), "thr_123")
			if tc.items {
				if err != nil || len(turns) != 2 {
					t.Fatalf("metadata history = %+v, %v", turns, err)
				}
				_, err = d.ReplyForTurn(context.Background(), "thr_123", "turn_2")
			}
			checkKind(t, err, codexappserver.Protocol)
			if !tc.items && len(turns) != 0 || strings.Contains(err.Error(), "working") {
				t.Fatalf("unsafe history result = %+v, %v", turns, err)
			}
			assertReaped(t, pidPath)
		})
	}
}

func TestMetadataInspectionWithoutItemPagination(t *testing.T) {
	d := fakeDriver(t, "item_unsupported", Config{})
	turns, err := d.InspectThread(context.Background(), "thr_123")
	if err != nil || len(turns) != 2 || turns[1].ID != "turn_2" || turns[1].Reply != "" {
		t.Fatalf("metadata history = %+v, %v", turns, err)
	}
	_, err = d.ReplyForTurn(context.Background(), "thr_123", "turn_2")
	checkKind(t, err, codexappserver.ServerError)
}

func TestAcceptanceFailureStopsBeforeCompletion(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "pid")
	t.Setenv("AGENT_DECK_CODEXDRIVER_PID_PATH", pidPath)
	d := fakeDriver(t, "callback_fail", Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	called := false
	ext, err := d.StartTurn(ctx, "thr_123", "private-prompt", func(id string) error {
		called = id == "turn_456"
		return errors.New("private-store-error")
	})
	checkKind(t, err, codexappserver.AcceptanceFailed)
	if !called || ext.ID != "" || strings.Contains(err.Error(), "private") {
		t.Fatalf("result = %+v, callback = %t, error = %v", ext, called, err)
	}
	assertReaped(t, pidPath)
}

func TestCancellationAndSecretSafeErrors(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "pid")
	t.Setenv("AGENT_DECK_CODEXDRIVER_PID_PATH", pidPath)
	d := fakeDriver(t, "turn_block", Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := d.StartTurn(ctx, "thr_123", "private-prompt", func(string) error { return nil })
	checkKind(t, err, codexappserver.Canceled)
	assertReaped(t, pidPath)
	for _, mode := range []string{"process_exit", "server_error"} {
		t.Run(mode, func(t *testing.T) {
			d := fakeDriver(t, mode, Config{})
			_, err := d.InspectThread(context.Background(), "thr_123")
			if mode == "process_exit" {
				checkKind(t, err, codexappserver.ProcessExited)
			} else {
				checkKind(t, err, codexappserver.ServerError)
			}
			for _, secret := range []string{"private-prompt", "private-stderr", "private-server-error"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error exposed %q", secret)
				}
			}
		})
	}
}

func TestSerialOperationRespectsWaitingContext(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "ready")
	t.Setenv("AGENT_DECK_CODEXDRIVER_READY", ready)
	d := fakeDriver(t, "inspect_block", Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { _, err := d.InspectThread(ctx, "thr_123"); firstDone <- err }()
	deadline := time.After(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("first operation did not enter fake server")
		case <-time.After(time.Millisecond):
		}
	}
	waitCtx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	_, err := d.OpenThread(waitCtx)
	checkKind(t, err, codexappserver.Canceled)
	cancel()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("blocked operation did not stop")
	}
}
