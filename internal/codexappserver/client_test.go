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
	}
	_ = json.Unmarshal(init["params"], &metadata)
	if metadata.ClientInfo.Name != "agent_deck" {
		os.Exit(5)
	}
	_ = w.Encode(map[string]any{"id": json.RawMessage(init["id"]), "result": map[string]any{"userAgent": "test"}})
	expect("initialized")
	method := "thread/start"
	if mode == "resume" {
		method = "thread/resume"
	}
	thread := expect(method)
	if method == "thread/start" && strings.Contains(string(thread["params"]), "model") {
		os.Exit(6)
	}
	if method == "thread/resume" && !strings.Contains(string(thread["params"]), `"threadId":"thr_123"`) {
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
	r, err := c.RunTurn(context.Background(), "private prompt", func(thread, turn string) { acceptedThread, acceptedTurn = thread, turn })
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
	r, err := c.RunTurn(ctx, "private prompt", func(_, turn string) { accepted = turn })
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
		_, _ = c.RunTurn(context.Background(), "prompt", func(_, turn string) { accepted <- turn })
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
