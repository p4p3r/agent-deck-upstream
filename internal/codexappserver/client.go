// Package codexappserver speaks the stable Codex app-server stdio protocol.
// It owns one subprocess and serializes requests and turns on that connection.
package codexappserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

const maxFrame = 1 << 20
const maxStderr = 16 << 10

var errFrameTooLarge = errors.New("frame too large")

// Config identifies an executable and optional thread settings.
type Config struct {
	Executable string
	CWD        string
	Model      string // Empty inherits the Codex configuration.
}

// Kind classifies errors without exposing server messages, stderr, or input.
type Kind string

const (
	Invalid       Kind = "invalid"
	Canceled      Kind = "canceled"
	ProcessExited Kind = "process_exited"
	Protocol      Kind = "protocol"
	FrameTooLarge Kind = "frame_too_large"
	ServerError   Kind = "server_error"
	TurnFailed    Kind = "turn_failed"
	Interrupted   Kind = "interrupted"
)

type Error struct {
	Kind Kind
	Op   string
}

func (e *Error) Error() string { return fmt.Sprintf("codex app-server %s: %s", e.Op, e.Kind) }

type Message struct {
	ID    string
	Text  string
	Phase string
}

type Result struct {
	ThreadID string
	TurnID   string
	Status   string
	Messages []Message // Completed agentMessage items, in arrival order.
	Text     string    // Last final_answer, or last agent message if no phase is supplied.
}

// Client is single-threaded at the protocol boundary; concurrent callers wait
// for the active operation. Close may be called concurrently at any time.
type Client struct {
	mu         sync.Mutex
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stdout     io.ReadCloser
	stderrIn   io.ReadCloser
	stderrDone chan struct{}
	frames     chan []byte
	closed     chan struct{}
	done       chan struct{}
	closeOne   sync.Once
	readErr    error
	stderr     boundedWriter
	nextID     int
	threadID   string
	cfg        Config
}

type boundedWriter struct {
	mu        sync.Mutex
	data      []byte
	truncated bool
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	room := maxStderr - len(w.data)
	if room < n {
		w.truncated = true
		n = room
	}
	w.data = append(w.data, p[:n]...)
	return len(p), nil
}

// StderrSize reports capture size and truncation without disclosing its content.
func (c *Client) StderrSize() (int, bool) {
	c.stderr.mu.Lock()
	defer c.stderr.mu.Unlock()
	return len(c.stderr.data), c.stderr.truncated
}

// Start launches an explicit argv (without a shell) and completes the handshake.
func Start(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Executable == "" {
		return nil, &Error{Invalid, "start"}
	}
	cmd := exec.Command(cfg.Executable, "app-server", "--listen", "stdio://")
	if cfg.CWD != "" {
		cmd.Dir = cfg.CWD
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, &Error{ProcessExited, "start"}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, &Error{ProcessExited, "start"}
	}
	// A file-backed stderr avoids exec.Cmd's unbounded copy goroutine in Wait.
	// We own the reader and can close it even when a descendant holds the pipe.
	stderrIn, stderrOut, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, &Error{ProcessExited, "start"}
	}
	cmd.Stderr = stderrOut
	c := &Client{cmd: cmd, stdin: stdin, stdout: stdout, stderrIn: stderrIn, stderrDone: make(chan struct{}), frames: make(chan []byte, 32), closed: make(chan struct{}), done: make(chan struct{}), cfg: cfg}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderrIn.Close()
		_ = stderrOut.Close()
		return nil, &Error{ProcessExited, "start"}
	}
	_ = stderrOut.Close() // Only the child should retain the write end.
	go func() {
		_, _ = io.Copy(&c.stderr, stderrIn)
		close(c.stderrDone)
	}()
	go c.readFrames(stdout)
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if _, err := c.request(ctx, "initialize", map[string]any{
		"clientInfo": map[string]string{"name": "agent_deck", "title": "Agent Deck", "version": "0.1.0"},
	}, nil); err != nil {
		_ = c.Close()
		return nil, err
	}
	if err := c.write(ctx, map[string]any{"method": "initialized", "params": map[string]any{}}, "initialize"); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Client) readFrames(stdout io.ReadCloser) {
	defer close(c.done)
	defer close(c.frames)
	r := bufio.NewReaderSize(stdout, 64<<10)
	for {
		frame, err := readFrame(r)
		if err != nil {
			c.readErr = err
			if errors.Is(err, errFrameTooLarge) {
				_ = c.cmd.Process.Kill()
			}
			break
		}
		select {
		case c.frames <- frame:
		case <-c.closed:
			_ = c.cmd.Wait()
			return
		}
	}
	_ = c.cmd.Wait()
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(frame)+len(part) > maxFrame {
			return nil, errFrameTooLarge
		}
		frame = append(frame, part...)
		if err == nil {
			return frame[:len(frame)-1], nil
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
	}
}

// Close kills and reaps the child, including after a canceled operation.
func (c *Client) Close() error {
	c.closeOne.Do(func() {
		close(c.closed)
		_ = c.stdin.Close()
		_ = c.stdout.Close()
		_ = c.stderrIn.Close()
		_ = c.cmd.Process.Kill()
	})
	<-c.done
	<-c.stderrDone
	return nil
}

func (c *Client) write(ctx context.Context, v any, op string) error {
	b, err := json.Marshal(v)
	if err != nil {
		return &Error{Invalid, op}
	}
	if len(b)+1 > maxFrame {
		return &Error{FrameTooLarge, op}
	}
	b = append(b, '\n')
	if _, err = c.stdin.Write(b); err != nil {
		if ctx.Err() != nil {
			return &Error{Canceled, op}
		}
		return &Error{ProcessExited, op}
	}
	return nil
}

type envelope struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
	Params json.RawMessage `json:"params"`
}

func (c *Client) read(ctx context.Context, op string) (envelope, error) {
	select {
	case <-ctx.Done():
		return envelope{}, &Error{Canceled, op}
	case b, ok := <-c.frames:
		if !ok {
			if ctx.Err() != nil {
				return envelope{}, &Error{Canceled, op}
			}
			if errors.Is(c.readErr, errFrameTooLarge) {
				return envelope{}, &Error{FrameTooLarge, op}
			}
			return envelope{}, &Error{ProcessExited, op}
		}
		var e envelope
		if len(b) == 0 || !json.Valid(b) || json.Unmarshal(b, &e) != nil {
			return envelope{}, &Error{Protocol, op}
		}
		return e, nil
	}
}

func (c *Client) request(ctx context.Context, method string, params any, notify func(envelope) error) (json.RawMessage, error) {
	c.nextID++
	id := c.nextID
	if err := c.write(ctx, map[string]any{"id": id, "method": method, "params": params}, method); err != nil {
		return nil, err
	}
	for {
		e, err := c.read(ctx, method)
		if err != nil {
			return nil, err
		}
		if len(e.ID) == 0 {
			if e.Method == "" {
				return nil, &Error{Protocol, method}
			}
			if notify != nil {
				if err := notify(e); err != nil {
					return nil, err
				}
			}
			continue
		}
		var got int
		if json.Unmarshal(e.ID, &got) != nil || got != id || e.Method != "" {
			return nil, &Error{Protocol, method}
		}
		if len(e.Error) != 0 && string(e.Error) != "null" {
			return nil, &Error{ServerError, method}
		}
		if len(e.Result) == 0 || string(e.Result) == "null" {
			return nil, &Error{Protocol, method}
		}
		return e.Result, nil
	}
}

// StartThread creates a thread and returns the server's authoritative ID.
func (c *Client) StartThread(ctx context.Context) (string, error) {
	params := map[string]any{}
	if c.cfg.Model != "" {
		params["model"] = c.cfg.Model
	}
	if c.cfg.CWD != "" {
		params["cwd"] = c.cfg.CWD
	}
	return c.openThread(ctx, "thread/start", params)
}

// ResumeThread reopens a stored thread and returns the ID from the response.
func (c *Client) ResumeThread(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "", &Error{Invalid, "thread/resume"}
	}
	params := map[string]string{"threadId": id}
	if c.cfg.CWD != "" {
		params["cwd"] = c.cfg.CWD
	}
	if c.cfg.Model != "" {
		params["model"] = c.cfg.Model
	}
	return c.openThread(ctx, "thread/resume", params)
}

func (c *Client) openThread(ctx context.Context, method string, params any) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if c.threadID != "" {
		return "", &Error{Invalid, method}
	}
	raw, err := c.request(ctx, method, params, nil)
	if err != nil {
		_ = c.Close()
		return "", err
	}
	var response struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID == "" || (method == "thread/resume" && response.Thread.ID != params.(map[string]string)["threadId"]) {
		_ = c.Close()
		return "", &Error{Protocol, method}
	}
	c.threadID = response.Thread.ID
	return c.threadID, nil
}

// RunTurn calls accepted once the turn/start response supplies the authoritative
// ID, before waiting for completion. A failed turn still returns its IDs/status.
func (c *Client) RunTurn(ctx context.Context, prompt string, accepted func(threadID, turnID string)) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	r := Result{ThreadID: c.threadID}
	if c.threadID == "" {
		return r, &Error{Invalid, "turn/start"}
	}
	var early []envelope
	raw, err := c.request(ctx, "turn/start", map[string]any{
		"threadId": c.threadID, "input": []map[string]string{{"type": "text", "text": prompt}},
	}, func(e envelope) error {
		switch e.Method {
		case "turn/started", "item/completed", "turn/completed":
			if len(early) == 32 {
				return &Error{Protocol, "turn/start"}
			}
			early = append(early, e)
		}
		return nil
	})
	if err != nil {
		_ = c.Close()
		return r, err
	}
	var response struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Turn.ID == "" {
		_ = c.Close()
		return r, &Error{Protocol, "turn/start"}
	}
	r.TurnID = response.Turn.ID
	if accepted != nil {
		accepted(r.ThreadID, r.TurnID)
	}
	for _, e := range early {
		if done, err := applyEvent(&r, e); done || err != nil {
			if err != nil {
				_ = c.Close()
			}
			return r, err
		}
	}
	for {
		e, err := c.read(ctx, "turn")
		if err != nil {
			_ = c.Close()
			return r, err
		}
		if len(e.ID) != 0 || e.Method == "" {
			_ = c.Close()
			return r, &Error{Protocol, "turn"}
		}
		if done, err := applyEvent(&r, e); done || err != nil {
			if err != nil {
				_ = c.Close()
			}
			return r, err
		}
	}
}

func applyEvent(r *Result, e envelope) (bool, error) {
	switch e.Method {
	case "turn/started", "turn/completed":
		var p struct {
			Turn struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turn"`
			ThreadID string `json:"threadId"`
		}
		if json.Unmarshal(e.Params, &p) != nil || p.Turn.ID != r.TurnID || (p.ThreadID != "" && p.ThreadID != r.ThreadID) {
			return false, &Error{Protocol, "turn"}
		}
		if e.Method == "turn/started" {
			return false, nil
		}
		r.Status = p.Turn.Status
		switch r.Status {
		case "completed":
			return true, nil
		case "failed":
			return true, &Error{TurnFailed, "turn"}
		case "interrupted":
			return true, &Error{Interrupted, "turn"}
		default:
			return true, &Error{Protocol, "turn"}
		}
	case "item/completed":
		var p struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Item     struct {
				Type  string `json:"type"`
				ID    string `json:"id"`
				Text  string `json:"text"`
				Phase string `json:"phase"`
			} `json:"item"`
		}
		if json.Unmarshal(e.Params, &p) != nil || p.ThreadID != r.ThreadID || p.TurnID != r.TurnID {
			return false, &Error{Protocol, "item/completed"}
		}
		if p.Item.Type == "agentMessage" {
			m := Message{p.Item.ID, p.Item.Text, p.Item.Phase}
			r.Messages = append(r.Messages, m)
			if m.Phase == "final_answer" {
				r.Text = m.Text
			}
			if m.Phase == "" {
				hasFinal := false
				for _, prior := range r.Messages {
					hasFinal = hasFinal || prior.Phase == "final_answer"
				}
				if !hasFinal {
					r.Text = m.Text
				}
			}
		}
	}
	return false, nil // Unknown notifications are forward-compatible.
}
