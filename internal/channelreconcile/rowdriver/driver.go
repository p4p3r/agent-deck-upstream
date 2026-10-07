// Package rowdriver adapts the public Agent Deck correlated send CLI to the
// provider-neutral channel reconciler.
package rowdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

const maxResponseBytes = 64 << 10

var (
	ErrConfig   = errors.New("rowdriver: invalid configuration")
	ErrCommand  = errors.New("rowdriver: command failed")
	ErrProtocol = errors.New("rowdriver: invalid response")
)

type State string

const (
	Queued            State = "queued"
	Preparing         State = "preparing"
	Accepted          State = "accepted"
	Completed         State = "completed"
	Refused           State = "refused"
	BindingChanged    State = "binding_changed"
	Expired           State = "expired"
	Indeterminate     State = "indeterminate"
	ResultUnavailable State = "result_unavailable"
)

type AcceptedTurn struct {
	ReceiptID      string `json:"receipt_id"`
	InstanceID     string `json:"instance_id"`
	CodexSessionID string `json:"codex_session_id"`
	TurnGeneration string `json:"turn_generation"`
	AcceptedAt     string `json:"accepted_at"`
}

type Completion struct {
	TurnGeneration string `json:"turn_generation"`
	CompletedAt    string `json:"completed_at,omitempty"`
}

type Operation struct {
	SchemaVersion  int
	SendID         string
	SessionID      string
	IdempotencyKey string
	RowBinding     string
	State          State
	AcceptedTurn   *AcceptedTurn
	Completion     *Completion
	Content        string
	Code           string
	RetrySafe      bool
}

type Config struct {
	Executable string
	Profile    string
	SessionID  string
	RowBinding string
}

type commandFunc func(context.Context, io.Reader, []string) ([]byte, []byte, error)

type Driver struct {
	config  Config
	command commandFunc
}

func New(config Config) (*Driver, error) {
	if !validOpaque(config.Executable, 4096) || !validOpaque(config.SessionID, 512) ||
		!validOpaque(config.RowBinding, 512) || strings.ContainsAny(config.Executable, "\r\n\x00") ||
		strings.ContainsAny(config.Profile, "\r\n\x00") {
		return nil, ErrConfig
	}
	return &Driver{config: config, command: executeCommand(config.Executable)}, nil
}

func (d *Driver) Close() error { return nil }

func (d *Driver) SubmitRowOperation(ctx context.Context, idempotencyKey, body string) (Operation, error) {
	if d == nil || !validOpaque(idempotencyKey, 512) {
		return Operation{}, ErrConfig
	}
	args := profileArgs(d.config.Profile,
		"session", "send", d.config.SessionID, "--queue",
		"--idempotency-key", idempotencyKey,
		"--expected-row-binding", d.config.RowBinding,
		"--message-file", "-", "--json")
	op, err := d.invoke(ctx, strings.NewReader(body), true, args...)
	if err != nil {
		return Operation{}, err
	}
	if op.IdempotencyKey != idempotencyKey {
		return Operation{}, ErrProtocol
	}
	return op, nil
}

func (d *Driver) RowOperationStatus(ctx context.Context, sendID string) (Operation, error) {
	if d == nil || !validOpaque(sendID, 512) {
		return Operation{}, ErrConfig
	}
	op, err := d.invoke(ctx, nil, false, profileArgs(d.config.Profile, "session", "send-status", sendID, "--json")...)
	if err != nil {
		return Operation{}, err
	}
	if op.SendID != sendID {
		return Operation{}, ErrProtocol
	}
	return op, nil
}

func profileArgs(profile string, args ...string) []string {
	if profile == "" {
		return args
	}
	return append([]string{"-p", profile}, args...)
}

type response struct {
	SchemaVersion  *int          `json:"schema_version"`
	Success        *bool         `json:"success,omitempty"`
	Error          string        `json:"error,omitempty"`
	SendID         string        `json:"send_id"`
	SessionID      string        `json:"session_id"`
	IdempotencyKey string        `json:"idempotency_key"`
	RowBinding     string        `json:"row_binding_token"`
	State          State         `json:"operation_state"`
	Attempts       *int          `json:"attempts,omitempty"`
	AcceptedTurn   *AcceptedTurn `json:"accepted_turn,omitempty"`
	Completion     *Completion   `json:"completion,omitempty"`
	Content        *string       `json:"content,omitempty"`
	Code           string        `json:"code,omitempty"`
	RetrySafe      *bool         `json:"retry_safe,omitempty"`
}

func (d *Driver) invoke(ctx context.Context, stdin io.Reader, submit bool, args ...string) (Operation, error) {
	if ctx == nil || d.command == nil {
		return Operation{}, ErrConfig
	}
	stdout, stderr, runErr := d.command(ctx, stdin, args)
	if len(stdout) > maxResponseBytes || len(stderr) > maxResponseBytes {
		return Operation{}, ErrProtocol
	}
	op, err := decodeResponse(stdout, d.config, submit)
	if err != nil {
		if ctx.Err() != nil {
			return Operation{}, ctx.Err()
		}
		if runErr != nil {
			return Operation{}, ErrCommand
		}
		return Operation{}, err
	}
	if runErr != nil && !terminal(op.State) {
		return Operation{}, ErrCommand
	}
	return op, nil
}

func executeCommand(executable string) commandFunc {
	return func(ctx context.Context, stdin io.Reader, args []string) ([]byte, []byte, error) {
		cmd := exec.CommandContext(ctx, executable, args...)
		cmd.Stdin = stdin
		var stdout, stderr limitedBuffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		if stdout.overflow || stderr.overflow {
			return make([]byte, maxResponseBytes+1), nil, err
		}
		return stdout.Bytes(), stderr.Bytes(), err
	}
}

type limitedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := maxResponseBytes - b.Len()
	if remaining <= 0 {
		b.overflow = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.Buffer.Write(p[:remaining])
		b.overflow = true
		return len(p), nil
	}
	return b.Buffer.Write(p)
}

func decodeResponse(data []byte, config Config, submit bool) (Operation, error) {
	if len(data) == 0 || len(data) > maxResponseBytes || rejectDuplicateJSON(data) != nil {
		return Operation{}, ErrProtocol
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw response
	if err := dec.Decode(&raw); err != nil {
		return Operation{}, ErrProtocol
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Operation{}, ErrProtocol
	}
	if raw.SchemaVersion == nil || *raw.SchemaVersion != 1 || !validOpaque(raw.SendID, 512) ||
		raw.SessionID != config.SessionID || raw.RowBinding != config.RowBinding ||
		!validOpaque(raw.IdempotencyKey, 512) || !validState(raw.State) {
		return Operation{}, ErrProtocol
	}
	if raw.Attempts != nil && *raw.Attempts < 0 {
		return Operation{}, ErrProtocol
	}
	if submit != (raw.Success != nil) || raw.Success != nil && *raw.Success && raw.Error != "" ||
		raw.Success != nil && !*raw.Success && raw.Error == "" || !submit && raw.Error != "" {
		return Operation{}, ErrProtocol
	}
	if raw.AcceptedTurn != nil {
		a := raw.AcceptedTurn
		if !validOpaque(a.ReceiptID, 512) || a.InstanceID != raw.SessionID ||
			!validOpaque(a.CodexSessionID, 512) || !validOpaque(a.TurnGeneration, 1024) ||
			!strings.HasPrefix(a.TurnGeneration, a.CodexSessionID+":") || a.AcceptedAt == "" {
			return Operation{}, ErrProtocol
		}
	}
	if (raw.State == Accepted || raw.State == Completed || raw.State == ResultUnavailable) && raw.AcceptedTurn == nil {
		return Operation{}, ErrProtocol
	}
	if raw.Completion != nil {
		if raw.AcceptedTurn == nil || raw.Completion.TurnGeneration != raw.AcceptedTurn.TurnGeneration {
			return Operation{}, ErrProtocol
		}
	}
	if raw.State == Completed {
		if raw.AcceptedTurn == nil || raw.Completion == nil || raw.Content == nil || raw.RetrySafe != nil ||
			raw.Code != "" && (raw.Code != string(Completed) || raw.Success == nil || *raw.Success) {
			return Operation{}, ErrProtocol
		}
	} else if raw.Content != nil || raw.Completion != nil {
		return Operation{}, ErrProtocol
	}
	if terminalError(raw.State) {
		if raw.Code != string(raw.State) || raw.RetrySafe == nil || raw.AcceptedTurn != nil && raw.State != ResultUnavailable ||
			raw.Success != nil && *raw.Success {
			return Operation{}, ErrProtocol
		}
	} else if raw.Code != "" || raw.RetrySafe != nil {
		if raw.State != Completed {
			return Operation{}, ErrProtocol
		}
	}
	if raw.Success != nil && *raw.Success && terminal(raw.State) {
		return Operation{}, ErrProtocol
	}
	content := ""
	if raw.Content != nil {
		content = *raw.Content
	}
	retrySafe := false
	if raw.RetrySafe != nil {
		retrySafe = *raw.RetrySafe
	}
	return Operation{
		SchemaVersion: *raw.SchemaVersion, SendID: raw.SendID, SessionID: raw.SessionID,
		IdempotencyKey: raw.IdempotencyKey, RowBinding: raw.RowBinding, State: raw.State,
		AcceptedTurn: raw.AcceptedTurn, Completion: raw.Completion, Content: content,
		Code: raw.Code, RetrySafe: retrySafe,
	}, nil
}

func validState(state State) bool {
	switch state {
	case Queued, Preparing, Accepted, Completed, Refused, BindingChanged, Expired, Indeterminate, ResultUnavailable:
		return true
	default:
		return false
	}
}

func terminalError(state State) bool {
	return state == Refused || state == BindingChanged || state == Expired ||
		state == Indeterminate || state == ResultUnavailable
}

func terminal(state State) bool { return state == Completed || terminalError(state) }

func validOpaque(value string, max int) bool {
	if value == "" || len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] <= ' ' || value[i] == 0x7f {
			return false
		}
	}
	return true
}

func rejectDuplicateJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 16 {
			return ErrProtocol
		}
		token, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				keyToken, err := dec.Token()
				key, ok := keyToken.(string)
				if err != nil || !ok || seen[key] {
					return ErrProtocol
				}
				seen[key] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			end, err := dec.Token()
			if err != nil || end != json.Delim('}') {
				return ErrProtocol
			}
		case '[':
			for dec.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			end, err := dec.Token()
			if err != nil || end != json.Delim(']') {
				return ErrProtocol
			}
		default:
			return fmt.Errorf("%w: delimiter", ErrProtocol)
		}
		return nil
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return ErrProtocol
	}
	return nil
}
