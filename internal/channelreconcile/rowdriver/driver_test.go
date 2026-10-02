package rowdriver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

const (
	testRow     = "immutable-row"
	testBinding = "opaque-binding"
	testKey     = "gateway-attempt"
	testSend    = "01K6FAKESEND00000000000000"
)

func responseFor(state State) map[string]any {
	r := map[string]any{
		"schema_version": 1, "send_id": testSend, "session_id": testRow,
		"idempotency_key": testKey, "row_binding_token": testBinding,
		"operation_state": state,
	}
	if state == Accepted || state == Completed || state == ResultUnavailable {
		r["accepted_turn"] = map[string]any{
			"receipt_id": "receipt", "instance_id": testRow, "codex_session_id": "codex-session",
			"turn_generation": "codex-session:turn-a", "accepted_at": "2026-09-30T00:00:00Z",
		}
	}
	if state == Completed {
		r["completion"] = map[string]any{"turn_generation": "codex-session:turn-a"}
		r["content"] = "synthetic completion"
	}
	if terminalError(state) {
		r["code"] = state
		r["retry_safe"] = state == Refused || state == BindingChanged || state == Expired
	}
	return r
}

func submitResponseFor(state State) map[string]any {
	r := responseFor(state)
	if terminal(state) {
		r["success"] = false
		r["error"] = "fixed failure"
		if state == Completed {
			r["code"] = state
		}
	} else {
		r["success"] = true
	}
	return r
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fakeDriver(t *testing.T, response []byte, runErr error) (*Driver, *[]string, *string) {
	t.Helper()
	var gotArgs []string
	var gotBody string
	d := &Driver{config: Config{Executable: "/fake/agent-deck", Profile: "fixture", SessionID: testRow, RowBinding: testBinding}}
	d.command = func(_ context.Context, stdin io.Reader, args []string) ([]byte, []byte, error) {
		gotArgs = slices.Clone(args)
		if stdin != nil {
			body, err := io.ReadAll(stdin)
			if err != nil {
				t.Fatal(err)
			}
			gotBody = string(body)
		}
		return response, nil, runErr
	}
	return d, &gotArgs, &gotBody
}

func TestRowDriverUsesOnlyCorrelatedPublicCLIAndStdin(t *testing.T) {
	d, args, body := fakeDriver(t, encode(t, submitResponseFor(Queued)), nil)
	op, err := d.SubmitRowOperation(context.Background(), testKey, "synthetic private body")
	if err != nil || op.SendID != testSend {
		t.Fatalf("submit: op=%+v err=%v", op, err)
	}
	want := []string{"-p", "fixture", "session", "send", testRow, "--queue", "--idempotency-key", testKey,
		"--expected-row-binding", testBinding, "--message-file", "-", "--json"}
	if !slices.Equal(*args, want) || *body != "synthetic private body" {
		t.Fatalf("command mismatch: args=%q body=%q", *args, *body)
	}

	d, args, body = fakeDriver(t, encode(t, responseFor(Accepted)), nil)
	if _, err := d.RowOperationStatus(context.Background(), testSend); err != nil {
		t.Fatal(err)
	}
	want = []string{"-p", "fixture", "session", "send-status", testSend, "--json"}
	if !slices.Equal(*args, want) || *body != "" {
		t.Fatalf("status command mismatch: args=%q body=%q", *args, *body)
	}
}

func TestRowDriverAcceptsEveryBoundedOperationState(t *testing.T) {
	states := []State{Queued, Preparing, Accepted, Completed, Refused, BindingChanged, Expired, Indeterminate, ResultUnavailable}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			d, _, _ := fakeDriver(t, encode(t, responseFor(state)), nil)
			op, err := d.RowOperationStatus(context.Background(), testSend)
			if err != nil || op.State != state {
				t.Fatalf("state=%s op=%+v err=%v", state, op, err)
			}
			if state == Completed && (op.AcceptedTurn == nil || op.Completion == nil || op.Content != "synthetic completion") {
				t.Fatalf("completed evidence missing: %+v", op)
			}
			if terminal(state) {
				d, _, _ = fakeDriver(t, encode(t, submitResponseFor(state)), errors.New("fixed CLI exit"))
				op, err = d.SubmitRowOperation(context.Background(), testKey, "synthetic request")
				if err != nil || op.State != state {
					t.Fatalf("terminal submit state=%s op=%+v err=%v", state, op, err)
				}
			}
		})
	}
}

func TestRowDriverRejectsMalformedOversizeDuplicateAndMismatchedJSON(t *testing.T) {
	valid := responseFor(Queued)
	cases := map[string][]byte{
		"malformed":        []byte(`{"schema_version":`),
		"unknown":          []byte(`{"schema_version":1,"send_id":"` + testSend + `","session_id":"` + testRow + `","idempotency_key":"` + testKey + `","row_binding_token":"` + testBinding + `","operation_state":"queued","surprise":true}`),
		"duplicate":        []byte(`{"schema_version":1,"schema_version":1,"send_id":"` + testSend + `","session_id":"` + testRow + `","idempotency_key":"` + testKey + `","row_binding_token":"` + testBinding + `","operation_state":"queued"}`),
		"duplicate_nested": []byte(`{"schema_version":1,"send_id":"` + testSend + `","session_id":"` + testRow + `","idempotency_key":"` + testKey + `","row_binding_token":"` + testBinding + `","operation_state":"accepted","accepted_turn":{"receipt_id":"receipt","receipt_id":"again","instance_id":"` + testRow + `","codex_session_id":"codex-session","turn_generation":"codex-session:turn-a","accepted_at":"now"}}`),
		"oversize":         []byte(`{"padding":"` + strings.Repeat("x", maxResponseBytes) + `"}`),
	}
	changedRow := map[string]any{}
	for k, v := range valid {
		changedRow[k] = v
	}
	changedRow["session_id"] = "different-row"
	cases["row_identity"] = encode(t, changedRow)
	changedBinding := map[string]any{}
	for k, v := range valid {
		changedBinding[k] = v
	}
	changedBinding["row_binding_token"] = "different-binding"
	cases["binding_identity"] = encode(t, changedBinding)
	missingAcceptance := responseFor(Accepted)
	delete(missingAcceptance, "accepted_turn")
	cases["missing_acceptance"] = encode(t, missingAcceptance)

	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			d, _, _ := fakeDriver(t, payload, nil)
			if _, err := d.RowOperationStatus(context.Background(), testSend); !errors.Is(err, ErrProtocol) {
				t.Fatalf("error=%v, want protocol", err)
			}
		})
	}
}

func TestRowDriverRejectsGenerationAndBodyStateMismatch(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"later_generation": func(r map[string]any) {
			r["completion"] = map[string]any{"turn_generation": "codex-session:turn-b"}
		},
		"early_content": func(r map[string]any) {
			r["operation_state"] = Accepted
			delete(r, "completion")
		},
		"wrong_attempt": func(r map[string]any) { r["idempotency_key"] = "another-attempt" },
	} {
		t.Run(name, func(t *testing.T) {
			r := responseFor(Completed)
			mutate(r)
			d, _, _ := fakeDriver(t, encode(t, r), nil)
			if name == "wrong_attempt" {
				_, err := d.SubmitRowOperation(context.Background(), testKey, "body")
				if !errors.Is(err, ErrProtocol) {
					t.Fatalf("error=%v", err)
				}
				return
			}
			if _, err := d.RowOperationStatus(context.Background(), testSend); !errors.Is(err, ErrProtocol) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestRowDriverResponseLossIsRetryableOnlyByIdempotentCaller(t *testing.T) {
	d, _, _ := fakeDriver(t, nil, errors.New("synthetic response loss"))
	if _, err := d.SubmitRowOperation(context.Background(), testKey, "body"); !errors.Is(err, ErrCommand) {
		t.Fatalf("error=%v, want command", err)
	}
}

func TestRowDriverCancellationIsPrivate(t *testing.T) {
	d := &Driver{config: Config{Executable: "/fake/agent-deck", SessionID: testRow, RowBinding: testBinding}}
	d.command = func(ctx context.Context, _ io.Reader, _ []string) ([]byte, []byte, error) {
		<-ctx.Done()
		return []byte("synthetic private body"), []byte("synthetic private stderr"), ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := d.SubmitRowOperation(ctx, testKey, "synthetic private body")
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "private") {
		t.Fatalf("cancellation error=%v", err)
	}
}
