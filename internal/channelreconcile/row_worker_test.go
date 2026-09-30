package channelreconcile

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/rowdriver"
)

type fakeRowDriver struct {
	submit func(string, string) (rowdriver.Operation, error)
	status func(string) (rowdriver.Operation, error)
}

func (f *fakeRowDriver) SubmitRowOperation(_ context.Context, key, body string) (rowdriver.Operation, error) {
	return f.submit(key, body)
}

func (f *fakeRowDriver) RowOperationStatus(_ context.Context, id string) (rowdriver.Operation, error) {
	return f.status(id)
}

func rowWorkerFixture(t *testing.T, bodies ...string) (*channelgateway.Store, string) {
	t.Helper()
	s, err := channelgateway.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.CreateConversation(context.Background(), channelgateway.Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor",
		RowInstanceID: "immutable-row", RowBinding: "opaque-binding",
		Mode: channelgateway.ChannelStream, AllowedSenders: []string{"sender"},
	}); err != nil {
		t.Fatal(err)
	}
	for i, body := range bodies {
		_, err := s.Ingest(context.Background(), channelgateway.Inbound{
			ConversationID: "conversation", EventID: "event-" + string(rune('a'+i)),
			MessageID: "message-" + string(rune('a'+i)), ChannelID: "channel", SenderID: "sender", Body: body,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return s, "conversation"
}

func operation(key string, state rowdriver.State) rowdriver.Operation {
	op := rowdriver.Operation{
		SchemaVersion: 1, SendID: "send-one", SessionID: "immutable-row",
		IdempotencyKey: key, RowBinding: "opaque-binding", State: state,
	}
	if state == rowdriver.Accepted || state == rowdriver.Completed || state == rowdriver.ResultUnavailable {
		op.AcceptedTurn = &rowdriver.AcceptedTurn{
			ReceiptID: "receipt", InstanceID: "immutable-row", CodexSessionID: "codex-session",
			TurnGeneration: "codex-session:turn-a", AcceptedAt: "now",
		}
	}
	if state == rowdriver.Completed {
		op.Completion = &rowdriver.Completion{TurnGeneration: "codex-session:turn-a"}
		op.Content = "synthetic assistant result"
	}
	if state == rowdriver.Refused || state == rowdriver.BindingChanged || state == rowdriver.Expired ||
		state == rowdriver.Indeterminate || state == rowdriver.ResultUnavailable {
		op.Code = string(state)
	}
	return op
}

func TestRowReconcileRepeatsLostSubmitWithSameGlobalAttemptID(t *testing.T) {
	store, conversation := rowWorkerFixture(t, "synthetic request")
	var keys, bodies []string
	count := 0
	driver := &fakeRowDriver{
		submit: func(key, body string) (rowdriver.Operation, error) {
			keys, bodies = append(keys, key), append(bodies, body)
			count++
			if count == 1 {
				return rowdriver.Operation{}, errors.New("response lost")
			}
			return operation(key, rowdriver.Queued), nil
		},
		status: func(string) (rowdriver.Operation, error) {
			t.Fatal("status before send id")
			return rowdriver.Operation{}, nil
		},
	}
	w := &Worker{Store: store, RowDriver: driver}
	if _, err := w.RunOne(context.Background(), conversation); !errors.Is(err, ErrDriver) {
		t.Fatalf("first result=%v", err)
	}
	if _, err := w.RunOne(context.Background(), conversation); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] || !slices.Equal(bodies, []string{"synthetic request", "synthetic request"}) {
		t.Fatalf("submit retries keys=%q bodies=%q", keys, bodies)
	}
}

func TestRowReconcilePersistsAcceptedTupleAndOnlyExactCompletion(t *testing.T) {
	store, conversation := rowWorkerFixture(t, "synthetic request")
	var key string
	states := []rowdriver.State{rowdriver.Accepted, rowdriver.Completed}
	driver := &fakeRowDriver{
		submit: func(k, _ string) (rowdriver.Operation, error) {
			key = k
			return operation(k, rowdriver.Queued), nil
		},
		status: func(id string) (rowdriver.Operation, error) {
			if id != "send-one" || len(states) == 0 {
				t.Fatal("unexpected status")
			}
			state := states[0]
			states = states[1:]
			return operation(key, state), nil
		},
	}
	w := &Worker{Store: store, RowDriver: driver}
	for i, want := range []State{InProgress, InProgress, Completed} {
		got, err := w.RunOne(context.Background(), conversation)
		if err != nil || got.State != want {
			t.Fatalf("step %d result=%+v err=%v", i, got, err)
		}
	}
	items, err := store.PendingOutbox(context.Background(), conversation, 10)
	if err != nil || len(items) != 1 || items[0].Kind != "reply" || items[0].Body != "synthetic assistant result" {
		t.Fatalf("outbox=%+v err=%v", items, err)
	}
}

func TestRowReconcileTerminalErrorsCreateOneBodyFreeStatusReceipt(t *testing.T) {
	states := []rowdriver.State{rowdriver.Refused, rowdriver.BindingChanged, rowdriver.Expired,
		rowdriver.Indeterminate, rowdriver.ResultUnavailable}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			store, conversation := rowWorkerFixture(t, "synthetic request")
			driver := &fakeRowDriver{
				submit: func(key, _ string) (rowdriver.Operation, error) { return operation(key, state), nil },
				status: func(string) (rowdriver.Operation, error) {
					t.Fatal("terminal submit was polled")
					return rowdriver.Operation{}, nil
				},
			}
			w := &Worker{Store: store, RowDriver: driver}
			got, err := w.RunOne(context.Background(), conversation)
			if err != nil || got.State != Completed {
				t.Fatalf("result=%+v err=%v", got, err)
			}
			items, err := store.PendingOutbox(context.Background(), conversation, 10)
			if err != nil || len(items) != 1 || items[0].Kind != "status" || items[0].Body != "" {
				t.Fatalf("outbox=%+v err=%v", items, err)
			}
			if got, err := w.RunOne(context.Background(), conversation); err != nil || got.State != Idle {
				t.Fatalf("terminal replay=%+v err=%v", got, err)
			}
		})
	}
}

func TestRowReconcileRejectsIdentityAndGenerationMismatch(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*rowdriver.Operation)
		want   error
	}{
		"attempt": {
			mutate: func(op *rowdriver.Operation) { op.IdempotencyKey = "different-attempt" },
			want:   channelgateway.ErrConflict,
		},
		"generation": {
			mutate: func(op *rowdriver.Operation) { op.Completion.TurnGeneration = "codex-session:turn-b" },
			want:   channelgateway.ErrInvalid,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, conversation := rowWorkerFixture(t, "synthetic request")
			driver := &fakeRowDriver{
				submit: func(key, _ string) (rowdriver.Operation, error) {
					op := operation(key, rowdriver.Completed)
					tc.mutate(&op)
					return op, nil
				},
				status: func(string) (rowdriver.Operation, error) { return rowdriver.Operation{}, errors.New("unexpected") },
			}
			_, err := (&Worker{Store: store, RowDriver: driver}).RunOne(context.Background(), conversation)
			if !errors.Is(err, tc.want) {
				t.Fatalf("mismatch error=%v, want %v", err, tc.want)
			}
			items, listErr := store.PendingOutbox(context.Background(), conversation, 10)
			if listErr != nil || len(items) != 0 {
				t.Fatalf("mismatch produced outbox: %+v err=%v", items, listErr)
			}
		})
	}
}

func TestRowReconcileProtocolFailureStopsInsteadOfRetrying(t *testing.T) {
	store, conversation := rowWorkerFixture(t, "synthetic request")
	driver := &fakeRowDriver{
		submit: func(string, string) (rowdriver.Operation, error) {
			return rowdriver.Operation{}, rowdriver.ErrProtocol
		},
		status: func(string) (rowdriver.Operation, error) {
			t.Fatal("status after rejected protocol response")
			return rowdriver.Operation{}, nil
		},
	}
	if _, err := (&Worker{Store: store, RowDriver: driver}).RunOne(context.Background(), conversation); !errors.Is(err, channelgateway.ErrConflict) {
		t.Fatalf("protocol error=%v, want fatal conflict", err)
	}
}
