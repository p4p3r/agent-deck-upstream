// Package channelreconcile advances provider-neutral gateway turns through
// durable Agent Deck row operations.
package channelreconcile

import (
	"context"
	"errors"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/rowdriver"
)

var ErrDriver = errors.New("channelreconcile: row driver failure")

type State string

const (
	Idle                State = "idle"
	Completed           State = "completed"
	InProgress          State = "in_progress"
	NeedsReconciliation State = "needs_reconciliation"
)

type Result struct {
	TurnID string
	State  State
}

// ExternalTurn remains only as compile-time compatibility for isolated legacy
// adapter packages. Runtime reconciliation never consumes it.
type ExternalTurn struct {
	ID     string
	Status string
	Reply  string
}

type RowDriver interface {
	SubmitRowOperation(context.Context, string, string) (rowdriver.Operation, error)
	RowOperationStatus(context.Context, string) (rowdriver.Operation, error)
}

type Worker struct {
	Store     *channelgateway.Store
	RowDriver RowDriver
	// Driver is ignored. It keeps old isolated test fixtures source-compatible
	// while preventing any runtime path from owning a Codex app-server thread.
	Driver any
}

func gatewayOperation(op rowdriver.Operation) (channelgateway.RowOperation, bool) {
	next := channelgateway.RowOperation{
		ID: op.SendID, State: channelgateway.RowOperationState(op.State), Content: op.Content,
	}
	if op.AcceptedTurn != nil {
		next.AcceptedCodexSession = op.AcceptedTurn.CodexSessionID
		next.AcceptedGeneration = op.AcceptedTurn.TurnGeneration
	}
	if op.Completion != nil {
		next.CompletionGeneration = op.Completion.TurnGeneration
	}
	switch op.State {
	case rowdriver.Refused, rowdriver.BindingChanged, rowdriver.Expired,
		rowdriver.Indeterminate, rowdriver.ResultUnavailable:
		next.TerminalError = string(op.State)
		return next, true
	case rowdriver.Completed:
		return next, true
	case rowdriver.Queued, rowdriver.Preparing, rowdriver.Accepted:
		return next, false
	default:
		return channelgateway.RowOperation{}, false
	}
}

// RunOne reserves or recovers one ledger turn. Loss of the submit response is
// safe: the durable gateway attempt ID is reused as the core idempotency key.
// Once a send ID is stored, only send-status is called for that operation.
func (w *Worker) RunOne(ctx context.Context, conversationID string) (Result, error) {
	if w == nil || w.Store == nil || w.RowDriver == nil || conversationID == "" {
		return Result{}, channelgateway.ErrInvalid
	}
	turn, err := w.Store.NextTurn(ctx, conversationID)
	if err != nil {
		return Result{}, err
	}
	if turn == nil {
		return Result{State: Idle}, nil
	}
	result := Result{TurnID: turn.ID, State: InProgress}
	attempt, _, err := w.Store.PrepareRowOperation(ctx, turn.ID)
	if err != nil {
		return result, err
	}
	var op rowdriver.Operation
	if attempt.Operation.ID == "" {
		op, err = w.RowDriver.SubmitRowOperation(ctx, attempt.ID, turn.Body)
	} else {
		op, err = w.RowDriver.RowOperationStatus(ctx, attempt.Operation.ID)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return result, err
		}
		if errors.Is(err, rowdriver.ErrConfig) || errors.Is(err, rowdriver.ErrProtocol) {
			return result, channelgateway.ErrConflict
		}
		return result, ErrDriver
	}
	if op.IdempotencyKey != attempt.ID {
		return result, channelgateway.ErrConflict
	}
	next, terminal := gatewayOperation(op)
	if next.ID == "" {
		return result, channelgateway.ErrConflict
	}
	if _, err := w.Store.ApplyRowOperation(ctx, turn.ID, attempt.ID, next); err != nil {
		return result, err
	}
	if terminal {
		result.State = Completed
	}
	return result, nil
}
