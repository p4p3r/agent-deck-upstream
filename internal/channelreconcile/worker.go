// Package channelreconcile advances one provider-neutral gateway turn using an
// externally owned agent thread. It has no transport or network implementation.
package channelreconcile

import (
	"context"
	"errors"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

var ErrDriver = errors.New("channelreconcile: agent driver failure")

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

type ExternalTurn struct {
	ID     string
	Status string // inProgress, completed, failed, or interrupted
	Reply  string // final reply text; empty is a valid completed result
}

// Driver owns a private agent thread per conversation. InspectThread must
// return the complete, chronological, authoritative stored history; it returns
// an error if history is incomplete, ambiguous, or thread ownership is unsure.
// StartTurn must call accepted with the authoritative ID before returning a
// result, and stop/return if accepted fails. A prepared submission is never
// replayed, even when InspectThread sees no new turn.
type Driver interface {
	OpenThread(context.Context) (string, error)
	ResumeThread(context.Context, string) error
	InspectThread(context.Context, string) ([]ExternalTurn, error)
	StartTurn(context.Context, string, string, func(string) error) (ExternalTurn, error)
}

type Worker struct {
	Store  *channelgateway.Store
	Driver Driver
}

func afterBaseline(turns []ExternalTurn, baseline string) ([]ExternalTurn, bool) {
	seen := map[string]bool{}
	start := 0
	if baseline != "" {
		start = -1
	}
	for i, turn := range turns {
		if turn.ID == "" || seen[turn.ID] {
			return nil, false
		}
		seen[turn.ID] = true
		switch turn.Status {
		case "inProgress", "completed", "failed", "interrupted":
		default:
			return nil, false
		}
		if turn.ID == baseline {
			start = i + 1
		}
	}
	if start < 0 {
		return nil, false
	}
	return turns[start:], true
}

func (w *Worker) unresolved(ctx context.Context, t *channelgateway.Turn, a channelgateway.Attempt) (Result, error) {
	r := Result{TurnID: t.ID, State: NeedsReconciliation}
	err := w.Store.MarkNeedsReconciliation(ctx, t.ID, a.ID, a.State)
	if errors.Is(err, channelgateway.ErrConflict) {
		// A concurrent callback or completion won the race. Report its
		// persisted disposition rather than the stale inspection's result.
		current, completed, readErr := w.Store.AttemptStatus(ctx, t.ID)
		if readErr != nil {
			return r, readErr
		}
		if current.ID != a.ID {
			return r, channelgateway.ErrConflict
		}
		if completed && current.State == channelgateway.AttemptCompleted {
			r.State = Completed
		} else if current.State == channelgateway.AttemptAccepted {
			r.State = InProgress
		} else if current.State != channelgateway.NeedsReconciliation {
			return r, channelgateway.ErrConflict
		}
		return r, nil
	}
	return r, err
}

func (w *Worker) reconcile(ctx context.Context, t *channelgateway.Turn, a channelgateway.Attempt, threadID string) (Result, error) {
	r := Result{TurnID: t.ID, State: NeedsReconciliation}
	turns, err := w.Driver.InspectThread(ctx, threadID)
	if err != nil {
		return r, ErrDriver
	}
	after, valid := afterBaseline(turns, a.BaselineTurnID)
	if !valid || len(after) != 1 || a.ExternalTurnID != "" && after[0].ID != a.ExternalTurnID {
		return w.unresolved(ctx, t, a)
	}
	ext := after[0]
	if err := w.Store.AcceptAttempt(ctx, t.ID, a.ID, ext.ID); err != nil {
		return r, err
	}
	a.State = channelgateway.AttemptAccepted
	switch ext.Status {
	case "completed":
		_, err := w.Store.CompleteAttempt(ctx, t.ID, a.ID, ext.ID, ext.Reply)
		if err != nil {
			return r, err
		}
		return Result{TurnID: t.ID, State: Completed}, nil
	case "inProgress":
		return Result{TurnID: t.ID, State: InProgress}, nil
	default:
		return w.unresolved(ctx, t, a)
	}
}

// RunOne reserves or recovers one ledger turn. Calling it concurrently through
// separate Store handles is safe: only the creator of a prepared attempt may
// submit; all other callers inspect the persisted attempt.
func (w *Worker) RunOne(ctx context.Context, conversationID string) (Result, error) {
	if w == nil || w.Store == nil || w.Driver == nil || conversationID == "" {
		return Result{}, channelgateway.ErrInvalid
	}
	t, err := w.Store.NextTurn(ctx, conversationID)
	if err != nil {
		return Result{}, err
	}
	if t == nil {
		return Result{State: Idle}, nil
	}
	r := Result{TurnID: t.ID, State: NeedsReconciliation}
	b, err := w.Store.AgentBinding(ctx, conversationID)
	if err != nil {
		return r, err
	}
	threadID := b.AgentThreadID
	newThread := false
	if threadID == "" {
		if t.AttemptState != channelgateway.Unprepared {
			return r, channelgateway.ErrConflict
		}
		threadID, err = w.Driver.OpenThread(ctx)
		if err != nil || threadID == "" {
			return r, ErrDriver
		}
		if err := w.Store.BindAgentThread(ctx, conversationID, threadID); err != nil {
			// A racing opener's external thread is an unused orphan.
			return r, err
		}
		newThread = true
	}
	if t.AttemptState != channelgateway.Unprepared {
		return w.reconcile(ctx, t, channelgateway.Attempt{
			ID: t.AttemptID, State: t.AttemptState, BaselineTurnID: t.BaselineTurnID,
			ExternalTurnID: t.ExternalTurnID,
		}, threadID)
	}
	if t.AcceptanceID != "" { // legacy acceptance cannot be submitted again
		return r, channelgateway.ErrConflict
	}
	turns, err := w.Driver.InspectThread(ctx, threadID)
	if err != nil {
		return r, ErrDriver
	}
	after, valid := afterBaseline(turns, b.LastExternalTurnID)
	if !valid || len(after) != 0 {
		return r, channelgateway.ErrConflict
	}
	if !newThread {
		if err := w.Driver.ResumeThread(ctx, threadID); err != nil {
			return r, ErrDriver
		}
	}
	a, created, err := w.Store.PrepareAttempt(ctx, t.ID, b.LastExternalTurnID)
	if err != nil {
		return r, err
	}
	if !created {
		return w.reconcile(ctx, t, a, threadID)
	}
	acceptedID := ""
	ext, err := w.Driver.StartTurn(ctx, threadID, t.Body, func(externalTurnID string) error {
		if err := w.Store.AcceptAttempt(ctx, t.ID, a.ID, externalTurnID); err != nil {
			return err
		}
		acceptedID = externalTurnID
		return nil
	})
	if err != nil || acceptedID == "" || ext.ID != acceptedID {
		// The request may have reached the agent before the response failed.
		// A later invocation must inspect it; it must never call StartTurn again.
		if acceptedID == "" {
			_, _ = w.unresolved(ctx, t, a)
		}
		return r, ErrDriver
	}
	a.State = channelgateway.AttemptAccepted
	switch ext.Status {
	case "completed":
		_, err := w.Store.CompleteAttempt(ctx, t.ID, a.ID, ext.ID, ext.Reply)
		if err != nil {
			return r, err
		}
		return Result{TurnID: t.ID, State: Completed}, nil
	case "inProgress":
		return Result{TurnID: t.ID, State: InProgress}, nil
	default:
		return w.unresolved(ctx, t, a)
	}
}
