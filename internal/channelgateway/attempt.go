package channelgateway

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

// AttemptState makes an unfinished external submission explicit in the ledger.
type AttemptState string

const (
	Unprepared          AttemptState = "unprepared"
	Prepared            AttemptState = "prepared"
	AttemptAccepted     AttemptState = "accepted"
	NeedsReconciliation AttemptState = "needs_reconciliation"
	AttemptCompleted    AttemptState = "completed"
)

type Binding struct {
	AgentThreadID      string
	LastExternalTurnID string
}

type Attempt struct {
	ID             string
	State          AttemptState
	BaselineTurnID string
	ExternalTurnID string
}

// RowOperationState is the monotonic public Agent Deck operation state stored
// by the gateway. Empty means the gateway attempt is durable but the CLI
// response carrying its send ID has not yet been committed.
type RowOperationState string

const (
	RowQueued            RowOperationState = "queued"
	RowPreparing         RowOperationState = "preparing"
	RowAccepted          RowOperationState = "accepted"
	RowCompleted         RowOperationState = "completed"
	RowRefused           RowOperationState = "refused"
	RowBindingChanged    RowOperationState = "binding_changed"
	RowExpired           RowOperationState = "expired"
	RowIndeterminate     RowOperationState = "indeterminate"
	RowResultUnavailable RowOperationState = "result_unavailable"
)

type RowOperation struct {
	ID                   string
	State                RowOperationState
	AcceptedCodexSession string
	AcceptedGeneration   string
	CompletionGeneration string
	Content              string
	TerminalError        string
}

type RowAttempt struct {
	ID        string
	Operation RowOperation
}

// PrepareRowOperation durably assigns the gateway attempt ID used as the
// global core idempotency key. A restart may repeat SubmitRowOperation with
// this same ID until the core send ID is committed.
func (s *Store) PrepareRowOperation(ctx context.Context, turnID string) (RowAttempt, bool, error) {
	var out RowAttempt
	created := false
	if turnID == "" {
		return out, false, ErrInvalid
	}
	err := s.write(ctx, func(tx *writeTx) error {
		var status string
		var attemptState AttemptState
		err := tx.row(`SELECT status,attempt_id,attempt_state,row_operation_id,operation_state,
			accepted_codex_session_id,accepted_turn_generation,terminal_error
			FROM turns WHERE id=?`, turnID).
			Scan(&status, &out.ID, &attemptState, &out.Operation.ID, &out.Operation.State,
				&out.Operation.AcceptedCodexSession, &out.Operation.AcceptedGeneration, &out.Operation.TerminalError)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if status != "active" {
			return ErrConflict
		}
		if out.ID != "" {
			if attemptState != Prepared && attemptState != AttemptAccepted {
				return ErrConflict
			}
			return nil
		}
		if attemptState != Unprepared {
			return ErrConflict
		}
		out.ID = uuid.NewString()
		if _, err := tx.exec(`UPDATE turns SET attempt_id=?,attempt_state='prepared' WHERE id=?`, out.ID, turnID); err != nil {
			return err
		}
		created = true
		return nil
	})
	return out, created, err
}

// RowAttemptStatus reads the persisted operation without reserving new work.
func (s *Store) RowAttemptStatus(ctx context.Context, turnID string) (RowAttempt, bool, error) {
	var out RowAttempt
	if turnID == "" {
		return out, false, ErrInvalid
	}
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status,attempt_id,row_operation_id,operation_state,
		accepted_codex_session_id,accepted_turn_generation,terminal_error FROM turns WHERE id=?`, turnID).
		Scan(&status, &out.ID, &out.Operation.ID, &out.Operation.State,
			&out.Operation.AcceptedCodexSession, &out.Operation.AcceptedGeneration, &out.Operation.TerminalError)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, ErrNotFound
	}
	if err != nil {
		return out, false, ErrStorage
	}
	return out, status == "completed", nil
}

// ApplyRowOperation persists one monotonic core observation. Only an exact
// completed generation can create a reply. Fixed terminal states create one
// body-free status receipt and finish the gateway turn without a reply.
func (s *Store) ApplyRowOperation(ctx context.Context, turnID, attemptID string, next RowOperation) (*OutboxItem, error) {
	if turnID == "" || attemptID == "" || next.ID == "" || !validRowOperationState(next.State) {
		return nil, ErrInvalid
	}
	if next.State == RowAccepted || next.State == RowCompleted || next.State == RowResultUnavailable {
		if next.AcceptedCodexSession == "" || next.AcceptedGeneration == "" {
			return nil, ErrInvalid
		}
	} else if next.AcceptedCodexSession != "" || next.AcceptedGeneration != "" {
		return nil, ErrInvalid
	}
	if next.State == RowCompleted {
		if next.CompletionGeneration != next.AcceptedGeneration || next.TerminalError != "" {
			return nil, ErrInvalid
		}
	} else if next.CompletionGeneration != "" {
		return nil, ErrInvalid
	}
	if rowTerminalError(next.State) {
		if next.TerminalError != string(next.State) || next.Content != "" {
			return nil, ErrInvalid
		}
	} else if next.TerminalError != "" {
		return nil, ErrInvalid
	}
	var out *OutboxItem
	err := s.write(ctx, func(tx *writeTx) error {
		var conversationID, status, storedAttempt, mode, root string
		var attemptState AttemptState
		var current RowOperation
		err := tx.row(`SELECT t.conversation_id,t.status,t.attempt_id,t.attempt_state,t.row_operation_id,
			t.operation_state,t.accepted_codex_session_id,t.accepted_turn_generation,t.terminal_error,
			c.mode,s.root_thread_id
			FROM turns t JOIN conversations c ON c.id=t.conversation_id JOIN segments s ON s.id=t.segment_id
			WHERE t.id=?`, turnID).
			Scan(&conversationID, &status, &storedAttempt, &attemptState, &current.ID, &current.State,
				&current.AcceptedCodexSession, &current.AcceptedGeneration, &current.TerminalError, &mode, &root)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if storedAttempt != attemptID || current.ID != "" && current.ID != next.ID {
			return ErrConflict
		}
		if !rowTransitionAllowed(current.State, next.State) ||
			current.AcceptedCodexSession != "" && current.AcceptedCodexSession != next.AcceptedCodexSession ||
			current.AcceptedGeneration != "" && current.AcceptedGeneration != next.AcceptedGeneration ||
			current.TerminalError != "" && current.TerminalError != next.TerminalError {
			return ErrConflict
		}
		if status == "completed" {
			if current.State != next.State || !rowTerminal(next.State) {
				return ErrConflict
			}
			out, err = loadTurnOutbox(tx, turnID)
			if err != nil {
				return err
			}
			if next.State == RowCompleted {
				if out == nil && next.Content != "" || out != nil && (out.Kind != "reply" || out.Body != next.Content) {
					return ErrConflict
				}
			} else if out == nil || out.Kind != "status" || out.Body != "" {
				return ErrConflict
			}
			return nil
		}
		if status != "active" || attemptState != Prepared && attemptState != AttemptAccepted {
			return ErrConflict
		}
		newAttemptState := Prepared
		if next.State == RowAccepted {
			newAttemptState = AttemptAccepted
		}
		if rowTerminal(next.State) {
			newAttemptState = AttemptCompleted
		}
		if _, err := tx.exec(`UPDATE turns SET row_operation_id=?,operation_state=?,
			accepted_codex_session_id=?,accepted_turn_generation=?,terminal_error=?,attempt_state=? WHERE id=?`,
			next.ID, next.State, next.AcceptedCodexSession, next.AcceptedGeneration, next.TerminalError,
			newAttemptState, turnID); err != nil {
			return err
		}
		if !rowTerminal(next.State) {
			return nil
		}
		if _, err := tx.exec(`UPDATE turns SET status='completed' WHERE id=?`, turnID); err != nil {
			return err
		}
		kind, body := "status", ""
		if next.State == RowCompleted {
			kind, body = "reply", next.Content
		}
		if kind == "status" || body != "" {
			threadID := root
			if Mode(mode) == ChannelStream {
				threadID = ""
			}
			out = &OutboxItem{ID: uuid.NewString(), ConversationID: conversationID, TurnID: turnID,
				Kind: kind, ThreadID: threadID, Body: body, State: PendingDelivery}
			if _, err := tx.exec(`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,body,state)
				VALUES(?,?,?,?,?,?,'pending')`, out.ID, conversationID, turnID, kind, threadID, body); err != nil {
				return err
			}
		}
		if _, err := tx.exec(`UPDATE conversations SET last_row_operation_id=? WHERE id=?`, next.ID, conversationID); err != nil {
			return err
		}
		return promotePending(tx, conversationID)
	})
	return out, err
}

func validRowOperationState(state RowOperationState) bool {
	switch state {
	case RowQueued, RowPreparing, RowAccepted, RowCompleted, RowRefused, RowBindingChanged,
		RowExpired, RowIndeterminate, RowResultUnavailable:
		return true
	default:
		return false
	}
}

func rowTerminalError(state RowOperationState) bool {
	return state == RowRefused || state == RowBindingChanged || state == RowExpired ||
		state == RowIndeterminate || state == RowResultUnavailable
}

func rowTerminal(state RowOperationState) bool {
	return state == RowCompleted || rowTerminalError(state)
}

func rowTransitionAllowed(from, to RowOperationState) bool {
	if from == to || from == "" {
		return true
	}
	if rowTerminal(from) {
		return false
	}
	switch from {
	case RowQueued:
		return to == RowPreparing || to == RowAccepted || rowTerminal(to)
	case RowPreparing:
		return to == RowAccepted || rowTerminal(to)
	case RowAccepted:
		return to == RowCompleted || to == RowResultUnavailable
	default:
		return false
	}
}

// EmptyCreateReplacementSafe proves that this conductor has never reserved or
// submitted a turn on the currently bound thread. Accepted inbound messages
// which have not become turns are deliberately allowed to survive rotation.
func (s *Store) EmptyCreateReplacementSafe(ctx context.Context, conversationID, currentThreadID string) (bool, error) {
	if conversationID == "" || currentThreadID == "" {
		return false, ErrInvalid
	}
	state, err := readEmptyCreateState(s.db.QueryRowContext(ctx, emptyCreateStateSQL, conversationID, conversationID, conversationID, conversationID))
	if err != nil {
		return false, err
	}
	return state.threadID == currentThreadID && state.empty(), nil
}

// ReplaceEmptyCreateThread atomically rotates an unused create binding. A
// concurrent or corrupted ledger with any turn/attempt/submission evidence is
// rejected, even if an earlier read considered replacement safe.
func (s *Store) ReplaceEmptyCreateThread(ctx context.Context, conversationID, oldThreadID, newThreadID string) error {
	if conversationID == "" || oldThreadID == "" || newThreadID == "" || oldThreadID == newThreadID {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		state, err := readEmptyCreateState(tx.row(emptyCreateStateSQL, conversationID, conversationID, conversationID, conversationID))
		if err != nil {
			return err
		}
		if state.threadID != oldThreadID || !state.empty() {
			return ErrConflict
		}
		var owners int
		if err := tx.row(`SELECT count(*) FROM conversations WHERE agent_thread_id=? AND id<>?`, newThreadID, conversationID).Scan(&owners); err != nil {
			return ErrStorage
		}
		if owners != 0 {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE conversations SET agent_thread_id=? WHERE id=? AND agent_thread_id=?`, newThreadID, conversationID, oldThreadID)
		return err
	})
}

const emptyCreateStateSQL = `SELECT c.agent_thread_id,c.last_external_turn_id,c.next_turn,
	(SELECT count(*) FROM turns WHERE conversation_id=?),
	(SELECT count(*) FROM inbound_events WHERE conversation_id=? AND turn_id IS NOT NULL),
	(SELECT count(*) FROM outbox WHERE conversation_id=?)
	FROM conversations c WHERE c.id=?`

type emptyCreateState struct {
	threadID, cursor             string
	nextTurn, turns, linked, out int64
}

func (s emptyCreateState) empty() bool {
	return s.cursor == "" && s.nextTurn == 0 && s.turns == 0 && s.linked == 0 && s.out == 0
}

func readEmptyCreateState(row interface{ Scan(...any) error }) (emptyCreateState, error) {
	var s emptyCreateState
	err := row.Scan(&s.threadID, &s.cursor, &s.nextTurn, &s.turns, &s.linked, &s.out)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotFound
	}
	if err != nil {
		return s, ErrStorage
	}
	return s, nil
}

func (s *Store) AgentBinding(ctx context.Context, conversationID string) (Binding, error) {
	var b Binding
	if conversationID == "" {
		return b, ErrInvalid
	}
	err := s.db.QueryRowContext(ctx, `SELECT agent_thread_id,last_external_turn_id FROM conversations WHERE id=?`, conversationID).
		Scan(&b.AgentThreadID, &b.LastExternalTurnID)
	if errors.Is(err, sql.ErrNoRows) {
		return b, ErrNotFound
	}
	if err != nil {
		return b, ErrStorage
	}
	return b, nil
}

// AttemptStatus reads an exact ledger turn without reserving the next one.
// It exposes no inbound or outbound payload.
func (s *Store) AttemptStatus(ctx context.Context, turnID string) (Attempt, bool, error) {
	var a Attempt
	if turnID == "" {
		return a, false, ErrInvalid
	}
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status,attempt_id,attempt_state,baseline_turn_id,external_turn_id
		FROM turns WHERE id=?`, turnID).
		Scan(&status, &a.ID, &a.State, &a.BaselineTurnID, &a.ExternalTurnID)
	if errors.Is(err, sql.ErrNoRows) {
		return a, false, ErrNotFound
	}
	if err != nil {
		return a, false, ErrStorage
	}
	return a, status == "completed", nil
}

// BindAgentThread is immutable. If a concurrent opener loses the race, its
// unbound external thread is an orphan and must never receive a gateway turn.
func (s *Store) BindAgentThread(ctx context.Context, conversationID, threadID string) error {
	if conversationID == "" || threadID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var existing string
		err := tx.row(`SELECT agent_thread_id FROM conversations WHERE id=?`, conversationID).Scan(&existing)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if existing == threadID {
			return nil
		}
		if existing != "" {
			return ErrConflict
		}
		var owners int
		if err := tx.row(`SELECT count(*) FROM conversations WHERE agent_thread_id=? AND id<>?`,
			threadID, conversationID).Scan(&owners); err != nil {
			return ErrStorage
		}
		if owners != 0 {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE conversations SET agent_thread_id=? WHERE id=?`, threadID, conversationID)
		return err
	})
}

// BindAgentThreadAtCursor adopts a private thread whose prior completed turns
// were verified by the caller. The thread and historical cursor commit in one
// transaction, before any gateway turn can be submitted. It only initializes
// an unbound conversation without ledger turns. An exact retry is idempotent.
func (s *Store) BindAgentThreadAtCursor(ctx context.Context, conversationID, threadID, cursor string) error {
	if conversationID == "" || threadID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var existing, currentCursor string
		err := tx.row(`SELECT agent_thread_id,last_external_turn_id FROM conversations WHERE id=?`, conversationID).
			Scan(&existing, &currentCursor)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		var turns, owners int
		if err := tx.row(`SELECT count(*) FROM turns WHERE conversation_id=?`, conversationID).Scan(&turns); err != nil {
			return ErrStorage
		}
		if turns != 0 {
			return ErrConflict
		}
		if existing != "" {
			if existing == threadID && currentCursor == cursor {
				return nil
			}
			return ErrConflict
		}
		if currentCursor != "" {
			return ErrConflict
		}
		if err := tx.row(`SELECT count(*) FROM conversations WHERE agent_thread_id=? AND id<>?`,
			threadID, conversationID).Scan(&owners); err != nil {
			return ErrStorage
		}
		if owners != 0 {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE conversations SET agent_thread_id=?,last_external_turn_id=? WHERE id=?`,
			threadID, cursor, conversationID)
		return err
	})
}

// PrepareAttempt snapshots the last completed external turn in the same write
// transaction that claims the ledger turn. Only created=true may submit it.
func (s *Store) PrepareAttempt(ctx context.Context, turnID, expectedBaseline string) (Attempt, bool, error) {
	var a Attempt
	var created bool
	if turnID == "" {
		return a, false, ErrInvalid
	}
	err := s.write(ctx, func(tx *writeTx) error {
		var status, acceptanceID, threadID, cursor string
		err := tx.row(`SELECT t.status,t.acceptance_id,t.attempt_id,t.attempt_state,
			t.baseline_turn_id,t.external_turn_id,c.agent_thread_id,c.last_external_turn_id
			FROM turns t JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?`, turnID).
			Scan(&status, &acceptanceID, &a.ID, &a.State, &a.BaselineTurnID, &a.ExternalTurnID, &threadID, &cursor)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if status != "active" {
			return ErrConflict
		}
		if a.State != Unprepared {
			return nil
		}
		if threadID == "" || cursor != expectedBaseline || acceptanceID != "" || a.ID != "" {
			return ErrConflict
		}
		a = Attempt{ID: uuid.NewString(), State: Prepared, BaselineTurnID: cursor}
		if _, err := tx.exec(`UPDATE turns SET attempt_id=?,attempt_state='prepared',baseline_turn_id=? WHERE id=?`,
			a.ID, cursor, turnID); err != nil {
			return err
		}
		created = true
		return nil
	})
	return a, created, err
}

// AcceptAttempt is called as soon as the external service supplies its
// authoritative turn ID. A late callback may resolve a racing uncertainty mark.
func (s *Store) AcceptAttempt(ctx context.Context, turnID, attemptID, externalTurnID string) error {
	if turnID == "" || attemptID == "" || externalTurnID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var status, id, existing, conversationID string
		var state AttemptState
		err := tx.row(`SELECT status,attempt_id,attempt_state,external_turn_id,conversation_id FROM turns WHERE id=?`, turnID).
			Scan(&status, &id, &state, &existing, &conversationID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if id != attemptID || existing != "" && existing != externalTurnID {
			return ErrConflict
		}
		if status == "completed" && state == AttemptCompleted && existing == externalTurnID {
			return nil
		}
		if status != "active" || state != Prepared && state != AttemptAccepted && state != NeedsReconciliation {
			return ErrConflict
		}
		var prior int
		if err := tx.row(`SELECT count(*) FROM turns WHERE conversation_id=? AND external_turn_id=? AND id<>?`,
			conversationID, externalTurnID, turnID).Scan(&prior); err != nil {
			return ErrStorage
		}
		if prior != 0 {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE turns SET attempt_state='accepted',external_turn_id=?,acceptance_id=? WHERE id=?`,
			externalTurnID, externalTurnID, turnID)
		return err
	})
}

// MarkNeedsReconciliation uses the state observed by an inspector as a CAS, so
// a stale observation cannot overwrite an acceptance callback or completion.
func (s *Store) MarkNeedsReconciliation(ctx context.Context, turnID, attemptID string, expected AttemptState) error {
	if turnID == "" || attemptID == "" || expected != Prepared && expected != AttemptAccepted && expected != NeedsReconciliation {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var status, id string
		var state AttemptState
		err := tx.row(`SELECT status,attempt_id,attempt_state FROM turns WHERE id=?`, turnID).Scan(&status, &id, &state)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if status != "active" || id != attemptID || state != expected {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE turns SET attempt_state='needs_reconciliation' WHERE id=?`, turnID)
		return err
	})
}

// CompleteAttempt atomically commits the terminal reply, outbox record, and
// external cursor. An identical retry returns the original outbox item.
func (s *Store) CompleteAttempt(ctx context.Context, turnID, attemptID, externalTurnID, replyBody string) (*OutboxItem, error) {
	if turnID == "" || attemptID == "" || externalTurnID == "" {
		return nil, ErrInvalid
	}
	var out *OutboxItem
	err := s.write(ctx, func(tx *writeTx) error {
		var conversationID, status, id, externalID, baseline, cursor, mode, root string
		var state AttemptState
		err := tx.row(`SELECT t.conversation_id,t.status,t.attempt_id,t.attempt_state,t.external_turn_id,
			t.baseline_turn_id,c.last_external_turn_id,c.mode,s.root_thread_id
			FROM turns t JOIN conversations c ON c.id=t.conversation_id JOIN segments s ON s.id=t.segment_id
			WHERE t.id=?`, turnID).
			Scan(&conversationID, &status, &id, &state, &externalID, &baseline, &cursor, &mode, &root)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if id != attemptID || externalID != externalTurnID {
			return ErrConflict
		}
		if status == "completed" && state == AttemptCompleted {
			out, err = loadTurnOutbox(tx, turnID)
			if err != nil {
				return err
			}
			if out == nil && replyBody != "" || out != nil && out.Body != replyBody {
				return ErrConflict
			}
			return nil
		}
		if status != "active" || state != AttemptAccepted || baseline != cursor {
			return ErrConflict
		}
		if _, err := tx.exec(`UPDATE turns SET status='completed',attempt_state='completed' WHERE id=?`, turnID); err != nil {
			return err
		}
		if replyBody != "" {
			threadID := root
			if Mode(mode) == ChannelStream {
				threadID = ""
			}
			out = &OutboxItem{ID: uuid.NewString(), ConversationID: conversationID, TurnID: turnID,
				Kind: "reply", ThreadID: threadID, Body: replyBody, State: PendingDelivery}
			if _, err := tx.exec(`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,body,state)
				VALUES(?,?,?,?,?,?,'pending')`, out.ID, conversationID, turnID, out.Kind, out.ThreadID, out.Body); err != nil {
				return err
			}
		}
		if _, err := tx.exec(`UPDATE conversations SET last_external_turn_id=? WHERE id=?`, externalTurnID, conversationID); err != nil {
			return err
		}
		return promotePending(tx, conversationID)
	})
	return out, err
}
