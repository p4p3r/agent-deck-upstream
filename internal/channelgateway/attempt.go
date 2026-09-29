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
				Kind: "reply", ThreadID: threadID, Body: replyBody}
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
