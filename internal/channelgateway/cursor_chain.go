package channelgateway

import (
	"context"
	"database/sql"
	"errors"
)

// CursorChain is the ledger's completed suffix after an immutable bootstrap
// anchor. Active is the sole unfinished turn, if one has been reserved.
type CursorChain struct {
	Completed []string
	Active    *Attempt
}

// ValidateCursorChain reads a single SQLite snapshot. It proves that every
// cursor advance since bootstrap is backed by a contiguous accepted and
// completed ledger attempt, while allowing one final unresolved attempt.
// Callers must separately compare the chain with authoritative agent history.
func (s *Store) ValidateCursorChain(ctx context.Context, conversationID, threadID, anchor string) (CursorChain, error) {
	var chain CursorChain
	if s == nil || conversationID == "" || threadID == "" {
		return chain, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return chain, ErrStorage
	}
	defer tx.Rollback()
	var bound, cursor string
	var nextTurn int64
	err = tx.QueryRowContext(ctx, `SELECT agent_thread_id,last_external_turn_id,next_turn FROM conversations WHERE id=?`, conversationID).
		Scan(&bound, &cursor, &nextTurn)
	if errors.Is(err, sql.ErrNoRows) {
		return chain, ErrNotFound
	}
	if err != nil {
		return chain, ErrStorage
	}
	if bound != threadID || nextTurn < 0 {
		return chain, ErrConflict
	}
	rows, err := tx.QueryContext(ctx, `SELECT number,status,attempt_id,attempt_state,baseline_turn_id,
		external_turn_id,acceptance_id FROM turns WHERE conversation_id=? ORDER BY number`, conversationID)
	if err != nil {
		return chain, ErrStorage
	}
	expected := anchor
	seen := make(map[string]bool)
	var count int64
	for rows.Next() {
		var number int64
		var status, attemptID, state, baseline, externalID, acceptanceID string
		if rows.Scan(&number, &status, &attemptID, &state, &baseline, &externalID, &acceptanceID) != nil {
			_ = rows.Close()
			return chain, ErrStorage
		}
		count++
		if number != count || chain.Active != nil {
			_ = rows.Close()
			return chain, ErrConflict
		}
		switch status {
		case "completed":
			if state != string(AttemptCompleted) || attemptID == "" || externalID == "" ||
				acceptanceID != externalID || baseline != expected || seen[externalID] {
				_ = rows.Close()
				return chain, ErrConflict
			}
			seen[externalID] = true
			chain.Completed = append(chain.Completed, externalID)
			expected = externalID
		case "active":
			a := &Attempt{ID: attemptID, State: AttemptState(state), BaselineTurnID: baseline, ExternalTurnID: externalID}
			if !validActiveChainAttempt(*a, acceptanceID, expected) {
				_ = rows.Close()
				return chain, ErrConflict
			}
			chain.Active = a
		default:
			_ = rows.Close()
			return chain, ErrConflict
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return chain, ErrStorage
	}
	if err := rows.Close(); err != nil {
		return chain, ErrStorage
	}
	if count != nextTurn || cursor != expected {
		return chain, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return chain, ErrStorage
	}
	return chain, nil
}

func validActiveChainAttempt(a Attempt, acceptanceID, cursor string) bool {
	switch a.State {
	case Unprepared:
		return a.ID == "" && a.BaselineTurnID == "" && a.ExternalTurnID == "" && acceptanceID == ""
	case Prepared:
		return a.ID != "" && a.BaselineTurnID == cursor && a.ExternalTurnID == "" && acceptanceID == ""
	case AttemptAccepted:
		return a.ID != "" && a.BaselineTurnID == cursor && a.ExternalTurnID != "" && acceptanceID == a.ExternalTurnID
	case NeedsReconciliation:
		return a.ID != "" && a.BaselineTurnID == cursor &&
			((a.ExternalTurnID == "" && acceptanceID == "") || (a.ExternalTurnID != "" && acceptanceID == a.ExternalTurnID))
	default:
		return false
	}
}
