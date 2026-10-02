package channelgateway

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

type conversationState struct {
	mode    Mode
	channel string
	active  string
	pending string
}

func loadConversation(tx *writeTx, id string) (conversationState, error) {
	var c conversationState
	var active, pending sql.NullString
	err := tx.row(`SELECT mode,channel_id,active_segment_id,pending_segment_id FROM conversations WHERE id=?`, id).
		Scan(&c.mode, &c.channel, &active, &pending)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, ErrStorage
	}
	c.active, c.pending = active.String, pending.String
	return c, nil
}

func segmentRoot(tx *writeTx, id string) (string, error) {
	if id == "" {
		return "", nil
	}
	var root string
	if err := tx.row(`SELECT root_thread_id FROM segments WHERE id=?`, id).Scan(&root); err != nil {
		return "", ErrStorage
	}
	return root, nil
}

// promotePending changes segment ownership only after the old segment has no
// active turn or queued events. New old-thread events are rejected while a
// successor is pending, so this drain has a finite boundary.
func promotePending(tx *writeTx, conversationID string) error {
	c, err := loadConversation(tx, conversationID)
	if err != nil || c.pending == "" {
		return err
	}
	if c.active != "" {
		var remaining int
		err = tx.row(`SELECT
			(SELECT count(*) FROM turns WHERE conversation_id=? AND status='active') +
			(SELECT count(*) FROM inbound_events WHERE conversation_id=? AND segment_id=?
			 AND disposition='accepted' AND turn_id IS NULL)`, conversationID, conversationID, c.active).Scan(&remaining)
		if err != nil {
			return ErrStorage
		}
		if remaining != 0 {
			return nil
		}
		if _, err := tx.exec(`UPDATE segments SET state='superseded',superseded_by=? WHERE id=?`, c.pending, c.active); err != nil {
			return err
		}
	}
	if _, err := tx.exec(`UPDATE segments SET state='open' WHERE id=?`, c.pending); err != nil {
		return err
	}
	_, err = tx.exec(`UPDATE conversations SET active_segment_id=?,pending_segment_id=NULL WHERE id=?`, c.pending, conversationID)
	return err
}

// Ingest atomically authenticates, deduplicates, and routes an external event.
// The adapter should acknowledge delivery only after this call succeeds.
func (s *Store) Ingest(ctx context.Context, in Inbound) (IntakeResult, error) {
	var out IntakeResult
	if in.ConversationID == "" || in.EventID == "" || in.MessageID == "" || in.ChannelID == "" || in.SenderID == "" {
		return out, ErrInvalid
	}
	err := s.write(ctx, func(tx *writeTx) error {
		c, err := loadConversation(tx, in.ConversationID)
		if err != nil {
			return err
		}
		var allowed int
		if err := tx.row(`SELECT count(*) FROM allowed_senders WHERE conversation_id=? AND sender_id=?`, in.ConversationID, in.SenderID).Scan(&allowed); err != nil {
			return ErrStorage
		}
		if c.channel != in.ChannelID || allowed != 1 {
			return ErrUnauthorized
		}

		var segment sql.NullString
		var disposition, pointer string
		err = tx.row(`SELECT disposition,segment_id,pointer_thread_id FROM inbound_events WHERE conversation_id=? AND event_id=?`, in.ConversationID, in.EventID).
			Scan(&disposition, &segment, &pointer)
		if err == nil {
			out.Disposition, out.Duplicate, out.SegmentID, out.PointerThreadID = Disposition(disposition), true, segment.String, pointer
			if segment.Valid {
				if err := tx.row(`SELECT state FROM segments WHERE id=?`, segment.String).Scan(&out.SegmentState); err != nil {
					return ErrStorage
				}
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ErrStorage
		}

		out.Disposition = Ignored
		if c.mode == ChannelStream {
			if in.ThreadID == "" {
				out.Disposition, out.SegmentID = Accepted, c.active
			}
		} else if in.ThreadID != "" || in.Mentioned {
			// A top-level message needs a mention even if its message ID
			// happens to equal an existing segment's root thread ID.
			root := in.ThreadID
			if root == "" {
				root = in.MessageID
			}
			var segmentID, state string
			err = tx.row(`SELECT id,state FROM segments WHERE conversation_id=? AND root_thread_id=?`, in.ConversationID, root).
				Scan(&segmentID, &state)
			switch {
			case err == nil:
				out.SegmentID = segmentID
				switch state {
				case "pending":
					out.Disposition = Accepted
				case "open":
					if c.pending == "" {
						out.Disposition = Accepted
					} else {
						out.Disposition = RejectedPending
						out.PointerThreadID, err = segmentRoot(tx, c.pending)
					}
				case "superseded":
					out.Disposition = RejectedSuperseded
					out.PointerThreadID, err = segmentRoot(tx, c.active)
				default:
					return ErrStorage
				}
				if err != nil {
					return err
				}
			case errors.Is(err, sql.ErrNoRows):
				if in.Mentioned {
					if c.pending != "" {
						out.Disposition = RejectedPending
						out.PointerThreadID, err = segmentRoot(tx, c.pending)
						if err != nil {
							return err
						}
					} else {
						out.Disposition, out.SegmentID = Accepted, uuid.NewString()
						state = "open"
						if c.active != "" {
							state = "pending"
						}
						if _, err := tx.exec(`INSERT INTO segments(id,conversation_id,root_thread_id,state,created_event_id) VALUES(?,?,?,?,?)`, out.SegmentID, in.ConversationID, root, state, in.EventID); err != nil {
							return err
						}
						field := "active_segment_id"
						if state == "pending" {
							field = "pending_segment_id"
						}
						if _, err := tx.exec(`UPDATE conversations SET `+field+`=? WHERE id=?`, out.SegmentID, in.ConversationID); err != nil {
							return err
						}
					}
				}
			default:
				return ErrStorage
			}
		}

		var body any
		if out.Disposition == Accepted {
			body = in.Body
		}
		var sid any
		if out.SegmentID != "" {
			sid = out.SegmentID
		}
		if _, err := tx.exec(`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,pointer_thread_id)
			VALUES(?,?,?,?,?,?,?,?)`, in.ConversationID, in.EventID, in.MessageID, in.ThreadID, sid, body, out.Disposition, out.PointerThreadID); err != nil {
			return err
		}
		if err := promotePending(tx, in.ConversationID); err != nil {
			return err
		}
		if out.SegmentID != "" {
			if err := tx.row(`SELECT state FROM segments WHERE id=?`, out.SegmentID).Scan(&out.SegmentState); err != nil {
				return ErrStorage
			}
		}
		return nil
	})
	return out, err
}

func loadActiveTurn(tx *writeTx, conversationID string) (*Turn, error) {
	var t Turn
	err := tx.row(`SELECT t.id,t.conversation_id,t.number,t.segment_id,e.event_id,e.message_id,e.thread_id,e.body,
		t.acceptance_id,t.attempt_id,t.attempt_state,t.baseline_turn_id,t.external_turn_id,
		t.row_operation_id,t.operation_state,t.accepted_codex_session_id,t.accepted_turn_generation,t.terminal_error
		FROM turns t JOIN inbound_events e ON e.ordinal=t.event_ordinal
		WHERE t.conversation_id=? AND t.status='active'`, conversationID).
		Scan(&t.ID, &t.ConversationID, &t.Number, &t.SegmentID, &t.EventID, &t.MessageID, &t.ThreadID,
			&t.Body, &t.AcceptanceID, &t.AttemptID, &t.AttemptState, &t.BaselineTurnID, &t.ExternalTurnID,
			&t.RowOperationID, &t.OperationState, &t.CodexSessionID, &t.TurnGeneration, &t.TerminalError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrStorage
	}
	return &t, nil
}

// NextTurn returns the unfinished turn after restart, or atomically reserves
// the next eligible event. A nonempty AcceptanceID means the driver must first
// reconcile that existing external attempt.
func (s *Store) NextTurn(ctx context.Context, conversationID string) (*Turn, error) {
	if conversationID == "" {
		return nil, ErrInvalid
	}
	var out *Turn
	err := s.write(ctx, func(tx *writeTx) error {
		c, err := loadConversation(tx, conversationID)
		if err != nil {
			return err
		}
		out, err = loadActiveTurn(tx, conversationID)
		if err != nil || out != nil {
			return err
		}
		if err := promotePending(tx, conversationID); err != nil {
			return err
		}
		c, err = loadConversation(tx, conversationID)
		if err != nil || c.active == "" {
			return err
		}
		var ordinal int64
		var t Turn
		err = tx.row(`SELECT ordinal,event_id,message_id,thread_id,body FROM inbound_events
			WHERE conversation_id=? AND segment_id=? AND disposition='accepted' AND turn_id IS NULL
			ORDER BY ordinal LIMIT 1`, conversationID, c.active).
			Scan(&ordinal, &t.EventID, &t.MessageID, &t.ThreadID, &t.Body)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return ErrStorage
		}
		t.ID, t.ConversationID, t.SegmentID, t.AttemptState = uuid.NewString(), conversationID, c.active, Unprepared
		if err := tx.row(`SELECT next_turn FROM conversations WHERE id=?`, conversationID).Scan(&t.Number); err != nil {
			return ErrStorage
		}
		t.Number++
		if _, err := tx.exec(`UPDATE conversations SET next_turn=? WHERE id=?`, t.Number, conversationID); err != nil {
			return err
		}
		if _, err := tx.exec(`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status)
			VALUES(?,?,?,?,?,'active')`, t.ID, conversationID, t.Number, ordinal, c.active); err != nil {
			return err
		}
		if _, err := tx.exec(`UPDATE inbound_events SET turn_id=? WHERE ordinal=?`, t.ID, ordinal); err != nil {
			return err
		}
		out = &t
		return nil
	})
	return out, err
}

// AcceptTurn records the driver's stable external acceptance ID. A different
// acceptance ID cannot take over the same turn.
func (s *Store) AcceptTurn(ctx context.Context, turnID, acceptanceID string) error {
	if turnID == "" || acceptanceID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var status, existing string
		var attemptState AttemptState
		err := tx.row(`SELECT status,acceptance_id,attempt_state FROM turns WHERE id=?`, turnID).
			Scan(&status, &existing, &attemptState)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if attemptState != Unprepared {
			return ErrConflict
		}
		if existing != "" {
			if existing == acceptanceID {
				return nil
			}
			return ErrConflict
		}
		if status != "active" {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE turns SET acceptance_id=? WHERE id=?`, acceptanceID, turnID)
		return err
	})
}

func loadTurnOutbox(tx *writeTx, turnID string) (*OutboxItem, error) {
	var o OutboxItem
	err := tx.row(`SELECT id,conversation_id,turn_id,kind,thread_id,body,state,delivery_attempt_id,external_message_id FROM outbox WHERE turn_id=?`, turnID).
		Scan(&o.ID, &o.ConversationID, &o.TurnID, &o.Kind, &o.ThreadID, &o.Body, &o.State, &o.DeliveryAttemptID, &o.ExternalMessageID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrStorage
	}
	return &o, nil
}

// CompleteTurn commits completion and an optional reply in one transaction.
// Repeating the identical call returns the same outbox item; a changed reply
// under the same acceptance ID is a conflict.
func (s *Store) CompleteTurn(ctx context.Context, turnID, acceptanceID, replyBody string) (*OutboxItem, error) {
	if turnID == "" || acceptanceID == "" {
		return nil, ErrInvalid
	}
	var out *OutboxItem
	err := s.write(ctx, func(tx *writeTx) error {
		var conversationID, status, existing, mode, root string
		var attemptState AttemptState
		err := tx.row(`SELECT t.conversation_id,t.status,t.acceptance_id,c.mode,s.root_thread_id,t.attempt_state
			FROM turns t JOIN conversations c ON c.id=t.conversation_id JOIN segments s ON s.id=t.segment_id
			WHERE t.id=?`, turnID).
			Scan(&conversationID, &status, &existing, &mode, &root, &attemptState)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if attemptState != Unprepared {
			return ErrConflict
		}
		if existing != acceptanceID {
			return ErrConflict
		}
		if status == "completed" {
			out, err = loadTurnOutbox(tx, turnID)
			if err != nil {
				return err
			}
			if out == nil && replyBody != "" || out != nil && out.Body != replyBody {
				return ErrConflict
			}
			return nil
		}
		if status != "active" {
			return ErrConflict
		}
		if _, err := tx.exec(`UPDATE turns SET status='completed' WHERE id=?`, turnID); err != nil {
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
		return promotePending(tx, conversationID)
	})
	return out, err
}

// PendingOutbox returns durable delivery work in insertion order. Its body is
// privileged payload, and callers must keep it out of diagnostics.
func (s *Store) PendingOutbox(ctx context.Context, conversationID string, limit int) ([]OutboxItem, error) {
	if conversationID == "" || limit <= 0 {
		return nil, ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,conversation_id,turn_id,kind,thread_id,body,state,delivery_attempt_id,external_message_id
		FROM outbox WHERE conversation_id=? AND state='pending' ORDER BY ordinal LIMIT ?`, conversationID, limit)
	if err != nil {
		return nil, ErrStorage
	}
	defer rows.Close()
	var items []OutboxItem
	for rows.Next() {
		var o OutboxItem
		if err := rows.Scan(&o.ID, &o.ConversationID, &o.TurnID, &o.Kind, &o.ThreadID, &o.Body, &o.State, &o.DeliveryAttemptID, &o.ExternalMessageID); err != nil {
			return nil, ErrStorage
		}
		items = append(items, o)
	}
	if rows.Err() != nil {
		return nil, ErrStorage
	}
	return items, nil
}

// MarkDelivered is retained only for idempotent reads of an already-confirmed
// item. It cannot bypass a prepared delivery attempt.
func (s *Store) MarkDelivered(ctx context.Context, itemID, externalMessageID string) error {
	if itemID == "" || externalMessageID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var state, existing string
		err := tx.row(`SELECT state,external_message_id FROM outbox WHERE id=?`, itemID).Scan(&state, &existing)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if state == "delivered" {
			if existing == externalMessageID {
				return nil
			}
			return ErrConflict
		}
		return ErrConflict
	})
}
