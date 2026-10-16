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

// reopenAsGuest answers an explicit address in a thread this conversation has
// left. The segment comes back as a guest whatever it was before: being asked
// one more question is not a reason to resume following a thread, and a guest
// claims no slot, so picking one thread back up cannot park another.
func reopenAsGuest(tx *writeTx, segmentID string) error {
	_, err := tx.exec(`UPDATE segments SET state='open',ownership='guest' WHERE id=?`, segmentID)
	return err
}

// closeSegments executes a leave. It returns how many segments it closed.
//
// Closing the ACTIVE segment promotes the pending one in its place; without
// that the conversation keeps an active segment that answers nothing and the
// bot goes quiet everywhere, which is the obvious way to get this wrong.
func closeSegments(tx *writeTx, conversationID string, c conversationState, threadID string, control Control) (int, error) {
	// Every query below selects the same set, so the counts and the update can
	// never disagree: one transaction, one predicate.
	where := `conversation_id=? AND state IN ('open','pending')`
	args := []any{conversationID}
	if control != ControlLeaveAll {
		if threadID == "" {
			// "leave" at the top level names no thread, so there is nothing to
			// leave. Reported as zero rather than as an error: the operator
			// still gets an answer saying so.
			return 0, nil
		}
		where += ` AND root_thread_id=?`
		args = append(args, threadID)
	}
	counts := func(extra string, extraArgs ...any) (int, error) {
		var n int
		query := `SELECT count(*) FROM segments WHERE ` + where + extra
		if err := tx.row(query, append(append([]any{}, args...), extraArgs...)...).Scan(&n); err != nil {
			return 0, ErrStorage
		}
		return n, nil
	}
	closed, err := counts("")
	if err != nil || closed == 0 {
		return 0, err
	}
	closedActive, err := counts(` AND id=?`, c.active)
	if err != nil {
		return 0, err
	}
	closedPending, err := counts(` AND id=?`, c.pending)
	if err != nil {
		return 0, err
	}
	if _, err := tx.exec(`UPDATE segments SET state='closed' WHERE `+where, args...); err != nil {
		return 0, err
	}
	switch {
	case closedActive > 0 && closedPending == 0 && c.pending != "":
		if _, err := tx.exec(`UPDATE conversations SET active_segment_id=pending_segment_id,pending_segment_id=NULL WHERE id=?`, conversationID); err != nil {
			return 0, err
		}
	case closedActive > 0:
		if _, err := tx.exec(`UPDATE conversations SET active_segment_id=NULL WHERE id=?`, conversationID); err != nil {
			return 0, err
		}
	}
	if closedPending > 0 {
		if _, err := tx.exec(`UPDATE conversations SET pending_segment_id=NULL WHERE id=?`, conversationID); err != nil {
			return 0, err
		}
	}
	return closed, nil
}

// Ingest atomically authenticates, deduplicates, and routes an external event.
// The adapter should acknowledge delivery only after this call succeeds.
func (s *Store) Ingest(ctx context.Context, in Inbound) (IntakeResult, error) {
	var out IntakeResult
	if in.ConversationID == "" || in.EventID == "" || in.MessageID == "" || in.ChannelID == "" || in.SenderID == "" || len(in.Body) > MaxStoredBodyBytes {
		return out, ErrInvalid
	}
	body := in.Body
	messageID := in.MessageID
	threadID := in.ThreadID
	in.EventID = s.alias("event", in.EventID)
	in.MessageID = s.alias("message", in.MessageID)
	in.ThreadID = s.alias("thread", in.ThreadID)
	in.ChannelID = s.alias("channel", in.ChannelID)
	in.SenderID = s.alias("sender", in.SenderID)
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
		if c.mode == ThreadSegments {
			if s.saveThreadID(tx, messageID) != nil || s.saveThreadID(tx, threadID) != nil {
				return ErrStorage
			}
		}
		// Sealed in both modes: acting on a message later -- a reaction, a
		// status -- is not a property of how the channel is read.
		if s.saveMessageID(tx, messageID) != nil {
			return ErrStorage
		}

		out.Disposition = Ignored
		if in.Control != ControlNone && c.mode == ThreadSegments {
			// A control is executed here, inside the same transaction that
			// deduplicates the event, so a Slack retry cannot run it twice and
			// cannot post its confirmation twice either. It never reaches the
			// agent: the disposition stays Ignored.
			closed, err := closeSegments(tx, in.ConversationID, c, in.ThreadID, in.Control)
			if err != nil {
				return err
			}
			out.ControlResult = ControlResult{Control: in.Control, Closed: closed}
		} else if c.mode == ChannelStream {
			if in.ThreadID == "" {
				out.Disposition, out.SegmentID = Accepted, c.active
			}
		} else if in.ThreadID != "" || in.Mentioned {
			// A top-level message needs a mention even if its message ID
			// happens to equal an existing segment's root thread ID.
			root := in.ThreadID
			if root == "" {
				root = s.alias("thread", messageID)
			}
			var segmentID, state, owner string
			err = tx.row(`SELECT id,state,ownership FROM segments WHERE conversation_id=? AND root_thread_id=?`, in.ConversationID, root).
				Scan(&segmentID, &state, &owner)
			switch {
			case err == nil:
				out.SegmentID = segmentID
				// A guest thread answers only what addresses it, and never
				// follows a conversation between two other parties.
				if owner == "guest" && !in.Mentioned {
					state = "muted"
				}
				switch state {
				case "muted":
				case "pending":
					out.Disposition = Accepted
				case "open":
					if c.pending == "" || owner == "guest" {
						out.Disposition = Accepted
					} else {
						out.Disposition = RejectedPending
						out.PointerThreadID, err = segmentRoot(tx, c.pending)
					}
				case "closed":
					// Left. An unmentioned reply is nothing to do with us; an
					// explicit address is answered once, as a guest, which is
					// also how the thread can be picked up again.
					if in.Mentioned {
						if err = reopenAsGuest(tx, segmentID); err == nil {
							out.Disposition, out.SegmentID = Accepted, segmentID
						}
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
					// A guest is never parked behind a pending successor: it
					// claims no slot, so there is nothing for it to wait for.
					if c.pending != "" && in.ThreadID == "" {
						out.Disposition = RejectedPending
						out.PointerThreadID, err = segmentRoot(tx, c.pending)
						if err != nil {
							return err
						}
					} else if in.ThreadID != "" {
						// A mention inside a thread somebody else opened enters
						// it as a guest. It answers that message and never an
						// unmentioned reply, and it claims neither the active
						// nor the pending slot: being asked a question in
						// someone else's thread must not park the thread the
						// operator is actually working in. On 2026-10-14 two
						// boxes were cross-mentioned in a third box's thread,
						// each took ownership of it, and then answered every
						// later reply -- including the ones telling them to
						// stop.
						out.Disposition, out.SegmentID = Accepted, uuid.NewString()
						if _, err := tx.exec(`INSERT INTO segments(id,conversation_id,root_thread_id,state,ownership,created_event_id) VALUES(?,?,?,'open','guest',?)`, out.SegmentID, in.ConversationID, root, in.EventID); err != nil {
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

		var contentRef string
		if out.Disposition == Accepted {
			if err := s.saveInbound(tx, in.EventID, body); err != nil {
				return err
			}
			contentRef = in.EventID
		}
		var sid any
		if out.SegmentID != "" {
			sid = out.SegmentID
		}
		if _, err := tx.exec(`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,content_ref,disposition,pointer_thread_id,seen_at)
			VALUES(?,?,?,?,?,?,?,?,?)`, in.ConversationID, in.EventID, in.MessageID, in.ThreadID, sid, contentRef, out.Disposition, out.PointerThreadID, s.now().Unix()); err != nil {
			return err
		}
		if err := promotePending(tx, in.ConversationID); err != nil {
			return err
		}
		if out.SegmentID != "" {
			if err := tx.row(`SELECT state,ownership FROM segments WHERE id=?`, out.SegmentID).Scan(&out.SegmentState, &out.SegmentOwner); err != nil {
				return ErrStorage
			}
		}
		return nil
	})
	if err == nil && out.PointerThreadID != "" {
		out.PointerThreadID, err = s.exactThread(out.PointerThreadID)
	}
	return out, err
}

func loadActiveTurn(tx *writeTx, conversationID string) (*Turn, error) {
	var t Turn
	err := tx.row(`SELECT t.id,t.conversation_id,t.number,t.segment_id,e.event_id,e.message_id,e.thread_id,e.content_ref,
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
		if err != nil {
			return err
		}
		// No active segment is not the same as no work: a guest thread carries
		// its own queue and owns no slot.
		var ordinal int64
		var t Turn
		var segmentID string
		// The active segment's queue, plus anything addressed to this bot in a
		// thread it is only a guest in. A guest owns no slot, so without this
		// its messages would be accepted and never answered.
		err = tx.row(`SELECT ordinal,segment_id,event_id,message_id,thread_id,content_ref FROM inbound_events
			WHERE conversation_id=? AND disposition='accepted' AND turn_id IS NULL
			  AND (segment_id=? OR segment_id IN
				(SELECT id FROM segments WHERE conversation_id=? AND ownership='guest'))
			ORDER BY ordinal LIMIT 1`, conversationID, c.active, conversationID).
			Scan(&ordinal, &segmentID, &t.EventID, &t.MessageID, &t.ThreadID, &t.Body)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return ErrStorage
		}
		t.ID, t.ConversationID, t.SegmentID, t.AttemptState = uuid.NewString(), conversationID, segmentID, Unprepared
		if err := tx.row(`SELECT next_turn FROM conversations WHERE id=?`, conversationID).Scan(&t.Number); err != nil {
			return ErrStorage
		}
		t.Number++
		if _, err := tx.exec(`UPDATE conversations SET next_turn=? WHERE id=?`, t.Number, conversationID); err != nil {
			return err
		}
		if _, err := tx.exec(`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status)
			VALUES(?,?,?,?,?,'active')`, t.ID, conversationID, t.Number, ordinal, segmentID); err != nil {
			return err
		}
		if _, err := tx.exec(`UPDATE inbound_events SET turn_id=? WHERE ordinal=?`, t.ID, ordinal); err != nil {
			return err
		}
		out = &t
		return nil
	})
	if err != nil || out == nil {
		return out, err
	}
	if out.Body == "" {
		return nil, ErrStorage
	}
	body, err := s.inboundBody(out.Body)
	if err != nil {
		return nil, err
	}
	out.Body = body
	if out.ThreadID != "" {
		out.ThreadID, err = s.exactThread(out.ThreadID)
		if err != nil {
			return nil, err
		}
	}
	// A turn that predates message sealing -- anything migrated from v4 -- has
	// no exact timestamp to recover. That costs a reaction, not a turn, so the
	// alias is dropped and the caller sees an empty MessageID rather than an
	// error that would strand real work.
	if out.MessageID != "" {
		exact, err := s.exactMessage(out.MessageID)
		if err != nil {
			exact = ""
		}
		out.MessageID = exact
	}
	return out, nil
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
	err := tx.row(`SELECT id,conversation_id,turn_id,kind,thread_id,content_ref,state,delivery_attempt_id,external_message_id FROM outbox WHERE turn_id=?`, turnID).
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
			if out == nil && replyBody != "" {
				return ErrConflict
			}
			if out != nil {
				got, err := s.outboundBody(out.ID)
				if err != nil || got != replyBody {
					return ErrConflict
				}
				out.Body = got
			}
			return nil
		}
		if status != "active" {
			return ErrConflict
		}
		if _, err := tx.exec(`UPDATE turns SET status='completed',completed_at=? WHERE id=?`, s.now().Unix(), turnID); err != nil {
			return err
		}
		if replyBody != "" {
			threadID := root
			if Mode(mode) == ChannelStream {
				threadID = ""
			}
			out = &OutboxItem{ID: uuid.NewString(), ConversationID: conversationID, TurnID: turnID,
				Kind: "reply", ThreadID: threadID, Body: replyBody, State: PendingDelivery}
			if err := s.saveOutbound(tx, out.ID, replyBody); err != nil {
				return err
			}
			if _, err := tx.exec(`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,content_ref,state)
				VALUES(?,?,?,?,?,?,'pending')`, out.ID, conversationID, turnID, out.Kind, out.ThreadID, s.alias("outbox", out.ID)); err != nil {
				return err
			}
		}
		return promotePending(tx, conversationID)
	})
	if err == nil && out != nil && out.ThreadID != "" {
		out.ThreadID, err = s.exactThread(out.ThreadID)
	}
	return out, err
}

// PendingOutbox returns durable delivery work in insertion order. Its body is
// privileged payload, and callers must keep it out of diagnostics.
func (s *Store) PendingOutbox(ctx context.Context, conversationID string, limit int) ([]OutboxItem, error) {
	if conversationID == "" || limit <= 0 {
		return nil, ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,conversation_id,turn_id,kind,thread_id,content_ref,state,delivery_attempt_id,external_message_id
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
	for i := range items {
		if items[i].Kind == "reply" {
			body, err := s.outboundBody(items[i].ID)
			if err != nil {
				return nil, err
			}
			items[i].Body = body
		} else {
			items[i].Body = ""
		}
		if items[i].ThreadID != "" {
			items[i].ThreadID, err = s.exactThread(items[i].ThreadID)
			if err != nil {
				return nil, err
			}
		}
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
			if existing == s.alias("providermessage", externalMessageID) {
				return nil
			}
			return ErrConflict
		}
		return ErrConflict
	})
}
