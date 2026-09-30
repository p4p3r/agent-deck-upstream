package channelgateway

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

// DeliveryState records whether an outbox item may be posted. An uncertain
// item needs external reconciliation; it must not be automatically retried.
type DeliveryState string

const (
	PendingDelivery   DeliveryState = "pending"
	SendingDelivery   DeliveryState = "sending"
	UncertainDelivery DeliveryState = "uncertain"
	DeliveredDelivery DeliveryState = "delivered"
)

type DeliveryAttempt struct {
	ID        string
	Item      OutboxItem
	ChannelID string
	State     DeliveryState
}

// ConversationRoute exposes only the immutable routing binding, not sender
// identities or message content.
func (s *Store) ConversationRoute(ctx context.Context, conversationID string) (Mode, string, error) {
	if conversationID == "" {
		return "", "", ErrInvalid
	}
	var mode Mode
	var channelID string
	err := s.db.QueryRowContext(ctx, `SELECT mode,channel_id FROM conversations WHERE id=?`, conversationID).Scan(&mode, &channelID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", ErrStorage
	}
	return mode, channelID, nil
}

// OutboxRecord reads an item without claiming it, so a transport can reject
// an incompatible route before altering delivery state.
func (s *Store) OutboxRecord(ctx context.Context, itemID string) (OutboxItem, error) {
	var o OutboxItem
	if itemID == "" {
		return o, ErrInvalid
	}
	err := s.db.QueryRowContext(ctx, `SELECT id,conversation_id,turn_id,kind,thread_id,body,state,delivery_attempt_id,external_message_id
		FROM outbox WHERE id=?`, itemID).
		Scan(&o.ID, &o.ConversationID, &o.TurnID, &o.Kind, &o.ThreadID, &o.Body,
			&o.State, &o.DeliveryAttemptID, &o.ExternalMessageID)
	if errors.Is(err, sql.ErrNoRows) {
		return o, ErrNotFound
	}
	if err != nil {
		return o, ErrStorage
	}
	return o, nil
}

// PrepareDelivery atomically claims a pending item before any provider call.
// Only created=true authorizes the caller to post it. Other states are read
// back with created=false, including an in-flight attempt after a restart.
func (s *Store) PrepareDelivery(ctx context.Context, itemID string) (DeliveryAttempt, bool, error) {
	var a DeliveryAttempt
	if itemID == "" {
		return a, false, ErrInvalid
	}
	created := false
	err := s.write(ctx, func(tx *writeTx) error {
		err := tx.row(`SELECT o.id,o.conversation_id,o.turn_id,o.kind,o.thread_id,o.body,
			o.state,o.delivery_attempt_id,o.external_message_id,c.channel_id
			FROM outbox o JOIN conversations c ON c.id=o.conversation_id WHERE o.id=?`, itemID).
			Scan(&a.Item.ID, &a.Item.ConversationID, &a.Item.TurnID, &a.Item.Kind, &a.Item.ThreadID,
				&a.Item.Body, &a.Item.State, &a.Item.DeliveryAttemptID, &a.Item.ExternalMessageID, &a.ChannelID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if a.Item.State == PendingDelivery {
			if a.Item.DeliveryAttemptID != "" || a.Item.ExternalMessageID != "" {
				return ErrConflict
			}
			a.ID = uuid.NewString()
			if _, err := tx.exec(`UPDATE outbox SET state='sending',delivery_attempt_id=? WHERE id=? AND state='pending'`, a.ID, itemID); err != nil {
				return err
			}
			a.Item.State, a.Item.DeliveryAttemptID = SendingDelivery, a.ID
			created = true
		} else {
			a.ID = a.Item.DeliveryAttemptID
		}
		a.State = a.Item.State
		return nil
	})
	return a, created, err
}

// RecoverInFlight records crash/overlap uncertainty before a drain selects
// pending work. It never puts an item back into the sendable state.
func (s *Store) RecoverInFlight(ctx context.Context, conversationID string) (int64, error) {
	if conversationID == "" {
		return 0, ErrInvalid
	}
	var changed int64
	err := s.write(ctx, func(tx *writeTx) error {
		if _, err := loadConversation(tx, conversationID); err != nil {
			return err
		}
		r, err := tx.exec(`UPDATE outbox SET state='uncertain' WHERE conversation_id=? AND state='sending'`, conversationID)
		if err != nil {
			return err
		}
		changed, err = r.RowsAffected()
		if err != nil {
			return ErrStorage
		}
		return nil
	})
	return changed, err
}

// MarkDeliveryUncertain is safe against a late success: it cannot downgrade
// delivered, and it cannot alter another attempt's state.
func (s *Store) MarkDeliveryUncertain(ctx context.Context, itemID, attemptID string) error {
	if itemID == "" || attemptID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var state DeliveryState
		var id string
		err := tx.row(`SELECT state,delivery_attempt_id FROM outbox WHERE id=?`, itemID).Scan(&state, &id)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if id != attemptID {
			return ErrConflict
		}
		if state == UncertainDelivery || state == DeliveredDelivery {
			return nil
		}
		if state != SendingDelivery {
			return ErrConflict
		}
		_, err = tx.exec(`UPDATE outbox SET state='uncertain' WHERE id=?`, itemID)
		return err
	})
}

// ConfirmDelivery accepts only the exact persisted attempt, configured
// channel, and provider message ID. It can resolve a racing uncertainty mark.
func (s *Store) ConfirmDelivery(ctx context.Context, itemID, attemptID, channelID, providerMessageID string) error {
	if itemID == "" || attemptID == "" || channelID == "" || providerMessageID == "" {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var state DeliveryState
		var id, existing, boundChannel string
		err := tx.row(`SELECT o.state,o.delivery_attempt_id,o.external_message_id,c.channel_id
			FROM outbox o JOIN conversations c ON c.id=o.conversation_id WHERE o.id=?`, itemID).
			Scan(&state, &id, &existing, &boundChannel)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		if id != attemptID || channelID != boundChannel {
			return ErrConflict
		}
		if state == DeliveredDelivery {
			if existing == providerMessageID {
				return nil
			}
			return ErrConflict
		}
		if state != SendingDelivery && state != UncertainDelivery || existing != "" {
			return ErrConflict
		}
		var duplicate int
		if err := tx.row(`SELECT count(*) FROM outbox WHERE conversation_id=(SELECT conversation_id FROM outbox WHERE id=?)
			AND external_message_id=? AND id<>?`, itemID, providerMessageID, itemID).Scan(&duplicate); err != nil {
			return ErrStorage
		}
		if duplicate != 0 {
			return ErrConflict
		}
		if _, err := tx.exec(`UPDATE outbox SET state='delivered',external_message_id=? WHERE id=?`, providerMessageID, itemID); err != nil {
			return err
		}
		return nil
	})
}
