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
	exact, err := s.channelID(conversationID)
	if err != nil || channelID != s.alias("channel", exact) {
		return "", "", ErrStorage
	}
	return mode, exact, nil
}

// OutboxRecord reads an item without claiming it, so a transport can reject
// an incompatible route before altering delivery state.
func (s *Store) OutboxRecord(ctx context.Context, itemID string) (OutboxItem, error) {
	var o OutboxItem
	var expired int64
	if itemID == "" {
		return o, ErrInvalid
	}
	err := s.db.QueryRowContext(ctx, `SELECT id,conversation_id,turn_id,kind,thread_id,content_ref,state,delivery_attempt_id,external_message_id,content_expired_at
		FROM outbox WHERE id=?`, itemID).
		Scan(&o.ID, &o.ConversationID, &o.TurnID, &o.Kind, &o.ThreadID, &o.Body,
			&o.State, &o.DeliveryAttemptID, &o.ExternalMessageID, &expired)
	if errors.Is(err, sql.ErrNoRows) {
		return o, ErrNotFound
	}
	if err != nil {
		return o, ErrStorage
	}
	if expired > 0 {
		o.Body = ""
	} else if o.Kind == "reply" {
		o.Body, err = s.outboundBody(o.ID)
		if err != nil {
			return OutboxItem{}, err
		}
	} else {
		o.Body = ""
	}
	if o.State == DeliveredDelivery && expired == 0 {
		exact, err := s.providerMessage(o.ID)
		if err != nil || o.ExternalMessageID != s.alias("providermessage", exact) {
			return OutboxItem{}, ErrStorage
		}
		o.ExternalMessageID = exact
	}
	if expired > 0 {
		o.ExternalMessageID = ""
	}
	if o.ThreadID != "" {
		o.ThreadID, err = s.exactThread(o.ThreadID)
		if err != nil {
			return OutboxItem{}, err
		}
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
	var expired int64
	err := s.write(ctx, func(tx *writeTx) error {
		err := tx.row(`SELECT o.id,o.conversation_id,o.turn_id,o.kind,o.thread_id,o.content_ref,
			o.state,o.delivery_attempt_id,o.external_message_id,c.channel_id,o.content_expired_at
			FROM outbox o JOIN conversations c ON c.id=o.conversation_id WHERE o.id=?`, itemID).
			Scan(&a.Item.ID, &a.Item.ConversationID, &a.Item.TurnID, &a.Item.Kind, &a.Item.ThreadID,
				&a.Item.Body, &a.Item.State, &a.Item.DeliveryAttemptID, &a.Item.ExternalMessageID, &a.ChannelID, &expired)
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
	if err != nil {
		return a, created, err
	}
	exact, err := s.channelID(a.Item.ConversationID)
	if err != nil || a.ChannelID != s.alias("channel", exact) {
		return DeliveryAttempt{}, false, ErrStorage
	}
	a.ChannelID = exact
	if expired > 0 {
		a.Item.Body = ""
	} else if a.Item.Kind == "reply" {
		a.Item.Body, err = s.outboundBody(a.Item.ID)
		if err != nil {
			return DeliveryAttempt{}, false, err
		}
	} else {
		a.Item.Body = ""
	}
	if a.Item.State == DeliveredDelivery && expired == 0 {
		exactID, err := s.providerMessage(a.Item.ID)
		if err != nil || a.Item.ExternalMessageID != s.alias("providermessage", exactID) {
			return DeliveryAttempt{}, false, ErrStorage
		}
		a.Item.ExternalMessageID = exactID
	}
	if expired > 0 {
		a.Item.ExternalMessageID = ""
	}
	if a.Item.ThreadID != "" {
		a.Item.ThreadID, err = s.exactThread(a.Item.ThreadID)
		if err != nil {
			return DeliveryAttempt{}, false, err
		}
	}
	return a, created, nil
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
		r, err := tx.exec(`UPDATE outbox SET state='uncertain',uncertain_at=? WHERE conversation_id=? AND state='sending'`, s.now().Unix(), conversationID)
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
		_, err = tx.exec(`UPDATE outbox SET state='uncertain',uncertain_at=? WHERE id=?`, s.now().Unix(), itemID)
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
		var id, existing, boundChannel, conversationID string
		var expired int64
		err := tx.row(`SELECT o.state,o.delivery_attempt_id,o.external_message_id,c.channel_id,o.conversation_id,o.content_expired_at
			FROM outbox o JOIN conversations c ON c.id=o.conversation_id WHERE o.id=?`, itemID).
			Scan(&state, &id, &existing, &boundChannel, &conversationID, &expired)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return ErrStorage
		}
		exactChannel, bindingErr := s.channelID(conversationID)
		if bindingErr != nil || id != attemptID || channelID != exactChannel || s.alias("channel", channelID) != boundChannel {
			return ErrConflict
		}
		if state == DeliveredDelivery {
			if existing == s.alias("providermessage", providerMessageID) {
				return nil
			}
			return ErrConflict
		}
		if state != SendingDelivery && state != UncertainDelivery || existing != "" {
			return ErrConflict
		}
		var duplicate int
		if err := tx.row(`SELECT count(*) FROM outbox WHERE conversation_id=(SELECT conversation_id FROM outbox WHERE id=?)
			AND external_message_id=? AND id<>?`, itemID, s.alias("providermessage", providerMessageID), itemID).Scan(&duplicate); err != nil {
			return ErrStorage
		}
		if duplicate != 0 {
			return ErrConflict
		}
		if expired == 0 {
			if err := s.saveProviderMessage(itemID, providerMessageID); err != nil {
				return err
			}
		}
		if _, err := tx.exec(`UPDATE outbox SET state='delivered',external_message_id=?,delivered_at=? WHERE id=?`, s.alias("providermessage", providerMessageID), s.now().Unix(), itemID); err != nil {
			return err
		}
		return nil
	})
}
