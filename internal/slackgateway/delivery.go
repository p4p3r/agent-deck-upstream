package slackgateway

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

var (
	ErrPost       = errors.New("slackgateway: post outcome uncertain")
	ErrPostResult = errors.New("slackgateway: ambiguous post result")
)

const MaxPostCharacters = 40000

type PostResult struct {
	OK        bool
	ChannelID string
	TS        string
}

// Sender has no thread parameter: a channel_stream reply is always top-level.
// A future network implementation must omit thread_ts from chat.postMessage.
type Sender interface {
	PostTopLevel(context.Context, string, string) (PostResult, error)
}

type DeliveryWorker struct {
	Store          *channelgateway.Store
	Sender         Sender
	ConversationID string
	ChannelID      string
}

type DeliveryResult struct {
	ItemID     string
	AttemptID  string
	State      channelgateway.DeliveryState
	ProviderTS string
}

func validTS(ts string) bool {
	if len(ts) == 0 || len(ts) > 128 {
		return false
	}
	for i := 0; i < len(ts); i++ {
		if ts[i] <= ' ' || ts[i] == 127 {
			return false
		}
	}
	return true
}

func (w *DeliveryWorker) route(ctx context.Context) error {
	if w == nil || w.Store == nil || w.Sender == nil || w.ConversationID == "" || w.ChannelID == "" {
		return ErrConfig
	}
	mode, channel, err := w.Store.ConversationRoute(ctx, w.ConversationID)
	if err != nil {
		return err
	}
	if mode != channelgateway.ChannelStream || channel != w.ChannelID {
		return ErrConfig
	}
	return nil
}

// DeliverOne checks the immutable route before claiming. Only the transaction
// winner posts. A failed or ambiguous call becomes uncertain, never pending.
func (w *DeliveryWorker) DeliverOne(ctx context.Context, itemID string) (DeliveryResult, error) {
	r := DeliveryResult{ItemID: itemID}
	if err := w.route(ctx); err != nil {
		return r, err
	}
	item, err := w.Store.OutboxRecord(ctx, itemID)
	if err != nil {
		return r, err
	}
	if item.ConversationID != w.ConversationID || item.Kind != "reply" || item.ThreadID != "" {
		return r, ErrConfig
	}
	a, created, err := w.Store.PrepareDelivery(ctx, itemID)
	if err != nil {
		return r, err
	}
	r.AttemptID, r.State, r.ProviderTS = a.ID, a.State, a.Item.ExternalMessageID
	if a.Item.ConversationID != w.ConversationID || a.ChannelID != w.ChannelID || a.Item.ThreadID != "" {
		return r, ErrConfig
	}
	if !created {
		return r, nil
	}
	// Do not let cancellation turn a returned provider result into a stranded
	// sending row. A crash still leaves it for RecoverInFlight.
	persistCtx := context.WithoutCancel(ctx)
	if !utf8.ValidString(a.Item.Body) || utf8.RuneCountInString(a.Item.Body) > MaxPostCharacters {
		if err := w.Store.MarkDeliveryUncertain(persistCtx, itemID, a.ID); err != nil {
			return r, err
		}
		r.State = channelgateway.UncertainDelivery
		return r, ErrPostResult
	}
	post, postErr := w.Sender.PostTopLevel(ctx, a.ChannelID, a.Item.Body)
	if postErr != nil || !post.OK || post.ChannelID != a.ChannelID || !validTS(post.TS) {
		if err := w.Store.MarkDeliveryUncertain(persistCtx, itemID, a.ID); err != nil {
			return r, err
		}
		r.State = channelgateway.UncertainDelivery
		if postErr != nil {
			return r, ErrPost
		}
		return r, ErrPostResult
	}
	if err := w.Store.ConfirmDelivery(persistCtx, itemID, a.ID, post.ChannelID, post.TS); err != nil {
		// An exact post succeeded but could not be committed. The same
		// attempt may later be reconciled, but must not be resent.
		_ = w.Store.MarkDeliveryUncertain(persistCtx, itemID, a.ID)
		return r, err
	}
	r.State, r.ProviderTS = channelgateway.DeliveredDelivery, post.TS
	return r, nil
}

// DrainPending is the restart path. It first converts all previously sending
// items to uncertain, then selects only pending items for fresh claims. A
// concurrent drainer may mark an active attempt uncertain; its exact success
// may still confirm the same attempt, but the second drainer never resends it.
func (w *DeliveryWorker) DrainPending(ctx context.Context, limit int) ([]DeliveryResult, error) {
	if limit <= 0 {
		return nil, channelgateway.ErrInvalid
	}
	if err := w.route(ctx); err != nil {
		return nil, err
	}
	if _, err := w.Store.RecoverInFlight(ctx, w.ConversationID); err != nil {
		return nil, err
	}
	items, err := w.Store.PendingOutbox(ctx, w.ConversationID, limit)
	if err != nil {
		return nil, err
	}
	results := make([]DeliveryResult, 0, len(items))
	for _, item := range items {
		r, err := w.DeliverOne(ctx, item.ID)
		results = append(results, r)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}
