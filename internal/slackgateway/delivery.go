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

const fixedOperationFailure = "The request could not be completed safely."

// Pointer bodies are fixed text. They carry no operator content and no
// conductor output, only a thread reference the operator can already see.
const (
	pointerBusy       = "Working in another thread; this is parked until that one finishes."
	pointerSuperseded = "This conversation has been replaced; the active one continues elsewhere."
	pointerFollow     = " Follow it here: "
)

type PostResult struct {
	OK        bool
	ChannelID string
	TS        string
}

// A channel_stream reply is always top-level, so PostTopLevel must omit
// thread_ts from chat.postMessage. A thread_segments reply always belongs to a
// segment, so PostInThread must send that segment's root ts and never post to
// the channel root. The two are separate methods so neither mode can reach the
// other's call by passing an empty string.
type Sender interface {
	PostTopLevel(context.Context, string, string) (PostResult, error)
	PostInThread(ctx context.Context, channelID, threadTS, body string) (PostResult, error)
	// Permalink is advisory: a pointer is still worth posting without a link,
	// so a failure here is not a delivery failure.
	Permalink(ctx context.Context, channelID, ts string) (string, error)
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

func (w *DeliveryWorker) route(ctx context.Context) (channelgateway.Mode, error) {
	if w == nil || w.Store == nil || w.Sender == nil || w.ConversationID == "" || w.ChannelID == "" {
		return "", ErrConfig
	}
	mode, channel, err := w.Store.ConversationRoute(ctx, w.ConversationID)
	if err != nil {
		return "", err
	}
	if mode != channelgateway.ChannelStream && mode != channelgateway.ThreadSegments {
		return "", ErrConfig
	}
	if channel != w.ChannelID {
		return "", ErrConfig
	}
	return mode, nil
}

// threadingAgrees rejects an item whose threading contradicts the mode. A
// channel_stream item carrying a thread, or a thread_segments item without
// one, means the ledger and this boundary disagree about where a reply goes,
// and posting it anywhere would be a guess.
func threadingAgrees(mode channelgateway.Mode, threadID string) bool {
	if mode == channelgateway.ChannelStream {
		return threadID == ""
	}
	return threadID != ""
}

// DeliverOne checks the immutable route before claiming. Only the transaction
// winner posts. A failed or ambiguous call becomes uncertain, never pending.
func (w *DeliveryWorker) DeliverOne(ctx context.Context, itemID string) (DeliveryResult, error) {
	r := DeliveryResult{ItemID: itemID}
	mode, err := w.route(ctx)
	if err != nil {
		return r, err
	}
	item, err := w.Store.OutboxRecord(ctx, itemID)
	if err != nil {
		return r, err
	}
	if item.ConversationID != w.ConversationID || item.Kind != "reply" && item.Kind != "status" ||
		!threadingAgrees(mode, item.ThreadID) || item.Kind == "status" && item.Body != "" {
		return r, ErrConfig
	}
	a, created, err := w.Store.PrepareDelivery(ctx, itemID)
	if err != nil {
		return r, err
	}
	r.AttemptID, r.State, r.ProviderTS = a.ID, a.State, a.Item.ExternalMessageID
	if a.Item.ConversationID != w.ConversationID || a.ChannelID != w.ChannelID ||
		!threadingAgrees(mode, a.Item.ThreadID) {
		return r, ErrConfig
	}
	if !created {
		return r, nil
	}
	// Do not let cancellation turn a returned provider result into a stranded
	// sending row. A crash still leaves it for RecoverInFlight.
	persistCtx := context.WithoutCancel(ctx)
	body := a.Item.Body
	if a.Item.Kind == "status" {
		body = fixedOperationFailure
	}
	if !utf8.ValidString(body) || utf8.RuneCountInString(body) > MaxPostCharacters {
		if err := w.Store.MarkDeliveryUncertain(persistCtx, itemID, a.ID); err != nil {
			return r, err
		}
		r.State = channelgateway.UncertainDelivery
		return r, ErrPostResult
	}
	post, postErr := PostResult{}, error(nil)
	if mode == channelgateway.ChannelStream {
		post, postErr = w.Sender.PostTopLevel(ctx, a.ChannelID, body)
	} else {
		post, postErr = w.Sender.PostInThread(ctx, a.ChannelID, a.Item.ThreadID, body)
	}
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

// PostPointer answers a parked message in the thread it arrived in.
//
// It is deliberately outside the outbox: an advisory note has no turn to hang
// off, nothing downstream reconciles it, and a pointer that arrives late is
// worse than one that is lost. It is therefore best effort, and the caller
// treats a failure as nothing more than a missing courtesy.
func (w *DeliveryWorker) PostPointer(ctx context.Context, p Pointer) error {
	if !p.Wanted() {
		return ErrConfig
	}
	mode, err := w.route(ctx)
	if err != nil {
		return err
	}
	// Only a segmented conversation can park anything, and only it threads.
	if mode != channelgateway.ThreadSegments {
		return ErrConfig
	}
	body := pointerBusy
	if p.Superseded {
		body = pointerSuperseded
	}
	if link, err := w.Sender.Permalink(ctx, w.ChannelID, p.PointerThreadTS); err == nil && link != "" {
		body += pointerFollow + link
	}
	post, err := w.Sender.PostInThread(ctx, w.ChannelID, p.ThreadTS, body)
	if err != nil || !post.OK {
		return ErrPost
	}
	return nil
}

// DrainPending is the restart path. It first converts all previously sending
// items to uncertain, then selects only pending items for fresh claims. A
// concurrent drainer may mark an active attempt uncertain; its exact success
// may still confirm the same attempt, but the second drainer never resends it.
func (w *DeliveryWorker) DrainPending(ctx context.Context, limit int) ([]DeliveryResult, error) {
	if limit <= 0 {
		return nil, channelgateway.ErrInvalid
	}
	if _, err := w.route(ctx); err != nil {
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
