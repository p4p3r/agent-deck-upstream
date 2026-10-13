package slackgateway

import (
	"context"
	"errors"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

var (
	ErrPost       = errors.New("slackgateway: post outcome uncertain")
	ErrPostResult = errors.New("slackgateway: ambiguous post result")
)

const fixedOperationFailure = "The request could not be completed safely."

// Pointer bodies are fixed text. They carry no operator content and no
// conductor output, only a thread reference the operator can already see.
// Lifecycle marks. Each is one emoji on the operator's own message, which is
// the cheapest honest answer to "did it hear me, and is it still going?".
// Agent-session statuses. "active" is the finished state, not a cleared one:
// the indicator does not disappear when the app posts, so a turn that ends
// without setting it leaves the thread spinning forever.
const (
	SessionWorking = "processing"
	SessionIdle    = "active"
)

const (
	ReactionAccepted = "eyes"
	ReactionDone     = "white_check_mark"
	ReactionFailed   = "x"
)

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
	// AddReaction is advisory for the same reason. It is also the only signal
	// an operator gets between sending a message and reading an answer.
	AddReaction(ctx context.Context, channelID, ts, name string) error
	// SetSessionStatus drives the thread's agent-session indicator. Advisory:
	// an app not declared as an agent, or a workspace without the feature,
	// simply has no indicator.
	SetSessionStatus(ctx context.Context, channelID, threadTS, status, title, initiatorUserID string) error
	// RemoveReaction takes back one of this bot's own marks, so a message
	// carries the state it is in rather than every state it has been in.
	RemoveReaction(ctx context.Context, channelID, ts, name string) error
}

type DeliveryWorker struct {
	Store          *channelgateway.Store
	Sender         Sender
	ConversationID string
	ChannelID      string
	// Clock is injectable so the advisory backoff below is testable without
	// sleeping. Nil means time.Now.
	Clock func() time.Time

	// Advisory calls -- marks and indicators -- are suppressed for a window
	// after one fails, so an app that has not been granted the scope, or a
	// workspace without agent sessions, costs one call a minute rather than
	// one per turn. Replies are never suppressed.
	advisoryMu    sync.Mutex
	advisoryRetry time.Time
}

const advisoryBackoff = time.Minute

func (w *DeliveryWorker) now() time.Time {
	if w.Clock != nil {
		return w.Clock()
	}
	return time.Now()
}

// advisoryAllowed reports whether advisory calls are open, and records a
// failure window when told of one.
func (w *DeliveryWorker) advisoryAllowed() bool {
	w.advisoryMu.Lock()
	defer w.advisoryMu.Unlock()
	return !w.now().Before(w.advisoryRetry)
}

func (w *DeliveryWorker) advisoryFailed() {
	w.advisoryMu.Lock()
	defer w.advisoryMu.Unlock()
	w.advisoryRetry = w.now().Add(advisoryBackoff)
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
	// Invalid UTF-8 is a storage fault and stays uncertain. Length is not: a
	// reply too long for one message is shortened with the loss stated, never
	// dropped, because silence reads as a dead conductor.
	if !utf8.ValidString(body) {
		if err := w.Store.MarkDeliveryUncertain(persistCtx, itemID, a.ID); err != nil {
			return r, err
		}
		r.State = channelgateway.UncertainDelivery
		return r, ErrPostResult
	}
	body = replyBody(body)
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
	// The answer is out; mark the message that asked for it. A status item is
	// the fixed-failure path, so it marks failure instead.
	reaction := ReactionDone
	if a.Item.Kind == "status" {
		reaction = ReactionFailed
	}
	w.markTurn(persistCtx, a.Item.TurnID, reaction)
	w.sessionIdleForTurn(persistCtx, a.Item.TurnID)
	return r, nil
}

// Mark puts one lifecycle reaction on a message. Advisory in full: an app that
// has not been granted reactions yet, or a message too old to react to, costs
// the mark and nothing else.
func (w *DeliveryWorker) Mark(ctx context.Context, messageTS, reaction string) {
	if w == nil || w.Sender == nil || messageTS == "" || reaction == "" || !w.advisoryAllowed() {
		return
	}
	if w.Sender.AddReaction(ctx, w.ChannelID, messageTS, reaction) != nil {
		w.advisoryFailed()
	}
}

// remark replaces one mark with another, so a message shows the state it is in
// rather than every state it has been in. The new mark is added first: if the
// removal is the call that fails, a message carrying both is a better outcome
// than one carrying none.
func (w *DeliveryWorker) remark(ctx context.Context, messageTS, from, to string) {
	if w == nil || w.Sender == nil || messageTS == "" || to == "" || !w.advisoryAllowed() {
		return
	}
	if w.Sender.AddReaction(ctx, w.ChannelID, messageTS, to) != nil {
		w.advisoryFailed()
		return
	}
	if from != "" && from != to {
		_ = w.Sender.RemoveReaction(ctx, w.ChannelID, messageTS, from)
	}
}

// markTurn resolves a turn's opening message before marking it. A turn that
// predates message sealing has no timestamp to mark, which is not an error.
func (w *DeliveryWorker) markTurn(ctx context.Context, turnID, reaction string) {
	if w == nil || w.Store == nil || turnID == "" {
		return
	}
	ts, err := w.Store.TurnMessage(ctx, turnID)
	if err != nil || ts == "" {
		return
	}
	w.remark(ctx, ts, ReactionAccepted, reaction)
}

// Session drives the agent-session indicator for one thread. Advisory in full,
// and meaningless outside thread_segments, where there is no thread to drive.
func (w *DeliveryWorker) Session(ctx context.Context, threadTS, status, title, initiator string) {
	if w == nil || w.Sender == nil || threadTS == "" || status == "" {
		return
	}
	if !w.advisoryAllowed() {
		return
	}
	mode, err := w.route(ctx)
	if err != nil || mode != channelgateway.ThreadSegments {
		return
	}
	if w.Sender.SetSessionStatus(ctx, w.ChannelID, threadTS, status, title, initiator) != nil {
		w.advisoryFailed()
	}
}

// sessionIdleForTurn returns a turn's thread to its idle state once its answer
// is out. Looked up rather than passed because delivery runs after the socket
// callback that knew it.
func (w *DeliveryWorker) sessionIdleForTurn(ctx context.Context, turnID string) {
	if w == nil || w.Store == nil || turnID == "" {
		return
	}
	// Another turn in the same thread may already be running: intake and
	// delivery race by construction. Clearing the indicator here would show
	// the thread idle while the conductor is working, which is precisely the
	// lie this indicator exists to prevent.
	if busy, err := w.Store.SegmentHasOpenTurn(ctx, turnID); err != nil || busy {
		return
	}
	thread, err := w.Store.TurnThread(ctx, turnID)
	if err != nil || thread == "" {
		return
	}
	w.Session(ctx, thread, SessionIdle, "", "")
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
