package channelstream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// AttentionSource summarises work that is waiting for the conductor.
//
// It replaces the heartbeat unit, which shelled out every fifteen minutes and
// typed its summary into the conductor's pane -- a second writer on a row the
// gateway owns, with no serialisation against an in-flight turn. The summary
// now travels the same path a turn does, behind the same single writer.
//
// An empty summary means there is nothing to say, which is the ordinary case.
type AttentionSource interface {
	Attention(ctx context.Context) (string, error)
}

// attend tells the conductor what is waiting for it, at most once per
// interval, and only when the summary has changed.
//
// It deliberately does not create a turn. A turn carries a Slack message to
// answer and a thread to answer in; this has neither, and inventing one would
// put words in an operator's mouth. The conductor's response therefore lands
// in its pane, exactly as the heartbeat's did, and anything it chooses to tell
// Slack it says in its own thread.
func (r *Runner) attend(ctx context.Context) {
	if r.Attention == nil || r.AttentionInterval <= 0 || r.Worker == nil || r.Worker.RowDriver == nil {
		return
	}
	now := r.now()
	if !r.lastAttention.IsZero() && now.Sub(r.lastAttention) < r.AttentionInterval {
		return
	}
	// Set before the work, not after: a source that fails or hangs must not be
	// retried on every pass of the pump.
	r.lastAttention = now

	// Never speak over an in-flight turn. The row takes one operation at a
	// time, and the answer to a Slack message matters more than a nudge.
	busy, err := r.Handler.Store.ConversationBusy(ctx, r.Handler.Config.ConversationID)
	if err != nil || busy {
		return
	}
	summary, err := r.Attention.Attention(ctx)
	if err != nil || summary == "" {
		return
	}
	digest := sha256.Sum256([]byte(summary))
	fingerprint := hex.EncodeToString(digest[:])
	if fingerprint == r.attentionDigest {
		// Nothing changed since the last nudge. Repeating it would cost the
		// conductor a whole turn to read something it has already seen.
		return
	}
	if _, err := r.Worker.RowDriver.SubmitRowOperation(ctx, "attention-"+fingerprint[:16], summary); err != nil {
		// Advisory in full: the next interval tries again, and a failure here
		// never touches the Slack path.
		return
	}
	r.attentionDigest = fingerprint
}

// now is injectable for tests. The Runner's Clock only makes timers, which is
// what the pump needs and not what an interval check needs.
func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
