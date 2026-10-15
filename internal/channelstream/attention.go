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
	// Delivered records that the row accepted this exact summary, so the next
	// call can report the delta against it. It mirrors the two-phase commit
	// the heartbeat CLI already used: print, send, then commit. A send that
	// reports success is not a delivery, and committing before the row has it
	// loses the nudge outright.
	Delivered(ctx context.Context, summary string) error
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
	if err != nil {
		r.logf("attention: busy check failed: %v", err)
		return
	}
	if busy {
		r.logf("attention: skipped, a turn or an undelivered reply owns the row")
		return
	}
	summary, err := r.Attention.Attention(ctx)
	if err != nil {
		r.logf("attention: source failed: %v", err)
		return
	}
	if summary == "" {
		r.logf("attention: nothing waiting")
		return
	}
	digest := sha256.Sum256([]byte(summary))
	fingerprint := hex.EncodeToString(digest[:])
	if fingerprint == r.attentionDigest {
		// Nothing changed since the last nudge. Repeating it would cost the
		// conductor a whole turn to read something it has already seen.
		r.logf("attention: unchanged since the last nudge, digest=%s", fingerprint[:16])
		return
	}
	op, err := r.Worker.RowDriver.SubmitRowOperation(ctx, "attention-"+fingerprint[:16], summary)
	if err != nil {
		// Advisory in full: the next interval tries again, and a failure here
		// never touches the Slack path.
		r.logf("attention: submit failed: digest=%s: %v", fingerprint[:16], err)
		return
	}
	// A submit returns as soon as the send is QUEUED. The row types it later,
	// so this line records what the driver actually said: a send that never
	// leaves "queued" is the shape of a nudge that reaches the ledger and
	// never reaches the pane.
	r.logf("attention: submitted digest=%s send=%s state=%s bytes=%d",
		fingerprint[:16], op.SendID, op.State, len(summary))
	r.attentionDigest = fingerprint
	// Only now: the source's own delta state must not advance past what the
	// row actually received.
	if err := r.Attention.Delivered(ctx, summary); err != nil {
		r.logf("attention: commit failed: digest=%s: %v", fingerprint[:16], err)
	}
}

// logf writes one line about the attention path. The gateway logs nothing
// else, so these lines are the only evidence of a nudge that was accepted and
// then lost; they are at most one per interval per decision.
func (r *Runner) logf(format string, args ...any) {
	if r.Log == nil {
		return
	}
	r.Log(format, args...)
}

// now is injectable for tests. The Runner's Clock only makes timers, which is
// what the pump needs and not what an interval check needs.
func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
