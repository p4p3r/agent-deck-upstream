package channelstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/stretchr/testify/require"
)

type fakeAttention struct {
	mu        sync.Mutex
	summary   string
	err       error
	calls     int
	delivered []string
}

func (f *fakeAttention) Attention(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.summary, f.err
}

func (f *fakeAttention) Delivered(_ context.Context, summary string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, summary)
	return nil
}

func (f *fakeAttention) set(summary string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.summary = summary
}

// bodies returns what actually reached the row.
func submitted(d *fakeDriver) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.bodies...)
}

func attentive(t *testing.T) (*runnerFixture, *fakeAttention, *time.Time) {
	t.Helper()
	f := newRunnerFixture(t)
	source := &fakeAttention{summary: "1 session waiting: impl"}
	now := time.Unix(1700000000, 0)
	f.runner.Attention = source
	f.runner.AttentionInterval = 15 * time.Minute
	f.runner.Now = func() time.Time { return now }
	return f, source, &now
}

func TestAttentionReachesTheRowOnceUntilItChanges(t *testing.T) {
	ctx := context.Background()
	f, source, now := attentive(t)

	f.runner.attend(ctx)
	require.Equal(t, []string{"1 session waiting: impl"}, submitted(f.driver))

	// Same summary, a later interval: the conductor has already read it, and
	// repeating costs it a whole turn.
	*now = now.Add(time.Hour)
	f.runner.attend(ctx)
	require.Len(t, submitted(f.driver), 1)

	source.set("2 sessions waiting: impl, review")
	*now = now.Add(time.Hour)
	f.runner.attend(ctx)
	require.Equal(t, []string{"1 session waiting: impl", "2 sessions waiting: impl, review"}, submitted(f.driver))
}

func TestAttentionWaitsForItsInterval(t *testing.T) {
	ctx := context.Background()
	f, source, now := attentive(t)
	f.runner.attend(ctx)
	source.set("something else entirely")
	*now = now.Add(time.Minute)
	f.runner.attend(ctx)
	require.Len(t, submitted(f.driver), 1, "a changed summary still waits for the interval")
}

// The row takes one operation at a time. A nudge must never speak over the
// answer to a Slack message.
func TestAttentionIsSilentWhileATurnIsInFlight(t *testing.T) {
	ctx := context.Background()
	f, _, _ := attentive(t)
	_, err := f.store.Ingest(ctx, channelgateway.Inbound{
		ConversationID: runConversation, EventID: "ev1", MessageID: "ts1",
		ChannelID: runChannel, SenderID: runUser, Body: "real work",
	})
	require.NoError(t, err)
	turn, err := f.store.NextTurn(ctx, runConversation)
	require.NoError(t, err)
	require.NotNil(t, turn)

	f.runner.attend(ctx)
	require.Empty(t, submitted(f.driver), "an active turn owns the row")
}

// A reply still waiting to be posted is work in flight too.
func TestAttentionIsSilentWhileAReplyIsUndelivered(t *testing.T) {
	ctx := context.Background()
	f, _, _ := attentive(t)
	_, err := f.store.Ingest(ctx, channelgateway.Inbound{
		ConversationID: runConversation, EventID: "ev1", MessageID: "ts1",
		ChannelID: runChannel, SenderID: runUser, Body: "real work",
	})
	require.NoError(t, err)
	turn, err := f.store.NextTurn(ctx, runConversation)
	require.NoError(t, err)
	require.NoError(t, f.store.AcceptTurn(ctx, turn.ID, "accepted"))
	_, err = f.store.CompleteTurn(ctx, turn.ID, "accepted", "an answer nobody has posted yet")
	require.NoError(t, err)

	f.runner.attend(ctx)
	require.Empty(t, submitted(f.driver))
}

func TestAttentionSaysNothingWhenThereIsNothingToSay(t *testing.T) {
	ctx := context.Background()
	f, source, _ := attentive(t)
	source.set("")
	f.runner.attend(ctx)
	require.Empty(t, submitted(f.driver))
}

// A failing source must not be retried on every pass of the pump, and must
// never disturb the Slack path.
func TestAttentionFailureIsQuietAndRateLimited(t *testing.T) {
	ctx := context.Background()
	f, source, now := attentive(t)
	source.mu.Lock()
	source.err = errors.New("listing failed")
	source.mu.Unlock()

	f.runner.attend(ctx)
	f.runner.attend(ctx)
	source.mu.Lock()
	calls := source.calls
	source.mu.Unlock()
	require.Equal(t, 1, calls, "the interval applies to failures too")
	require.Empty(t, submitted(f.driver))

	source.mu.Lock()
	source.err = nil
	source.mu.Unlock()
	source.set("recovered")
	*now = now.Add(time.Hour)
	f.runner.attend(ctx)
	require.Equal(t, []string{"recovered"}, submitted(f.driver))
}

// The source's delta state must not advance past what the row received: a
// nudge committed before delivery is a nudge lost.
func TestDeliveredRecordsOnlyWhatTheRowAccepted(t *testing.T) {
	ctx := context.Background()
	f, source, now := attentive(t)
	f.runner.attend(ctx)
	source.mu.Lock()
	require.Equal(t, []string{"1 session waiting: impl"}, source.delivered)
	source.mu.Unlock()

	f.driver.mu.Lock()
	f.driver.startErr = errors.New("row refused")
	f.driver.mu.Unlock()
	source.set("2 sessions waiting: impl, review")
	*now = now.Add(time.Hour)
	f.runner.attend(ctx)
	source.mu.Lock()
	require.Len(t, source.delivered, 1, "a refused submit commits nothing")
	source.mu.Unlock()
}

func TestAttentionIsOffByDefault(t *testing.T) {
	f := newRunnerFixture(t)
	f.runner.attend(context.Background())
	require.Empty(t, submitted(f.driver))
}
