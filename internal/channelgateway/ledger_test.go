package channelgateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func testStore(t *testing.T, mode Mode) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.db")
	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.CreateConversation(context.Background(), Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding", Mode: mode,
		AllowedSenders: []string{"alice"},
	}))
	return s, path
}

func inbound(event, message, thread string, mention bool) Inbound {
	return Inbound{
		ConversationID: "conversation", EventID: event, MessageID: message,
		ThreadID: thread, ChannelID: "channel", SenderID: "alice",
		Mentioned: mention, Body: "body-" + event,
	}
}

func complete(t *testing.T, s *Store, turn *Turn, response string) *OutboxItem {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "accept-"+turn.EventID))
	item, err := s.CompleteTurn(ctx, turn.ID, "accept-"+turn.EventID, response)
	require.NoError(t, err)
	return item
}

func TestAuthorizationAndEventDedup(t *testing.T) {
	s, path := testStore(t, ThreadSegments)
	ctx := context.Background()
	secret := "secret-body-and-token"
	wrongChannel := inbound("unauthorized-1", "m1", "", true)
	wrongChannel.ChannelID, wrongChannel.Body = "other-channel", secret
	_, err := s.Ingest(ctx, wrongChannel)
	require.ErrorIs(t, err, ErrUnauthorized)
	require.NotContains(t, err.Error(), secret)
	wrongSender := inbound("unauthorized-2", "m2", "", true)
	wrongSender.SenderID, wrongSender.Body = "mallory", secret
	_, err = s.Ingest(ctx, wrongSender)
	require.ErrorIs(t, err, ErrUnauthorized)
	require.NotContains(t, err.Error(), secret)

	first := inbound("event-1", "m1", "", true)
	first.Body = secret
	r, err := s.Ingest(ctx, first)
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)
	require.Equal(t, "open", r.SegmentState)
	require.False(t, r.Duplicate)
	first.Body = "changed-on-retry"
	r, err = s.Ingest(ctx, first)
	require.NoError(t, err)
	require.True(t, r.Duplicate)
	require.Equal(t, Accepted, r.Disposition)
	turn, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, secret, turn.Body)

	ignored := inbound("event-2", "unmentioned", "", false)
	r, err = s.Ingest(ctx, ignored)
	require.NoError(t, err)
	require.Equal(t, Ignored, r.Disposition)
	ignored.Mentioned = true
	r, err = s.Ingest(ctx, ignored)
	require.NoError(t, err)
	require.True(t, r.Duplicate)
	require.Equal(t, Ignored, r.Disposition)

	var count int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM inbound_events WHERE conversation_id='conversation'`).Scan(&count))
	require.Equal(t, 2, count)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestChannelStreamTopLevelAndSerializedTurns(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	ctx := context.Background()
	r, err := s.Ingest(ctx, inbound("first", "top-1", "", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)
	r, err = s.Ingest(ctx, inbound("thread-reply", "reply-1", "top-1", true))
	require.NoError(t, err)
	require.Equal(t, Ignored, r.Disposition)
	r, err = s.Ingest(ctx, inbound("second", "top-2", "", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)

	first, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Number)
	require.Equal(t, s.alias("event", "first"), first.EventID)
	again, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, first.ID, again.ID)
	item := complete(t, s, first, "response-1")
	require.Empty(t, item.ThreadID)
	items, err := s.PendingOutbox(ctx, "conversation", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Empty(t, items[0].ThreadID)

	second, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, int64(2), second.Number)
	require.Equal(t, s.alias("event", "second"), second.EventID)
	complete(t, s, second, "")
	none, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Nil(t, none)
}

func TestUnmentionedTopLevelDoesNotReuseOwnedThreadRoot(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	r, err := s.Ingest(ctx, inbound("mention", "shared-root", "", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)

	// Distinct event IDs can report the same message ID (for example an edit).
	// It is still top-level input, not a reply in the owned thread.
	r, err = s.Ingest(ctx, inbound("unmentioned", "shared-root", "", false))
	require.NoError(t, err)
	require.Equal(t, Ignored, r.Disposition)
	require.Empty(t, r.SegmentID)
	r, err = s.Ingest(ctx, inbound("reply", "reply-id", "shared-root", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)

	first, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, s.alias("event", "mention"), first.EventID)
	complete(t, s, first, "")
	second, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, s.alias("event", "reply"), second.EventID)
}

func TestThreadSegmentsPendingBoundaryAndSupersededPointer(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	r, err := s.Ingest(ctx, inbound("first", "root-old", "", true))
	require.NoError(t, err)
	require.Equal(t, "open", r.SegmentState)
	oldSegment := r.SegmentID
	first, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, oldSegment, first.SegmentID)

	// This event predates the successor. It must finish before ownership moves.
	r, err = s.Ingest(ctx, inbound("already-accepted", "reply-old", "root-old", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)
	r, err = s.Ingest(ctx, inbound("new-mention", "root-new", "", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)
	require.Equal(t, "pending", r.SegmentState)
	newSegment := r.SegmentID
	r, err = s.Ingest(ctx, inbound("pending-reply", "reply-new", "root-new", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, r.Disposition)
	require.Equal(t, newSegment, r.SegmentID)
	r, err = s.Ingest(ctx, inbound("late-old", "late", "root-old", false))
	require.NoError(t, err)
	require.Equal(t, RejectedPending, r.Disposition)
	require.Equal(t, "root-new", r.PointerThreadID)
	r, err = s.Ingest(ctx, inbound("extra-mention", "root-third", "", true))
	require.NoError(t, err)
	require.Equal(t, RejectedPending, r.Disposition)
	require.Equal(t, "root-new", r.PointerThreadID)

	complete(t, s, first, "first-response")
	oldQueued, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, s.alias("event", "already-accepted"), oldQueued.EventID)
	require.Equal(t, int64(2), oldQueued.Number)
	complete(t, s, oldQueued, "")
	newTurn, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, s.alias("event", "new-mention"), newTurn.EventID)
	require.Equal(t, int64(3), newTurn.Number)
	require.Equal(t, newSegment, newTurn.SegmentID)

	r, err = s.Ingest(ctx, inbound("superseded-reply", "reply-old-2", "root-old", false))
	require.NoError(t, err)
	require.Equal(t, RejectedSuperseded, r.Disposition)
	require.Equal(t, "root-new", r.PointerThreadID)
	r, err = s.Ingest(ctx, inbound("unknown-reply", "unknown", "foreign-thread", false))
	require.NoError(t, err)
	require.Equal(t, Ignored, r.Disposition)

	complete(t, s, newTurn, "new-response")
	pendingReply, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, s.alias("event", "pending-reply"), pendingReply.EventID)
	require.Equal(t, int64(4), pendingReply.Number)
}

func TestRecoveryAcceptanceAndDurableOutbox(t *testing.T) {
	s, path := testStore(t, ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("event", "root", "", true))
	require.NoError(t, err)
	turn, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "agent-acceptance"))
	require.NoError(t, s.Close())

	s, err = Open(path)
	require.NoError(t, err)
	recovered, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, turn.ID, recovered.ID)
	require.Equal(t, "agent-acceptance", recovered.AcceptanceID)
	require.ErrorIs(t, s.AcceptTurn(ctx, turn.ID, "another-attempt"), ErrConflict)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "agent-acceptance"))
	require.ErrorIs(t, func() error { _, e := s.CompleteTurn(ctx, turn.ID, "another-attempt", "secret-reply"); return e }(), ErrConflict)

	item, err := s.CompleteTurn(ctx, turn.ID, "agent-acceptance", "secret-reply")
	require.NoError(t, err)
	require.Equal(t, "root", item.ThreadID)
	again, err := s.CompleteTurn(ctx, turn.ID, "agent-acceptance", "secret-reply")
	require.NoError(t, err)
	require.Equal(t, item.ID, again.ID)
	_, err = s.CompleteTurn(ctx, turn.ID, "agent-acceptance", "changed-secret")
	require.ErrorIs(t, err, ErrConflict)
	require.NotContains(t, err.Error(), "changed-secret")
	require.NoError(t, s.Close())

	s, err = Open(path)
	require.NoError(t, err)
	items, err := s.PendingOutbox(ctx, "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, item.ID, items[0].ID)
	require.Equal(t, "secret-reply", items[0].Body)
	require.ErrorIs(t, s.MarkDelivered(ctx, item.ID, "external-message"), ErrConflict)
	delivery, created, err := s.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, item.ID, delivery.Item.ID)
	require.NoError(t, s.ConfirmDelivery(ctx, item.ID, delivery.ID, "channel", "external-message"))
	require.NoError(t, s.MarkDelivered(ctx, item.ID, "external-message"))
	require.NoError(t, s.MarkDelivered(ctx, item.ID, "external-message"))
	err = s.MarkDelivered(ctx, item.ID, "other-message")
	require.ErrorIs(t, err, ErrConflict)
	require.NotContains(t, err.Error(), "other-message")
	items, err = s.PendingOutbox(ctx, "conversation", 10)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestSchemaMismatchFailsClosed(t *testing.T) {
	s, path := testStore(t, ChannelStream)
	_, err := s.db.Exec(`UPDATE channelgateway_schema SET version=999`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	_, err = Open(path)
	require.True(t, errors.Is(err, ErrSchema))
	require.False(t, strings.Contains(err.Error(), path))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Positive(t, info.Size())
}

func TestOpenTightensExistingLedgerPermissions(t *testing.T) {
	s, path := testStore(t, ChannelStream)
	require.NoError(t, s.Close())
	require.NoError(t, os.Chmod(path, 0o644))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	reopened, err := Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.NoError(t, reopened.CreateConversation(context.Background(), Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding", Mode: ChannelStream,
		AllowedSenders: []string{"alice"},
	}))
}

func TestConcurrentReservationsAcrossStoreHandles(t *testing.T) {
	s, path := testStore(t, ChannelStream)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("one", "m1", "", false))
	require.NoError(t, err)
	other, err := Open(path)
	require.NoError(t, err)
	defer other.Close()

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan *Turn, 2)
	errors := make(chan error, 2)
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			<-start
			turn, err := store.NextTurn(ctx, "conversation")
			results <- turn
			errors <- err
		}(store)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var ids []string
	for turn := range results {
		require.NotNil(t, turn)
		require.Equal(t, int64(1), turn.Number)
		ids = append(ids, turn.ID)
	}
	require.Len(t, ids, 2)
	require.Equal(t, ids[0], ids[1])
}
