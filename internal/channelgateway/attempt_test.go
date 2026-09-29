package channelgateway

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func attemptFixture(t *testing.T) (*Store, string, *Turn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "attempt.db")
	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	require.NoError(t, s.CreateConversation(ctx, Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor", Mode: ChannelStream,
		AllowedSenders: []string{"alice"},
	}))
	_, err = s.Ingest(ctx, Inbound{ConversationID: "conversation", EventID: "event", MessageID: "message",
		ChannelID: "channel", SenderID: "alice", Body: "private prompt"})
	require.NoError(t, err)
	turn, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.NotNil(t, turn)
	return s, path, turn
}

func TestAgentThreadBindingSurvivesRestartAndRejectsTakeover(t *testing.T) {
	s, path, _ := attemptFixture(t)
	ctx := context.Background()
	b, err := s.AgentBinding(ctx, "conversation")
	require.NoError(t, err)
	require.Empty(t, b.AgentThreadID)
	require.Empty(t, b.LastExternalTurnID)
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	require.NoError(t, s.Close())
	reopened, err := Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	b, err = reopened.AgentBinding(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "thread-1", b.AgentThreadID)
	require.NoError(t, reopened.BindAgentThread(ctx, "conversation", "thread-1"))
	require.ErrorIs(t, reopened.BindAgentThread(ctx, "conversation", "thread-2"), ErrConflict)
	require.ErrorIs(t, reopened.BindAgentThread(ctx, "conversation", ""), ErrInvalid)
}

func TestAgentThreadCannotBindTwoConversations(t *testing.T) {
	s, _, _ := attemptFixture(t)
	ctx := context.Background()
	require.NoError(t, s.CreateConversation(ctx, Conversation{
		ID: "second", ChannelID: "other-channel", ConductorID: "other-conductor",
		Mode: ChannelStream, AllowedSenders: []string{"bob"},
	}))
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "shared-thread"))
	require.ErrorIs(t, s.BindAgentThread(ctx, "second", "shared-thread"), ErrConflict)
	b, err := s.AgentBinding(ctx, "second")
	require.NoError(t, err)
	require.Empty(t, b.AgentThreadID)
	require.NoError(t, s.BindAgentThread(ctx, "second", "other-thread"))
}

func TestAttemptTransitionsRecoverAndDeduplicateOutbox(t *testing.T) {
	s, path, turn := attemptFixture(t)
	ctx := context.Background()
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	a, created, err := s.PrepareAttempt(ctx, turn.ID, "")
	require.NoError(t, err)
	require.True(t, created)
	require.NotEmpty(t, a.ID)
	require.Empty(t, a.BaselineTurnID)
	require.NoError(t, s.Close())
	s, err = Open(path)
	require.NoError(t, err)
	defer s.Close()
	recovered, created, err := s.PrepareAttempt(ctx, turn.ID, "")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, a.ID, recovered.ID)
	require.NoError(t, s.AcceptAttempt(ctx, turn.ID, a.ID, "external-1"))
	require.NoError(t, s.AcceptAttempt(ctx, turn.ID, a.ID, "external-1"))
	require.ErrorIs(t, s.AcceptAttempt(ctx, turn.ID, a.ID, "external-2"), ErrConflict)
	item, err := s.CompleteAttempt(ctx, turn.ID, a.ID, "external-1", "private reply")
	require.NoError(t, err)
	require.NotNil(t, item)
	require.Empty(t, item.ThreadID)
	again, err := s.CompleteAttempt(ctx, turn.ID, a.ID, "external-1", "private reply")
	require.NoError(t, err)
	require.Equal(t, item.ID, again.ID)
	_, err = s.CompleteAttempt(ctx, turn.ID, a.ID, "external-2", "private reply")
	require.ErrorIs(t, err, ErrConflict)
	require.NotContains(t, err.Error(), "private reply")
	b, err := s.AgentBinding(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "external-1", b.LastExternalTurnID)
	items, err := s.PendingOutbox(ctx, "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, item.ID, items[0].ID)
}

func TestSequentialTurnsCannotReuseExternalTurnID(t *testing.T) {
	s, _, first := attemptFixture(t)
	ctx := context.Background()
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	a, created, err := s.PrepareAttempt(ctx, first.ID, "")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, s.AcceptAttempt(ctx, first.ID, a.ID, "external-1"))
	_, err = s.CompleteAttempt(ctx, first.ID, a.ID, "external-1", "first reply")
	require.NoError(t, err)
	_, err = s.Ingest(ctx, Inbound{ConversationID: "conversation", EventID: "event-2",
		MessageID: "message-2", ChannelID: "channel", SenderID: "alice", Body: "second prompt"})
	require.NoError(t, err)
	second, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.NotNil(t, second)
	a, created, err = s.PrepareAttempt(ctx, second.ID, "external-1")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "external-1", a.BaselineTurnID)
	require.ErrorIs(t, s.AcceptAttempt(ctx, second.ID, a.ID, "external-1"), ErrConflict)
	require.NoError(t, s.AcceptAttempt(ctx, second.ID, a.ID, "external-2"))
	_, err = s.CompleteAttempt(ctx, second.ID, a.ID, "external-2", "second reply")
	require.NoError(t, err)
	b, err := s.AgentBinding(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "external-2", b.LastExternalTurnID)
}

func TestConcurrentPrepareOnlyOneCreatorAndLateAcceptance(t *testing.T) {
	s, path, turn := attemptFixture(t)
	require.NoError(t, s.BindAgentThread(context.Background(), "conversation", "thread-1"))
	other, err := Open(path)
	require.NoError(t, err)
	defer other.Close()
	ctx := context.Background()
	start := make(chan struct{})
	type prepared struct {
		a       Attempt
		created bool
		err     error
	}
	got := make(chan prepared, 2)
	var wg sync.WaitGroup
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			<-start
			a, created, err := store.PrepareAttempt(ctx, turn.ID, "")
			got <- prepared{a, created, err}
		}(store)
	}
	close(start)
	wg.Wait()
	close(got)
	var id string
	var creators int
	for p := range got {
		require.NoError(t, p.err)
		if id == "" {
			id = p.a.ID
		}
		require.Equal(t, id, p.a.ID)
		if p.created {
			creators++
		}
	}
	require.Equal(t, 1, creators)
	require.NotEmpty(t, id)
	require.NoError(t, s.MarkNeedsReconciliation(ctx, turn.ID, id, Prepared))
	// The authoritative callback can race with the observer. It must persist
	// the accepted ID so reconciliation does not orphan a started turn.
	require.NoError(t, other.AcceptAttempt(ctx, turn.ID, id, "external-1"))
	active, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "external-1", active.ExternalTurnID)
	require.ErrorIs(t, s.AcceptAttempt(ctx, turn.ID, id, "external-2"), ErrConflict)
}

func TestPrepareBaselineIsCheckedAtomically(t *testing.T) {
	s, _, turn := attemptFixture(t)
	ctx := context.Background()
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	_, created, err := s.PrepareAttempt(ctx, turn.ID, "not-the-cursor")
	require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrInvalid))
	require.False(t, created)
	a, created, err := s.PrepareAttempt(ctx, turn.ID, "")
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, a.BaselineTurnID)
}
