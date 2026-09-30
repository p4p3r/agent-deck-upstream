package channelgateway

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func pendingDeliveryFixture(t *testing.T, mode Mode) (*Store, string, *OutboxItem) {
	t.Helper()
	s, path := testStore(t, mode)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("delivery-event", "root", "", mode == ThreadSegments))
	require.NoError(t, err)
	turn, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.NotNil(t, turn)
	item := complete(t, s, turn, "private reply body")
	require.NotNil(t, item)
	return s, path, item
}

func TestDeliveryClaimSurvivesRestartAndLegacyMarkCannotBypass(t *testing.T) {
	s, path, item := pendingDeliveryFixture(t, ChannelStream)
	ctx := context.Background()
	require.ErrorIs(t, s.MarkDelivered(ctx, item.ID, "provider-ts"), ErrConflict)
	attempt, created, err := s.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.True(t, created)
	require.NotEmpty(t, attempt.ID)
	require.Equal(t, item.ID, attempt.Item.ID)
	require.Equal(t, "channel", attempt.ChannelID)
	require.ErrorIs(t, s.MarkDelivered(ctx, item.ID, "provider-ts"), ErrConflict)
	require.NoError(t, s.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	recovered, created, err := reopened.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, attempt.ID, recovered.ID)
	require.NoError(t, reopened.MarkDeliveryUncertain(ctx, item.ID, attempt.ID))
	require.ErrorIs(t, reopened.MarkDelivered(ctx, item.ID, "provider-ts"), ErrConflict)
	// A late exact response from the original sender can still complete its own
	// attempt after an observer has marked it uncertain.
	require.NoError(t, reopened.ConfirmDelivery(ctx, item.ID, attempt.ID, "channel", "provider-ts"))
	require.NoError(t, reopened.ConfirmDelivery(ctx, item.ID, attempt.ID, "channel", "provider-ts"))
	require.ErrorIs(t, reopened.ConfirmDelivery(ctx, item.ID, attempt.ID, "wrong-channel", "provider-ts"), ErrConflict)
	require.ErrorIs(t, reopened.ConfirmDelivery(ctx, item.ID, attempt.ID, "channel", "other-ts"), ErrConflict)
	require.NoError(t, reopened.MarkDelivered(ctx, item.ID, "provider-ts"))
	require.ErrorIs(t, reopened.MarkDelivered(ctx, item.ID, "other-ts"), ErrConflict)
	_ = reopened.MarkDeliveryUncertain(ctx, item.ID, attempt.ID)
	items, err := reopened.PendingOutbox(ctx, "conversation", 10)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestConcurrentDeliveryClaimHasOneCreator(t *testing.T) {
	s, path, item := pendingDeliveryFixture(t, ChannelStream)
	other, err := Open(path)
	require.NoError(t, err)
	defer other.Close()
	ctx := context.Background()
	type result struct {
		attempt DeliveryAttempt
		created bool
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			<-start
			a, created, err := store.PrepareDelivery(ctx, item.ID)
			results <- result{a, created, err}
		}(store)
	}
	close(start)
	wg.Wait()
	close(results)
	var id string
	var creators int
	for r := range results {
		require.NoError(t, r.err)
		if id == "" {
			id = r.attempt.ID
		}
		require.Equal(t, id, r.attempt.ID)
		if r.created {
			creators++
		}
	}
	require.NotEmpty(t, id)
	require.Equal(t, 1, creators)
}

func TestPreviousDeliverySchemaFailsClosedWithoutMigration(t *testing.T) {
	s, path := testStore(t, ChannelStream)
	_, err := s.db.Exec(`UPDATE channelgateway_schema SET version=?`, schemaVersion-1)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	_, err = Open(path)
	require.True(t, errors.Is(err, ErrSchema))
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	var version int
	require.NoError(t, db.QueryRow(`SELECT version FROM channelgateway_schema`).Scan(&version))
	require.Equal(t, schemaVersion-1, version)
}

func TestSegmentedOutboxStillRoutesToOwnedRoot(t *testing.T) {
	s, _, item := pendingDeliveryFixture(t, ThreadSegments)
	require.Equal(t, "root", item.ThreadID)
	attempt, created, err := s.PrepareDelivery(context.Background(), item.ID)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "root", attempt.Item.ThreadID)
}
