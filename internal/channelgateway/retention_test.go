package channelgateway

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSealRollbackRemovesUnreferencedRecord(t *testing.T) {
	s, path := testStore(t, ChannelStream)
	ctx := context.Background()
	_, err := s.db.Exec(`CREATE TRIGGER fail_inbound BEFORE INSERT ON inbound_events BEGIN SELECT RAISE(ABORT, 'forced insertion failure'); END`)
	require.NoError(t, err)
	_, err = s.Ingest(ctx, inbound("rolled-back", "message", "", false))
	require.ErrorIs(t, err, ErrStorage)
	alias := s.alias("event", "rolled-back")
	_, err = os.Stat(filepath.Join(filepath.Dir(path), "spool", "inbound-"+alias))
	require.ErrorIs(t, err, os.ErrNotExist)
	var count int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM inbound_events WHERE event_id=?`, alias).Scan(&count))
	require.Zero(t, count)
}

func TestPruneRecoversInterruptedSealsAfterRestart(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "gateway.sqlite")
	key := makeSyntheticKey()
	s, err := Open(path, key)
	require.NoError(t, err)
	require.NoError(t, s.CreateConversation(context.Background(), Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding", Mode: ChannelStream,
		AllowedSenders: []string{"alice"},
	}))
	_, err = s.Ingest(context.Background(), inbound("committed", "message", "", false))
	require.NoError(t, err)
	committed := s.alias("event", "committed")
	orphan := s.alias("event", "interrupted")
	require.NoError(t, s.spool.WriteImmutable("inbound", orphan, []byte("orphan private body")))
	outboxAlias := s.alias("outbox", "interrupted-outbox")
	require.NoError(t, s.spool.WriteImmutable("outbound", outboxAlias, []byte("orphan reply")))
	require.NoError(t, s.spool.WriteImmutable("provider", outboxAlias, []byte("orphan provider")))
	threadAlias := s.alias("thread", "interrupted-thread")
	require.NoError(t, s.spool.WriteImmutable("thread", threadAlias, []byte("interrupted-thread")))
	bindingAlias := s.alias("conversation", "interrupted-conversation")
	require.NoError(t, s.saveBinding(nil, "interrupted-conversation", "channel"))
	legacyBindingAlias := s.alias("conversation", "legacy-interrupted-conversation")
	require.NoError(t, s.spool.WriteImmutable("binding", legacyBindingAlias, []byte(`{"channel_id":"channel"}`)))
	require.NoError(t, s.Close())

	s, err = Open(path, key)
	require.NoError(t, err)
	defer s.Close()
	removed := 0
	for i := 0; i < 3; i++ {
		result, err := s.Prune(context.Background(), RetentionPolicy{}, 8)
		require.NoError(t, err)
		removed += result.OrphansDeleted
		if removed == 6 {
			break
		}
	}
	require.Equal(t, 6, removed)
	for _, record := range []struct{ domain, alias string }{
		{"inbound", orphan}, {"outbound", outboxAlias}, {"provider", outboxAlias}, {"thread", threadAlias}, {"binding", bindingAlias}, {"binding", legacyBindingAlias},
	} {
		_, err = s.spool.Read(record.domain, record.alias)
		require.Error(t, err, record.domain)
	}
	body, err := s.spool.Read("inbound", committed)
	require.NoError(t, err)
	require.Equal(t, "body-committed", string(body))
	_, err = s.channelID("conversation")
	require.NoError(t, err)
}

func TestLegacySealedBindingRemainsUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	s, err := Open(path, makeSyntheticKey())
	require.NoError(t, err)
	defer s.Close()
	old := []byte(`{"channel_id":"channel"}`)
	alias := s.alias("conversation", "conversation")
	require.NoError(t, s.spool.WriteImmutable("binding", alias, old))
	require.NoError(t, s.CreateConversation(context.Background(), Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding", Mode: ChannelStream,
		AllowedSenders: []string{"alice"},
	}))
	got, err := s.spool.Read("binding", alias)
	require.NoError(t, err)
	require.Equal(t, old, got)
	_, err = s.Prune(context.Background(), RetentionPolicy{}, 8)
	require.NoError(t, err)
	got, err = s.spool.Read("binding", alias)
	require.NoError(t, err)
	require.Equal(t, old, got)
}

func TestRetentionExpiresDeliveredContentThenMetadata(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	now := time.Unix(1_800_000_000, 0).UTC()
	s.now = func() time.Time { return now }
	ctx := context.Background()
	if result, err := s.Ingest(ctx, inbound("retention-event", "retention-message", "", false)); err != nil || result.Disposition != Accepted {
		t.Fatal("ingress failed")
	}
	turn, err := s.NextTurn(ctx, "conversation")
	if err != nil || turn == nil {
		t.Fatal("turn missing")
	}
	item := complete(t, s, turn, "synthetic-private-reply")
	if item == nil {
		t.Fatal("reply missing")
	}
	claim, created, err := s.PrepareDelivery(ctx, item.ID)
	if err != nil || !created {
		t.Fatal("delivery claim missing")
	}
	if err := s.ConfirmDelivery(ctx, item.ID, claim.ID, "channel", "synthetic-provider-id"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24*time.Hour - time.Second)
	if result, err := s.Prune(ctx, RetentionPolicy{}, 8); err != nil || result.ContentDeleted != 0 {
		t.Fatal("content expired early")
	}
	now = now.Add(2 * time.Second)
	if result, err := s.Prune(ctx, RetentionPolicy{}, 8); err != nil || result.ContentDeleted != 2 {
		t.Fatal("terminal content did not expire")
	}
	after, err := s.OutboxRecord(ctx, item.ID)
	if err != nil || after.State != DeliveredDelivery || after.Body != "" || after.ExternalMessageID != "" {
		t.Fatal("delivered ledger state changed after expiry")
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM outbox WHERE id=?`, item.ID).Scan(&state); err != nil || state != string(DeliveredDelivery) {
		t.Fatal("terminal metadata expired early")
	}
	now = time.Unix(1_800_000_000, 0).Add(90*24*time.Hour + time.Second)
	if result, err := s.Prune(ctx, RetentionPolicy{}, 8); err != nil || result.MetadataDeleted != 1 {
		t.Fatal("terminal metadata did not expire")
	}
	if err := s.db.QueryRow(`SELECT state FROM outbox WHERE id=?`, item.ID).Scan(&state); err == nil {
		t.Fatal("expired metadata remained")
	}
}

func TestRetentionRacesLateConfirmationWithoutResend(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	initial := time.Unix(1_800_000_000, 0).UTC()
	var clockMu sync.Mutex
	now := initial
	s.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	ctx := context.Background()
	if _, err := s.Ingest(ctx, inbound("race-event", "race-message", "", false)); err != nil {
		t.Fatal(err)
	}
	turn, err := s.NextTurn(ctx, "conversation")
	if err != nil || turn == nil {
		t.Fatal("turn missing")
	}
	item := complete(t, s, turn, "synthetic-race-reply")
	claim, created, err := s.PrepareDelivery(ctx, item.ID)
	if err != nil || !created {
		t.Fatal("claim missing")
	}
	if err := s.MarkDeliveryUncertain(ctx, item.ID, claim.ID); err != nil {
		t.Fatal(err)
	}
	clockMu.Lock()
	now = initial.Add(8 * 24 * time.Hour)
	clockMu.Unlock()
	var wg sync.WaitGroup
	var pruneErr, confirmErr error
	wg.Add(2)
	go func() { defer wg.Done(); _, pruneErr = s.Prune(ctx, RetentionPolicy{}, 8) }()
	go func() {
		defer wg.Done()
		confirmErr = s.ConfirmDelivery(ctx, item.ID, claim.ID, "channel", "synthetic-late-provider")
	}()
	wg.Wait()
	if pruneErr != nil || confirmErr != nil {
		t.Fatal("retention/confirmation race failed")
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM outbox WHERE id=?`, item.ID).Scan(&state); err != nil || state != string(DeliveredDelivery) {
		t.Fatal("late confirmation lost ownership")
	}
	pending, err := s.PendingOutbox(ctx, "conversation", 8)
	if err != nil || len(pending) != 0 {
		t.Fatal("late confirmation became resendable")
	}
}

func TestRetentionKeepsUncertainOwnershipAndActiveWork(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	now := time.Unix(1_800_000_000, 0).UTC()
	s.now = func() time.Time { return now }
	ctx := context.Background()
	_, _ = s.Ingest(ctx, inbound("uncertain-event", "uncertain-message", "", false))
	turn, err := s.NextTurn(ctx, "conversation")
	if err != nil || turn == nil {
		t.Fatal("turn missing")
	}
	item := complete(t, s, turn, "synthetic-uncertain-reply")
	if item == nil {
		t.Fatal("reply missing")
	}
	claim, created, err := s.PrepareDelivery(ctx, item.ID)
	if err != nil || !created {
		t.Fatal("claim missing")
	}
	if err := s.MarkDeliveryUncertain(ctx, item.ID, claim.ID); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Ingest(ctx, inbound("queued-event", "queued-message", "", false))
	now = now.Add(7*24*time.Hour + time.Second)
	if result, err := s.Prune(ctx, RetentionPolicy{}, 8); err != nil || result.ContentDeleted != 2 {
		t.Fatal("uncertain content did not expire")
	}
	now = now.Add(91 * 24 * time.Hour)
	if _, err := s.Prune(ctx, RetentionPolicy{}, 8); err != nil {
		t.Fatal(err)
	}
	after, err := s.OutboxRecord(ctx, item.ID)
	if err != nil || after.State != UncertainDelivery {
		t.Fatal("uncertain ownership was lost")
	}
	pending, err := s.PendingOutbox(ctx, "conversation", 8)
	if err != nil || len(pending) != 0 {
		t.Fatal("uncertain item became sendable")
	}
	next, err := s.NextTurn(ctx, "conversation")
	if err != nil || next == nil || next.Body != "body-queued-event" {
		t.Fatal("queued content expired")
	}
}
