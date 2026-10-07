package channelgateway

import (
	"context"
	"sync"
	"testing"
	"time"
)

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
