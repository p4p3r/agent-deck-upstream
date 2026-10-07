package channelgateway

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHealthProjectionTracksDurableBacklogConductorAndEgress(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	now := time.Unix(1_800_000_000, 0).UTC()
	s.now = func() time.Time { return now }
	ctx := context.Background()

	projection, err := s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, HealthProjection{BacklogState: "empty", EgressState: "clear", ConductorState: "idle"}, projection)

	in := inbound("SENTINEL-EXACT-EVENT", "SENTINEL-EXACT-MESSAGE", "", false)
	in.Body = "SENTINEL-PRIVATE-BODY"
	_, err = s.Ingest(ctx, in)
	require.NoError(t, err)
	now = now.Add(9*time.Second + 999*time.Millisecond)
	projection, err = s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, "pending", projection.BacklogState)
	require.Equal(t, int64(9), *projection.OldestAgeSeconds)
	require.Equal(t, "idle", projection.ConductorState)
	require.Equal(t, "clear", projection.EgressState)

	turn, err := s.NextTurn(ctx, "conversation")
	require.NoError(t, err)
	require.NotNil(t, turn)
	projection, err = s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, "working", projection.ConductorState)
	require.Equal(t, int64(9), *projection.TurnAgeSeconds)

	item := complete(t, s, turn, "SENTINEL-PRIVATE-REPLY")
	projection, err = s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, "pending", projection.BacklogState)
	require.Equal(t, "idle", projection.ConductorState)

	attempt, created, err := s.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.True(t, created)
	projection, err = s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, "unknown", projection.EgressState)

	require.NoError(t, s.MarkDeliveryUncertain(ctx, item.ID, attempt.ID))
	projection, err = s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, "uncertain", projection.EgressState)

	require.NoError(t, s.ConfirmDelivery(ctx, item.ID, attempt.ID, "channel", "SENTINEL-EXACT-PROVIDER-ID"))
	projection, err = s.HealthProjection(ctx, "conversation", now)
	require.NoError(t, err)
	require.Equal(t, HealthProjection{BacklogState: "empty", EgressState: "clear", ConductorState: "idle"}, projection)

	encoded, err := json.Marshal(projection)
	require.NoError(t, err)
	for _, private := range []string{"SENTINEL-PRIVATE-BODY", "SENTINEL-PRIVATE-REPLY", "SENTINEL-EXACT-EVENT", "SENTINEL-EXACT-MESSAGE", "SENTINEL-EXACT-PROVIDER-ID", "channel", "alice"} {
		require.False(t, strings.Contains(string(encoded), private), private)
	}
}

func TestHealthProjectionAgeBoundariesAndClockSkew(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	now := time.Unix(1_800_000_000, 0).UTC()
	s.now = func() time.Time { return now }
	_, err := s.Ingest(context.Background(), inbound("age-event", "age-message", "", false))
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		now  time.Time
		age  int64
	}{
		{"same second", now, 0},
		{"subsecond", now.Add(999 * time.Millisecond), 0},
		{"next second", now.Add(time.Second), 1},
		{"future timestamp clamps", now.Add(-time.Second), 0},
		{"maximum clamps", now.Add((315_360_000 + 10) * time.Second), 315_360_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection, err := s.HealthProjection(context.Background(), "conversation", tc.now)
			require.NoError(t, err)
			require.Equal(t, tc.age, *projection.OldestAgeSeconds)
		})
	}
}

func TestHealthProjectionHonorsQueryTimeout(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	connection, err := s.db.Conn(context.Background())
	require.NoError(t, err)
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = s.HealthProjection(ctx, "conversation", time.Now())
	require.ErrorIs(t, err, ErrStorage)
	require.Less(t, time.Since(started), 300*time.Millisecond)
}
