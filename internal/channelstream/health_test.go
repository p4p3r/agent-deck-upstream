package channelstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPumpHealthEveryStateAndAgeBoundary(t *testing.T) {
	runner := &Runner{}
	base := time.Unix(1_800_000_000, 0).UTC()

	got := runner.PumpHealth(base, false, false)
	require.Equal(t, PumpHealth{State: "unknown"}, got)
	got = runner.PumpHealth(base, true, false)
	require.Equal(t, PumpHealth{State: "idle"}, got)
	got = runner.PumpHealth(base, true, true)
	require.Equal(t, PumpHealth{State: "unknown"}, got)

	runner.recordPumpProgress(base)
	for _, tc := range []struct {
		name  string
		at    time.Time
		state string
		age   int64
	}{
		{"fresh zero", base, "fresh", 0},
		{"fresh upper", base.Add(10*time.Second - time.Nanosecond), "fresh", 9},
		{"stale lower", base.Add(10 * time.Second), "stale", 10},
		{"stale upper", base.Add(30*time.Second - time.Nanosecond), "stale", 29},
		{"stalled lower", base.Add(30 * time.Second), "stalled", 30},
		{"maximum clamps", base.Add((315_360_000 + 1) * time.Second), "stalled", 315_360_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runner.PumpHealth(tc.at, true, true)
			require.Equal(t, tc.state, got.State)
			require.NotNil(t, got.LastProgressAgeSeconds)
			require.Equal(t, tc.age, *got.LastProgressAgeSeconds)
		})
	}

	got = runner.PumpHealth(base.Add(-time.Second), true, true)
	require.Equal(t, PumpHealth{State: "unknown"}, got)
	got = runner.PumpHealth(base.Add(time.Hour), true, false)
	require.Equal(t, PumpHealth{State: "idle"}, got)
}

func TestPumpProgressTransitionIsSafeUnderConcurrentSampling(t *testing.T) {
	runner := &Runner{}
	base := time.Unix(1_800_000_000, 0).UTC()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			runner.recordPumpProgress(base.Add(time.Duration(i) * time.Millisecond))
		}
	}()
	for i := 0; i < 1000; i++ {
		got := runner.PumpHealth(base.Add(time.Second), true, true)
		require.Contains(t, []string{"unknown", "fresh"}, got.State)
	}
	<-done
}
