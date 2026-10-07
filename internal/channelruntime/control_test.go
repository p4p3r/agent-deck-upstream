package channelruntime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/slackcontrol"
	"github.com/stretchr/testify/require"
)

type fakeControlServer struct {
	completed chan slackcontrol.Completion
	done      chan error
	closed    atomic.Bool
	onClose   func()
}

func newFakeControlServer() *fakeControlServer {
	return &fakeControlServer{completed: make(chan slackcontrol.Completion, 4), done: make(chan error, 1)}
}

func (s *fakeControlServer) Completed() <-chan slackcontrol.Completion { return s.completed }
func (s *fakeControlServer) Done() <-chan error                        { return s.done }
func (s *fakeControlServer) Close() error {
	if s.closed.CompareAndSwap(false, true) && s.onClose != nil {
		s.onClose()
	}
	return nil
}

func controlConfig() Config {
	config := rowConfig()
	config.ControlSocket = "/fixture/control.sock"
	config.BinarySHA256 = strings.Repeat("a", 64)
	config.ConfigSHA256 = strings.Repeat("b", 64)
	config.RowBindingAlias = "row_binding_alias_fixture"
	return config
}

func TestControlLifecyclePublishesRetryableExit75BeforeOrderedShutdown(t *testing.T) {
	config := controlConfig()
	request := testRowRequest(t, config)
	driver := &testRowDriver{}
	runnerRelease := make(chan struct{})
	server := newFakeControlServer()
	specCh := make(chan controlSpec, 1)
	var runCalls atomic.Int32
	d, leaseClosed := testRowDeps(driver, func(context.Context, *channelstream.Runner) error {
		runCalls.Add(1)
		<-runnerRelease
		return errors.New("synthetic retryable runner exit")
	})
	d.control = func(spec controlSpec) (controlServer, error) {
		specCh <- spec
		server.onClose = func() {
			if driver.closed != 0 || *leaseClosed != 0 {
				t.Error("control closed after driver or lease")
			}
		}
		return server, nil
	}
	timer := make(chan time.Time)
	d.after = func(duration time.Duration) <-chan time.Time {
		require.Equal(t, retryableExitReportWindow, duration)
		return timer
	}
	result := make(chan error, 1)
	go func() { result <- run(context.Background(), request, d) }()

	var spec controlSpec
	select {
	case spec = <-specCh:
	case <-time.After(time.Second):
		t.Fatal("control server was not started")
	}
	running, err := spec.Snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, "running", running.Runner.State)
	require.Nil(t, running.Runner.ExitCode)
	require.Equal(t, "empty", running.Backlog.State)
	require.Equal(t, "idle", running.Pump.State)
	require.Equal(t, slackcontrol.Identity{BinarySHA256: config.BinarySHA256, ConfigSHA256: config.ConfigSHA256, RowBindingAlias: config.RowBindingAlias}, running.Identity)

	close(runnerRelease)
	require.Eventually(t, func() bool {
		exited, snapshotErr := spec.Snapshot(context.Background())
		return snapshotErr == nil && exited.Runner.State == "exited" && exited.Runner.ExitCode != nil &&
			*exited.Runner.ExitCode == 75 && !exited.Runner.Terminal && !exited.Runner.Degraded
	}, time.Second, time.Millisecond)
	server.completed <- slackcontrol.Completion{RunnerState: "exited", Recoverable: false}
	select {
	case <-result:
		t.Fatal("unknown post-exit handshake ended the report window")
	case <-time.After(20 * time.Millisecond):
	}
	server.completed <- slackcontrol.Completion{RunnerState: "exited", Recoverable: true}
	select {
	case err := <-result:
		cause, terminal := Classification(err)
		require.Equal(t, "unknown", cause)
		require.False(t, terminal)
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish after post-exit handshake")
	}
	require.Equal(t, int32(1), runCalls.Load(), "control sampling started another pump")
	require.True(t, server.closed.Load())
	require.Equal(t, 1, driver.closed)
	require.Equal(t, 1, *leaseClosed)
}

func TestControlLifecycleTerminalExitDoesNotBecomeRecoverable(t *testing.T) {
	config := controlConfig()
	request := testRowRequest(t, config)
	server := newFakeControlServer()
	specCh := make(chan controlSpec, 1)
	d, _ := testRowDeps(&testRowDriver{}, func(context.Context, *channelstream.Runner) error {
		return channelstream.ErrConfig
	})
	d.control = func(spec controlSpec) (controlServer, error) {
		specCh <- spec
		return server, nil
	}
	d.after = func(time.Duration) <-chan time.Time {
		t.Fatal("terminal exit entered retryable report window")
		return nil
	}
	err := run(context.Background(), request, d)
	cause, terminal := Classification(err)
	require.Equal(t, "config", cause)
	require.True(t, terminal)
	require.True(t, server.closed.Load())
}

func TestControlSamplingAndRunnerTransitionAreRaceSafe(t *testing.T) {
	config := controlConfig()
	request := testRowRequest(t, config)
	server := newFakeControlServer()
	specCh := make(chan controlSpec, 1)
	release := make(chan struct{})
	d, _ := testRowDeps(&testRowDriver{}, func(context.Context, *channelstream.Runner) error {
		<-release
		return errors.New("retryable")
	})
	d.control = func(spec controlSpec) (controlServer, error) { specCh <- spec; return server, nil }
	timer := make(chan time.Time, 1)
	d.after = func(time.Duration) <-chan time.Time { return timer }
	result := make(chan error, 1)
	go func() { result <- run(context.Background(), request, d) }()
	spec := <-specCh
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				status, err := spec.Snapshot(context.Background())
				require.NoError(t, err)
				require.Contains(t, []string{"running", "exited"}, status.Runner.State)
			}
		}()
	}
	close(release)
	wg.Wait()
	timer <- time.Now()
	<-result
}

func TestControlSamplerTerminalDegradedAndUnknownStatesFailClosed(t *testing.T) {
	store, err := channelgateway.Open(t.TempDir()+"/gateway.sqlite", bytes.Repeat([]byte{0x42}, 32))
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.CreateConversation(context.Background(), channelgateway.Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding",
		Mode: channelgateway.ChannelStream, AllowedSenders: []string{"sender"},
	}))
	runner := &channelstream.Runner{}
	supervisor := newSupervisorState()
	sampler := &controlSampler{
		identity: slackcontrol.Identity{
			BinarySHA256: strings.Repeat("a", 64), ConfigSHA256: strings.Repeat("b", 64), RowBindingAlias: "row_binding_alias_fixture",
		},
		conversation: "conversation", store: store, runner: runner, supervisor: supervisor,
		now: func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	unknown, err := sampler.snapshot(canceled)
	require.NoError(t, err)
	require.Equal(t, "unknown", unknown.Pump.State)
	require.Equal(t, "unknown", unknown.Backlog.State)
	require.Equal(t, "unknown", unknown.Egress.State)
	require.Equal(t, "unknown", unknown.Conductor.State)

	supervisor.finish(78, true, false)
	terminal, err := sampler.snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, "exited", terminal.Runner.State)
	require.Equal(t, 78, *terminal.Runner.ExitCode)
	require.True(t, terminal.Runner.Terminal)
	require.False(t, terminal.Runner.Degraded)

	supervisor = newSupervisorState()
	supervisor.finish(75, false, true)
	sampler.supervisor = supervisor
	degraded, err := sampler.snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, 75, *degraded.Runner.ExitCode)
	require.False(t, degraded.Runner.Terminal)
	require.True(t, degraded.Runner.Degraded)
}
