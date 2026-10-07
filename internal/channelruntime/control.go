package channelruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/slackcontrol"
)

const retryableExitReportWindow = 90 * time.Second

type controlServer interface {
	io.Closer
	Completed() <-chan slackcontrol.Completion
	Done() <-chan error
}

type controlSpec struct {
	Path     string
	Snapshot func(context.Context) (slackcontrol.Status, error)
}

type supervisorState struct {
	mu       sync.RWMutex
	state    string
	exitCode *int
	terminal bool
	degraded bool
}

func newSupervisorState() *supervisorState { return &supervisorState{state: "running"} }

func (s *supervisorState) finish(code int, terminal, degraded bool) {
	s.mu.Lock()
	s.state, s.exitCode, s.terminal, s.degraded = "exited", &code, terminal, degraded
	s.mu.Unlock()
}

func (s *supervisorState) status(runner *channelstream.Runner) slackcontrol.RunnerStatus {
	s.mu.RLock()
	state, exitCode, terminal, degraded := s.state, s.exitCode, s.terminal, s.degraded
	s.mu.RUnlock()
	if state == "running" && runner != nil {
		degraded = runner.Status().Degraded
		_, terminal = runner.TerminalCause()
	}
	var copied *int
	if exitCode != nil {
		value := *exitCode
		copied = &value
	}
	return slackcontrol.RunnerStatus{State: state, ExitCode: copied, Terminal: terminal, Degraded: degraded}
}

type controlSampler struct {
	identity     slackcontrol.Identity
	conversation string
	store        *channelgateway.Store
	runner       *channelstream.Runner
	supervisor   *supervisorState
	now          func() time.Time
}

func (s *controlSampler) snapshot(ctx context.Context) (slackcontrol.Status, error) {
	result := slackcontrol.Status{Identity: s.identity, Runner: s.supervisor.status(s.runner)}
	now := s.now().UTC()
	projection, err := s.store.HealthProjection(ctx, s.conversation, now)
	if err != nil {
		result.Pump = slackcontrol.PumpStatus{State: "unknown"}
		result.Backlog = slackcontrol.BacklogStatus{State: "unknown"}
		result.Egress = slackcontrol.EgressStatus{State: "unknown"}
		result.Conductor = slackcontrol.ConductorStatus{State: "unknown"}
		return result, nil
	}
	pump := s.runner.PumpHealth(now, true, projection.BacklogState == "pending")
	result.Pump = slackcontrol.PumpStatus{State: pump.State, LastProgressAgeSeconds: pump.LastProgressAgeSeconds}
	result.Backlog = slackcontrol.BacklogStatus{State: projection.BacklogState, OldestAgeSeconds: projection.OldestAgeSeconds}
	result.Egress = slackcontrol.EgressStatus{State: projection.EgressState}
	result.Conductor = slackcontrol.ConductorStatus{State: projection.ConductorState, TurnAgeSeconds: projection.TurnAgeSeconds}
	return result, nil
}

func startControl(spec controlSpec) (controlServer, error) {
	return slackcontrol.Start(slackcontrol.Config{
		Path: spec.Path, UID: os.Getuid(), IOTimeout: 500 * time.Millisecond, StatusTimeout: 250 * time.Millisecond,
		Snapshot: spec.Snapshot,
	})
}

func runnerResult(ctx context.Context, runner *channelstream.Runner, err error) error {
	if err == nil || errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	cause, terminal := runner.TerminalCause()
	if errors.Is(err, channelstream.ErrConfig) || errors.Is(err, channelstream.ErrRunning) {
		cause, terminal = channelstream.ErrorConfig, true
	}
	if terminal {
		return &Error{Kind: KindRunner, Cause: string(cause), Terminal: true}
	}
	return &Error{Kind: KindRunner, Cause: "unknown", Terminal: false}
}

func resultExit(err error) (code int, terminal bool) {
	if err == nil {
		return 0, false
	}
	_, terminal = Classification(err)
	if terminal {
		return 78, true
	}
	return 75, false
}

func runWithControl(ctx context.Context, cfg Config, store *channelgateway.Store, runner *channelstream.Runner, d dependencies) error {
	supervisor := newSupervisorState()
	sampler := &controlSampler{
		identity: slackcontrol.Identity{
			BinarySHA256: cfg.BinarySHA256, ConfigSHA256: cfg.ConfigSHA256, RowBindingAlias: cfg.RowBindingAlias,
		},
		conversation: cfg.ConversationID, store: store, runner: runner, supervisor: supervisor, now: time.Now,
	}
	server, err := d.control(controlSpec{Path: cfg.ControlSocket, Snapshot: sampler.snapshot})
	if err != nil {
		return &Error{Kind: KindRunner, Cause: "socket", Terminal: true}
	}
	defer server.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resultCh := make(chan error, 1)
	go func() { resultCh <- d.run(runCtx, runner) }()

	var result error
	select {
	case runErr := <-resultCh:
		result = runnerResult(ctx, runner, runErr)
	case serveErr, ok := <-server.Done():
		cancel()
		<-resultCh
		if (!ok || serveErr != nil) && ctx.Err() == nil {
			result = &Error{Kind: KindRunner, Cause: "socket", Terminal: true}
		}
	case <-ctx.Done():
		cancel()
		<-resultCh
	}
	code, terminal := resultExit(result)
	degraded := runner.Status().Degraded
	supervisor.finish(code, terminal, degraded)
	if code != 75 || terminal || degraded || ctx.Err() != nil {
		return result
	}
	after := d.after
	if after == nil {
		after = time.After
	}
	timer := after(retryableExitReportWindow)
	for {
		select {
		case completion := <-server.Completed():
			if completion.Recoverable {
				return result
			}
		case <-timer:
			return result
		case <-ctx.Done():
			return result
		case <-server.Done():
			return result
		}
	}
}
