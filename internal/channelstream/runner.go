// Package channelstream composes the source-only Slack channel_stream gateway.
// It owns no configuration source, credentials, process lifecycle, or schema.
package channelstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
)

const (
	defaultPollInterval = 2 * time.Second
	defaultBackoffMin   = time.Second
	defaultBackoffMax   = 30 * time.Second
	drainBatch          = 32
	workBatch           = 32
	retentionBatch      = 64
	retentionInterval   = time.Hour
)

var (
	ErrConfig       = errors.New("channelstream: invalid configuration")
	ErrRunning      = errors.New("channelstream: already running")
	ErrLinkDisabled = errors.New("channelstream: socket link disabled")
	ErrFatal        = errors.New("channelstream: terminal failure")
)

// Socket opens exactly one Socket Mode connection per Run call. Production
// callers use slacknetwork.SocketClient; a local fake can provide the same
// bounded callback protocol without a network or a credential.
type Socket interface {
	Run(context.Context, slacknetwork.EnvelopeHandler) error
}

// Timer and Clock make polling and reconnect delays deterministic in local
// tests. A timer is stopped on every cancellation or alternate wake-up.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type Clock interface {
	NewTimer(time.Duration) Timer
}

type realClock struct{}
type realTimer struct{ *time.Timer }

func (realClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }
func (t realTimer) C() <-chan time.Time          { return t.Timer.C }

type Phase string

const (
	PhaseStopped Phase = "stopped"
	PhaseStartup Phase = "startup"
	PhaseSession Phase = "session"
	PhaseBackoff Phase = "backoff"
)

type ErrorClass string

const (
	ErrorNone         ErrorClass = "none"
	ErrorSocket       ErrorClass = "socket"
	ErrorWork         ErrorClass = "work"
	ErrorDelivery     ErrorClass = "delivery"
	ErrorConfig       ErrorClass = "config"
	ErrorProtocol     ErrorClass = "protocol"
	ErrorLinkDisabled ErrorClass = "link_disabled"
)

// Status contains only categories. Neither external text, opaque provider IDs,
// errors, nor credentials can enter a normal status response.
type Status struct {
	Phase     Phase
	Work      channelreconcile.State
	Degraded  bool
	LastError ErrorClass
}

// Runner owns one conversation's socket lifecycle and sole work pump. It does
// not own the Store or its shutdown. A second concurrent Run is rejected.
type Runner struct {
	Socket       Socket
	Handler      slackgateway.Handler
	Worker       *channelreconcile.Worker
	Delivery     *slackgateway.DeliveryWorker
	PollInterval time.Duration
	BackoffMin   time.Duration
	BackoffMax   time.Duration
	Clock        Clock

	mu            sync.Mutex
	running       bool
	terminal      bool
	status        Status
	progress      atomic.Uint64
	lastRetention time.Time // pump goroutine only
}

// TerminalCause returns a fixed code only when this run ended in a known
// terminal safety failure. LastError alone may describe a transient outage.
func (r *Runner) TerminalCause() (ErrorClass, bool) {
	if r == nil {
		return ErrorConfig, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status.LastError, r.terminal
}

func (r *Runner) Status() Status {
	if r == nil {
		return Status{Phase: PhaseStopped, LastError: ErrorConfig}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.status
	if s.Phase == "" {
		s.Phase = PhaseStopped
	}
	if s.LastError == "" {
		s.LastError = ErrorNone
	}
	return s
}

func (r *Runner) update(fn func(*Status)) {
	r.mu.Lock()
	fn(&r.status)
	r.mu.Unlock()
}

// Preserve the first terminal cause, whether it came from the socket or pump.
func (r *Runner) setError(class ErrorClass, terminal bool) {
	r.mu.Lock()
	if !r.terminal {
		r.status.LastError = class
		if terminal {
			r.terminal = true
		}
	}
	r.mu.Unlock()
}

func (r *Runner) timings() (poll, min, max time.Duration, clock Clock, ok bool) {
	poll, min, max, clock = r.PollInterval, r.BackoffMin, r.BackoffMax, r.Clock
	if poll < 0 || min < 0 || max < 0 || min > 0 && min < 100*time.Millisecond {
		// A caller-configured near-zero reconnect delay can busy-loop.
		return 0, 0, 0, nil, false
	}
	if poll == 0 {
		poll = defaultPollInterval
	}
	if min == 0 {
		min = defaultBackoffMin
	}
	if max == 0 {
		max = defaultBackoffMax
	}
	if max < min {
		return 0, 0, 0, nil, false
	}
	if clock == nil {
		clock = realClock{}
	}
	return poll, min, max, clock, true
}

func (r *Runner) valid() bool {
	if r.Socket == nil || r.Handler.Store == nil || r.Worker == nil || r.Worker.Store == nil || r.Worker.RowDriver == nil ||
		r.Delivery == nil || r.Delivery.Store == nil || r.Delivery.Sender == nil {
		return false
	}
	c := r.Handler.Config
	if c.ConversationID == "" || c.AppID == "" || c.TeamID == "" || c.ChannelID == "" || c.BotUserID == "" || len(c.AllowedUserIDs) == 0 ||
		r.Worker.Store != r.Handler.Store || r.Delivery.Store != r.Handler.Store ||
		r.Delivery.ConversationID != c.ConversationID || r.Delivery.ChannelID != c.ChannelID {
		return false
	}
	seen := make(map[string]bool, len(c.AllowedUserIDs))
	for _, id := range c.AllowedUserIDs {
		if id == "" || id == c.BotUserID || seen[id] {
			return false
		}
		seen[id] = true
	}
	_, _, _, _, ok := r.timings()
	return ok
}

func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func wait(ctx context.Context, clock Clock, d time.Duration, wake <-chan struct{}) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C():
		return true
	case <-wake:
		return true
	}
}

func fatalDependency(err error) bool {
	return errors.Is(err, channelgateway.ErrSchema) || errors.Is(err, channelgateway.ErrInvalid) ||
		errors.Is(err, channelgateway.ErrNotFound) || errors.Is(err, channelgateway.ErrUnauthorized) ||
		errors.Is(err, channelgateway.ErrConflict) ||
		errors.Is(err, slackgateway.ErrConfig) || errors.Is(err, slacknetwork.ErrConfig)
}

func (r *Runner) drain(ctx context.Context, wake chan<- struct{}) error {
	for batch := 0; batch < workBatch; batch++ {
		results, err := r.Delivery.DrainPending(ctx, drainBatch)
		if err != nil {
			if fatalDependency(err) {
				r.setError(ErrorConfig, true)
				return ErrFatal
			}
			r.setError(ErrorDelivery, false)
			return nil // ambiguous sends are uncertain; later pending work survives
		}
		if len(results) < drainBatch {
			return nil
		}
	}
	signal(wake) // bounded batch yields before further durable work
	return nil
}

func (r *Runner) step(ctx context.Context, wake chan<- struct{}) error {
	if time.Since(r.lastRetention) >= retentionInterval {
		result, err := r.Handler.Store.Prune(ctx, channelgateway.RetentionPolicy{}, retentionBatch)
		if err != nil {
			if fatalDependency(err) {
				r.setError(ErrorConfig, true)
				return ErrFatal
			}
			r.setError(ErrorWork, false)
			return nil
		}
		if result.ContentDeleted+result.MetadataDeleted+result.OrphansDeleted < retentionBatch {
			r.lastRetention = time.Now()
		}
	}
	if err := r.drain(ctx, wake); err != nil {
		return err
	}
	if r.Status().Degraded {
		return nil
	}
	for i := 0; i < workBatch; i++ {
		result, err := r.Worker.RunOne(ctx, r.Handler.Config.ConversationID)
		if err != nil {
			if fatalDependency(err) {
				r.setError(ErrorConfig, true)
				return ErrFatal
			}
			r.setError(ErrorWork, false)
			return nil
		}
		r.update(func(s *Status) { s.Work = result.State })
		switch result.State {
		case channelreconcile.Completed:
			if err := r.drain(ctx, wake); err != nil {
				return err
			}
		case channelreconcile.Idle, channelreconcile.InProgress:
			return nil
		case channelreconcile.NeedsReconciliation:
			r.update(func(s *Status) { s.Degraded = true })
			return nil
		default:
			r.setError(ErrorProtocol, true)
			return ErrFatal
		}
	}
	signal(wake) // do not monopolize the work pump on a large backlog
	return nil
}

func (r *Runner) pump(ctx context.Context, wake chan struct{}, poll time.Duration, clock Clock, fatal chan<- error) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.step(ctx, wake); err != nil {
			select {
			case fatal <- err:
			default:
			}
			return
		}
		if !wait(ctx, clock, poll, wake) {
			return
		}
	}
}

func doubled(d, cap time.Duration) time.Duration {
	if d >= cap/2 {
		return cap
	}
	return d * 2
}

func (r *Runner) callback(ctx context.Context, raw []byte, ack func(context.Context, []byte) error, wake chan<- struct{}) error {
	result, err := r.Handler.Handle(ctx, raw, ack)
	if err != nil {
		return err
	}
	if !result.Ignored && result.Intake.Disposition == channelgateway.Accepted {
		r.progress.Add(1)
		// A duplicate may be the retry after the first ack failed. Its
		// persisted turn still needs a wake; successful ack is socket progress.
		signal(wake) // Handle has returned only after the exact ack succeeds
	}
	return nil
}

func (r *Runner) socketFailure(err, callbackErr error) (terminal bool, result error, class ErrorClass, immediate bool) {
	var disconnect *slacknetwork.DisconnectError
	if errors.As(err, &disconnect) {
		if disconnect.Reason == slacknetwork.DisconnectLinkDisabled {
			return true, ErrLinkDisabled, ErrorLinkDisabled, false
		}
		if disconnect.Reason == slacknetwork.DisconnectWarning || disconnect.Reason == slacknetwork.DisconnectRefreshRequested {
			return false, nil, ErrorSocket, true
		}
	}
	if errors.Is(err, slacknetwork.ErrCallback) {
		if fatalDependency(callbackErr) {
			return true, ErrFatal, ErrorConfig, false
		}
		if errors.Is(callbackErr, slackgateway.ErrEnvelope) {
			return true, ErrFatal, ErrorProtocol, false
		}
		if callbackErr == nil {
			return true, ErrFatal, ErrorProtocol, false
		}
		return false, nil, ErrorSocket, false
	}
	if errors.Is(err, slacknetwork.ErrOpenTransient) {
		return false, nil, ErrorSocket, false
	}
	if errors.Is(err, slacknetwork.ErrOpenProtocol) || errors.Is(err, slacknetwork.ErrOpenUnknown) {
		return true, ErrFatal, ErrorProtocol, false
	}
	if errors.Is(err, slacknetwork.ErrConfig) || errors.Is(err, slacknetwork.ErrOpenAuth) ||
		errors.Is(err, slacknetwork.ErrOpenConfig) || errors.Is(err, slacknetwork.ErrOpen) {
		// A legacy, unclassified ErrOpen still fails closed.
		return true, ErrFatal, ErrorConfig, false
	}
	if errors.Is(err, slacknetwork.ErrProtocol) || errors.Is(err, slacknetwork.ErrDisconnected) {
		return true, ErrFatal, ErrorProtocol, false
	}
	if errors.Is(err, slacknetwork.ErrReconnect) || errors.Is(err, slacknetwork.ErrAck) {
		return false, nil, ErrorSocket, false
	}
	return true, ErrFatal, ErrorProtocol, false
}

func contextResult(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return nil
}

// Run serves one conversation until cancellation or a terminal condition.
// It never replays a callback, agent attempt, or ambiguous Slack post locally.
// Reconnect creates only one socket at a time and uses capped backoff. A
// canceled Run returns nil after both the socket and work pump exit.
func (r *Runner) Run(ctx context.Context) error {
	if r == nil || ctx == nil {
		return ErrConfig
	}
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrRunning
	}
	r.running = true
	r.terminal = false
	r.status = Status{Phase: PhaseStartup, Work: channelreconcile.Idle, LastError: ErrorNone}
	r.progress.Store(0)
	r.lastRetention = time.Time{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.running = false
		r.status.Phase = PhaseStopped
		r.mu.Unlock()
	}()
	if !r.valid() {
		r.setError(ErrorConfig, true)
		return ErrConfig
	}
	poll, min, max, clock, _ := r.timings()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wake := make(chan struct{}, 1)
	fatal := make(chan error, 1)
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		r.pump(runCtx, wake, poll, clock, fatal)
		if len(fatal) != 0 {
			cancel() // interrupt an open socket when the work pump fails
		}
	}()
	defer func() { cancel(); <-pumpDone }()

	backoff := min
	firstRefresh := true
	for runCtx.Err() == nil {
		before := r.progress.Load()
		r.update(func(s *Status) { s.Phase = PhaseSession })
		var callbackErr error
		err := r.Socket.Run(runCtx, func(c context.Context, raw []byte, ack func(context.Context, []byte) error) error {
			callbackErr = r.callback(c, raw, ack, wake)
			return callbackErr
		})
		if ctx.Err() != nil {
			return contextResult(ctx)
		}
		if runCtx.Err() != nil {
			select {
			case pumpErr := <-fatal:
				return pumpErr
			default:
				return ErrFatal // pump interrupted the socket
			}
		}
		terminal, result, class, immediate := r.socketFailure(err, callbackErr)
		if terminal {
			r.setError(class, true)
			return result
		}
		select {
		case pumpErr := <-fatal:
			return pumpErr
		default:
		}
		r.setError(class, false)
		if r.progress.Load() != before {
			backoff, firstRefresh = min, true
		}
		delay := backoff
		if immediate && firstRefresh {
			delay, firstRefresh = 0, false
		} else {
			backoff = doubled(backoff, max)
		}
		r.update(func(s *Status) { s.Phase = PhaseBackoff })
		if !wait(runCtx, clock, delay, nil) {
			if ctx.Err() != nil {
				return contextResult(ctx)
			}
			return ErrFatal
		}
	}
	if ctx.Err() != nil {
		return contextResult(ctx)
	}
	select {
	case pumpErr := <-fatal:
		return pumpErr
	default:
		return ErrFatal
	}
}
