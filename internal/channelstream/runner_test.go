package channelstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/rowdriver"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
	"github.com/stretchr/testify/require"
)

const (
	runConversation = "conversation"
	runChannel      = "C-bound"
	runTeam         = "T-bound"
	runApp          = "A-bound"
	runUser         = "U-allowed"
	runBot          = "U-bot"
	runPrivate      = "private-prompt-marker"
)

type manualTimer struct {
	ch       chan time.Time
	mu       sync.Mutex
	stopped  bool
	duration time.Duration
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }
func (t *manualTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped
	t.stopped = true
	return wasActive
}
func (t *manualTimer) fire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.stopped {
		select {
		case t.ch <- time.Now():
		default:
		}
	}
}
func (t *manualTimer) active() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.stopped
}

type manualClock struct {
	created  chan *manualTimer
	mu       sync.Mutex
	onCreate func(time.Duration)
}

func newManualClock() *manualClock { return &manualClock{created: make(chan *manualTimer, 64)} }
func (c *manualClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	hook := c.onCreate
	c.mu.Unlock()
	if hook != nil {
		hook(d)
	}
	t := &manualTimer{ch: make(chan time.Time, 1), duration: d}
	c.created <- t
	return t
}
func (c *manualClock) setOnCreate(hook func(time.Duration)) {
	c.mu.Lock()
	c.onCreate = hook
	c.mu.Unlock()
}
func (c *manualClock) nextNonPoll(t *testing.T) *manualTimer {
	t.Helper()
	for {
		timer := c.next(t)
		if timer.duration != time.Minute {
			return timer
		}
	}
}
func (c *manualClock) nextActive(t *testing.T, duration time.Duration) *manualTimer {
	t.Helper()
	for {
		timer := c.next(t)
		if timer.duration == duration && timer.active() {
			return timer
		}
	}
}
func (c *manualClock) next(t *testing.T) *manualTimer {
	t.Helper()
	select {
	case timer := <-c.created:
		return timer
	case <-time.After(4 * time.Second):
		t.Fatal("runner did not arm expected timer")
		return nil
	}
}

type fakeSocket struct {
	mu       sync.Mutex
	handler  slacknetwork.EnvelopeHandler
	runs     int
	steps    []error // nil means hold the session until canceled
	entered  chan int
	onRun    func(int, slacknetwork.EnvelopeHandler) error
	onRunCtx func(context.Context, int, slacknetwork.EnvelopeHandler) error
}

func newFakeSocket(steps ...error) *fakeSocket {
	return &fakeSocket{steps: steps, entered: make(chan int, 64)}
}
func (s *fakeSocket) Run(ctx context.Context, handler slacknetwork.EnvelopeHandler) error {
	s.mu.Lock()
	s.runs++
	n := s.runs
	s.handler = handler
	var step error
	if n <= len(s.steps) {
		step = s.steps[n-1]
	}
	s.mu.Unlock()
	s.entered <- n
	if s.onRun != nil {
		if err := s.onRun(n, handler); err != nil {
			return err
		}
	}
	if s.onRunCtx != nil {
		if err := s.onRunCtx(ctx, n, handler); err != nil {
			return err
		}
	}
	if step != nil {
		return step
	}
	<-ctx.Done()
	return slacknetwork.ErrCanceled
}
func (s *fakeSocket) waitRun(t *testing.T, want int) {
	t.Helper()
	select {
	case got := <-s.entered:
		require.Equal(t, want, got)
	case <-time.After(4 * time.Second):
		t.Fatal("socket run did not start")
	}
}
func (s *fakeSocket) emit(ctx context.Context, raw []byte, ack func([]byte) error) error {
	s.mu.Lock()
	h := s.handler
	s.mu.Unlock()
	if h == nil {
		return errors.New("test socket has no handler")
	}
	return h(ctx, raw, func(_ context.Context, body []byte) error { return ack(body) })
}
func (s *fakeSocket) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.runs }

type fakeDriver struct {
	mu         sync.Mutex
	turns      []channelreconcile.ExternalTurn
	starts     int
	inspects   int
	status     string
	startErr   error
	inspectErr error
	bodies     []string
	started    chan int
	startWait  <-chan struct{}
	openWait   <-chan struct{}
	keys       map[string]string
	resultKey  string
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{status: "completed", started: make(chan int, 64), keys: map[string]string{}}
}
func (d *fakeDriver) OpenThread(ctx context.Context) (string, error) {
	if d.openWait != nil {
		select {
		case <-d.openWait:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "agent-thread", nil
}
func (d *fakeDriver) ResumeThread(context.Context, string) error { return nil }
func (d *fakeDriver) InspectThread(context.Context, string) ([]channelreconcile.ExternalTurn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inspects++
	return append([]channelreconcile.ExternalTurn(nil), d.turns...), d.inspectErr
}
func (d *fakeDriver) StartTurn(ctx context.Context, _ string, body string, accepted func(string) error) (channelreconcile.ExternalTurn, error) {
	d.mu.Lock()
	d.starts++
	n := d.starts
	d.bodies = append(d.bodies, body)
	ext := channelreconcile.ExternalTurn{ID: fmt.Sprintf("external-%d", n), Status: d.status, Reply: fmt.Sprintf("reply-%d", n)}
	d.turns = append(d.turns, ext)
	err := d.startErr
	d.mu.Unlock()
	d.started <- n
	if d.startWait != nil {
		select {
		case <-d.startWait:
		case <-ctx.Done():
			return channelreconcile.ExternalTurn{}, ctx.Err()
		}
	}
	if err != nil {
		return channelreconcile.ExternalTurn{}, err
	}
	if err := accepted(ext.ID); err != nil {
		return channelreconcile.ExternalTurn{}, err
	}
	return ext, nil
}
func (d *fakeDriver) waitStart(t *testing.T, want int) {
	t.Helper()
	select {
	case got := <-d.started:
		require.Equal(t, want, got)
	case <-time.After(4 * time.Second):
		t.Fatal("agent turn did not start")
	}
}
func (d *fakeDriver) counts() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts, d.inspects
}
func (d *fakeDriver) completeFirst() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.turns[0].Status = "completed"
}

func (d *fakeDriver) SubmitRowOperation(ctx context.Context, key, body string) (rowdriver.Operation, error) {
	d.mu.Lock()
	d.starts++
	n := d.starts
	d.bodies = append(d.bodies, body)
	id := fmt.Sprintf("row-send-%d", n)
	d.keys[id] = key
	ext := channelreconcile.ExternalTurn{ID: id, Status: d.status, Reply: fmt.Sprintf("reply-%d", n)}
	d.turns = append(d.turns, ext)
	err := d.startErr
	resultKey := key
	if d.resultKey != "" {
		resultKey = d.resultKey
	}
	d.mu.Unlock()
	d.started <- n
	if d.startWait != nil {
		select {
		case <-d.startWait:
		case <-ctx.Done():
			return rowdriver.Operation{}, ctx.Err()
		}
	}
	if err != nil {
		return rowdriver.Operation{}, err
	}
	return fakeRowOperation(resultKey, ext), nil
}

func (d *fakeDriver) RowOperationStatus(_ context.Context, id string) (rowdriver.Operation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inspects++
	if d.inspectErr != nil {
		return rowdriver.Operation{}, d.inspectErr
	}
	for _, turn := range d.turns {
		if turn.ID == id {
			return fakeRowOperation(d.keys[id], turn), nil
		}
	}
	return rowdriver.Operation{}, errors.New("missing row operation")
}

func fakeRowOperation(key string, turn channelreconcile.ExternalTurn) rowdriver.Operation {
	op := rowdriver.Operation{
		SchemaVersion: 1, SendID: turn.ID, SessionID: "row", IdempotencyKey: key,
		RowBinding: "binding", State: rowdriver.Accepted,
		AcceptedTurn: &rowdriver.AcceptedTurn{
			ReceiptID: "receipt-" + turn.ID, InstanceID: "row", CodexSessionID: "codex-session",
			TurnGeneration: "codex-session:" + turn.ID, AcceptedAt: "now",
		},
	}
	switch turn.Status {
	case "completed":
		op.State = rowdriver.Completed
		op.Completion = &rowdriver.Completion{TurnGeneration: op.AcceptedTurn.TurnGeneration}
		op.Content = turn.Reply
	case "failed", "interrupted":
		op.State = rowdriver.Indeterminate
		op.AcceptedTurn = nil
		op.Code = string(op.State)
	}
	return op
}

type fakeSender struct {
	mu        sync.Mutex
	posts     int
	texts     []string
	failFirst bool
	failAll   bool
	posted    chan int
}

func newFakeSender() *fakeSender { return &fakeSender{posted: make(chan int, 64)} }
func (s *fakeSender) PostTopLevel(_ context.Context, channel, text string) (slackgateway.PostResult, error) {
	s.mu.Lock()
	s.posts++
	n := s.posts
	s.texts = append(s.texts, text)
	fail := s.failAll || s.failFirst && n == 1
	s.mu.Unlock()
	s.posted <- n
	if fail {
		return slackgateway.PostResult{}, errors.New("provider-private-error-marker")
	}
	return slackgateway.PostResult{OK: true, ChannelID: channel, TS: fmt.Sprintf("%d.000001", n)}, nil
}
func (s *fakeSender) waitPost(t *testing.T, want int) {
	t.Helper()
	select {
	case got := <-s.posted:
		require.Equal(t, want, got)
	case <-time.After(4 * time.Second):
		t.Fatal("post did not occur")
	}
}
func (s *fakeSender) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.posts }

type runnerFixture struct {
	store  *channelgateway.Store
	path   string
	socket *fakeSocket
	driver *fakeDriver
	sender *fakeSender
	clock  *manualClock
	runner *Runner
}

func newRunnerFixture(t *testing.T, steps ...error) *runnerFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runner.db")
	store, err := channelgateway.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.CreateConversation(context.Background(), channelgateway.Conversation{
		ID: runConversation, ChannelID: runChannel, ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding", Mode: channelgateway.ChannelStream, AllowedSenders: []string{runUser},
	}))
	socket, driver, sender, clock := newFakeSocket(steps...), newFakeDriver(), newFakeSender(), newManualClock()
	handler := slackgateway.Handler{Store: store, Config: slackgateway.Config{
		ConversationID: runConversation, AppID: runApp, TeamID: runTeam, ChannelID: runChannel, BotUserID: runBot, AllowedUserIDs: []string{runUser},
	}}
	r := &Runner{Socket: socket, Handler: handler, Worker: &channelreconcile.Worker{Store: store, RowDriver: driver, Driver: driver},
		Delivery:     &slackgateway.DeliveryWorker{Store: store, Sender: sender, ConversationID: runConversation, ChannelID: runChannel},
		PollInterval: time.Minute, BackoffMin: time.Second, BackoffMax: 4 * time.Second, Clock: clock}
	return &runnerFixture{store: store, path: path, socket: socket, driver: driver, sender: sender, clock: clock, runner: r}
}
func (f *runnerFixture) start(t *testing.T) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.runner.Run(ctx) }()
	return cancel, done
}
func stopRunner(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("runner did not stop")
	}
}
func eventFrame(t *testing.T, envelope, event, body string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"type": "events_api", "envelope_id": envelope, "accepts_response_payload": false,
		"payload": map[string]any{"type": "event_callback", "team_id": runTeam, "api_app_id": runApp, "event_id": event,
			"event": map[string]any{"type": "message", "channel_type": "group", "channel": runChannel, "user": runUser, "ts": event + ".000001", "text": body}},
	})
	require.NoError(t, err)
	return raw
}
func (f *runnerFixture) emit(t *testing.T, envelope, event, body string) error {
	t.Helper()
	return f.socket.emit(context.Background(), eventFrame(t, envelope, event, body), func(raw []byte) error {
		var ack map[string]any
		if err := json.Unmarshal(raw, &ack); err != nil {
			return err
		}
		if ack["envelope_id"] != envelope {
			return errors.New("wrong acknowledgment")
		}
		return nil
	})
}

func TestRunnerNeverStartsBeforeDurableIngestCommit(t *testing.T) {
	f := newRunnerFixture(t)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	initial := f.clock.nextActive(t, time.Minute)
	// Hold SQLite's write lock. Handler.Ingest cannot commit while this
	// transaction owns it; force a pump tick to exercise the competing reader.
	db, err := sql.Open("sqlite", f.path)
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	require.NoError(t, err)
	locked := true
	defer func() {
		if locked {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	started := make(chan struct{})
	emitDone := make(chan error, 1)
	go func() {
		close(started)
		emitDone <- f.socket.emit(context.Background(), eventFrame(t, "envelope-1", "event-1", runPrivate), func([]byte) error { return nil })
	}()
	<-started
	initial.fire()
	var persisted int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM inbound_events`).Scan(&persisted))
	require.Zero(t, persisted, "event became visible before Ingest commit")
	starts, _ := f.driver.counts()
	require.Zero(t, starts, "turn started before Ingest committed")
	_, err = conn.ExecContext(context.Background(), "COMMIT")
	require.NoError(t, err)
	locked = false
	require.NoError(t, <-emitDone)
	f.driver.waitStart(t, 1)
	f.sender.waitPost(t, 1)
}

func TestRunnerMayProcessCommittedEventWhileAckBlockedThenFails(t *testing.T) {
	f := newRunnerFixture(t)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	initial := f.clock.nextActive(t, time.Minute)
	ackEntered, release := make(chan struct{}), make(chan struct{})
	emitDone := make(chan error, 1)
	go func() {
		emitDone <- f.socket.emit(context.Background(), eventFrame(t, "envelope-1", "event-1", runPrivate), func([]byte) error {
			close(ackEntered)
			<-release
			return errors.New("ambiguous-ack-marker")
		})
	}()
	select {
	case <-ackEntered:
	case <-time.After(4 * time.Second):
		t.Fatal("ack not requested after commit")
	}
	db, err := sql.Open("sqlite", f.path)
	require.NoError(t, err)
	defer db.Close()
	var persisted int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM inbound_events`).Scan(&persisted))
	require.Equal(t, 1, persisted, "ack callback ran before durable intake")
	initial.fire() // polling may process the committed event before ack returns
	f.driver.waitStart(t, 1)
	f.sender.waitPost(t, 1)
	close(release)
	err = <-emitDone
	require.Error(t, err)
	require.NotContains(t, err.Error(), "ambiguous-ack-marker")
	starts, _ := f.driver.counts()
	require.Equal(t, 1, starts)
}

func TestRunnerStartupProcessesCommittedEventWithoutRedelivery(t *testing.T) {
	f := newRunnerFixture(t)
	ctx := context.Background()
	_, err := f.store.Ingest(ctx, channelgateway.Inbound{ConversationID: runConversation, EventID: "startup-event", MessageID: "startup-msg", ChannelID: runChannel, SenderID: runUser, Body: runPrivate})
	require.NoError(t, err)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.driver.waitStart(t, 1)
	f.sender.waitPost(t, 1)
	f.socket.waitRun(t, 1)
	starts, _ := f.driver.counts()
	require.Equal(t, 1, starts)
	require.Equal(t, 1, f.socket.count(), "no provider redelivery is needed at startup")
}

func TestRunnerBurstCoalescesWakeupsButDrainsEveryTurn(t *testing.T) {
	f := newRunnerFixture(t)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	for i := 1; i <= 5; i++ {
		require.NoError(t, f.emit(t, fmt.Sprintf("envelope-%d", i), fmt.Sprintf("event-%d", i), fmt.Sprintf("body-%d", i)))
	}
	for i := 1; i <= 5; i++ {
		f.driver.waitStart(t, i)
		f.sender.waitPost(t, i)
	}
	starts, _ := f.driver.counts()
	require.Equal(t, 5, starts)
	require.Equal(t, 5, f.sender.count())
}

func TestRunnerInProgressDoesNotStartSecondTurn(t *testing.T) {
	f := newRunnerFixture(t)
	f.driver.status = "inProgress"
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	require.NoError(t, f.emit(t, "envelope-1", "event-1", "body-1"))
	f.driver.waitStart(t, 1)
	require.NoError(t, f.emit(t, "envelope-2", "event-2", "body-2"))
	timer := f.clock.nextActive(t, time.Minute)
	starts, _ := f.driver.counts()
	require.Equal(t, 1, starts)
	f.driver.mu.Lock()
	f.driver.status = "completed"
	f.driver.mu.Unlock()
	f.driver.completeFirst()
	timer.fire()
	f.driver.waitStart(t, 2)
}

func TestRunnerStatusNeverContainsPrivateBodies(t *testing.T) {
	f := newRunnerFixture(t, slacknetwork.ErrOpen)
	_, err := f.store.Ingest(context.Background(), channelgateway.Inbound{ConversationID: runConversation, EventID: "event", MessageID: "message", ChannelID: runChannel, SenderID: runUser, Body: runPrivate})
	require.NoError(t, err)
	err = f.runner.Run(context.Background())
	require.Error(t, err)
	for _, secret := range []string{runPrivate, "provider-private-error-marker"} {
		require.NotContains(t, err.Error(), secret)
		require.NotContains(t, fmt.Sprint(f.runner.Status()), secret)
	}
	require.False(t, strings.Contains(fmt.Sprint(f.runner.Status()), "body-"))
}

func TestRunnerReconnectBackoffIsCappedAndCancellationInterruptsIt(t *testing.T) {
	f := newRunnerFixture(t, slacknetwork.ErrReconnect, slacknetwork.ErrReconnect, slacknetwork.ErrReconnect, slacknetwork.ErrReconnect)
	cancel, done := f.start(t)
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second} {
		f.socket.waitRun(t, i+1)
		timer := f.clock.nextNonPoll(t)
		require.Equal(t, want, timer.duration)
		require.Equal(t, PhaseBackoff, f.runner.Status().Phase)
		if i == 3 {
			stopRunner(t, cancel, done)
			require.False(t, timer.active(), "canceled backoff timer was not stopped")
			return
		}
		timer.fire()
	}
}

func TestRunnerTransientOpenBackoffCapsResetsAndCancels(t *testing.T) {
	f := newRunnerFixture(t,
		slacknetwork.ErrOpenTransient, slacknetwork.ErrOpenTransient,
		slacknetwork.ErrOpenTransient, slacknetwork.ErrOpenTransient)
	raw := eventFrame(t, "open-progress-envelope", "open-progress-event", runPrivate)
	f.socket.onRun = func(n int, h slacknetwork.EnvelopeHandler) error {
		if n != 4 {
			return nil
		}
		return h(context.Background(), raw, func(context.Context, []byte) error { return nil })
	}
	cancel, done := f.start(t)
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, time.Second} {
		f.socket.waitRun(t, i+1)
		timer := f.clock.nextNonPoll(t)
		require.Equal(t, want, timer.duration)
		require.Equal(t, ErrorSocket, f.runner.Status().LastError)
		if i == 3 {
			stopRunner(t, cancel, done)
			require.False(t, timer.active())
			return
		}
		timer.fire()
	}
}

func TestRunnerRejectsNanosecondBackoffBeforeSocket(t *testing.T) {
	f := newRunnerFixture(t, slacknetwork.ErrOpenTransient)
	f.runner.BackoffMin = time.Nanosecond
	err := f.runner.Run(context.Background())
	require.ErrorIs(t, err, ErrConfig)
	require.Equal(t, ErrorConfig, f.runner.Status().LastError)
	require.Zero(t, f.socket.count(), "invalid retry delay must not create a connection")
}

func TestRunnerZeroBackoffUsesDefault(t *testing.T) {
	f := newRunnerFixture(t, slacknetwork.ErrOpenTransient)
	f.runner.BackoffMin = 0
	f.runner.BackoffMax = 0
	cancel, done := f.start(t)
	f.socket.waitRun(t, 1)
	timer := f.clock.nextNonPoll(t)
	require.Equal(t, time.Second, timer.duration)
	stopRunner(t, cancel, done)
}

func TestRunnerPumpFatalOutranksConcurrentTransientOpen(t *testing.T) {
	f := newRunnerFixture(t, slacknetwork.ErrOpenTransient)
	ctx := context.Background()
	_, err := f.store.Ingest(ctx, channelgateway.Inbound{
		ConversationID: runConversation, EventID: "fatal-event", MessageID: "fatal-message", ChannelID: runChannel, SenderID: runUser, Body: runPrivate,
	})
	require.NoError(t, err)
	openWait := make(chan struct{})
	f.driver.startWait = openWait
	f.driver.resultKey = "different-attempt"
	f.socket.onRunCtx = func(ctx context.Context, _ int, _ slacknetwork.EnvelopeHandler) error {
		<-ctx.Done() // the fake transient open returns only after pump fatal cancels it
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- f.runner.Run(context.Background()) }()
	f.socket.waitRun(t, 1)
	close(openWait)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrFatal)
		require.Equal(t, ErrorConfig, f.runner.Status().LastError,
			"transient socket class must not overwrite fatal pump class")
		cause, terminal := f.runner.TerminalCause()
		require.True(t, terminal)
		require.Equal(t, ErrorConfig, cause)
		require.Equal(t, 1, f.socket.count())
	case <-time.After(4 * time.Second):
		t.Fatal("fatal pump did not terminate socket")
	}
}

func TestRunnerTerminalStatusIsStickyAgainstTransientSocketClass(t *testing.T) {
	for _, class := range []ErrorClass{ErrorConfig, ErrorProtocol} {
		t.Run(string(class), func(t *testing.T) {
			f := newRunnerFixture(t)
			f.runner.setError(class, true)
			f.runner.setError(ErrorSocket, false)
			require.Equal(t, class, f.runner.Status().LastError)
		})
	}
}

func TestRunnerTransientStatusHasNoTerminalCause(t *testing.T) {
	f := newRunnerFixture(t)
	f.runner.setError(ErrorSocket, false)
	cause, terminal := f.runner.TerminalCause()
	require.False(t, terminal)
	require.Equal(t, ErrorSocket, cause)
}

func TestRunnerDeadlineIsRetryable(t *testing.T) {
	f := newRunnerFixture(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := f.runner.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline was treated as cancellation")
	}
	_, terminal := f.runner.TerminalCause()
	if terminal {
		t.Fatal("deadline acquired a terminal safety cause")
	}
}

func TestRunnerTerminalOpenClassStopsOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		class ErrorClass
	}{
		{"auth", slacknetwork.ErrOpenAuth, ErrorConfig},
		{"config", slacknetwork.ErrOpenConfig, ErrorConfig},
		{"protocol", slacknetwork.ErrOpenProtocol, ErrorProtocol},
		{"unknown", slacknetwork.ErrOpenUnknown, ErrorProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunnerFixture(t, tc.err)
			err := f.runner.Run(context.Background())
			require.ErrorIs(t, err, ErrFatal)
			require.Equal(t, tc.class, f.runner.Status().LastError)
			cause, terminal := f.runner.TerminalCause()
			require.True(t, terminal)
			require.Equal(t, tc.class, cause)
			require.Equal(t, 1, f.socket.count(), "terminal open class must not reconnect")
			require.NotContains(t, err.Error(), runPrivate)
		})
	}
}

func TestRunnerDisconnectClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		step  error
		want  error
		class ErrorClass
	}{
		{"link-disabled", &slacknetwork.DisconnectError{Reason: slacknetwork.DisconnectLinkDisabled}, ErrLinkDisabled, ErrorLinkDisabled},
		{"open-unknown", slacknetwork.ErrOpen, ErrFatal, ErrorConfig},
		{"protocol", slacknetwork.ErrProtocol, ErrFatal, ErrorProtocol},
		{"config", slacknetwork.ErrConfig, ErrFatal, ErrorConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunnerFixture(t, tc.step)
			err := f.runner.Run(context.Background())
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, tc.class, f.runner.Status().LastError)
			require.Equal(t, PhaseStopped, f.runner.Status().Phase)
			require.Equal(t, 1, f.socket.count())
		})
	}
}

func TestRunnerTerminalSocketStatusSurvivesLateWorkerError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		socketErr error
		want      ErrorClass
	}{
		{"protocol", slacknetwork.ErrProtocol, ErrorProtocol},
		{"link-disabled", &slacknetwork.DisconnectError{Reason: slacknetwork.DisconnectLinkDisabled}, ErrorLinkDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunnerFixture(t, tc.socketErr)
			f.driver.startWait = make(chan struct{}) // cancellation, not release, ends the worker
			_, err := f.store.Ingest(context.Background(), channelgateway.Inbound{
				ConversationID: runConversation, EventID: "terminal-event", MessageID: "terminal-message",
				ChannelID: runChannel, SenderID: runUser, Body: runPrivate,
			})
			require.NoError(t, err)
			f.socket.onRun = func(_ int, _ slacknetwork.EnvelopeHandler) error {
				select {
				case <-f.driver.started:
					return nil
				case <-time.After(4 * time.Second):
					return errors.New("worker did not enter StartTurn")
				}
			}
			err = f.runner.Run(context.Background())
			require.Error(t, err)
			require.Equal(t, tc.want, f.runner.Status().LastError,
				"a cancellation-induced worker error must not overwrite the first terminal socket cause")
			require.NotContains(t, err.Error(), runPrivate)
		})
	}
}

func TestRunnerWarningRefreshImmediateOnceThenBackoff(t *testing.T) {
	f := newRunnerFixture(t,
		&slacknetwork.DisconnectError{Reason: slacknetwork.DisconnectWarning},
		&slacknetwork.DisconnectError{Reason: slacknetwork.DisconnectRefreshRequested},
		&slacknetwork.DisconnectError{Reason: slacknetwork.DisconnectRefreshRequested})
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	f.socket.waitRun(t, 2) // first scheduled refresh is immediate
	timer := f.clock.nextNonPoll(t)
	require.Equal(t, time.Second, timer.duration, "repeated no-progress refresh must back off")
	timer.fire()
	f.socket.waitRun(t, 3)
	next := f.clock.nextNonPoll(t)
	require.Equal(t, 2*time.Second, next.duration)
}

func TestRunnerAcceptedDuplicateWakesAfterAckFailure(t *testing.T) {
	f := newRunnerFixture(t)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	_ = f.clock.nextActive(t, time.Minute) // no independent startup check can process the event
	err := f.socket.emit(context.Background(), eventFrame(t, "envelope-1", "same-event", runPrivate), func([]byte) error {
		return errors.New("private-ack-failure-marker")
	})
	require.Error(t, err)
	starts, _ := f.driver.counts()
	require.Zero(t, starts)
	require.NoError(t, f.emit(t, "envelope-2", "same-event", "different-body"))
	f.driver.waitStart(t, 1) // duplicate ack should wake durable, previously unprocessed intake
	f.sender.waitPost(t, 1)
	require.NoError(t, f.emit(t, "envelope-3", "same-event", "third-body"))
	timer := f.clock.nextActive(t, time.Minute)
	timer.fire()
	_ = f.clock.nextActive(t, time.Minute)
	starts, _ = f.driver.counts()
	require.Equal(t, 1, starts)
}

func TestRunnerLostWakeupDuringIdleToWaitTransition(t *testing.T) {
	f := newRunnerFixture(t)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	initial := f.clock.nextActive(t, time.Minute)
	f.clock.setOnCreate(func(d time.Duration) {
		if d != time.Minute {
			return
		}
		f.clock.setOnCreate(nil)
		if err := f.emit(t, "late-envelope", "late-event", runPrivate); err != nil {
			t.Error(err)
		}
	})
	initial.fire()           // force the pump through its last idle check
	f.driver.waitStart(t, 1) // no second timer is fired
	f.sender.waitPost(t, 1)
}

func TestRunnerConcurrentRunRejectedAndSocketCanceled(t *testing.T) {
	f := newRunnerFixture(t)
	cancel, done := f.start(t)
	f.socket.waitRun(t, 1)
	require.ErrorIs(t, f.runner.Run(context.Background()), ErrRunning)
	require.Equal(t, 1, f.socket.count())
	stopRunner(t, cancel, done)
	require.Equal(t, PhaseStopped, f.runner.Status().Phase)
}

func TestRunnerUncertainDeliveryDoesNotBlockLaterPendingItem(t *testing.T) {
	f := newRunnerFixture(t)
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		_, err := f.store.Ingest(ctx, channelgateway.Inbound{
			ConversationID: runConversation, EventID: fmt.Sprintf("event-%d", i), MessageID: fmt.Sprintf("message-%d", i),
			ChannelID: runChannel, SenderID: runUser, Body: fmt.Sprintf("body-%d", i),
		})
		require.NoError(t, err)
		result, err := f.runner.Worker.RunOne(ctx, runConversation)
		require.NoError(t, err)
		require.Equal(t, channelreconcile.Completed, result.State)
	}
	items, err := f.store.PendingOutbox(ctx, runConversation, 10)
	require.NoError(t, err)
	require.Len(t, items, 2)
	f.sender.failFirst = true
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.sender.waitPost(t, 1)
	// The first provider outcome is ambiguous; the row is never returned to
	// pending, while the second row remains eligible for the next pump pass.
	timer := f.clock.nextActive(t, time.Minute)
	status := f.runner.Status()
	require.Equal(t, ErrorDelivery, status.LastError)
	require.NotContains(t, fmt.Sprint(status), "provider-private-error-marker")
	require.NotContains(t, fmt.Sprint(status), "body-1")
	first, err := f.store.OutboxRecord(ctx, items[0].ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.UncertainDelivery, first.State)
	second, err := f.store.OutboxRecord(ctx, items[1].ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.PendingDelivery, second.State)
	timer.fire()
	f.sender.waitPost(t, 2)
	_ = f.clock.nextActive(t, time.Minute)
	first, err = f.store.OutboxRecord(ctx, items[0].ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.UncertainDelivery, first.State)
	second, err = f.store.OutboxRecord(ctx, items[1].ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.DeliveredDelivery, second.State)
	require.Equal(t, 2, f.sender.count())
}

func TestRunnerDeliveryFailureDoesNotStarveInboundWork(t *testing.T) {
	f := newRunnerFixture(t)
	ctx := context.Background()
	_, err := f.store.Ingest(ctx, channelgateway.Inbound{
		ConversationID: runConversation, EventID: "outbox-event", MessageID: "outbox-message",
		ChannelID: runChannel, SenderID: runUser, Body: "outbox-body",
	})
	require.NoError(t, err)
	result, err := f.runner.Worker.RunOne(ctx, runConversation)
	require.NoError(t, err)
	require.Equal(t, channelreconcile.Completed, result.State)

	items, err := f.store.PendingOutbox(ctx, runConversation, 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = f.store.Ingest(ctx, channelgateway.Inbound{
		ConversationID: runConversation, EventID: "inbound-event", MessageID: "inbound-message",
		ChannelID: runChannel, SenderID: runUser, Body: "inbound-body",
	})
	require.NoError(t, err)

	f.sender.failAll = true
	progressed, err := f.runner.step(ctx, make(chan struct{}, 1))
	require.NoError(t, err)
	starts, _ := f.driver.counts()
	require.Equal(t, 2, starts, "later inbound agent turn did not start")
	require.True(t, progressed)
	require.Equal(t, 2, f.sender.count())

	items, err = f.store.PendingOutbox(ctx, runConversation, 10)
	require.NoError(t, err)
	require.Empty(t, items, "uncertain deliveries must not be retried")
	db, err := sql.Open("sqlite", f.path)
	require.NoError(t, err)
	defer db.Close()
	var uncertain int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT count(*) FROM outbox WHERE conversation_id=? AND state=?`,
		runConversation, channelgateway.UncertainDelivery).Scan(&uncertain))
	require.Equal(t, 2, uncertain)
}

func TestRunnerCancelWhileAgentWorkerIsBlocked(t *testing.T) {
	f := newRunnerFixture(t)
	blocked := make(chan struct{})
	f.driver.startWait = blocked
	_, err := f.store.Ingest(context.Background(), channelgateway.Inbound{
		ConversationID: runConversation, EventID: "blocked-event", MessageID: "blocked-message", ChannelID: runChannel, SenderID: runUser, Body: runPrivate,
	})
	require.NoError(t, err)
	cancel, done := f.start(t)
	f.driver.waitStart(t, 1)
	stopRunner(t, cancel, done)
	// The driver returned on context cancellation; the test never releases
	// blocked, and Run still joins its work pump before exiting.
	starts, _ := f.driver.counts()
	require.Equal(t, 1, starts)
}

func TestRunnerBackoffResetsAfterAcknowledgedSessionProgress(t *testing.T) {
	f := newRunnerFixture(t, slacknetwork.ErrReconnect, slacknetwork.ErrReconnect, slacknetwork.ErrReconnect)
	raw := eventFrame(t, "progress-envelope", "progress-event", runPrivate)
	f.socket.onRun = func(n int, h slacknetwork.EnvelopeHandler) error {
		if n != 3 {
			return nil
		}
		return h(context.Background(), raw, func(context.Context, []byte) error { return nil })
	}
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	first := f.clock.nextNonPoll(t)
	require.Equal(t, time.Second, first.duration)
	first.fire()
	f.socket.waitRun(t, 2)
	second := f.clock.nextNonPoll(t)
	require.Equal(t, 2*time.Second, second.duration)
	second.fire()
	f.socket.waitRun(t, 3)
	third := f.clock.nextNonPoll(t)
	require.Equal(t, time.Second, third.duration, "acknowledged intake should reset reconnect backoff")
}

func TestRunnerDriverFailurePollsWithoutSecondPump(t *testing.T) {
	f := newRunnerFixture(t)
	f.driver.startErr = errors.New("driver failed with " + runPrivate)
	_, err := f.store.Ingest(context.Background(), channelgateway.Inbound{
		ConversationID: runConversation, EventID: "driver-event", MessageID: "driver-message", ChannelID: runChannel, SenderID: runUser, Body: runPrivate,
	})
	require.NoError(t, err)
	cancel, done := f.start(t)
	defer stopRunner(t, cancel, done)
	f.socket.waitRun(t, 1)
	f.driver.waitStart(t, 1)
	timer := f.clock.nextActive(t, time.Minute)
	starts, inspects := f.driver.counts()
	require.Equal(t, 1, starts)
	require.Zero(t, inspects)
	require.Equal(t, ErrorWork, f.runner.Status().LastError)
	require.NotContains(t, fmt.Sprint(f.runner.Status()), runPrivate)
	f.driver.mu.Lock()
	f.driver.startErr = nil
	f.driver.mu.Unlock()
	timer.fire()
	f.driver.waitStart(t, 2)
	f.sender.waitPost(t, 1)
	require.Equal(t, 1, f.socket.count(), "work recovery must not spawn a second socket/pump")
}

func TestRunnerCallbackFailuresClassifyWithoutLeakingPayload(t *testing.T) {
	for _, tc := range []struct {
		name       string
		raw        []byte
		ackFailure bool
		terminal   bool
		class      ErrorClass
	}{
		{"malformed", []byte(`{"type":"events_api","envelope_id":"E","payload":{}}`), false, true, ErrorProtocol},
		{"ack-failure", eventFrame(t, "ack-envelope", "ack-event", runPrivate), true, false, ErrorSocket},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunnerFixture(t)
			f.socket.onRun = func(_ int, h slacknetwork.EnvelopeHandler) error {
				err := h(context.Background(), tc.raw, func(context.Context, []byte) error {
					if tc.ackFailure {
						return errors.New("private-ack-error-marker")
					}
					return nil
				})
				require.Error(t, err)
				return slacknetwork.ErrCallback
			}
			if tc.terminal {
				err := f.runner.Run(context.Background())
				require.ErrorIs(t, err, ErrFatal)
				require.Equal(t, tc.class, f.runner.Status().LastError)
				require.NotContains(t, err.Error(), runPrivate)
				return
			}
			cancel, done := f.start(t)
			defer stopRunner(t, cancel, done)
			f.socket.waitRun(t, 1)
			timer := f.clock.nextNonPoll(t)
			require.Equal(t, time.Second, timer.duration)
			require.Equal(t, tc.class, f.runner.Status().LastError)
			require.NotContains(t, fmt.Sprint(f.runner.Status()), "private-ack-error-marker")
		})
	}
}

func TestRunnerInvalidConfigurationFailsBeforeSocket(t *testing.T) {
	f := newRunnerFixture(t)
	f.runner.Handler.Config.AllowedUserIDs = nil
	err := f.runner.Run(context.Background())
	require.ErrorIs(t, err, ErrConfig)
	require.Zero(t, f.socket.count())
	require.Equal(t, ErrorConfig, f.runner.Status().LastError)
}

func TestRunnerUnknownConversationIsFatalStoreFailure(t *testing.T) {
	f := newRunnerFixture(t)
	f.runner.Handler.Config.ConversationID = "missing-conversation"
	f.runner.Delivery.ConversationID = "missing-conversation"
	err := f.runner.Run(context.Background())
	require.ErrorIs(t, err, ErrFatal)
	require.Equal(t, ErrorConfig, f.runner.Status().LastError)
	require.Equal(t, PhaseStopped, f.runner.Status().Phase)
}

func TestRunnerIncompatibleSchemaCannotBeOpened(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-schema.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE channelgateway_schema(version INTEGER NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO channelgateway_schema(version) VALUES(2)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	store, err := channelgateway.Open(path)
	require.ErrorIs(t, err, channelgateway.ErrSchema)
	require.Nil(t, store, "runner must not receive an incompatible ledger")
}
