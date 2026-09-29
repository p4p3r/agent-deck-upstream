package channelreconcile

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/stretchr/testify/require"
)

type fakeDriver struct {
	mu               sync.Mutex
	turns            []ExternalTurn
	openCount        int
	resumeCount      int
	startCount       int
	inspectCount     int
	openErr          error
	onOpen           func()
	inspectErr       error
	onInspect        func()
	startErr         error
	afterAcceptedErr error
	onStarted        func()
	callback         bool
}

func (f *fakeDriver) OpenThread(context.Context) (string, error) {
	f.mu.Lock()
	f.openCount++
	id, err, hook := "thread-"+strconv.Itoa(f.openCount), f.openErr, f.onOpen
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

func (f *fakeDriver) ResumeThread(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeCount++
	return nil
}

func (f *fakeDriver) InspectThread(context.Context, string) ([]ExternalTurn, error) {
	f.mu.Lock()
	f.inspectCount++
	turns, err, hook := append([]ExternalTurn(nil), f.turns...), f.inspectErr, f.onInspect
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return turns, err
}

func (f *fakeDriver) StartTurn(_ context.Context, _ string, _ string, accepted func(string) error) (ExternalTurn, error) {
	f.mu.Lock()
	f.startCount++
	t := ExternalTurn{ID: "external-" + strconv.Itoa(f.startCount), Status: "completed", Reply: "private reply"}
	f.turns = append(f.turns, t) // The request reached the external agent.
	hook, call, startErr, afterErr := f.onStarted, f.callback, f.startErr, f.afterAcceptedErr
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if startErr != nil {
		return ExternalTurn{}, startErr
	}
	if call {
		if err := accepted(t.ID); err != nil {
			return ExternalTurn{}, err
		}
	}
	if afterErr != nil {
		return ExternalTurn{}, afterErr
	}
	return t, nil
}

func (f *fakeDriver) counts() (open, resume, start int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.openCount, f.resumeCount, f.startCount
}

func workerFixture(t *testing.T) (*channelgateway.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker.db")
	s, err := channelgateway.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	require.NoError(t, s.CreateConversation(ctx, channelgateway.Conversation{
		ID: "conversation", ChannelID: "channel", ConductorID: "conductor",
		Mode: channelgateway.ChannelStream, AllowedSenders: []string{"alice"},
	}))
	_, err = s.Ingest(ctx, channelgateway.Inbound{
		ConversationID: "conversation", EventID: "event-1", MessageID: "message-1",
		ChannelID: "channel", SenderID: "alice", Body: "private prompt",
	})
	require.NoError(t, err)
	return s, path
}

func activeTurn(t *testing.T, s *channelgateway.Store) *channelgateway.Turn {
	t.Helper()
	turn, err := s.NextTurn(context.Background(), "conversation")
	require.NoError(t, err)
	require.NotNil(t, turn)
	return turn
}

func TestWorkerCompletesOnceAndResumesDurableBinding(t *testing.T) {
	s, path := workerFixture(t)
	f := &fakeDriver{callback: true}
	r, err := (&Worker{Store: s, Driver: f}).RunOne(context.Background(), "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	require.NotEmpty(t, r.TurnID)
	items, err := s.PendingOutbox(context.Background(), "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "private reply", items[0].Body)
	require.NoError(t, s.Close())
	reopened, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	r, err = (&Worker{Store: reopened, Driver: f}).RunOne(context.Background(), "conversation")
	require.NoError(t, err)
	require.Equal(t, "idle", string(r.State))
	open, _, starts := f.counts()
	require.Equal(t, 1, open)
	require.Equal(t, 1, starts)
	items, err = reopened.PendingOutbox(context.Background(), "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestOpenBeforeBindingFailureLeavesUnusedOrphan(t *testing.T) {
	s, path := workerFixture(t)
	f := &fakeDriver{callback: true, onOpen: func() { _ = s.Close() }}
	_, err := (&Worker{Store: s, Driver: f}).RunOne(context.Background(), "conversation")
	require.Error(t, err)
	_, _, starts := f.counts()
	require.Zero(t, starts)
	reopened, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	b, err := reopened.AgentBinding(context.Background(), "conversation")
	require.NoError(t, err)
	require.Empty(t, b.AgentThreadID)
	f.onOpen = nil
	r, err := (&Worker{Store: reopened, Driver: f}).RunOne(context.Background(), "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	b, err = reopened.AgentBinding(context.Background(), "conversation")
	require.NoError(t, err)
	require.Equal(t, "thread-2", b.AgentThreadID)
	_, _, starts = f.counts()
	require.Equal(t, 1, starts)
}

func TestInspectBeforePrepareFailureCanRetrySafely(t *testing.T) {
	s, path := workerFixture(t)
	ctx := context.Background()
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	f := &fakeDriver{callback: true, onInspect: func() { _ = s.Close() }}
	_, err := (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
	require.Error(t, err)
	_, _, starts := f.counts()
	require.Zero(t, starts)
	reopened, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	turn := activeTurn(t, reopened)
	require.Empty(t, turn.AttemptID)
	f.onInspect = nil
	r, err := (&Worker{Store: reopened, Driver: f}).RunOne(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	_, _, starts = f.counts()
	require.Equal(t, 1, starts)
}

func TestPreparedAttemptWithoutObservedExternalTurnNeverRestarts(t *testing.T) {
	s, path := workerFixture(t)
	ctx := context.Background()
	turn := activeTurn(t, s)
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	a, created, err := s.PrepareAttempt(ctx, turn.ID, "")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, s.Close())
	reopened, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	f := &fakeDriver{callback: true}
	r, err := (&Worker{Store: reopened, Driver: f}).RunOne(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "needs_reconciliation", string(r.State))
	require.Equal(t, turn.ID, r.TurnID)
	_, _, starts := f.counts()
	require.Zero(t, starts)
	active := activeTurn(t, reopened)
	require.Equal(t, a.ID, active.AttemptID)
}

func TestAcceptedIDMissingFromInspectionWaitsForRecovery(t *testing.T) {
	s, _ := workerFixture(t)
	ctx := context.Background()
	turn := activeTurn(t, s)
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	a, created, err := s.PrepareAttempt(ctx, turn.ID, "")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, s.AcceptAttempt(ctx, turn.ID, a.ID, "external-1"))
	f := &fakeDriver{callback: true} // The authoritative ID is temporarily absent.
	r, err := (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "needs_reconciliation", string(r.State))
	active := activeTurn(t, s)
	require.Equal(t, "external-1", active.ExternalTurnID)
	_, _, starts := f.counts()
	require.Zero(t, starts)
	f.turns = []ExternalTurn{{ID: "external-1", Status: "completed", Reply: "recovered reply"}}
	r, err = (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	_, _, starts = f.counts()
	require.Zero(t, starts)
}

func TestPreparedInspectErrorNeverResubmits(t *testing.T) {
	s, _ := workerFixture(t)
	ctx := context.Background()
	turn := activeTurn(t, s)
	require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
	_, created, err := s.PrepareAttempt(ctx, turn.ID, "")
	require.NoError(t, err)
	require.True(t, created)
	f := &fakeDriver{callback: true, inspectErr: errors.New("inspection failed: private prompt")}
	r, err := (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
	require.ErrorIs(t, err, ErrDriver)
	require.Equal(t, "needs_reconciliation", string(r.State))
	require.NotContains(t, err.Error(), "private prompt")
	_, _, starts := f.counts()
	require.Zero(t, starts)
	f.inspectErr = nil
	f.turns = []ExternalTurn{{ID: "external-1", Status: "completed", Reply: "recovered reply"}}
	r, err = (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	_, _, starts = f.counts()
	require.Zero(t, starts)
}

func TestStaleInspectionReportsPersistedCallbackOrCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		complete   bool
	}{
		{"callback", "in_progress", false},
		{"completion", "completed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := workerFixture(t)
			ctx := context.Background()
			turn := activeTurn(t, s)
			require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
			a, created, err := s.PrepareAttempt(ctx, turn.ID, "")
			require.NoError(t, err)
			require.True(t, created)
			f := &fakeDriver{callback: true, onInspect: func() {
				require.NoError(t, s.AcceptAttempt(ctx, turn.ID, a.ID, "external-1"))
				if tc.complete {
					_, err := s.CompleteAttempt(ctx, turn.ID, a.ID, "external-1", "private reply")
					require.NoError(t, err)
				}
			}}
			// InspectThread captured an empty snapshot before the callback committed.
			r, err := (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
			require.NoError(t, err)
			require.Equal(t, tc.want, string(r.State))
			_, _, starts := f.counts()
			require.Zero(t, starts)
		})
	}
}

func TestLostAcceptanceCallbackRecoversUniqueExternalTurn(t *testing.T) {
	s, path := workerFixture(t)
	f := &fakeDriver{callback: true, onStarted: func() { _ = s.Close() }}
	_, err := (&Worker{Store: s, Driver: f}).RunOne(context.Background(), "conversation")
	require.Error(t, err)
	reopened, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	f.onStarted = nil
	r, err := (&Worker{Store: reopened, Driver: f}).RunOne(context.Background(), "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	_, _, starts := f.counts()
	require.Equal(t, 1, starts)
	items, err := reopened.PendingOutbox(context.Background(), "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestAcceptedBeforeCompletionRecoversWithoutSecondStart(t *testing.T) {
	s, path := workerFixture(t)
	f := &fakeDriver{callback: true, afterAcceptedErr: errors.New("connection lost")}
	_, err := (&Worker{Store: s, Driver: f}).RunOne(context.Background(), "conversation")
	require.Error(t, err)
	turn := activeTurn(t, s)
	require.Equal(t, "external-1", turn.ExternalTurnID)
	require.NoError(t, s.Close())
	reopened, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer reopened.Close()
	f.afterAcceptedErr = nil
	r, err := (&Worker{Store: reopened, Driver: f}).RunOne(context.Background(), "conversation")
	require.NoError(t, err)
	require.Equal(t, "completed", string(r.State))
	_, _, starts := f.counts()
	require.Equal(t, 1, starts)
	items, err := reopened.PendingOutbox(context.Background(), "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestUncertainOrFailedExternalResultRemainsUnresolved(t *testing.T) {
	for _, tc := range []struct {
		name  string
		turns []ExternalTurn
	}{
		{"zero candidates", nil},
		{"multiple candidates", []ExternalTurn{{ID: "external-1", Status: "completed", Reply: "one"}, {ID: "external-2", Status: "completed", Reply: "two"}}},
		{"failed candidate", []ExternalTurn{{ID: "external-1", Status: "failed"}}},
		{"candidate without result", []ExternalTurn{{ID: "external-1", Status: "inProgress"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := workerFixture(t)
			ctx := context.Background()
			turn := activeTurn(t, s)
			require.NoError(t, s.BindAgentThread(ctx, "conversation", "thread-1"))
			_, created, err := s.PrepareAttempt(ctx, turn.ID, "")
			require.NoError(t, err)
			require.True(t, created)
			f := &fakeDriver{callback: true, turns: tc.turns}
			r, err := (&Worker{Store: s, Driver: f}).RunOne(ctx, "conversation")
			require.NoError(t, err)
			if tc.name == "candidate without result" {
				require.Equal(t, "in_progress", string(r.State))
			} else {
				require.Equal(t, "needs_reconciliation", string(r.State))
			}
			_, _, starts := f.counts()
			require.Zero(t, starts)
			items, err := s.PendingOutbox(ctx, "conversation", 10)
			require.NoError(t, err)
			require.Empty(t, items)
		})
	}
}

func TestConcurrentWorkersOnlyStartOneExternalTurn(t *testing.T) {
	s, path := workerFixture(t)
	require.NoError(t, s.BindAgentThread(context.Background(), "conversation", "thread-1"))
	other, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer other.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	f := &fakeDriver{callback: true, onStarted: func() { close(entered); <-release }}
	firstErr := make(chan error, 1)
	go func() {
		_, err := (&Worker{Store: s, Driver: f}).RunOne(context.Background(), "conversation")
		firstErr <- err
	}()
	<-entered // The first call has reached the external service after PrepareAttempt.
	start := make(chan struct{})
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	for _, store := range []*channelgateway.Store{other} {
		wg.Add(1)
		go func(store *channelgateway.Store) {
			defer wg.Done()
			<-start
			_, err := (&Worker{Store: store, Driver: f}).RunOne(context.Background(), "conversation")
			errs <- err
		}(store)
	}
	close(start)
	wg.Wait()
	close(release)
	close(errs)
	require.NoError(t, <-firstErr)
	for err := range errs {
		require.NoError(t, err)
	}
	_, _, starts := f.counts()
	require.Equal(t, 1, starts)
	items, err := s.PendingOutbox(context.Background(), "conversation", 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestDriverDiagnosticsDoNotExposePrivateBody(t *testing.T) {
	s, _ := workerFixture(t)
	secret := "private prompt"
	f := &fakeDriver{openErr: errors.New("driver failed: " + secret)}
	r, err := (&Worker{Store: s, Driver: f}).RunOne(context.Background(), "conversation")
	require.Error(t, err)
	require.False(t, strings.Contains(err.Error(), secret))
	require.False(t, strings.Contains(fmt.Sprint(r), secret))
	_, _, starts := f.counts()
	require.Zero(t, starts)
}
