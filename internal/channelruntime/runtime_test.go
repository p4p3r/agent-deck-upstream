package channelruntime

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/conductorlock"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
	"github.com/stretchr/testify/require"
)

type testLock struct{ closed *int }

func (l testLock) Close() error { *l.closed++; return nil }

type testDriver struct {
	thread   string
	openID   string
	turns    []channelreconcile.ExternalTurn
	openErr  error
	replyErr error
	opens    int
	resumes  int
	starts   int
	replies  int
	closed   int
}

func (d *testDriver) OpenThread(context.Context) (string, error) {
	d.opens++
	if d.openErr != nil {
		return "", d.openErr
	}
	d.thread = d.openID
	if d.thread == "" {
		d.thread = "thread-1"
	}
	return d.thread, nil
}
func (d *testDriver) ResumeThread(_ context.Context, id string) error {
	d.resumes++
	if d.thread != "" && id != d.thread {
		return errors.New("wrong thread")
	}
	d.thread = id
	return nil
}
func (d *testDriver) InspectThread(_ context.Context, id string) ([]channelreconcile.ExternalTurn, error) {
	if id != d.thread {
		return nil, errors.New("wrong thread")
	}
	return slices.Clone(d.turns), nil
}
func (d *testDriver) ReplyForTurn(_ context.Context, id, turnID string) (string, error) {
	d.replies++
	if d.replyErr != nil {
		return "", d.replyErr
	}
	if id != d.thread {
		return "", errors.New("wrong thread")
	}
	for _, turn := range d.turns {
		if turn.ID == turnID {
			return turn.Reply, nil
		}
	}
	return "", errors.New("missing turn")
}
func (d *testDriver) StartTurn(_ context.Context, id, _ string, accepted func(string) error) (channelreconcile.ExternalTurn, error) {
	if id != d.thread {
		return channelreconcile.ExternalTurn{}, errors.New("wrong thread")
	}
	d.starts++
	turn := channelreconcile.ExternalTurn{ID: "new-turn", Status: "completed", Reply: "reply"}
	d.turns = append(d.turns, turn)
	if err := accepted(turn.ID); err != nil {
		return channelreconcile.ExternalTurn{}, err
	}
	return turn, nil
}
func (d *testDriver) Close() error { d.closed++; return nil }

type testSocket struct{}

func (testSocket) Run(context.Context, slacknetwork.EnvelopeHandler) error { return nil }

type testSender struct{}

func (testSender) PostTopLevel(context.Context, string, string) (slackgateway.PostResult, error) {
	return slackgateway.PostResult{}, nil
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{ConversationID: "conversation", ConductorID: "conductor", ChannelID: "channel",
		AllowedUserIDs: []string{"alice"}, AppToken: "app-secret", BotToken: "bot-secret",
		CodexExecutable: "codex", CodexCWD: t.TempDir()}
}

func testRequest(t *testing.T, mode Mode, thread string, cfg Config) Request {
	t.Helper()
	return Request{Name: "conductor", ConductorDir: filepath.Join(t.TempDir(), "conductor"),
		Mode: mode, ResumeThreadID: thread, LoadConfig: func() (Config, error) { return cfg, nil }}
}

func testDeps(driver func(Config) agentDriver, onRun func(context.Context, *channelstream.Runner) error) (dependencies, *int) {
	closed := new(int)
	return dependencies{
		acquire: func(_, _ string) (io.Closer, error) { return testLock{closed}, nil },
		identity: func(context.Context, string) (slacknetwork.Identity, error) {
			return slacknetwork.Identity{TeamID: "team", BotUserID: "bot-user"}, nil
		},
		driver: driver,
		socket: func(string) channelstream.Socket { return testSocket{} },
		sender: func(string) slackgateway.Sender { return testSender{} },
		run:    onRun,
	}, closed
}

func TestEmptyCreateRestartReplacesUnusedThreadAndRunsOnlyAfterReady(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	first, second := &testDriver{}, &testDriver{openID: "thread-2"}
	calls := 0
	d, closed := testDeps(func(Config) agentDriver {
		calls++
		if calls == 1 {
			return first
		}
		return second
	}, func(_ context.Context, runner *channelstream.Runner) error {
		m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
		require.NoError(t, err)
		require.Equal(t, phaseReady, m.Phase)
		b, err := runner.Worker.Store.AgentBinding(ctx, cfg.ConversationID)
		require.NoError(t, err)
		if calls == 1 {
			require.Equal(t, "thread-1", b.AgentThreadID)
		} else {
			require.Equal(t, "thread-2", b.AgentThreadID)
		}
		return nil
	})
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	require.NoError(t, run(ctx, r, d))
	require.NoError(t, run(ctx, r, d))
	require.Equal(t, 1, first.opens)
	require.Equal(t, 1, second.opens)
	require.Equal(t, 0, second.resumes)
	require.Equal(t, 2, *closed)
	require.Equal(t, 1, first.closed)
	require.Equal(t, 1, second.closed)
}

func TestPreparedCreateWithoutIDNeverCreatesReplacement(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	first := &testDriver{openErr: errors.New("response lost")}
	created := 0
	d, _ := testDeps(func(Config) agentDriver { created++; return first }, func(context.Context, *channelstream.Runner) error {
		t.Fatal("runner started after ambiguous create")
		return nil
	})
	require.Equal(t, string(KindUncertain), KindOf(run(ctx, r, d)))
	m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
	require.NoError(t, err)
	require.Equal(t, phasePrepared, m.Phase)
	require.Empty(t, m.ThreadID)
	require.Equal(t, string(KindUncertain), KindOf(run(ctx, r, d)))
	require.Equal(t, 1, created)
	require.Equal(t, 1, first.opens)
}

func TestColdResumeAdoptsHistoricalCursorAndStartsOneNewTurn(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	r := testRequest(t, ModeResume, "legacy-thread", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	driver := &testDriver{replyErr: errors.New("unsupported item pagination"), turns: []channelreconcile.ExternalTurn{
		{ID: "old-1", Status: "completed"}, {ID: "old-2", Status: "completed"},
	}}
	d, _ := testDeps(func(Config) agentDriver { return driver }, func(_ context.Context, runner *channelstream.Runner) error {
		b, err := runner.Worker.Store.AgentBinding(ctx, cfg.ConversationID)
		require.NoError(t, err)
		require.Equal(t, "old-2", b.LastExternalTurnID)
		_, err = runner.Worker.Store.Ingest(ctx, channelgateway.Inbound{
			ConversationID: cfg.ConversationID, EventID: "event", MessageID: "message",
			ChannelID: cfg.ChannelID, SenderID: "alice", Body: "prompt",
		})
		require.NoError(t, err)
		result, err := runner.Worker.RunOne(ctx, cfg.ConversationID)
		require.NoError(t, err)
		require.Equal(t, channelreconcile.Completed, result.State)
		return nil
	})
	require.NoError(t, run(ctx, r, d))
	require.Zero(t, driver.opens)
	require.Equal(t, 1, driver.starts)
	require.Zero(t, driver.replies, "cold resume and pre-submit must inspect metadata only")
	m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
	require.NoError(t, err)
	require.Equal(t, "old-2", m.Baseline)
}

func TestReadyMissingLedgerAndFreshSidecarFailClosed(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	driver := &testDriver{}
	d, _ := testDeps(func(Config) agentDriver { return driver }, func(context.Context, *channelstream.Runner) error { return nil })
	paths := pathsFor(r.ConductorDir)
	require.NoError(t, os.MkdirAll(filepath.Dir(paths.ledger), 0o700))
	require.NoError(t, os.WriteFile(paths.ledger+"-wal", []byte("ambiguous"), 0o600))
	require.Equal(t, string(KindManifest), KindOf(run(ctx, r, d)))
	require.Zero(t, driver.opens)
	require.NoError(t, os.Remove(paths.ledger+"-wal"))
	require.NoError(t, run(ctx, r, d))
	require.NoError(t, os.Remove(paths.ledger))
	require.Equal(t, string(KindStore), KindOf(run(ctx, r, d)))
}

func TestReadyRestartRejectsCursorMismatchInBothDirections(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	created := 0
	d, _ := testDeps(func(Config) agentDriver { created++; return &testDriver{} },
		func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	paths := pathsFor(r.ConductorDir)
	db, err := sql.Open("sqlite", paths.ledger)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE conversations SET last_external_turn_id='advanced' WHERE id='conversation'`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
	db, err = sql.Open("sqlite", paths.ledger)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE conversations SET last_external_turn_id='' WHERE id='conversation'`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	m, err := loadManifest(paths.manifest)
	require.NoError(t, err)
	m.Baseline = "advanced"
	require.NoError(t, saveManifest(paths.manifest, m))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
}

func TestPreparedEmptyCreateReplacementCrashWindows(t *testing.T) {
	for _, stage := range []string{"before_start", "after_id", "after_db_binding"} {
		t.Run(stage, func(t *testing.T) {
			cfg := testConfig(t)
			r := testRequest(t, ModeCreate, "", cfg)
			require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
			first, replacement := &testDriver{}, &testDriver{openID: "fresh-thread"}
			created := 0
			d, _ := testDeps(func(Config) agentDriver {
				created++
				if created == 1 {
					return first
				}
				return replacement
			}, func(context.Context, *channelstream.Runner) error { return nil })
			require.NoError(t, run(context.Background(), r, d))
			paths := pathsFor(r.ConductorDir)
			store, err := channelgateway.Open(paths.ledger)
			require.NoError(t, err)
			_, err = store.Ingest(context.Background(), channelgateway.Inbound{
				ConversationID: cfg.ConversationID, EventID: "queued", MessageID: "message",
				ChannelID: cfg.ChannelID, SenderID: "alice", Body: "pending prompt",
			})
			require.NoError(t, err)
			m, err := loadManifest(paths.manifest)
			require.NoError(t, err)
			m.Phase, m.PreviousThreadID, m.ThreadID = phasePrepared, "thread-1", ""
			if stage != "before_start" {
				m.ThreadID = "orphan-thread"
			}
			if stage == "after_db_binding" {
				require.NoError(t, store.ReplaceEmptyCreateThread(context.Background(), cfg.ConversationID, "thread-1", "orphan-thread"))
			}
			require.NoError(t, store.Close())
			require.NoError(t, saveManifest(paths.manifest, m))
			require.NoError(t, run(context.Background(), r, d))
			require.Equal(t, 1, replacement.opens)
			require.Zero(t, replacement.resumes)
			store, err = channelgateway.Open(paths.ledger)
			require.NoError(t, err)
			binding, err := store.AgentBinding(context.Background(), cfg.ConversationID)
			require.NoError(t, err)
			require.Equal(t, "fresh-thread", binding.AgentThreadID)
			turn, err := store.NextTurn(context.Background(), cfg.ConversationID)
			require.NoError(t, err)
			require.NotNil(t, turn, "pending inbound must survive replacement")
			require.NoError(t, store.Close())
		})
	}
}

func TestPreparedEmptyCreateReplacementRejectsTurnEvidence(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	created := 0
	d, _ := testDeps(func(Config) agentDriver { created++; return &testDriver{} },
		func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	paths := pathsFor(r.ConductorDir)
	store, err := channelgateway.Open(paths.ledger)
	require.NoError(t, err)
	_, err = store.Ingest(context.Background(), channelgateway.Inbound{
		ConversationID: cfg.ConversationID, EventID: "event", MessageID: "message",
		ChannelID: cfg.ChannelID, SenderID: "alice", Body: "prompt",
	})
	require.NoError(t, err)
	turn, err := store.NextTurn(context.Background(), cfg.ConversationID)
	require.NoError(t, err)
	require.NotNil(t, turn)
	require.NoError(t, store.Close())
	m, err := loadManifest(paths.manifest)
	require.NoError(t, err)
	m.Phase, m.PreviousThreadID, m.ThreadID = phasePrepared, "thread-1", ""
	require.NoError(t, saveManifest(paths.manifest, m))
	require.Equal(t, string(KindUncertain), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
}

func TestLostReplacementStartResponseRetriesOnlyAfterEmptyLedgerProof(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	first := &testDriver{}
	lost := &testDriver{openErr: errors.New("response lost after thread/start")}
	retry := &testDriver{openID: "retry-thread"}
	created := 0
	d, _ := testDeps(func(Config) agentDriver {
		created++
		switch created {
		case 1:
			return first
		case 2:
			return lost
		default:
			return retry
		}
	}, func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	require.Equal(t, string(KindUncertain), KindOf(run(context.Background(), r, d)))
	m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
	require.NoError(t, err)
	require.Equal(t, phasePrepared, m.Phase)
	require.Equal(t, "thread-1", m.PreviousThreadID)
	require.Empty(t, m.ThreadID)
	require.NoError(t, run(context.Background(), r, d))
	require.Equal(t, 1, retry.opens)
	m, err = loadManifest(pathsFor(r.ConductorDir).manifest)
	require.NoError(t, err)
	require.Equal(t, phaseReady, m.Phase)
	require.Equal(t, "retry-thread", m.ThreadID)
}

func TestLockPrecedesConfigAndIdentity(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	locked := false
	d, _ := testDeps(func(Config) agentDriver { return &testDriver{} }, func(context.Context, *channelstream.Runner) error { return nil })
	d.acquire = func(_, _ string) (io.Closer, error) { locked = true; return testLock{new(int)}, nil }
	r.LoadConfig = func() (Config, error) { require.True(t, locked); return cfg, nil }
	d.identity = func(context.Context, string) (slacknetwork.Identity, error) {
		require.True(t, locked)
		return slacknetwork.Identity{TeamID: "team", BotUserID: "bot-user"}, nil
	}
	require.NoError(t, run(context.Background(), r, d))
}

func TestHeldLockStopsBeforeConfigOrAnyRuntimeEffect(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	loaded := false
	r.LoadConfig = func() (Config, error) { loaded = true; return cfg, nil }
	d := dependencies{
		acquire: func(_, _ string) (io.Closer, error) { return nil, conductorlock.ErrHeld },
		identity: func(context.Context, string) (slacknetwork.Identity, error) {
			t.Fatal("identity called")
			return slacknetwork.Identity{}, nil
		},
		driver: func(Config) agentDriver { t.Fatal("driver created"); return nil },
		socket: func(string) channelstream.Socket { t.Fatal("socket created"); return nil },
		sender: func(string) slackgateway.Sender { t.Fatal("sender created"); return nil },
		run:    func(context.Context, *channelstream.Runner) error { t.Fatal("runner started"); return nil },
	}
	err := run(context.Background(), r, d)
	require.Equal(t, string(KindLease), KindOf(err))
	require.False(t, loaded)
	require.Equal(t, "channelruntime: lease", err.Error())
}

func TestConfigLoadFailureStopsBeforeProvidersAndRuntime(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	locked, loaded, closed := false, false, 0
	r.LoadConfig = func() (Config, error) {
		if !locked {
			t.Fatal("configuration loaded before lock")
		}
		loaded = true
		return cfg, errors.New("configuration")
	}
	d := dependencies{
		acquire: func(_, _ string) (io.Closer, error) { locked = true; return testLock{&closed}, nil },
		identity: func(context.Context, string) (slacknetwork.Identity, error) {
			t.Fatal("identity called after configuration failure")
			return slacknetwork.Identity{}, nil
		},
		driver: func(Config) agentDriver { t.Fatal("driver created after configuration failure"); return nil },
		socket: func(string) channelstream.Socket { t.Fatal("socket created after configuration failure"); return nil },
		sender: func(string) slackgateway.Sender { t.Fatal("sender created after configuration failure"); return nil },
		run: func(context.Context, *channelstream.Runner) error {
			t.Fatal("runner started after configuration failure")
			return nil
		},
	}
	err := run(context.Background(), r, d)
	if KindOf(err) != string(KindConfig) || !loaded || closed != 1 || err.Error() != "channelruntime: config" {
		t.Fatal("configuration failure boundary mismatch")
	}
}

func TestBootstrapFailureReapsDriverAndReleasesLockWithoutIngress(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	driver := &testDriver{openErr: errors.New("bot-secret app-secret private-body")}
	d, closed := testDeps(func(Config) agentDriver { return driver },
		func(context.Context, *channelstream.Runner) error { t.Fatal("runner started"); return nil })
	d.socket = func(string) channelstream.Socket { t.Fatal("socket created"); return nil }
	err := run(context.Background(), r, d)
	require.Equal(t, string(KindUncertain), KindOf(err))
	require.Equal(t, 1, driver.closed)
	require.Equal(t, 1, *closed)
	require.NotContains(t, err.Error(), "bot-secret")
	require.NotContains(t, err.Error(), "private-body")
}

func TestRunnerCancellationReapsDriverAndReleasesLock(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	driver := &testDriver{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, closed := testDeps(func(Config) agentDriver { return driver },
		func(ctx context.Context, _ *channelstream.Runner) error {
			m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
			require.NoError(t, err)
			require.Equal(t, phaseReady, m.Phase)
			cancel()
			return ctx.Err()
		})
	err := run(ctx, r, d)
	require.Equal(t, string(KindRunner), KindOf(err))
	require.Equal(t, 1, driver.closed)
	require.Equal(t, 1, *closed)
	require.False(t, strings.Contains(err.Error(), cfg.BotToken))
}

func TestRuntimeRefusesSymlinkedStateDirectoryBeforeReadingManifest(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	target := t.TempDir()
	require.NoError(t, os.Symlink(target, filepath.Join(r.ConductorDir, "slack-v2")))
	created := 0
	d, _ := testDeps(func(Config) agentDriver { created++; return &testDriver{} },
		func(context.Context, *channelstream.Runner) error { t.Fatal("runner started"); return nil })
	require.Equal(t, string(KindManifest), KindOf(run(context.Background(), r, d)))
	require.Zero(t, created)
}

func TestManifestRejectsConflictingDuplicateKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"thread_id":"first","thread_id":"second"}`), 0o600))
	_, err := loadManifest(path)
	require.Error(t, err)
}

func TestPreparedCreateWithKnownThreadIDResumesWithoutNewStart(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	m := newManifest(r, cfg, slacknetwork.Identity{TeamID: "team", BotUserID: "bot-user"})
	m.ThreadID, m.BaselineKnown = "thread-1", true
	require.NoError(t, saveManifest(pathsFor(r.ConductorDir).manifest, m))
	driver := &testDriver{}
	d, _ := testDeps(func(Config) agentDriver { return driver }, func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(ctx, r, d))
	require.Zero(t, driver.opens)
	require.Equal(t, 1, driver.resumes)
	ready, err := loadManifest(pathsFor(r.ConductorDir).manifest)
	require.NoError(t, err)
	require.Equal(t, phaseReady, ready.Phase)
}

func TestPreparedBoundCreateWithEmptyLedgerCanReplaceUnusableThread(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	first, replacement := &testDriver{}, &testDriver{openID: "replacement-thread"}
	created := 0
	d, _ := testDeps(func(Config) agentDriver {
		created++
		if created == 1 {
			return first
		}
		return replacement
	}, func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	paths := pathsFor(r.ConductorDir)
	m, err := loadManifest(paths.manifest)
	require.NoError(t, err)
	m.Phase = phasePrepared // crash after DB bind but before ready marker
	require.NoError(t, saveManifest(paths.manifest, m))
	require.NoError(t, run(context.Background(), r, d))
	require.Equal(t, 1, replacement.opens)
	require.Zero(t, replacement.resumes)
	ready, err := loadManifest(paths.manifest)
	require.NoError(t, err)
	require.Equal(t, phaseReady, ready.Phase)
	require.Equal(t, "replacement-thread", ready.ThreadID)
}

func TestColdResumeRejectsHistoricalActiveTurnBeforeSQLite(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeResume, "legacy-thread", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	driver := &testDriver{turns: []channelreconcile.ExternalTurn{{ID: "active", Status: "inProgress"}}}
	d, _ := testDeps(func(Config) agentDriver { return driver },
		func(context.Context, *channelstream.Runner) error { t.Fatal("runner started"); return nil })
	require.Equal(t, string(KindUncertain), KindOf(run(context.Background(), r, d)))
	require.NoError(t, requireAbsent(pathsFor(r.ConductorDir).ledger))
	m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
	require.NoError(t, err)
	require.Equal(t, phasePrepared, m.Phase)
}

func TestColdResumeAcceptsOnlyUnambiguousTerminalHistory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		turns []channelreconcile.ExternalTurn
		kind  Kind
		last  string
	}{
		{name: "terminal", turns: []channelreconcile.ExternalTurn{
			{ID: "a", Status: "failed"}, {ID: "b", Status: "interrupted"}, {ID: "c", Status: "completed"},
		}, last: "c"},
		{name: "in_progress", turns: []channelreconcile.ExternalTurn{{ID: "a", Status: "inProgress"}}, kind: KindUncertain},
		{name: "unknown", turns: []channelreconcile.ExternalTurn{{ID: "a", Status: "mystery"}}, kind: KindUncertain},
		{name: "empty_id", turns: []channelreconcile.ExternalTurn{{ID: "", Status: "completed"}}, kind: KindUncertain},
		{name: "duplicate_id", turns: []channelreconcile.ExternalTurn{{ID: "a", Status: "failed"}, {ID: "a", Status: "completed"}}, kind: KindUncertain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			r := testRequest(t, ModeResume, "legacy-thread", cfg)
			require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
			driver := &testDriver{turns: tc.turns}
			d, _ := testDeps(func(Config) agentDriver { return driver },
				func(context.Context, *channelstream.Runner) error { return nil })
			err := run(context.Background(), r, d)
			if tc.kind != "" {
				require.Equal(t, string(tc.kind), KindOf(err))
				require.NoError(t, requireAbsent(pathsFor(r.ConductorDir).ledger))
				return
			}
			require.NoError(t, err)
			m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
			require.NoError(t, err)
			require.Equal(t, tc.last, m.Baseline)
		})
	}
}

func TestReadyRestartValidatesCompletedChainAgainstAgentHistory(t *testing.T) {
	for _, stage := range []string{"normal", "broken_chain", "history_fork", "extra_turn"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t)
			r := testRequest(t, ModeResume, "legacy-thread", cfg)
			require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
			first := &testDriver{turns: []channelreconcile.ExternalTurn{{ID: "historical", Status: "failed"}}}
			d1, _ := testDeps(func(Config) agentDriver { return first }, func(_ context.Context, runner *channelstream.Runner) error {
				_, err := runner.Worker.Store.Ingest(ctx, channelgateway.Inbound{
					ConversationID: cfg.ConversationID, EventID: "event", MessageID: "message",
					ChannelID: cfg.ChannelID, SenderID: "alice", Body: "prompt",
				})
				require.NoError(t, err)
				result, err := runner.Worker.RunOne(ctx, cfg.ConversationID)
				require.NoError(t, err)
				require.Equal(t, channelreconcile.Completed, result.State)
				return nil
			})
			require.NoError(t, run(ctx, r, d1))
			m, err := loadManifest(pathsFor(r.ConductorDir).manifest)
			require.NoError(t, err)
			require.Equal(t, "historical", m.Baseline, "bootstrap anchor remains immutable")
			if stage == "broken_chain" {
				db, err := sql.Open("sqlite", pathsFor(r.ConductorDir).ledger)
				require.NoError(t, err)
				_, err = db.Exec(`UPDATE turns SET baseline_turn_id='fork' WHERE external_turn_id='new-turn'`)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
			history := []channelreconcile.ExternalTurn{
				{ID: "historical", Status: "failed"}, {ID: "new-turn", Status: "completed"},
			}
			if stage == "history_fork" {
				history[1].ID = "fork"
			}
			if stage == "extra_turn" {
				history = append(history, channelreconcile.ExternalTurn{ID: "rogue", Status: "completed"})
			}
			created := 0
			second := &testDriver{turns: history, replyErr: errors.New("unsupported item pagination")}
			d2, _ := testDeps(func(Config) agentDriver {
				created++
				return second
			}, func(context.Context, *channelstream.Runner) error { return nil })
			err = run(ctx, r, d2)
			switch stage {
			case "normal":
				require.NoError(t, err)
				require.Zero(t, second.replies, "READY history validation must inspect metadata only")
			case "broken_chain":
				require.Equal(t, string(KindStore), KindOf(err))
				require.Zero(t, created)
			default:
				require.Equal(t, string(KindUncertain), KindOf(err))
			}
		})
	}
}

func TestReadyRestartAllowsOnlyItsRecordedActiveExternalTurn(t *testing.T) {
	for _, extra := range []bool{false, true} {
		name := "one_recorded"
		if extra {
			name = "second_unrecorded"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t)
			r := testRequest(t, ModeCreate, "", cfg)
			require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
			d1, _ := testDeps(func(Config) agentDriver { return &testDriver{} },
				func(context.Context, *channelstream.Runner) error { return nil })
			require.NoError(t, run(ctx, r, d1))
			store, err := channelgateway.Open(pathsFor(r.ConductorDir).ledger)
			require.NoError(t, err)
			_, err = store.Ingest(ctx, channelgateway.Inbound{
				ConversationID: cfg.ConversationID, EventID: "event", MessageID: "message",
				ChannelID: cfg.ChannelID, SenderID: "alice", Body: "prompt",
			})
			require.NoError(t, err)
			turn, err := store.NextTurn(ctx, cfg.ConversationID)
			require.NoError(t, err)
			a, created, err := store.PrepareAttempt(ctx, turn.ID, "")
			require.NoError(t, err)
			require.True(t, created)
			require.NoError(t, store.AcceptAttempt(ctx, turn.ID, a.ID, "active-external"))
			require.NoError(t, store.Close())
			history := []channelreconcile.ExternalTurn{{ID: "active-external", Status: "inProgress"}}
			if extra {
				history = append(history, channelreconcile.ExternalTurn{ID: "rogue", Status: "completed"})
			}
			second := &testDriver{turns: history}
			d2, _ := testDeps(func(Config) agentDriver { return second },
				func(context.Context, *channelstream.Runner) error { return nil })
			err = run(ctx, r, d2)
			if extra {
				require.Equal(t, string(KindUncertain), KindOf(err))
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, second.opens)
			require.Equal(t, 1, second.resumes)
		})
	}
}

func TestExistingManifestAndLedgerRequirePrivateModes(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	d, _ := testDeps(func(Config) agentDriver { return &testDriver{} }, func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	paths := pathsFor(r.ConductorDir)
	require.NoError(t, os.Chmod(paths.manifest, 0o644))
	require.Equal(t, string(KindManifest), KindOf(run(context.Background(), r, d)))
	require.NoError(t, os.Chmod(paths.manifest, 0o600))
	require.NoError(t, os.Chmod(paths.ledger, 0o644))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
}

func TestReadyLedgerRejectsUnsafeSQLiteSidecarsBeforeDriver(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	created := 0
	d, _ := testDeps(func(Config) agentDriver {
		created++
		return &testDriver{}
	}, func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	require.Equal(t, 1, created)
	wal := pathsFor(r.ConductorDir).ledger + "-wal"
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "target"), wal))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
	require.NoError(t, os.Remove(wal))
	require.NoError(t, os.WriteFile(wal, []byte("unsafe"), 0o666))
	require.NoError(t, os.Chmod(wal, 0o666))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
}

func TestPrivateSQLiteSidecarsPermitSQLiteReadOnlyToOthers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	wal := path + "-wal"
	require.NoError(t, os.WriteFile(wal, []byte("sidecar"), 0o644))
	require.NoError(t, os.Chmod(wal, 0o644))
	require.NoError(t, requirePrivateSidecars(path))
	require.NoError(t, os.Link(wal, wal+"-extra"))
	require.Error(t, requirePrivateSidecars(path))
}

// The helper intentionally exits without Store.Close: SQLite must recover the
// WAL written by an unclean prior process on the next runtime start.
func TestUncleanWALChild(t *testing.T) {
	if os.Getenv("CHANNELRUNTIME_WAL_CHILD") != "1" {
		return
	}
	path := os.Getenv("CHANNELRUNTIME_WAL_PATH")
	store, err := channelgateway.Open(path)
	require.NoError(t, err)
	_, err = store.Ingest(context.Background(), channelgateway.Inbound{
		ConversationID: "conversation", EventID: "crash-event", MessageID: "crash-message",
		ChannelID: "channel", SenderID: "alice", Body: "crash prompt",
	})
	require.NoError(t, err)
	_, err = os.Lstat(path + "-wal")
	require.NoError(t, err)
	os.Exit(0)
}

func TestUncleanSQLiteWALRecoversOnReadyRestart(t *testing.T) {
	cfg := testConfig(t)
	r := testRequest(t, ModeCreate, "", cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	created := 0
	d, _ := testDeps(func(Config) agentDriver {
		created++
		if created == 2 {
			return &testDriver{openID: "thread-2"}
		}
		return &testDriver{}
	},
		func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	path := pathsFor(r.ConductorDir).ledger
	cmd := exec.Command(os.Args[0], "-test.run=^TestUncleanWALChild$")
	cmd.Env = append(os.Environ(), "CHANNELRUNTIME_WAL_CHILD=1", "CHANNELRUNTIME_WAL_PATH="+path)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	walInfo, err := os.Lstat(path + "-wal")
	require.NoError(t, err)
	require.True(t, walInfo.Mode().IsRegular())
	require.NoError(t, requirePrivateSidecars(path))
	require.NoError(t, run(context.Background(), r, d))
}
