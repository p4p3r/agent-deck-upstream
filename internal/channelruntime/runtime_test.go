package channelruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/rowdriver"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/conductorlock"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
	"github.com/stretchr/testify/require"
)

type testLock struct{ closed *int }

func (l testLock) Close() error { *l.closed++; return nil }

type testSocket struct{}

func (testSocket) Run(context.Context, slacknetwork.EnvelopeHandler) error { return nil }

type testSender struct{}

func (testSender) PostTopLevel(context.Context, string, string) (slackgateway.PostResult, error) {
	return slackgateway.PostResult{}, nil
}

type testRowDriver struct{ closed int }

func (*testRowDriver) SubmitRowOperation(context.Context, string, string) (rowdriver.Operation, error) {
	return rowdriver.Operation{}, errors.New("not invoked")
}

func (*testRowDriver) RowOperationStatus(context.Context, string) (rowdriver.Operation, error) {
	return rowdriver.Operation{}, errors.New("not invoked")
}

func (d *testRowDriver) Close() error { d.closed++; return nil }

func testRowRequest(t *testing.T, cfg Config) Request {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "conductor")
	require.NoError(t, os.Mkdir(dir, 0o700))
	return Request{
		Name: "conductor", ConductorDir: dir, Mode: ModeRow,
		LoadConfig: func() (Config, error) { return cfg, nil },
	}
}

func testRowDeps(driver *testRowDriver, onRun func(context.Context, *channelstream.Runner) error) (dependencies, *int) {
	closed := new(int)
	return dependencies{
		acquire: func(_, _ string) (io.Closer, error) { return testLock{closed}, nil },
		identity: func(context.Context, string) (slacknetwork.Identity, error) {
			return slacknetwork.Identity{TeamID: "team", BotUserID: "bot-user"}, nil
		},
		rowDriver: func(Config) (rowOperationDriver, error) { return driver, nil },
		socket:    func(string) channelstream.Socket { return testSocket{} },
		sender:    func(string) slackgateway.Sender { return testSender{} },
		run:       onRun,
	}, closed
}

func TestLockPrecedesConfigAndIdentity(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	locked := false
	driver := &testRowDriver{}
	d, _ := testRowDeps(driver, func(context.Context, *channelstream.Runner) error { return nil })
	d.acquire = func(_, _ string) (io.Closer, error) {
		locked = true
		return testLock{new(int)}, nil
	}
	r.LoadConfig = func() (Config, error) {
		require.True(t, locked)
		return cfg, nil
	}
	d.identity = func(context.Context, string) (slacknetwork.Identity, error) {
		require.True(t, locked)
		return slacknetwork.Identity{TeamID: "team", BotUserID: "bot-user"}, nil
	}
	require.NoError(t, run(context.Background(), r, d))
}

func TestHeldLockStopsBeforeConfigOrAnyRuntimeEffect(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	loaded := false
	r.LoadConfig = func() (Config, error) { loaded = true; return cfg, nil }
	d := dependencies{
		acquire: func(_, _ string) (io.Closer, error) { return nil, conductorlock.ErrHeld },
		identity: func(context.Context, string) (slacknetwork.Identity, error) {
			t.Fatal("identity called")
			return slacknetwork.Identity{}, nil
		},
		rowDriver: func(Config) (rowOperationDriver, error) { t.Fatal("row driver created"); return nil, nil },
		socket:    func(string) channelstream.Socket { t.Fatal("socket created"); return nil },
		sender:    func(string) slackgateway.Sender { t.Fatal("sender created"); return nil },
		run:       func(context.Context, *channelstream.Runner) error { t.Fatal("runner started"); return nil },
	}
	err := run(context.Background(), r, d)
	require.Equal(t, string(KindLease), KindOf(err))
	require.False(t, loaded)
	require.Equal(t, "channelruntime: lease", err.Error())
}

func TestConfigLoadFailureStopsBeforeProvidersAndRuntime(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
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
		rowDriver: func(Config) (rowOperationDriver, error) { t.Fatal("row driver created"); return nil, nil },
		socket:    func(string) channelstream.Socket { t.Fatal("socket created"); return nil },
		sender:    func(string) slackgateway.Sender { t.Fatal("sender created"); return nil },
		run:       func(context.Context, *channelstream.Runner) error { t.Fatal("runner started"); return nil },
	}
	err := run(context.Background(), r, d)
	if KindOf(err) != string(KindConfig) || !loaded || closed != 1 || err.Error() != "channelruntime: config" {
		t.Fatal("configuration failure boundary mismatch")
	}
}

func TestRowDriverConstructionFailureReleasesLockWithoutIngress(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	driver := &testRowDriver{}
	d, closed := testRowDeps(driver, func(context.Context, *channelstream.Runner) error {
		t.Fatal("runner started after row driver failure")
		return nil
	})
	d.rowDriver = func(Config) (rowOperationDriver, error) {
		return nil, errors.New("bot-secret app-secret private-body")
	}
	d.socket = func(string) channelstream.Socket { t.Fatal("socket created"); return nil }
	err := run(context.Background(), r, d)
	require.Equal(t, string(KindAgent), KindOf(err))
	require.Equal(t, 1, *closed)
	require.NotContains(t, err.Error(), "bot-secret")
	require.NotContains(t, err.Error(), "private-body")
}

func TestRunnerCancellationReapsRowDriverAndReleasesLock(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	driver := &testRowDriver{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, closed := testRowDeps(driver, func(ctx context.Context, _ *channelstream.Runner) error {
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
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	require.NoError(t, os.MkdirAll(r.ConductorDir, 0o700))
	target := t.TempDir()
	require.NoError(t, os.Symlink(target, filepath.Join(r.ConductorDir, "slack-v2")))
	created := 0
	driver := &testRowDriver{}
	d, _ := testRowDeps(driver, func(context.Context, *channelstream.Runner) error {
		t.Fatal("runner started")
		return nil
	})
	d.rowDriver = func(Config) (rowOperationDriver, error) {
		created++
		return driver, nil
	}
	require.Equal(t, string(KindManifest), KindOf(run(context.Background(), r, d)))
	require.Zero(t, created)
}

func TestManifestRejectsConflictingDuplicateKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":2,"version":2}`), 0o600))
	_, err := loadManifest(path)
	require.Error(t, err)
}

func TestExistingManifestAndLedgerRequirePrivateModes(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	driver := &testRowDriver{}
	d, _ := testRowDeps(driver, func(context.Context, *channelstream.Runner) error { return nil })
	require.NoError(t, run(context.Background(), r, d))
	paths := pathsFor(r.ConductorDir)
	require.NoError(t, os.Chmod(paths.manifest, 0o644))
	require.Equal(t, string(KindManifest), KindOf(run(context.Background(), r, d)))
	require.NoError(t, os.Chmod(paths.manifest, 0o600))
	require.NoError(t, os.Chmod(paths.ledger, 0o644))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
}

func TestReadyLedgerRejectsUnsafeSQLiteSidecarsBeforeDriver(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	driver := &testRowDriver{}
	created := 0
	d, _ := testRowDeps(driver, func(context.Context, *channelstream.Runner) error { return nil })
	d.rowDriver = func(Config) (rowOperationDriver, error) {
		created++
		return driver, nil
	}
	require.NoError(t, run(context.Background(), r, d))
	require.Equal(t, 1, created)
	wal := pathsFor(r.ConductorDir).ledger + "-wal"
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "target"), wal))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
	require.NoError(t, os.Remove(wal))
	require.NoError(t, os.WriteFile(wal, []byte("unsafe"), 0o600))
	require.NoError(t, os.Chmod(wal, 0o666))
	require.Equal(t, string(KindStore), KindOf(run(context.Background(), r, d)))
	require.Equal(t, 1, created)
}

func TestPrivateSQLiteSidecarsPermitSQLiteReadOnlyToOthers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	wal := path + "-wal"
	require.NoError(t, os.WriteFile(wal, []byte("sidecar"), 0o600))
	require.NoError(t, os.Chmod(wal, 0o644))
	require.NoError(t, requirePrivateSidecars(path))
	require.NoError(t, os.Link(wal, wal+"-extra"))
	require.Error(t, requirePrivateSidecars(path))
}

// The helper intentionally exits without Store.Close so restart recovery must
// consume a committed WAL left by the prior process.
func TestUncleanWALChild(t *testing.T) {
	if os.Getenv("CHANNELRUNTIME_WAL_CHILD") != "1" {
		return
	}
	path := os.Getenv("CHANNELRUNTIME_WAL_PATH")
	store, err := channelgateway.Open(path)
	require.NoError(t, err)
	_, err = store.Ingest(context.Background(), channelgateway.Inbound{
		ConversationID: "conversation", EventID: "crash-event", MessageID: "crash-message",
		ChannelID: "channel", SenderID: "allowed-user", Body: "crash prompt",
	})
	require.NoError(t, err)
	_, err = os.Lstat(path + "-wal")
	require.NoError(t, err)
	os.Exit(0)
}

func TestUncleanSQLiteWALRecoversOnReadyRestart(t *testing.T) {
	cfg := rowConfig()
	r := testRowRequest(t, cfg)
	driver := &testRowDriver{}
	created := 0
	d, _ := testRowDeps(driver, func(context.Context, *channelstream.Runner) error { return nil })
	d.rowDriver = func(Config) (rowOperationDriver, error) {
		created++
		return driver, nil
	}
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
	require.Equal(t, 2, created)
}
