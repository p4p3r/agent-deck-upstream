package slackcontrol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testNonce = "0123456789abcdef0123456789abcdef"

func fixtureStatus() Status {
	exit := (*int)(nil)
	return Status{
		Identity: Identity{
			BinarySHA256:    strings.Repeat("a", 64),
			ConfigSHA256:    strings.Repeat("b", 64),
			RowBindingAlias: "row_binding_alias_fixture",
		},
		Runner:    RunnerStatus{State: "running", ExitCode: exit, Terminal: false, Degraded: false},
		Pump:      PumpStatus{State: "fresh", LastProgressAgeSeconds: int64ptr(3)},
		Backlog:   BacklogStatus{State: "pending", OldestAgeSeconds: int64ptr(8)},
		Egress:    EgressStatus{State: "clear"},
		Conductor: ConductorStatus{State: "working", TurnAgeSeconds: int64ptr(7200)},
	}
}

func int64ptr(value int64) *int64 { return &value }

func startFixtureServer(t *testing.T, mutate func(*Config)) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "control.sock")
	cfg := Config{
		Path:          path,
		UID:           os.Getuid(),
		PeerUID:       func(*net.UnixConn) (int, error) { return os.Getuid(), nil },
		IOTimeout:     100 * time.Millisecond,
		StatusTimeout: 50 * time.Millisecond,
		Snapshot:      func(context.Context) (Status, error) { return fixtureStatus(), nil },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	server, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return server, path
}

func dialFixture(t *testing.T, path string) *net.UnixConn {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func writeRaw(t *testing.T, conn *net.UnixConn, data []byte) {
	t.Helper()
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
}

func readJSONLine(t *testing.T, reader *bufio.Reader) map[string]any {
	t.Helper()
	_ = reader
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(line, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func expectClosed(t *testing.T, conn *net.UnixConn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(conn).ReadByte(); err == nil {
		t.Fatal("connection remained open")
	}
}

func TestStrictHandshakeReturnsExactBodyFreeSchema(t *testing.T) {
	server, path := startFixtureServer(t, nil)
	conn := dialFixture(t, path)
	reader := bufio.NewReader(conn)
	writeRaw(t, conn, []byte(`{"version":1,"type":"hello","nonce":"`+testNonce+`"}`+"\n"))
	pong := readJSONLine(t, reader)
	wantTop := []string{"version", "type", "nonce", "identity", "runner", "pump", "backlog", "egress", "conductor"}
	if len(pong) != len(wantTop) {
		t.Fatalf("pong fields = %v", pong)
	}
	for _, field := range wantTop {
		if _, ok := pong[field]; !ok {
			t.Fatalf("missing pong field %q", field)
		}
	}
	if pong["version"] != float64(1) || pong["type"] != "pong" || pong["nonce"] != testNonce {
		t.Fatalf("pong header = %v", pong)
	}
	identity := pong["identity"].(map[string]any)
	if len(identity) != 3 || identity["binary_sha256"] != strings.Repeat("a", 64) ||
		identity["config_sha256"] != strings.Repeat("b", 64) || identity["row_binding_alias"] != "row_binding_alias_fixture" {
		t.Fatalf("identity = %v", identity)
	}
	writeRaw(t, conn, []byte(`{"version":1,"type":"close","nonce":"`+testNonce+`"}`+"\n"))
	closed := readJSONLine(t, reader)
	if len(closed) != 3 || closed["version"] != float64(1) || closed["type"] != "closed" || closed["nonce"] != testNonce {
		t.Fatalf("closed = %v", closed)
	}
	select {
	case completion := <-server.Completed():
		if completion.RunnerState != "running" || completion.Recoverable {
			t.Fatalf("completion = %+v", completion)
		}
	case <-time.After(time.Second):
		t.Fatal("successful handshake was not reported")
	}
}

func TestStrictHandshakeRejectsMalformedFrames(t *testing.T) {
	oversized := append([]byte(`{"version":1,"type":"hello","nonce":"`), []byte(strings.Repeat("0", MaxFrameBytes))...)
	oversized = append(oversized, []byte(`"}`+"\n")...)
	cases := map[string][]byte{
		"invalid utf8":    append([]byte{0xff}, '\n'),
		"oversized":       oversized,
		"duplicate":       []byte(`{"version":1,"version":1,"type":"hello","nonce":"` + testNonce + `"}` + "\n"),
		"unknown":         []byte(`{"version":1,"type":"hello","nonce":"` + testNonce + `","extra":1}` + "\n"),
		"missing":         []byte(`{"version":1,"type":"hello"}` + "\n"),
		"wrong type":      []byte(`{"version":1,"type":"close","nonce":"` + testNonce + `"}` + "\n"),
		"wrong version":   []byte(`{"version":2,"type":"hello","nonce":"` + testNonce + `"}` + "\n"),
		"uppercase nonce": []byte(`{"version":1,"type":"hello","nonce":"0123456789ABCDEF0123456789ABCDEF"}` + "\n"),
		"short nonce":     []byte(`{"version":1,"type":"hello","nonce":"0123"}` + "\n"),
	}
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			_, path := startFixtureServer(t, nil)
			conn := dialFixture(t, path)
			writeRaw(t, conn, frame)
			expectClosed(t, conn)
		})
	}
}

func TestStrictHandshakeRejectsNonceMismatchAndExtraFrames(t *testing.T) {
	for name, tail := range map[string]string{
		"nonce mismatch":        `{"version":1,"type":"close","nonce":"ffffffffffffffffffffffffffffffff"}` + "\n",
		"unknown close field":   `{"version":1,"type":"close","nonce":"` + testNonce + `","extra":true}` + "\n",
		"duplicate close field": `{"version":1,"type":"close","type":"close","nonce":"` + testNonce + `"}` + "\n",
		"extra frame":           `{"version":1,"type":"close","nonce":"` + testNonce + `"}` + "\n" + `{"version":1,"type":"close","nonce":"` + testNonce + `"}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, path := startFixtureServer(t, nil)
			conn := dialFixture(t, path)
			reader := bufio.NewReader(conn)
			writeRaw(t, conn, []byte(`{"version":1,"type":"hello","nonce":"`+testNonce+`"}`+"\n"))
			_ = readJSONLine(t, reader)
			writeRaw(t, conn, []byte(tail))
			expectClosed(t, conn)
		})
	}

	t.Run("premature close", func(t *testing.T) {
		_, path := startFixtureServer(t, nil)
		conn := dialFixture(t, path)
		writeRaw(t, conn, []byte(
			`{"version":1,"type":"hello","nonce":"`+testNonce+`"}`+"\n"+
				`{"version":1,"type":"close","nonce":"`+testNonce+`"}`+"\n"))
		expectClosed(t, conn)
	})
}

func TestInvalidSnapshotFailsClosedWithoutLeakingBody(t *testing.T) {
	const private = "SENTINEL-PRIVATE-BODY-DO-NOT-EMIT"
	_, path := startFixtureServer(t, func(cfg *Config) {
		cfg.Snapshot = func(context.Context) (Status, error) {
			value := fixtureStatus()
			value.Identity.RowBindingAlias = private
			value.Pump = PumpStatus{State: "idle"} // pending + idle is deliberately invalid
			return value, nil
		}
	})
	conn := dialFixture(t, path)
	writeRaw(t, conn, []byte(`{"version":1,"type":"hello","nonce":"`+testNonce+`"}`+"\n"))
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	data, _ := bufio.NewReader(conn).ReadBytes('\n')
	if strings.Contains(string(data), private) {
		t.Fatal("private snapshot material escaped")
	}
}

func TestRecoveryCompletionRequiresEveryFailClosedState(t *testing.T) {
	base := fixtureStatus()
	exit := 75
	base.Runner = RunnerStatus{State: "exited", ExitCode: &exit}
	if !recoveryEligible(base) {
		t.Fatal("qualified exit 75 was not recoverable")
	}
	cases := map[string]func(*Status){
		"terminal":          func(s *Status) { s.Runner.Terminal = true },
		"degraded":          func(s *Status) { s.Runner.Degraded = true },
		"other exit":        func(s *Status) { *s.Runner.ExitCode = 78 },
		"stale pump":        func(s *Status) { s.Pump.State = "stale" },
		"stalled pump":      func(s *Status) { s.Pump.State = "stalled" },
		"unknown pump":      func(s *Status) { s.Pump = PumpStatus{State: "unknown"} },
		"unknown backlog":   func(s *Status) { s.Backlog = BacklogStatus{State: "unknown"} },
		"uncertain egress":  func(s *Status) { s.Egress.State = "uncertain" },
		"unknown egress":    func(s *Status) { s.Egress.State = "unknown" },
		"unknown conductor": func(s *Status) { s.Conductor = ConductorStatus{State: "unknown"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			status := base
			exitCopy := *base.Runner.ExitCode
			status.Runner.ExitCode = &exitCopy
			mutate(&status)
			if recoveryEligible(status) {
				t.Fatal("fail-closed state became recoverable")
			}
		})
	}
}

func TestStatusWorkIsBounded(t *testing.T) {
	release := make(chan struct{})
	_, path := startFixtureServer(t, func(cfg *Config) {
		cfg.StatusTimeout = 30 * time.Millisecond
		cfg.Snapshot = func(ctx context.Context) (Status, error) {
			select {
			case <-release:
				return fixtureStatus(), nil
			case <-ctx.Done():
				return Status{}, ctx.Err()
			}
		}
	})
	defer close(release)
	conn := dialFixture(t, path)
	start := time.Now()
	writeRaw(t, conn, []byte(`{"version":1,"type":"hello","nonce":"`+testNonce+`"}`+"\n"))
	expectClosed(t, conn)
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("status timeout took %v", elapsed)
	}
}

func TestNegativeControlRejectsPeerUIDMismatchBeforeSampling(t *testing.T) {
	var sampled atomic.Bool
	_, path := startFixtureServer(t, func(cfg *Config) {
		cfg.PeerUID = func(*net.UnixConn) (int, error) { return os.Getuid() + 1, nil }
		cfg.Snapshot = func(context.Context) (Status, error) {
			sampled.Store(true)
			return fixtureStatus(), nil
		}
	})
	conn := dialFixture(t, path)
	_, _ = conn.Write([]byte(`{"version":1,"type":"hello","nonce":"` + testNonce + `"}` + "\n"))
	expectClosed(t, conn)
	if sampled.Load() {
		t.Fatal("weakened peer ownership reached status sampling")
	}
}

func TestSocketPathSafetyStaleRecoveryAndOwnedCleanup(t *testing.T) {
	t.Run("relative", func(t *testing.T) {
		_, err := Start(Config{Path: "control.sock", UID: os.Getuid(), Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil }})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("relative path error = %v", err)
		}
	})
	t.Run("unsafe parent", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := Start(Config{Path: filepath.Join(dir, "control.sock"), UID: os.Getuid(), Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil }})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("unsafe parent error = %v", err)
		}
	})
	t.Run("symlink parent", func(t *testing.T) {
		root := t.TempDir()
		target := t.TempDir()
		_ = os.Chmod(root, 0o700)
		_ = os.Chmod(target, 0o700)
		link := filepath.Join(root, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		_, err := Start(Config{Path: filepath.Join(link, "control.sock"), UID: os.Getuid(), Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil }})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("symlink parent error = %v", err)
		}
	})
	t.Run("existing regular", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.Chmod(dir, 0o700)
		path := filepath.Join(dir, "control.sock")
		if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Start(Config{Path: path, UID: os.Getuid(), Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil }})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("existing regular error = %v", err)
		}
		if data, _ := os.ReadFile(path); string(data) != "fixture" {
			t.Fatal("unsafe existing object changed")
		}
	})
	t.Run("stale socket", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.Chmod(dir, 0o700)
		path := filepath.Join(dir, "control.sock")
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		listener.SetUnlinkOnClose(false)
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		server, err := Start(Config{
			Path: path, UID: os.Getuid(), PeerUID: func(*net.UnixConn) (int, error) { return os.Getuid(), nil },
			Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("socket info = %v, %v", info, err)
		}
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("owned socket was not removed")
		}
	})
	t.Run("active socket", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.Chmod(dir, 0o700)
		path := filepath.Join(dir, "control.sock")
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = Start(Config{Path: path, UID: os.Getuid(), Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil }})
		if !errors.Is(err, ErrInUse) {
			t.Fatalf("active socket error = %v", err)
		}
	})
	t.Run("unsafe stale socket mode", func(t *testing.T) {
		dir := t.TempDir()
		_ = os.Chmod(dir, 0o700)
		path := filepath.Join(dir, "control.sock")
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		listener.SetUnlinkOnClose(false)
		if err := os.Chmod(path, 0o660); err != nil {
			t.Fatal(err)
		}
		_ = listener.Close()
		_, err = Start(Config{Path: path, UID: os.Getuid(), Snapshot: func(context.Context) (Status, error) { return fixtureStatus(), nil }})
		if !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("unsafe socket mode error = %v", err)
		}
	})
	t.Run("replacement is preserved", func(t *testing.T) {
		server, path := startFixtureServer(t, nil)
		moved := path + ".owned"
		if err := os.Rename(path, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		_ = server.Close()
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "replacement" {
			t.Fatal("replacement object was removed")
		}
	})
}

func TestReadAndShutdownAreBounded(t *testing.T) {
	accepted := make(chan struct{})
	server, path := startFixtureServer(t, func(cfg *Config) {
		cfg.IOTimeout = 40 * time.Millisecond
		cfg.StatusTimeout = 20 * time.Millisecond
		cfg.PeerUID = func(*net.UnixConn) (int, error) {
			close(accepted)
			return os.Getuid(), nil
		}
	})
	conn := dialFixture(t, path)
	<-accepted
	writeRaw(t, conn, []byte(`{"version":1`))
	start := time.Now()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("shutdown took %v", elapsed)
	}
}
