package tmux

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"al.essio.dev/pkg/shellescape"
	"github.com/creack/pty"
)

func TestSendEnterIfUnattached_RealClients(t *testing.T) {
	for _, mode := range []string{"unattached", "attached", "unrelated", "attach queued before guard", "changed target"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			keys := filepath.Join(dir, "keys")
			s := NewSession("guarded-enter", dir)
			command := "stty raw -echo; printf ready; exec cat > " + shellescape.Quote(keys)
			if out, err := s.tmuxCmd("new-session", "-d", "-s", s.Name, "-x", "100", "-y", "24", command).CombinedOutput(); err != nil {
				t.Fatalf("new fixture: %v (%s)", err, out)
			}
			t.Cleanup(func() { _ = s.Kill() })
			if err := s.tmuxCmd("set-option", "-t", s.Name, "status", "off").Run(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			var snapshot PaneSnapshot
			for {
				var err error
				snapshot, err = s.CapturePaneSnapshot()
				_, fileErr := os.Stat(keys)
				if err == nil && fileErr == nil && strings.Contains(snapshot.Raw, "ready") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("fixture not ready: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if snapshot.Geometry.AttachedClients != 0 {
				t.Fatal("fixture must start unattached")
			}
			if mode == "changed target" {
				snapshot.Geometry.SessionID = "$99999999"
			}
			if mode == "attached" || mode == "unrelated" || mode == "attach queued before guard" {
				target := s.Name
				if mode == "unrelated" {
					other := NewSession("guarded-unrelated", dir)
					if err := other.tmuxCmd("new-session", "-d", "-s", other.Name, "sleep 30").Run(); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = other.Kill() })
					target = other.Name
				}
				args := []string{"attach-session", "-t", "=" + target}
				if mode == "attach queued before guard" {
					args = append(args, ";")
					args = append(args, guardedEnterArgs(s.Name, snapshot.Geometry)...)
					args = append(args, ";", "set-option", "-g", "@guard-completed", "1")
				}
				cmd := s.tmuxCmd(args...)
				cmd.Env = append(os.Environ(), "TERM=xterm-256color")
				terminal, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 24, Cols: 100})
				if err != nil {
					t.Fatal(err)
				}
				go func() { _, _ = io.Copy(io.Discard, terminal) }()
				t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = terminal.Close() })
				for {
					out, err := s.tmuxCmd("display-message", "-p", "-t", "="+target+":", "#{session_attached}").Output()
					if err == nil && string(out) == "1\n" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("real client did not attach: %v", err)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			wantEnter := mode == "unattached" || mode == "unrelated"
			if mode == "attach queued before guard" {
				for {
					out, err := s.tmuxCmd("show-option", "-gqv", "@guard-completed").Output()
					if err == nil && string(out) == "1\n" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("queued guard did not finish")
					}
					time.Sleep(10 * time.Millisecond)
				}
			} else if err := s.SendEnterIfUnattached(snapshot.Geometry); (err == nil) != wantEnter {
				t.Fatalf("guard result: %v; want Enter=%v", err, wantEnter)
			}
			time.Sleep(40 * time.Millisecond)
			for {
				data, err := os.ReadFile(keys)
				if err != nil {
					t.Fatal(err)
				}
				if !wantEnter {
					if len(data) != 0 {
						t.Fatalf("withheld guard emitted %d keys", len(data))
					}
					break
				}
				if string(data) == "\r" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("expected exactly one Enter, got %q", data)
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("%s: physical Enters=%d", mode, map[bool]int{true: 1, false: 0}[wantEnter])
		})
	}
}

func TestSendEnterIfUnattached_InvalidEvidence(t *testing.T) {
	s := &Session{Name: "unused"}
	for _, g := range []PaneGeometry{
		{}, {PaneID: "%0", SessionID: "$0", AttachedClients: -1},
		{PaneID: "%0;send-keys Enter", SessionID: "$0"}, {PaneID: "%0", SessionID: "$"},
	} {
		if err := s.SendEnterIfUnattached(g); err == nil {
			t.Fatal("invalid evidence authorized Enter")
		}
	}
}

func TestSendKeysAndEnterIfUnattached_RealClients(t *testing.T) {
	for _, mode := range []string{"unattached", "attached", "unrelated", "changed target"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			keys := filepath.Join(dir, "keys")
			s := NewSession("guarded-enter", dir)
			command := "stty raw -echo; printf ready; exec cat > " + shellescape.Quote(keys)
			if out, err := s.tmuxCmd("new-session", "-d", "-s", s.Name, "-x", "100", "-y", "24", command).CombinedOutput(); err != nil {
				t.Fatalf("new fixture: %v (%s)", err, out)
			}
			t.Cleanup(func() { _ = s.Kill() })
			if err := s.tmuxCmd("set-option", "-t", s.Name, "status", "off").Run(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			var snapshot PaneSnapshot
			for {
				var err error
				snapshot, err = s.CapturePaneSnapshot()
				_, fileErr := os.Stat(keys)
				if err == nil && fileErr == nil && strings.Contains(snapshot.Raw, "ready") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("fixture not ready: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if snapshot.Geometry.AttachedClients != 0 {
				t.Fatal("fixture must start unattached")
			}
			if mode == "changed target" {
				snapshot.Geometry.SessionID = "$99999999"
			}
			if mode == "attached" || mode == "unrelated" || mode == "attach queued before guard" {
				target := s.Name
				if mode == "unrelated" {
					other := NewSession("guarded-unrelated", dir)
					if err := other.tmuxCmd("new-session", "-d", "-s", other.Name, "sleep 30").Run(); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = other.Kill() })
					target = other.Name
				}
				args := []string{"attach-session", "-t", "=" + target}
				cmd := s.tmuxCmd(args...)
				cmd.Env = append(os.Environ(), "TERM=xterm-256color")
				terminal, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 24, Cols: 100})
				if err != nil {
					t.Fatal(err)
				}
				go func() { _, _ = io.Copy(io.Discard, terminal) }()
				t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = terminal.Close() })
				for {
					out, err := s.tmuxCmd("display-message", "-p", "-t", "="+target+":", "#{session_attached}").Output()
					if err == nil && string(out) == "1\n" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("real client did not attach: %v", err)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			wantEnter := mode == "unattached" || mode == "unrelated"
			started, err := s.SendKeysAndEnterIfUnattached("synthetic body", snapshot.Geometry)
			if (err == nil) != wantEnter || started != wantEnter {
				t.Fatalf("admission result: started=%v err=%v; want transport=%v", started, err, wantEnter)
			}

			time.Sleep(40 * time.Millisecond)
			for {
				data, err := os.ReadFile(keys)
				if err != nil {
					t.Fatal(err)
				}
				if !wantEnter {
					if len(data) != 0 {
						t.Fatalf("withheld guard emitted %d keys", len(data))
					}
					break
				}
				if string(data) == "synthetic body\r" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("expected exact body and one Enter, got %q", data)
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("%s: body writes and Enters=%d", mode, map[bool]int{true: 1, false: 0}[wantEnter])
		})
	}
}
