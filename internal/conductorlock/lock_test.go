//go:build linux || darwin

package conductorlock

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLockOtherProcess(t *testing.T) {
	if os.Getenv("AGENTDECK_CONDUCTORLOCK_HELPER") == "1" {
		lock, err := Acquire("foo", os.Getenv("AGENTDECK_CONDUCTORLOCK_DIR"))
		if errors.Is(err, ErrHeld) {
			os.Exit(42)
		}
		if err != nil {
			os.Exit(43)
		}
		_ = lock.Close()
		os.Exit(0)
	}
	dir := filepath.Join(t.TempDir(), "foo")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := Acquire("foo", dir)
	if err != nil {
		t.Fatal(err)
	}
	other := func() int {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLockOtherProcess$")
		cmd.Env = append(os.Environ(), "AGENTDECK_CONDUCTORLOCK_HELPER=1", "AGENTDECK_CONDUCTORLOCK_DIR="+dir)
		err := cmd.Run()
		if err == nil {
			return 0
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return exit.ExitCode()
		}
		t.Fatal(err)
		return -1
	}
	if code := other(); code != 42 {
		t.Fatalf("contending process exit = %d, want 42", code)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if code := other(); code != 0 {
		t.Fatalf("post-close process exit = %d, want 0", code)
	}
}

func TestLockRejectsSymlinksAndBroadModes(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "foo")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire("../foo", dir); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid name = %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire("alias", alias); !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlinked named home = %v", err)
	}
	parentAlias := filepath.Join(root, "parent-alias")
	if err := os.Symlink(root, parentAlias); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire("foo", filepath.Join(parentAlias, "foo")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlinked ancestor = %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire("foo", dir); !errors.Is(err, ErrInvalid) {
		t.Fatalf("writable named home = %v", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	privateDir := filepath.Join(dir, ".agent-deck")
	if err := os.Mkdir(privateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(privateDir, "conductor.lock")); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire("foo", dir); !errors.Is(err, ErrIO) && !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlinked lock file = %v", err)
	}
}

func TestLockCloseIsIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "foo")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := Acquire("foo", dir)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
