package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelruntime"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

func TestConductorSlackV2EarlyDispatch(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{[]string{"conductor", "slack-v2", "run"}, true},
		{[]string{"conductor", "slack-v2", "--help"}, true},
		{[]string{"conductor", "status"}, false},
		{[]string{"session", "start"}, false},
		{[]string{"conductor"}, false},
	} {
		if got := isConductorSlackV2Command(tc.args); got != tc.want {
			t.Errorf("early dispatch for %v = %v, want %v", tc.args, got, tc.want)
		}
	}
}

func TestParseConductorSlackV2RunRequiresExplicitMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		mode channelruntime.Mode
		id   string
		ok   bool
	}{
		{"create", []string{"run", "sample", "--create"}, channelruntime.ModeCreate, "", true},
		{"resume", []string{"run", "sample", "--resume", "thread-exact"}, channelruntime.ModeResume, "thread-exact", true},
		{"missing", []string{"run", "sample"}, "", "", false},
		{"both", []string{"run", "sample", "--create", "--resume", "thread-exact"}, "", "", false},
		{"empty resume", []string{"run", "sample", "--resume", ""}, "", "", false},
		{"padded resume", []string{"run", "sample", "--resume", " thread-exact "}, "", "", false},
		{"traversal", []string{"run", "../sample", "--create"}, "", "", false},
		{"extra", []string{"run", "sample", "--create", "extra"}, "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConductorSlackV2Run(tc.args)
			if (err == nil) != tc.ok {
				t.Fatalf("error presence = %v, want success %v", err != nil, tc.ok)
			}
			if tc.ok && (got.mode != tc.mode || got.threadID != tc.id || got.name != "sample") {
				t.Fatalf("parsed mode/id/name = %q/%q/%q", got.mode, got.threadID, got.name)
			}
		})
	}
}

func TestConductorSlackV2HelpDoesNotInitializeRuntime(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := runConductorSlackV2Command("", []string{"--help"}, &out, &errOut); code != 0 {
		t.Fatalf("help exit = %d", code)
	}
	if !strings.Contains(out.String(), "--create | --resume") || errOut.Len() != 0 {
		t.Fatalf("unexpected help output")
	}
}

func TestConductorSlackV2RunRequiresSelectedBackendBeforeNetwork(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	t.Setenv("AGENTDECK_PROFILE", "default")
	session.ClearUserConfigCache()
	t.Cleanup(session.ClearUserConfigCache)
	dir, err := session.ConductorNameDir("sample")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := runConductorSlackV2Command("", []string{"run", "sample", "--create"}, &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "config") || out.Len() != 0 {
		t.Fatalf("unselected backend result: code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}
