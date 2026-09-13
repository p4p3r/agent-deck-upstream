// Tests for the three-tier fallback chain on service-mode spawn
// (v1.7.21). Service systemd-run fails → scope systemd-run attempted
// → direct tmux attempted. Any tier succeeding short-circuits; all
// tiers failing wraps ALL THREE diagnostics so operators can triage.
//
// Uses the same execCommand swappable-seam + failOnLauncher helpers
// as tmux_fallback_test.go so failures are injected at the process
// boundary without touching host PATH or systemd state.
package tmux

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isolatedSystemdRunLauncher exercises Start's service/scope argv construction
// without contacting the user service manager. Successful systemd-run calls
// are translated back to their inner tmux command, which inherits TestMain's
// private TMUX_TMPDIR. This preserves the success/fallback control flow while
// making an uncontained manager launch impossible from this unit-test file.
func isolatedSystemdRunLauncher(t *testing.T, failService bool, calls *[]string) func(string, ...string) *exec.Cmd {
	t.Helper()
	require.NotEmpty(t, os.Getenv("TMUX_TMPDIR"), "package TestMain must isolate the tmux socket")
	require.Equal(t, "1", os.Getenv(testIsolationMarkerEnv), "package TestMain must attest tmux isolation")
	require.Empty(t, os.Getenv("TMUX"), "isolated launcher must not inherit a tmux client socket")
	require.NotEqual(t, "/tmp", os.Getenv("TMUX_TMPDIR"), "tmux's default base is not isolated")
	return func(name string, arg ...string) *exec.Cmd {
		*calls = append(*calls, name+" "+strings.Join(arg, " "))
		if name != "systemd-run" {
			return exec.Command(name, arg...)
		}
		if failService && containsServiceUnitFlag(arg) {
			return exec.Command("false")
		}
		inner := stripSystemdRunPrefix(arg)
		if len(inner) == 0 || (len(inner) == len(arg) && strings.Join(inner, "\x00") == strings.Join(arg, "\x00")) {
			t.Errorf("systemd-run argv did not contain an inner tmux command: %v", arg)
			return exec.Command("false")
		}
		return exec.Command("tmux", inner...)
	}
}

// TestStart_Service_SuccessPath: service spawn works first time, no
// scope/direct retries attempted. Negative guard: execCommand counter
// asserts exactly one systemd-run invocation.
func TestStart_Service_SuccessPath(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skipf("no tmux binary available: %v", err)
	}

	original := execCommand
	var calls []string
	execCommand = isolatedSystemdRunLauncher(t, false, &calls)
	t.Cleanup(func() { execCommand = original })

	displayName := "test-svcok-" + randomServerSuffix(t)
	s := NewSession(displayName, "/tmp")
	s.LaunchAs = "service"
	t.Cleanup(func() {
		_ = exec.Command("tmux", "kill-session", "-t", s.Name).Run()
	})

	if err := s.Start(""); err != nil {
		t.Fatalf("service-mode spawn failed unexpectedly: %v", err)
	}

	// Successful service spawn must invoke systemd-run exactly once on success
	// (tmux binary probes from inside tmux itself don't go through execCommand).
	var systemdRunCalls int
	for _, c := range calls {
		if strings.HasPrefix(c, "systemd-run ") {
			systemdRunCalls++
		}
	}
	assert.Equal(t, 1, systemdRunCalls, "service success path must NOT retry; got %d systemd-run invocations", systemdRunCalls)
}

// TestStart_Service_WhenServiceFails_FallbackToScope: first systemd-run
// (service) returns non-zero; fallback tries systemd-run (scope);
// fallback succeeds; session ready.
//
// Simulation: inject a mock that returns exit-1 ONLY when the argv
// contains "--unit *.service" (i.e. service form), pass-through for the
// scope form and direct tmux.
func TestStart_Service_WhenServiceFails_FallbackToScope(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skipf("no tmux binary available: %v", err)
	}

	original := execCommand
	var calls []string
	execCommand = isolatedSystemdRunLauncher(t, true, &calls)
	t.Cleanup(func() { execCommand = original })

	buf := captureStatusLog(t)

	displayName := "test-svcfallscope-" + randomServerSuffix(t)
	s := NewSession(displayName, "/tmp")
	s.LaunchAs = "service"
	t.Cleanup(func() { _ = exec.Command("tmux", "kill-session", "-t", s.Name).Run() })

	if err := s.Start(""); err != nil {
		t.Fatalf("expected fallback from service → scope to recover, got error: %v\nlog: %s", err, buf.String())
	}

	logs := buf.String()
	assert.Contains(t, logs, "tmux_systemd_run_fallback",
		"service→scope fallback must emit a structured log for observability")
	require.Len(t, calls, 2, "service failure must make exactly one contained scope retry")
	assert.Contains(t, calls[1], "--scope", "second call must retain the scope fallback argv")
}

// TestStart_Service_WhenAllSystemdFail_FallbackToDirect: service AND
// scope both fail; direct tmux succeeds; session ready.
func TestStart_Service_WhenAllSystemdFail_FallbackToDirect(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skipf("no tmux binary available: %v", err)
	}

	original := execCommand
	execCommand = func(name string, arg ...string) *exec.Cmd {
		if name == "systemd-run" {
			return exec.Command("false") // fail on BOTH service AND scope
		}
		return exec.Command(name, arg...)
	}
	t.Cleanup(func() { execCommand = original })

	displayName := "test-svcfallall-" + randomServerSuffix(t)
	s := NewSession(displayName, "/tmp")
	s.LaunchAs = "service"
	t.Cleanup(func() { _ = exec.Command("tmux", "kill-session", "-t", s.Name).Run() })

	if err := s.Start(""); err != nil {
		t.Fatalf("expected fallback service→scope→direct to recover, got: %v", err)
	}
}

// TestStart_Service_AllThreePathsFail_ErrorIncludesAllThree: when
// every tier fails, the returned error must contain all three
// diagnostics so operators can triage in one log grep.
func TestStart_Service_AllThreePathsFail_ErrorIncludesAllThree(t *testing.T) {
	original := execCommand
	execCommand = func(name string, arg ...string) *exec.Cmd {
		if name == "systemd-run" || name == "tmux" {
			return exec.Command("false")
		}
		return exec.Command(name, arg...)
	}
	t.Cleanup(func() { execCommand = original })

	displayName := "test-svcallfail-" + randomServerSuffix(t)
	s := NewSession(displayName, "/tmp")
	s.LaunchAs = "service"
	t.Cleanup(func() { _ = exec.Command("tmux", "kill-session", "-t", s.Name).Run() })

	err := s.Start("")
	require.Error(t, err, "all three paths fail — Start must surface an error")
	msg := err.Error()
	assert.Contains(t, msg, "service path:",
		"operators must see the service-tier failure to diagnose dbus/manager issues")
	assert.Contains(t, msg, "scope path:",
		"operators must see the scope-tier failure to diagnose permission/linger issues")
	assert.Contains(t, msg, "direct retry:",
		"operators must see the direct-tmux failure to diagnose host-tmux issues")
}

// containsServiceUnitFlag scans argv for a "--unit X.service" pair. Used
// by the mock above to distinguish service-form systemd-run from
// scope-form at inject time.
func containsServiceUnitFlag(args []string) bool {
	for i, a := range args {
		if a == "--unit" && i+1 < len(args) && strings.HasSuffix(args[i+1], ".service") {
			return true
		}
	}
	return false
}
