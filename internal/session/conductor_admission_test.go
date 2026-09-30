package session

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func namedConductorForAdmission(t *testing.T, backend, agent string) *Instance {
	t.Helper()
	home := setupConductorTest(t)
	writeConductorConfig(t, home, "[conductors.foo]\nbackend = \""+backend+"\"\n")
	dir, err := ConductorNameDir("foo")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := SaveConductorMeta(&ConductorMeta{Name: "foo", Agent: agent, Profile: "default"}); err != nil {
		t.Fatal(err)
	}
	inst := NewInstanceWithGroupAndTool(ConductorSessionTitle("foo"), dir, "conductor", agent)
	inst.Command = agent
	return inst
}

func TestLegacyConductorAdmissionOldRowAndAllLaunchPaths(t *testing.T) {
	for _, agent := range []string{"claude", "codex", "hermes", "pi"} {
		t.Run(agent, func(t *testing.T) {
			inst := namedConductorForAdmission(t, ConductorBackendSlackV2, agent)
			inst.IsConductor = false // older setup rows did not set this flag
			if err := LegacyConductorAdmission(inst); !errors.Is(err, ErrLegacyConductorBlocked) {
				t.Fatalf("old row admission = %v", err)
			}
			for name, run := range map[string]func() error{
				"start":              inst.Start,
				"start with message": func() error { return inst.StartWithMessage("hello") },
				"restart":            inst.Restart,
				"restart with env":   func() error { return inst.RestartWithEnv(map[string]string{"TEST_KEY": "value"}) },
				"restart fresh":      inst.RestartFresh,
			} {
				if err := run(); !errors.Is(err, ErrLegacyConductorBlocked) {
					t.Errorf("%s = %v, want backend refusal", name, err)
				}
			}
		})
	}
}

func TestLegacyConductorAdmissionOrdinaryAndMalformed(t *testing.T) {
	inst := namedConductorForAdmission(t, ConductorBackendSlackV2, "codex")
	ordinary := NewInstanceWithGroupAndTool("conductor-foo", filepath.Join(t.TempDir(), "other"), "projects", "codex")
	if err := LegacyConductorAdmission(ordinary); err != nil {
		t.Fatalf("ordinary same-title session changed: %v", err)
	}
	ordinary.Title = "worker"
	if err := LegacyConductorAdmission(ordinary); err != nil {
		t.Fatalf("ordinary worker changed: %v", err)
	}
	inst.ProjectPath = filepath.Join(t.TempDir(), "wrong")
	if err := LegacyConductorAdmission(inst); !errors.Is(err, ErrConductorIdentityInvalid) {
		t.Fatalf("wrong conductor home = %v", err)
	}
}

func TestLegacyConductorAdmissionUnknownBackend(t *testing.T) {
	inst := namedConductorForAdmission(t, "unknown", "codex")
	if err := LegacyConductorAdmission(inst); err == nil || errors.Is(err, ErrLegacyConductorBlocked) {
		t.Fatalf("unknown backend admission = %v", err)
	}
}

func TestLegacyConductorAdmissionPreservesLegacyWithoutMetadata(t *testing.T) {
	inst := namedConductorForAdmission(t, ConductorBackendLegacy, "codex")
	if err := os.Remove(filepath.Join(inst.ProjectPath, "meta.json")); err != nil {
		t.Fatal(err)
	}
	inst.Tool = "claude" // Legacy admission must not tighten old metadata/tool behavior.
	if err := LegacyConductorAdmission(inst); err != nil {
		t.Fatalf("legacy backend admission = %v", err)
	}
}

func TestLegacyConductorAdmissionV2CannotBypassWithStaleMetadata(t *testing.T) {
	inst := namedConductorForAdmission(t, ConductorBackendSlackV2, "codex")
	if err := os.Remove(filepath.Join(inst.ProjectPath, "meta.json")); err != nil {
		t.Fatal(err)
	}
	inst.Tool = "claude"
	inst.IsConductor = false
	if err := LegacyConductorAdmission(inst); !errors.Is(err, ErrLegacyConductorBlocked) {
		t.Fatalf("slack-v2 backend with stale metadata = %v", err)
	}
}
