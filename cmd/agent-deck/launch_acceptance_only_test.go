package main

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/session"
)

type fakeFreshLaunchAcceptanceOps struct {
	instanceID string
	prior      string
	prepareErr error
	readyErr   error
	checkErrAt int
	reserveErr error
	sendResult string
	sendErr    error
	recordErr  error
	verdict    acceptanceOnlyResult

	checks   int
	reserves int
	sends    int
	records  int
	releases int
}

func (f *fakeFreshLaunchAcceptanceOps) InstanceID() string { return f.instanceID }
func (f *fakeFreshLaunchAcceptanceOps) PrepareFreshFence() error {
	if f.prepareErr != nil {
		return f.prepareErr
	}
	if f.prior != "" {
		return errors.New("fresh rollout already has a turn")
	}
	return nil
}
func (f *fakeFreshLaunchAcceptanceOps) WaitReady() error { return f.readyErr }
func (f *fakeFreshLaunchAcceptanceOps) ValidateFreshFence() error {
	f.checks++
	if f.checkErrAt == f.checks {
		return errors.New("first turn started outside this launch")
	}
	return nil
}
func (f *fakeFreshLaunchAcceptanceOps) ReserveSubmission() error {
	f.reserves++
	return f.reserveErr
}
func (f *fakeFreshLaunchAcceptanceOps) SendOnce() (string, error) {
	f.sends++
	return f.sendResult, f.sendErr
}
func (f *fakeFreshLaunchAcceptanceOps) RecordTransportOutcome(string) error {
	f.records++
	return f.recordErr
}
func (f *fakeFreshLaunchAcceptanceOps) AcceptedVerdict(string) acceptanceOnlyResult {
	return f.verdict
}
func (f *fakeFreshLaunchAcceptanceOps) Release() { f.releases++ }

func acceptedLaunchTestResult(t *testing.T, instanceID string) acceptanceOnlyResult {
	t.Helper()
	receipt := &codexAcceptedTurnReceipt{
		ReceiptID:      "01234567-89ab-4cde-8fab-0123456789ab",
		InstanceID:     instanceID,
		CodexSessionID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		TurnGeneration: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee:11111111-2222-4333-8444-555555555555",
		AcceptedAt:     time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	result, err := newAcceptanceOnlySuccessResult(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFreshLaunchAcceptanceHappyPathStartsFromNoPriorTurn(t *testing.T) {
	ops := &fakeFreshLaunchAcceptanceOps{
		instanceID: "instance-fresh",
		prior:      "", // Integration-shaped precondition: Codex has no prior turn.
		sendResult: deliverySubmitted,
		verdict:    acceptedLaunchTestResult(t, "instance-fresh"),
	}

	got := runFreshLaunchAcceptance(ops)
	if !got.Success || got.AcceptedTurn == nil || got.InstanceID != "instance-fresh" {
		t.Fatalf("result = %#v, want exact accepted first-turn receipt", got)
	}
	if ops.sends != 1 || ops.reserves != 1 || ops.records != 1 || ops.releases != 1 {
		t.Fatalf("counts: sends=%d reserves=%d records=%d releases=%d", ops.sends, ops.reserves, ops.records, ops.releases)
	}
}

func TestFreshCodexAcceptanceGuardRequiresAnEmptyExactRollout(t *testing.T) {
	dataDir := t.TempDir()
	codexDir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataDir)
	t.Setenv("CODEX_HOME", codexDir)
	path := filepath.Join(codexDir, "sessions", "2026", "09", "24", "rollout-test-thread-fresh.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"thread-fresh"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inst := &session.Instance{ID: "instance-fresh", Tool: "codex", CodexSessionID: "thread-fresh"}
	guard, err := acquireFreshCodexAcceptanceGuard(inst, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if guard.fence.priorTurnGeneration != "" || !guard.fence.available {
		t.Fatalf("fresh fence = %#v", guard.fence)
	}
	if err := guard.Prepare(inst.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := appendCodexTurnStart(path, "turn-raced"); err != nil {
		t.Fatal(err)
	}
	if err := validateCodexAcceptanceFence(inst, guard.fence); err == nil {
		t.Fatal("turn start after reservation must invalidate the pre-transport fence")
	}
	if err := guard.RecordTransportOutcome(deliveryTargetBusy, time.Now()); err != nil {
		guard.Release()
		t.Fatal(err)
	}
	guard.Release()

	if unexpected, err := acquireFreshCodexAcceptanceGuard(inst, time.Second); err == nil {
		unexpected.Release()
		t.Fatal("rollout with an existing turn must not be accepted as fresh")
	}
}

func TestFreshLaunchAcceptanceRefusesUnavailableOrNonFreshIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  fakeFreshLaunchAcceptanceOps
	}{
		{name: "identity unavailable", ops: fakeFreshLaunchAcceptanceOps{instanceID: "instance-unavailable", prepareErr: errors.New("live identity unavailable")}},
		{name: "identity collision", ops: fakeFreshLaunchAcceptanceOps{instanceID: "instance-collision", prepareErr: errors.New("identity collision")}},
		{name: "stale or unrelated rollout", ops: fakeFreshLaunchAcceptanceOps{instanceID: "instance-stale", prior: "thread:old-turn"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runFreshLaunchAcceptance(&tc.ops)
			if got.Success || got.Acceptance != acceptanceOnlyIndeterminate || got.InstanceID != tc.ops.instanceID {
				t.Fatalf("result = %#v", got)
			}
			if tc.ops.sends != 0 {
				t.Fatalf("send count = %d, want 0", tc.ops.sends)
			}
		})
	}
}

func TestFreshLaunchAcceptanceRejectsTaskStartRaceBeforeTransport(t *testing.T) {
	ops := &fakeFreshLaunchAcceptanceOps{
		instanceID: "instance-race",
		checkErrAt: 2, // The generation appears after reservation, before transport.
	}
	got := runFreshLaunchAcceptance(ops)
	if got.Success || got.Acceptance != acceptanceOnlyIndeterminate || got.InstanceID != "instance-race" {
		t.Fatalf("result = %#v", got)
	}
	if ops.sends != 0 || ops.reserves != 1 {
		t.Fatalf("send/reserve counts = %d/%d, want 0/1", ops.sends, ops.reserves)
	}
}

func TestFreshLaunchAcceptanceNeverRetriesIndeterminateTransport(t *testing.T) {
	ops := &fakeFreshLaunchAcceptanceOps{
		instanceID: "instance-uncertain",
		sendResult: deliverySendFailed,
		sendErr:    errors.New("transport outcome unknown"),
	}
	got := runFreshLaunchAcceptance(ops)
	if got.Success || got.Acceptance != acceptanceOnlyIndeterminate || got.InstanceID != "instance-uncertain" {
		t.Fatalf("result = %#v", got)
	}
	if ops.sends != 1 || ops.records != 1 {
		t.Fatalf("send/record counts = %d/%d, want 1/1", ops.sends, ops.records)
	}
}

func TestFreshLaunchAcceptanceTimeoutAndProcessDeathFailClosed(t *testing.T) {
	t.Run("process death before transport", func(t *testing.T) {
		ops := &fakeFreshLaunchAcceptanceOps{instanceID: "instance-dead", readyErr: errors.New("process exited")}
		got := runFreshLaunchAcceptance(ops)
		if got.Success || got.Acceptance != acceptanceOnlyIndeterminate || got.InstanceID != "instance-dead" || ops.sends != 0 {
			t.Fatalf("result=%#v sends=%d", got, ops.sends)
		}
	})
	t.Run("acceptance timeout after transport", func(t *testing.T) {
		ops := &fakeFreshLaunchAcceptanceOps{
			instanceID: "instance-timeout",
			sendResult: deliveryDelivered,
			verdict: newAcceptanceOnlyFailureResultForInstance(
				acceptanceOnlyCodeIndeterminate, acceptanceOnlyIndeterminate, deliveryDelivered, "instance-timeout",
			),
		}
		got := runFreshLaunchAcceptance(ops)
		if got.Success || got.Acceptance != acceptanceOnlyIndeterminate || got.InstanceID != "instance-timeout" || ops.sends != 1 {
			t.Fatalf("result=%#v sends=%d", got, ops.sends)
		}
	})
}

func TestLaunchAcceptanceValidationRejectsBeforeSpawn(t *testing.T) {
	valid := launchAcceptanceRequest{message: "do the work", tool: "codex"}
	if err := validateLaunchAcceptanceRequest(valid); err != nil {
		t.Fatalf("valid request: %v", err)
	}

	for _, tc := range []struct {
		name string
		edit func(*launchAcceptanceRequest)
	}{
		{name: "missing message", edit: func(r *launchAcceptanceRequest) { r.message = "  " }},
		{name: "no wait", edit: func(r *launchAcceptanceRequest) { r.noWait = true }},
		{name: "quiet", edit: func(r *launchAcceptanceRequest) { r.quiet = true }},
		{name: "non codex", edit: func(r *launchAcceptanceRequest) { r.tool = "claude" }},
		{name: "sandbox", edit: func(r *launchAcceptanceRequest) { r.sandbox = true }},
		{name: "capabilities", edit: func(r *launchAcceptanceRequest) { r.capabilities = true }},
		{name: "passthrough", edit: func(r *launchAcceptanceRequest) { r.commandPassthrough = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.edit(&req)
			if err := validateLaunchAcceptanceRequest(req); err == nil {
				t.Fatal("expected pre-spawn rejection")
			}
		})
	}
}

func TestLaunchAcceptanceFailureReceiptIsBoundedBodyFreeAndRetainsInstance(t *testing.T) {
	secret := "never-emit-this-prompt"
	result := newAcceptanceOnlyFailureResultForInstance(
		acceptanceOnlyCodeIndeterminate, acceptanceOnlyIndeterminate, deliveryUnverified, "instance-retained",
	)
	raw, err := marshalAcceptanceOnlyResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw)+1 > acceptanceOnlyResultMaxBytes {
		t.Fatalf("result length = %d", len(raw)+1)
	}
	if strings.Contains(string(raw), secret) || strings.Contains(string(raw), "message") || strings.Contains(string(raw), "response") {
		t.Fatalf("body-bearing result: %s", raw)
	}
	if result.InstanceID != "instance-retained" {
		t.Fatalf("instance_id = %q", result.InstanceID)
	}
}

func TestLaunchAcceptanceBoundaryAndOrdinaryFlagRegistration(t *testing.T) {
	if !acceptanceOnlyCommandRequested([]string{"launch", ".", "--acceptance-only=definitely-not-a-bool", "-m", "secret"}) {
		t.Fatal("malformed launch flag must activate the body-free diagnostic boundary")
	}
	if acceptanceOnlyCommandRequested([]string{"launch", ".", "--acceptance-only=false"}) {
		t.Fatal("explicit false must preserve ordinary launch behavior")
	}

	found := false
	handleLaunchCommand("_test", nil, func(fs *flag.FlagSet) {
		found = fs.Lookup("acceptance-only") != nil
	})
	if !found {
		t.Fatal("launch acceptance-only flag is not registered")
	}
}

func TestLaunchAcceptanceCLIRejectsInvalidityBeforeCreation(t *testing.T) {
	if encoded := os.Getenv("AGENT_DECK_TEST_LAUNCH_ACCEPTANCE_ARGS"); encoded != "" {
		var args []string
		if err := json.Unmarshal([]byte(encoded), &args); err != nil {
			panic(err)
		}
		handleLaunch("_test", args)
		return
	}

	secret := "launch-secret-must-not-escape"
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "missing message", args: []string{".", "-c", "codex", "--acceptance-only"}},
		{name: "no wait", args: []string{".", "-c", "codex", "-m", secret, "--no-wait", "--acceptance-only"}},
		{name: "non Codex", args: []string{".", "-c", "claude", "-m", secret, "--acceptance-only"}},
		{name: "sandbox", args: []string{".", "-c", "codex", "-m", secret, "--sandbox", "--acceptance-only"}},
		{name: "capabilities", args: []string{".", "-c", "codex", "-m", secret, "--capabilities", "--json", "--acceptance-only"}},
		{name: "malformed flag", args: []string{".", "-c", "codex", "-m", secret, "--acceptance-only=not-a-bool"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			configDir := t.TempDir()
			encoded, err := json.Marshal(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLaunchAcceptanceCLIRejectsInvalidityBeforeCreation$")
			cmd.Env = append(os.Environ(),
				"AGENT_DECK_TEST_LAUNCH_ACCEPTANCE_ARGS="+string(encoded),
				"XDG_DATA_HOME="+dataDir,
				"XDG_CONFIG_HOME="+configDir,
			)
			stdout, err := cmd.Output()
			if err == nil {
				t.Fatal("invalid launch unexpectedly exited zero")
			}
			exitErr, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatal(err)
			}
			if len(exitErr.Stderr) != 0 {
				t.Fatalf("stderr crossed body-free boundary: %q", exitErr.Stderr)
			}
			if len(stdout) > acceptanceOnlyResultMaxBytes || strings.Contains(string(stdout), secret) {
				t.Fatalf("unsafe result (%d bytes): %q", len(stdout), stdout)
			}
			var result acceptanceOnlyResult
			if err := json.Unmarshal(stdout, &result); err != nil {
				t.Fatalf("result is not JSON: %v: %q", err, stdout)
			}
			if result.Success || result.Acceptance != acceptanceOnlyNotAccepted || result.InstanceID != "" {
				t.Fatalf("result = %#v", result)
			}
			entries, err := os.ReadDir(filepath.Join(dataDir, "agent-deck"))
			if err == nil && len(entries) != 0 {
				t.Fatalf("pre-spawn rejection created state: %v", entries)
			}
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
		})
	}
}
