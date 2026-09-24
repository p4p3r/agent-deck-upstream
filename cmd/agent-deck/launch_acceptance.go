package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/send"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

const freshLaunchCodexIdentityTimeout = 10 * time.Second

type launchAcceptanceRequest struct {
	message            string
	tool               string
	noWait             bool
	quiet              bool
	sandbox            bool
	capabilities       bool
	commandPassthrough bool
}

func validateLaunchAcceptanceRequest(req launchAcceptanceRequest) error {
	if strings.TrimSpace(req.message) == "" {
		return fmt.Errorf("--acceptance-only requires a non-empty initial message")
	}
	if req.noWait {
		return fmt.Errorf("--acceptance-only is incompatible with --no-wait")
	}
	if req.quiet {
		return fmt.Errorf("--acceptance-only is incompatible with quiet output")
	}
	if !session.IsCodexCompatible(strings.TrimSpace(req.tool)) {
		return fmt.Errorf("--acceptance-only supports only Codex sessions")
	}
	if req.sandbox {
		return fmt.Errorf("--acceptance-only requires a local non-sandboxed Codex session")
	}
	if req.capabilities {
		return fmt.Errorf("--acceptance-only is incompatible with --capabilities")
	}
	if req.commandPassthrough {
		return fmt.Errorf("--acceptance-only requires an interactive Codex session")
	}
	return nil
}

// launchCommandOutput preserves the ordinary launch presentation while giving
// acceptance-only one body-free failure boundary. Once a new instance exists,
// every failure retains its immutable Agent Deck identity for recovery.
type launchCommandOutput struct {
	ordinary       *CLIOutput
	acceptanceOnly bool
	instanceID     string
}

func newLaunchCommandOutput(jsonMode, quietMode, acceptanceOnly bool) *launchCommandOutput {
	return &launchCommandOutput{
		ordinary:       NewCLIOutput(jsonMode, quietMode),
		acceptanceOnly: acceptanceOnly,
	}
}

func (o *launchCommandOutput) acceptanceFailure() acceptanceOnlyResult {
	if strings.TrimSpace(o.instanceID) == "" {
		return newAcceptanceOnlyFailureResult(
			acceptanceOnlyCodeInvalidOptions, acceptanceOnlyNotAccepted, "",
		)
	}
	return newAcceptanceOnlyFailureResultForInstance(
		acceptanceOnlyCodeIndeterminate, acceptanceOnlyIndeterminate, "", o.instanceID,
	)
}

func (o *launchCommandOutput) Error(message, code string) {
	if o.acceptanceOnly {
		emitAcceptanceOnlyResult(o.acceptanceFailure())
		os.Exit(1)
	}
	o.ordinary.Error(message, code)
}

func (o *launchCommandOutput) ErrorWithData(message, code string, extra map[string]interface{}) {
	if o.acceptanceOnly {
		emitAcceptanceOnlyResult(o.acceptanceFailure())
		os.Exit(1)
	}
	o.ordinary.ErrorWithData(message, code, extra)
}

func (o *launchCommandOutput) Success(message string, data interface{}) {
	if o.acceptanceOnly {
		emitAcceptanceOnlyResult(o.acceptanceFailure())
		os.Exit(1)
	}
	o.ordinary.Success(message, data)
}

type freshLaunchAcceptanceOps interface {
	InstanceID() string
	PrepareFreshFence() error
	WaitReady() error
	ValidateFreshFence() error
	ReserveSubmission() error
	SendOnce() (string, error)
	RecordTransportOutcome(string) error
	AcceptedVerdict(string) acceptanceOnlyResult
	Release()
}

func freshLaunchIndeterminate(instanceID, delivery string) acceptanceOnlyResult {
	return newAcceptanceOnlyFailureResultForInstance(
		acceptanceOnlyCodeIndeterminate, acceptanceOnlyIndeterminate, delivery, instanceID,
	)
}

// runFreshLaunchAcceptance is the protocol boundary for the new launch mode.
// There is exactly one call to SendOnce, and no branch retries it.
func runFreshLaunchAcceptance(ops freshLaunchAcceptanceOps) acceptanceOnlyResult {
	instanceID := strings.TrimSpace(ops.InstanceID())
	if err := ops.PrepareFreshFence(); err != nil {
		ops.Release()
		return freshLaunchIndeterminate(instanceID, "")
	}
	defer ops.Release()

	if err := ops.WaitReady(); err != nil {
		return freshLaunchIndeterminate(instanceID, "")
	}
	if err := ops.ValidateFreshFence(); err != nil {
		return freshLaunchIndeterminate(instanceID, "")
	}
	if err := ops.ReserveSubmission(); err != nil {
		return freshLaunchIndeterminate(instanceID, "")
	}
	// Close the task-start race between readiness and durable reservation. A
	// generation already present here cannot belong to the transport below.
	if err := ops.ValidateFreshFence(); err != nil {
		_ = ops.RecordTransportOutcome(deliveryTargetBusy) // definitive: no bytes sent
		return freshLaunchIndeterminate(instanceID, "")
	}

	delivery, sendErr := ops.SendOnce()
	if err := ops.RecordTransportOutcome(delivery); err != nil {
		return freshLaunchIndeterminate(instanceID, delivery)
	}
	if sendErr != nil {
		if acceptanceOnlyDefinitiveNonDelivery(delivery) {
			return newAcceptanceOnlyFailureResultForInstance(
				acceptanceOnlyCodeNotAccepted, acceptanceOnlyNotAccepted, delivery, instanceID,
			)
		}
		return freshLaunchIndeterminate(instanceID, delivery)
	}
	return ops.AcceptedVerdict(delivery)
}

type liveFreshLaunchAcceptanceOps struct {
	inst    *session.Instance
	peers   []*session.Instance
	storage *session.Storage
	message string

	guard *codexAcceptanceGuard
	fence codexAcceptanceFence
}

func (o *liveFreshLaunchAcceptanceOps) InstanceID() string {
	if o.inst == nil {
		return ""
	}
	return o.inst.ID
}

func (o *liveFreshLaunchAcceptanceOps) PrepareFreshFence() error {
	if err := bindFreshLiveCodexIdentity(o.inst, o.peers, o.storage, freshLaunchCodexIdentityTimeout); err != nil {
		return err
	}
	guard, err := acquireFreshCodexAcceptanceGuard(o.inst, codexAcceptanceLockTimeout)
	if err != nil {
		return err
	}
	o.guard = guard
	o.fence = guard.fence
	return nil
}

func (o *liveFreshLaunchAcceptanceOps) WaitReady() error {
	if o.inst == nil || o.inst.GetTmuxSession() == nil {
		return fmt.Errorf("Codex pane is unavailable")
	}
	return send.WaitForAgentReady(
		o.inst.GetTmuxSession(), o.inst.Tool, send.DefaultAgentReadyTimeout,
		send.PromptGates{CodexPrompt: true},
	)
}

func (o *liveFreshLaunchAcceptanceOps) ValidateFreshFence() error {
	return validateCodexAcceptanceFence(o.inst, o.fence)
}

func (o *liveFreshLaunchAcceptanceOps) ReserveSubmission() error {
	if o.guard == nil {
		return fmt.Errorf("Codex acceptance guard is unavailable")
	}
	return o.guard.Prepare(o.inst.ID, time.Now())
}

func (o *liveFreshLaunchAcceptanceOps) SendOnce() (string, error) {
	target := o.inst.GetTmuxSession()
	if target == nil {
		return deliveryPaneGone, fmt.Errorf("Codex pane is unavailable")
	}
	result, err := performSend(
		o.inst, target, o.message, false, defaultSendTuning(), "tmux", false,
		nil, nil, nil,
	)
	return result.delivery, err
}

func (o *liveFreshLaunchAcceptanceOps) RecordTransportOutcome(delivery string) error {
	if o.guard == nil {
		return fmt.Errorf("Codex acceptance guard is unavailable")
	}
	return o.guard.RecordTransportOutcome(delivery, time.Now())
}

func (o *liveFreshLaunchAcceptanceOps) AcceptedVerdict(delivery string) acceptanceOnlyResult {
	return acceptedTurnOnlyVerdict(o.inst, delivery, time.Now(), o.fence, o.guard)
}

func (o *liveFreshLaunchAcceptanceOps) Release() {
	if o.guard != nil {
		o.guard.Release()
		o.guard = nil
	}
}

func bindFreshLiveCodexIdentity(
	inst *session.Instance,
	peers []*session.Instance,
	storage *session.Storage,
	timeout time.Duration,
) error {
	if inst == nil || !session.IsCodexCompatible(inst.Tool) || !inst.CodexRolloutIsResolvableLocally() {
		return fmt.Errorf("fresh local Codex identity is unavailable")
	}
	if timeout <= 0 {
		timeout = freshLaunchCodexIdentityTimeout
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if !inst.Exists() {
			return fmt.Errorf("fresh Codex process exited before identity binding")
		}
		candidate, err := inst.LiveCodexSessionIdentity()
		if err != nil {
			lastErr = err
		} else if candidate != "" {
			if existing := strings.TrimSpace(inst.CodexSessionID); existing != "" && existing != candidate {
				return fmt.Errorf("fresh Codex process identity conflicts with an existing binding")
			}
			for _, peer := range peers {
				if peer == nil || peer.ID == inst.ID || !peer.Exists() {
					continue
				}
				if strings.TrimSpace(peer.CodexSessionID) == candidate || liveCodexSessionID(peer) == candidate {
					return fmt.Errorf("fresh Codex process identity is already owned by another live instance")
				}
			}
			if _, _, err := session.SetField(inst, session.FieldCodexSessionID, candidate, nil); err != nil {
				return fmt.Errorf("bind fresh Codex process identity: %w", err)
			}
			if storage == nil || storage.GetDB() == nil {
				return fmt.Errorf("persist fresh Codex process identity: storage unavailable")
			}
			if err := storage.GetDB().WriteCodexSessionBinding(inst.ID, inst.CodexSessionID, inst.CodexDetectedAt); err != nil {
				return fmt.Errorf("persist fresh Codex process identity: %w", err)
			}
			return nil
		}
		if !time.Now().Before(deadline) {
			if lastErr != nil {
				return fmt.Errorf("fresh Codex process identity unavailable: %w", lastErr)
			}
			return fmt.Errorf("fresh Codex process identity unavailable")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func acquireFreshCodexAcceptanceGuard(inst *session.Instance, timeout time.Duration) (*codexAcceptanceGuard, error) {
	if inst == nil || !session.IsCodexCompatible(inst.Tool) || !inst.CodexRolloutIsResolvableLocally() ||
		strings.TrimSpace(inst.CodexSessionID) == "" {
		return nil, fmt.Errorf("fresh Codex acceptance identity is unavailable")
	}
	lock, err := session.AcquireCodexAcceptanceLock(inst.CodexSessionID, timeout)
	if err != nil {
		return nil, err
	}
	generation, err := inst.LatestCodexTurnGeneration()
	if err != nil {
		lock.Release()
		return nil, fmt.Errorf("fresh Codex rollout is unavailable: %w", err)
	}
	if generation != "" {
		lock.Release()
		return nil, fmt.Errorf("fresh Codex rollout already contains a turn")
	}
	fence := codexAcceptanceFence{
		codexSessionID:      inst.CodexSessionID,
		priorTurnGeneration: "",
		available:           true,
	}
	if _, err := session.ReconcileCodexSubmissionMarker(inst.ID, inst.CodexSessionID, ""); err != nil {
		lock.Release()
		return nil, err
	}
	return &codexAcceptanceGuard{lock: lock, fence: fence}, nil
}
