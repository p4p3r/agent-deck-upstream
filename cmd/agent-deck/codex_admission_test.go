package main

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/tmux"
)

func TestCodexAdmission_RequiresStableTargetProofBeforeTyping(t *testing.T) {
	target := &mockSendRetryTarget{statuses: []string{"waiting"}}
	res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{
		retry: sendRetryOptions{maxRetries: 1, turnAdvanced: func() bool { return false }, codexFenceUnchanged: func() bool { return true }},
	})
	if err == nil || res.delivery != deliveryComposerBlocked || atomic.LoadInt32(&target.sendKeysCalls) != 0 || atomic.LoadInt32(&target.sendEnterCalls) != 0 {
		t.Fatalf("unproven target must refuse before typing: delivery=%q err=%v bodies=%d Enters=%d", res.delivery, err, target.sendKeysCalls, target.sendEnterCalls)
	}
}

func TestCodexAdmission_PretransportRefusals(t *testing.T) {
	for _, name := range []string{"unknown clients", "unknown session", "unknown server", "capture error", "draft", "busy", "status error", "fence error", "changed target"} {
		t.Run(name, func(t *testing.T) {
			inst, fence, advance := codexRecoveryFence(t)
			target := &codexEnterRecoveryTarget{acceptRetry: true, accept: advance}
			target.statuses = []string{"waiting"}
			opts := codexRecoveryOptions(inst, fence, 3)
			switch name {
			case "unknown clients":
				target.attachedClients = -1
			case "unknown session":
				target.snapshotAdjust = func(s *tmux.PaneSnapshot) { s.Geometry.SessionID = "$unknown" }
			case "unknown server":
				target.snapshotAdjust = func(s *tmux.PaneSnapshot) { s.Geometry.ServerPID = 0 }
			case "capture error":
				target.snapshotErr = errors.New("tmux unavailable")
			case "draft":
				target.body = "foreign draft"
			case "busy":
				target.statuses = []string{"active"}
			case "status error":
				target.statusErrs = []error{errors.New("status unavailable")}
			case "fence error":
				opts.codexFenceUnchanged = func() bool { return false }
			case "changed target":
				captures := 0
				target.snapshotAdjust = func(s *tmux.PaneSnapshot) {
					captures++
					if captures > 1 {
						s.Geometry.PaneID = "%8"
					}
				}
			}
			res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{retry: opts})
			if err == nil || res.delivery != deliveryComposerBlocked || target.sendKeysCalls != 0 || target.sendEnterCalls != 0 || target.acceptedTurns != 0 || target.guardCalls != 0 {
				t.Fatalf("pretransport refusal: delivery=%q err=%v bodies=%d Enters=%d accepted=%d guards=%d", res.delivery, err, target.sendKeysCalls, target.sendEnterCalls, target.acceptedTurns, target.guardCalls)
			}
		})
	}
}

func TestCodexAdmission_StableAttachedTargetCanSubmit(t *testing.T) {
	inst, fence, advance := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: advance, attachedClients: 1}
	target.statuses = []string{"waiting"}
	res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{
		retry: codexRecoveryOptions(inst, fence, 3),
	})
	if err != nil || res.delivery != deliverySubmitted || target.sendKeysCalls != 1 || target.sendEnterCalls != 1 || target.acceptedTurns != 1 {
		t.Fatalf("stable attached target: delivery=%q err=%v bodies=%d Enters=%d accepted=%d", res.delivery, err, target.sendKeysCalls, target.sendEnterCalls, target.acceptedTurns)
	}
}

func TestCodexAdmission_RecoveryIsOneAttempt(t *testing.T) {
	inst, fence, advance := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{accept: advance}
	target.statuses = []string{"waiting"}
	res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{retry: codexRecoveryOptions(inst, fence, 30)})
	if err == nil || res.delivery != deliveryTypedNotSubmitted || target.guardCalls != 1 || target.sendEnterCalls != 1 || target.sendKeysCalls != 1 || target.sendCtrlCCalls != 0 || target.acceptedTurns != 0 {
		t.Fatalf("one recovery attempt: delivery=%q err=%v bodies=%d Enters=%d guards=%d", res.delivery, err, target.sendKeysCalls, target.sendEnterCalls, target.guardCalls)
	}
}

func TestCodexAdmission_AttachAfterTypingCanRecoverOnceStable(t *testing.T) {
	inst, fence, advance := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: advance}
	target.statuses = []string{"waiting"}
	target.onStage = func() { target.attachedClients = 1 }
	res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{retry: codexRecoveryOptions(inst, fence, 3)})
	if err != nil || res.delivery != deliverySubmitted || target.guardCalls != 1 || target.sendEnterCalls != 1 || target.sendKeysCalls != 1 || target.acceptedTurns != 1 {
		t.Fatalf("attachment after typing: delivery=%q err=%v bodies=%d Enters=%d guards=%d", res.delivery, err, target.sendKeysCalls, target.sendEnterCalls, target.guardCalls)
	}
}

func TestCodexAdmission_RecoveryRequiresReadableUnchangedFence(t *testing.T) {
	inst, fence, advance := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: advance}
	target.statuses = []string{"waiting"}
	opts := codexRecoveryOptions(inst, fence, 3)
	opts.codexFenceUnchanged = func() bool { return target.sendKeysCalls == 0 }
	res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{retry: opts})
	if err == nil || res.delivery != deliveryTypedNotSubmitted || target.guardCalls != 0 || target.sendEnterCalls != 0 || target.sendKeysCalls != 1 || target.acceptedTurns != 0 {
		t.Fatalf("changed or unreadable fence: delivery=%q err=%v bodies=%d Enters=%d guards=%d", res.delivery, err, target.sendKeysCalls, target.sendEnterCalls, target.guardCalls)
	}
}
