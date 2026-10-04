package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/send"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/asheshgoplani/agent-deck/internal/tmux"
)

// This synthetic composer swallows the initial Enter while retaining exactly
// the supplied message. Only a recovery Enter can append the next rollout turn.
// It uses no tmux pane or provider process.
type codexEnterRecoveryTarget struct {
	mockSendRetryTarget
	body            string
	initialEnters   int
	acceptInitial   bool
	acceptRetry     bool
	accept          func()
	acceptAfter     int32
	acceptedTurns   int
	staleComposer   bool
	baselineFrame   string
	baselineErr     error
	postFrame       string
	postErr         error
	onStage         func()
	onCapture       func()
	snapshotAdjust  func(*tmux.PaneSnapshot)
	snapshotErr     error
	guardErr        error
	guardCalls      int
	attachedClients int
	acceptCaptures  int
	acceptPending   bool
}

func (m *codexEnterRecoveryTarget) CapturePaneFresh() (string, error) {
	if m.initialEnters == 0 {
		if m.baselineFrame != "" || m.baselineErr != nil {
			return m.baselineFrame, m.baselineErr
		}
	}
	raw := "• Previous turn complete\n\n› " + strings.ReplaceAll(m.body, "\n", "\n  ") + "\n\n  ? for shortcuts\n"
	if m.initialEnters > 0 {
		if m.acceptPending {
			m.acceptCaptures--
			if m.acceptCaptures == 0 {
				m.acceptPending = false
				m.acceptTurn()
			}
		}
		if m.onCapture != nil {
			m.onCapture()
		}
		if m.postFrame != "" || m.postErr != nil {
			return m.postFrame, m.postErr
		}
	}
	return raw, nil
}

const codexFixtureHeight = 24

// Synthetic rendering keeps the footer at a fixed absolute row. Explicit
// postFrame captures retain their supplied extent, including partial captures.
func codexViewportFrame(raw string) string {
	rows := strings.Count(raw, "\n")
	if !strings.HasSuffix(raw, "\n") {
		rows++
		raw += "\n"
	}
	if rows < codexFixtureHeight {
		raw = strings.Repeat("\n", codexFixtureHeight-rows) + raw
	}
	return raw
}

func (m *codexEnterRecoveryTarget) CapturePaneSnapshot() (tmux.PaneSnapshot, error) {
	raw, err := m.CapturePaneFresh()
	if err != nil {
		return tmux.PaneSnapshot{}, err
	}
	if m.initialEnters == 0 || m.postFrame == "" {
		raw = codexViewportFrame(raw)
	}
	cursorY := 0
	for i, row := range strings.Split(raw, "\n") {
		if strings.HasPrefix(row, "›") {
			cursorY = i
		}
	}
	if m.initialEnters > 0 {
		cursorY += strings.Count(m.body, "\n")
	}
	snapshot := tmux.PaneSnapshot{Raw: raw, Geometry: tmux.PaneGeometry{
		PaneID: "%7", SessionID: "$3", ServerPID: 12345, AttachedClients: m.attachedClients, Width: 100, Height: codexFixtureHeight, CursorX: 2, CursorY: cursorY,
	}}
	if m.snapshotAdjust != nil {
		m.snapshotAdjust(&snapshot)
	}
	return snapshot, m.snapshotErr
}

func (m *codexEnterRecoveryTarget) SendKeysAndEnter(message string) error {
	atomic.AddInt32(&m.sendKeysCalls, 1)
	m.body = message
	m.initialEnters++
	if m.onStage != nil {
		m.onStage()
	}
	if m.acceptInitial {
		m.acceptTurn()
	}
	return nil
}

func (m *codexEnterRecoveryTarget) SendEnter() error {
	atomic.AddInt32(&m.sendEnterCalls, 1)
	if m.acceptRetry && m.sendEnterCalls >= m.acceptAfter {
		if m.acceptCaptures > 0 {
			m.acceptPending = true
		} else {
			m.acceptTurn()
		}
	}
	return nil
}

func (m *codexEnterRecoveryTarget) SendEnterIfStable(_ tmux.PaneGeometry) error {
	m.guardCalls++
	if m.guardErr != nil {
		return m.guardErr
	}
	return m.SendEnter()
}

func (m *codexEnterRecoveryTarget) acceptTurn() {
	m.acceptedTurns++
	m.accept()
	if !m.staleComposer {
		m.body = ""
	}
}

func codexRecoveryFence(t *testing.T) (*session.Instance, codexAcceptanceFence, func()) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	const thread = "01a0cc41-3ddb-7170-98bc-27b021e69141"
	path := filepath.Join(home, "sessions", "2026", "09", "23", "rollout-2026-09-23T05-13-42-"+thread+".jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	prior := `{"type":"event_msg","payload":{"type":"task_started","turn_id":"prior-turn"}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"prior-turn"}}` + "\n"
	if err := os.WriteFile(path, []byte(prior), 0o600); err != nil {
		t.Fatal(err)
	}
	inst := &session.Instance{ID: "enter-recovery", Tool: "codex", CodexSessionID: thread}
	fence := captureCodexAcceptanceFence(inst)
	if !fence.available || codexTurnAdvancedPastFence(inst, fence) {
		t.Fatalf("invalid pre-send fence: %#v", fence)
	}
	accept := func() {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.WriteString(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"recovery-turn"}}` + "\n")
		if closeErr := f.Close(); writeErr != nil || closeErr != nil {
			t.Fatalf("append accepted turn: %v %v", writeErr, closeErr)
		}
	}
	return inst, fence, accept
}

func codexRecoveryOptions(inst *session.Instance, fence codexAcceptanceFence, checks int) sendRetryOptions {
	return sendRetryOptions{
		tool: "codex", maxRetries: checks, verifyDelivery: true,
		turnAdvanced:        func() bool { return codexTurnAdvancedPastFence(inst, fence) },
		codexFenceUnchanged: func() bool { return validateCodexAcceptanceFence(inst, fence) == nil },
	}
}

func (m *codexEnterRecoveryTarget) SendKeysAndEnterIfStable(message string, g tmux.PaneGeometry) (bool, error) {
	current, err := m.CapturePaneSnapshot()
	if err != nil || current.Geometry != g || g.AttachedClients < 0 {
		return false, errors.New("target changed")
	}
	return true, m.SendKeysAndEnter(message)
}

func TestCodexAdmission_PreTransportRefusalsTypeNothing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*codexEnterRecoveryTarget)
	}{
		{"snapshot error", func(target *codexEnterRecoveryTarget) { target.snapshotErr = errors.New("snapshot unavailable") }},
		{"indeterminate identity", func(target *codexEnterRecoveryTarget) {
			target.snapshotAdjust = func(snapshot *tmux.PaneSnapshot) { snapshot.Geometry.SessionID = "" }
		}},
		{"changed target", func(target *codexEnterRecoveryTarget) {
			captures := 0
			target.snapshotAdjust = func(snapshot *tmux.PaneSnapshot) {
				captures++
				if captures == 2 {
					snapshot.Geometry.PaneID = "%8"
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inst, fence, accept := codexRecoveryFence(t)
			target := &codexEnterRecoveryTarget{acceptRetry: true, accept: accept}
			target.statuses = []string{"waiting"}
			tc.setup(target)

			res, err := executeSend(target, "codex", "Inspect the synthetic admission fixture", false, sendExecTuning{
				retry: codexRecoveryOptions(inst, fence, 2),
			})
			if err == nil || res.delivery != deliveryComposerBlocked || target.sendKeysCalls != 0 ||
				target.sendEnterCalls != 0 || target.guardCalls != 0 || target.acceptedTurns != 0 {
				t.Fatalf("pre-transport refusal mutated target: delivery=%q err=%v target=%#v", res.delivery, err, target)
			}
		})
	}
}

func TestCodexEnterRecovery_SerializedGuardRefusal(t *testing.T) {
	inst, fence, advance := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: advance, guardErr: errors.New("target changed")}
	target.statuses = []string{"waiting"}
	res, err := executeSend(target, "codex", "Please inspect the synthetic fixture", false, sendExecTuning{retry: codexRecoveryOptions(inst, fence, 3)})
	if err == nil || res.delivery != deliveryTypedNotSubmitted || target.sendEnterCalls != 0 || target.acceptedTurns != 0 || codexTurnAdvancedPastFence(inst, fence) {
		t.Fatalf("guard refusal must withhold Enter: delivery=%q err=%v Enters=%d accepted=%d", res.delivery, err, target.sendEnterCalls, target.acceptedTurns)
	}
	if target.guardCalls != 1 || target.sendKeysCalls != 1 {
		t.Fatalf("refusal must latch without body resend: guards=%d bodies=%d", target.guardCalls, target.sendKeysCalls)
	}
}

func TestCodexEnterRecovery_AttemptsAtMostOnce(t *testing.T) {
	inst, fence, accept := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{accept: accept}
	target.statuses = []string{"waiting"}
	res, err := executeSend(target, "codex", "Please inspect the synthetic fixture", false, sendExecTuning{
		retry: codexRecoveryOptions(inst, fence, 5),
	})
	if err == nil || res.delivery != deliveryTypedNotSubmitted || target.sendKeysCalls != 1 ||
		target.guardCalls != 1 || target.sendEnterCalls != 1 || target.acceptedTurns != 0 {
		t.Fatalf("recovery budget exceeded one Enter: delivery=%q err=%v target=%#v", res.delivery, err, target)
	}
}

func TestCodexEnterRecovery_WaitsForPasteBurstCooldown(t *testing.T) {
	inst, fence, accept := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: accept}
	target.statuses = []string{"waiting"}
	opts := codexRecoveryOptions(inst, fence, 4)
	opts.codexRecoveryWait = time.Hour
	res, err := executeSend(target, "codex", "Please inspect the synthetic fixture", false, sendExecTuning{retry: opts})
	if err == nil || res.delivery != deliveryTypedNotSubmitted || target.sendKeysCalls != 1 ||
		target.guardCalls != 0 || target.sendEnterCalls != 0 || target.acceptedTurns != 0 {
		t.Fatalf("recovery fired inside cooldown: delivery=%q err=%v target=%#v", res.delivery, err, target)
	}
}

func TestCodexEnterRecovery_SwallowedInitialEnter(t *testing.T) {
	inst, fence, accept := codexRecoveryFence(t)
	const message = "Reply with exactly: recovered-pong"
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: accept}
	target.statuses = []string{"waiting"}
	res, err := executeSend(target, "codex", message, false, sendExecTuning{
		retry: codexRecoveryOptions(inst, fence, 4),
	})
	if target.initialEnters != 1 || atomic.LoadInt32(&target.sendKeysCalls) != 1 {
		t.Fatalf("expected exactly one initial body+Enter, got %d", target.initialEnters)
	}
	if err != nil || res.delivery != deliverySubmitted {
		t.Fatalf("swallowed initial Enter: delivery=%q err=%v; recovery Enters=%d; composer=%q; turn advanced=%v",
			res.delivery, err, target.sendEnterCalls, target.body, codexTurnAdvancedPastFence(inst, fence))
	}
	if target.sendEnterCalls != 1 || target.sendCtrlCCalls != 0 || target.sendChunkedCalls != 0 {
		t.Fatalf("recovery must send exactly one bare Enter: %#v", target)
	}
	if !codexTurnAdvancedPastFence(inst, fence) || target.body != "" {
		t.Fatal("recovery must consume the exact composer and advance the rollout turn")
	}
	if receipt := waitForAcceptedCodexTurn(inst, res.delivery, time.Now(), fence); receipt == nil || receipt.TurnGeneration != inst.CodexSessionID+":recovery-turn" {
		t.Fatalf("recovery must produce the fenced accepted-turn receipt: %#v", receipt)
	}
}

func TestCodexEnterRecovery_QueuedWorkerKeepsPostRecoveryWindow(t *testing.T) {
	inst, fence, accept := codexRecoveryFence(t)
	const message = "ok"
	target := &codexEnterRecoveryTarget{
		acceptRetry: true, accept: accept, acceptCaptures: 4,
	}
	target.statuses = []string{"waiting"}
	tun := sessionSendTuning(false, true, "codex")
	tun.retry.turnAdvanced = func() bool { return codexTurnAdvancedPastFence(inst, fence) }
	tun.retry.codexFenceUnchanged = func() bool { return validateCodexAcceptanceFence(inst, fence) == nil }
	window := time.Duration(tun.retry.verificationChecks(false)) * tun.retry.checkDelay
	needed := tun.retry.codexRecoveryWait + time.Duration(target.acceptCaptures)*tun.retry.checkDelay
	if window <= needed {
		t.Fatalf("queued verification window %v cannot observe recovery after %v", window, needed)
	}

	res, err := executeSend(target, "codex", message, false, tun)
	if err != nil || res.delivery != deliverySubmitted {
		t.Fatalf("queued short send: delivery=%q err=%v target=%#v", res.delivery, err, target)
	}
	if target.sendKeysCalls != 1 || target.guardCalls != 1 || target.sendEnterCalls != 1 || target.acceptedTurns != 1 {
		t.Fatalf("queued recovery duplicated transport: bodies=%d guards=%d Enters=%d accepted=%d",
			target.sendKeysCalls, target.guardCalls, target.sendEnterCalls, target.acceptedTurns)
	}
}

func TestCodexEnterRecovery_ExactVisibleMessages(t *testing.T) {
	for _, message := range []string{
		"ok",
		"Hi\n\nPlease inspect the synthetic fixture.",
		"Please inspect the synthetic\nfixture.",
		"Please inspect the synthetic fixture:\n  keep the existing guard\n\nThen report its result.",
	} {
		t.Run(message, func(t *testing.T) {
			inst, fence, accept := codexRecoveryFence(t)
			target := &codexEnterRecoveryTarget{acceptRetry: true, accept: accept}
			target.statuses = []string{"waiting"}
			res, err := executeSend(target, "codex", message, false, sendExecTuning{
				retry: codexRecoveryOptions(inst, fence, 4),
			})
			if err != nil || res.delivery != deliverySubmitted || target.sendEnterCalls != 1 || target.acceptedTurns != 1 {
				t.Fatalf("exact visible message: delivery=%q err=%v recovery Enters=%d accepted turns=%d", res.delivery, err, target.sendEnterCalls, target.acceptedTurns)
			}
		})
	}
}

func TestCodexEnterRecovery_IdleFooterLayouts(t *testing.T) {
	const message = "Please inspect the synthetic fixture"
	for _, footer := range []string{
		"? for shortcuts · 90% context left",
		"gpt-test default · /work/fixture       ⚠ 1 warning · f2 to view",
		"gpt-test · /work/fixture · Context 90% left · Context 10% used",
	} {
		t.Run(footer, func(t *testing.T) {
			inst, fence, accept := codexRecoveryFence(t)
			target := &codexEnterRecoveryTarget{acceptRetry: true, accept: accept}
			target.statuses = []string{"waiting"}
			// The earlier identical submitted message is history; the native
			// placeholder marks the clear composer owned by this operation.
			target.baselineFrame = "› " + message + "\n\n• Previous turn complete\n\n› Ask Codex to do anything\n\n  " + footer + "\n"
			target.onStage = func() { target.postFrame = codexViewportFrame("› " + message + "\n\n  " + footer + "\n") }
			res, err := executeSend(target, "codex", message, false, sendExecTuning{retry: codexRecoveryOptions(inst, fence, 3)})
			if err != nil || res.delivery != deliverySubmitted || target.sendEnterCalls != 1 || target.acceptedTurns != 1 {
				t.Fatalf("native idle layout: delivery=%q err=%v Enters=%d accepted turns=%d", res.delivery, err, target.sendEnterCalls, target.acceptedTurns)
			}
		})
	}
}

func TestCodexEnterRecovery_AlreadyAccepted(t *testing.T) {
	for _, duringCapture := range []bool{false, true} {
		t.Run(fmt.Sprint(duringCapture), func(t *testing.T) {
			inst, fence, accept := codexRecoveryFence(t)
			target := &codexEnterRecoveryTarget{acceptInitial: !duringCapture, acceptRetry: true, staleComposer: true, accept: accept}
			target.statuses = []string{"waiting"}
			if duringCapture {
				target.onCapture = func() {
					target.acceptTurn()
					target.onCapture = nil
				}
			}
			res, err := executeSend(target, "codex", "Reply with exactly: accepted-pong", false, sendExecTuning{
				retry: codexRecoveryOptions(inst, fence, 3),
			})
			if err != nil || res.delivery != deliverySubmitted || target.sendEnterCalls != 0 || target.acceptedTurns != 1 || target.sendKeysCalls != 1 {
				t.Fatalf("already accepted turn must receive no retry, even with a stale composer: delivery=%q err=%v target=%#v", res.delivery, err, target)
			}
		})
	}
}

func TestCodexEnterRecovery_ForeignFooterLookalikeBeforeAnchoredFooter(t *testing.T) {
	const message = "Please inspect the synthetic fixture"
	for _, continuation := range []string{"? for shortcuts", "gpt-test · Context 90% left"} {
		t.Run(continuation, func(t *testing.T) {
			inst, fence, accept := codexRecoveryFence(t)
			target := &codexEnterRecoveryTarget{acceptRetry: true, accept: accept}
			target.statuses = []string{"waiting"}
			target.onStage = func() { target.body += "\n\n" + continuation }
			res, err := executeSend(target, "codex", message, false, sendExecTuning{retry: codexRecoveryOptions(inst, fence, 3)})
			if err == nil || res.delivery != deliveryTypedNotSubmitted || target.sendEnterCalls != 0 || target.acceptedTurns != 0 {
				t.Fatalf("foreign continuation before the real footer: delivery=%q err=%v Enters=%d accepted turns=%d", res.delivery, err, target.sendEnterCalls, target.acceptedTurns)
			}
		})
	}
}

func TestCodexEnterRecovery_ExactCopiedFooterPartialCapture(t *testing.T) {
	const message = "Please inspect the synthetic fixture"
	const model = "gpt-test · /work/fixture · Context 90% left · Context 10% used"
	const shortcuts = "? for shortcuts · Warning: experimental features enabled"
	const footer = "\n\n  " + model + "\n  " + shortcuts + "\n"
	inst, fence, advance := codexRecoveryFence(t)
	target := &codexEnterRecoveryTarget{acceptRetry: true, accept: advance}
	target.statuses = []string{"waiting"}
	target.baselineFrame = "• Previous turn complete\n\n› Ask Codex to do anything" + footer
	snapshot, captureErr := target.CapturePaneSnapshot()
	if _, clear := send.CaptureClearCodexComposerFrame(send.PaneCapture{Raw: snapshot.Raw, OK: captureErr == nil, Geometry: &snapshot.Geometry}); !clear {
		t.Fatal("copied-footer fixture must begin with a structurally proven clear composer")
	}
	target.onStage = func() {
		target.body = message + "\n\n" + model + "\n" + shortcuts
		target.postFrame = "› " + message + footer
	}
	res, err := executeSend(target, "codex", message, false, sendExecTuning{retry: codexRecoveryOptions(inst, fence, 1)})
	if target.sendEnterCalls != 0 || target.acceptedTurns != 0 || codexTurnAdvancedPastFence(inst, fence) || err == nil || res.delivery != deliveryTypedNotSubmitted {
		t.Fatalf("copied footer in partial capture: Enters=%d accepted=%d advanced=%v delivery=%q err=%v", target.sendEnterCalls, target.acceptedTurns, codexTurnAdvancedPastFence(inst, fence), res.delivery, err)
	}
}

// An embedded interface deliberately exposes only the historical text capture.
type codexTextOnlyTarget struct{ sendRetryTarget }
