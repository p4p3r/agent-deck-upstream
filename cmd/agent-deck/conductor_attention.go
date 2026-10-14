package main

import (
	"context"
	"sync"

	"github.com/asheshgoplani/agent-deck/internal/session"
)

// heartbeatAttention is the gateway's source of "what is waiting for the
// conductor". It computes exactly what `conductor heartbeat-tick` computes,
// through the same shared helper, so the fleet keeps one definition of waiting
// rather than two that drift apart.
//
// The heartbeat unit that used to do this typed its summary straight into the
// conductor's pane, with no serialisation against an in-flight turn. The
// gateway takes the same text through the single writer instead.
type heartbeatAttention struct {
	profile string
	name    string
	rules   string

	mu      sync.Mutex
	pending session.HeartbeatTickState
	ready   bool
}

func (h *heartbeatAttention) Attention(context.Context) (string, error) {
	in, prev, err := heartbeatTickInputs(h.profile, h.name, h.rules)
	if err != nil {
		return "", err
	}
	msg, next := session.BuildHeartbeatTick(in, prev)
	h.mu.Lock()
	h.pending, h.ready = next, true
	h.mu.Unlock()
	if msg == "" {
		// Nothing changed. Commit anyway: the state advances so the next tick
		// compares against what is true now, exactly as the CLI does when it
		// prints nothing.
		_ = session.SaveHeartbeatTickState(h.name, next)
		h.mu.Lock()
		h.ready = false
		h.mu.Unlock()
	}
	return msg, nil
}

func (h *heartbeatAttention) Delivered(context.Context, string) error {
	h.mu.Lock()
	next, ready := h.pending, h.ready
	h.ready = false
	h.mu.Unlock()
	if !ready {
		return nil
	}
	return session.SaveHeartbeatTickState(h.name, next)
}
