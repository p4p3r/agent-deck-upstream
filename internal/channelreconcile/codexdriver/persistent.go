package codexdriver

import (
	"context"
	"errors"
	"sync"

	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/codexappserver"
)

// Persistent owns one app-server process and one private thread for a runtime.
// A failed process is never silently replaced: the ledger must reconcile any
// possibly submitted turn before a different runtime is allowed to start.
type Persistent struct {
	cfg      codexappserver.Config
	mu       sync.Mutex
	client   *codexappserver.Client
	threadID string
	closed   bool
}

func NewPersistent(cfg Config) *Persistent {
	return &Persistent{cfg: codexappserver.Config{
		Executable: cfg.Executable, CWD: cfg.CWD, Model: cfg.Model,
	}}
}

func (d *Persistent) connect(ctx context.Context) error {
	if d.closed || d.client != nil {
		return &codexappserver.Error{Kind: codexappserver.Invalid, Op: "driver"}
	}
	c, err := codexappserver.Start(ctx, d.cfg)
	if err != nil {
		return err
	}
	d.client = c
	return nil
}

func (d *Persistent) OpenThread(ctx context.Context) (string, error) {
	if d == nil {
		return "", &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/start"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.threadID != "" {
		return "", &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/start"}
	}
	if err := d.connect(ctx); err != nil {
		return "", err
	}
	id, err := d.client.StartThread(ctx)
	if err != nil {
		return "", err
	}
	d.threadID = id
	return id, nil
}

func (d *Persistent) ResumeThread(ctx context.Context, id string) error {
	if d == nil || id == "" {
		return &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/resume"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.threadID != "" && d.threadID != id {
		return &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/resume"}
	}
	if d.threadID == id {
		return nil
	}
	if err := d.connect(ctx); err != nil {
		return err
	}
	got, err := d.client.ResumeThread(ctx, id)
	if err != nil {
		return err
	}
	if got != id {
		return &codexappserver.Error{Kind: codexappserver.Protocol, Op: "thread/resume"}
	}
	d.threadID = id
	return nil
}

func (d *Persistent) InspectThread(ctx context.Context, id string) ([]channelreconcile.ExternalTurn, error) {
	if d == nil || id == "" {
		return nil, &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/read"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.client == nil || d.threadID != id {
		return nil, &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/read"}
	}
	h, err := d.client.ReadThread(ctx, id)
	if err != nil {
		return nil, err
	}
	turns := make([]channelreconcile.ExternalTurn, 0, len(h.Turns))
	for _, t := range h.Turns {
		turns = append(turns, channelreconcile.ExternalTurn{ID: t.ID, Status: t.Status, Reply: t.Reply})
	}
	return turns, nil
}

func (d *Persistent) ReplyForTurn(ctx context.Context, threadID, turnID string) (string, error) {
	if d == nil || threadID == "" || turnID == "" {
		return "", &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/items/list"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.client == nil || d.threadID != threadID {
		return "", &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/items/list"}
	}
	return d.client.ReadTurnReply(ctx, threadID, turnID)
}

func (d *Persistent) StartTurn(ctx context.Context, id, prompt string, accepted func(string) error) (channelreconcile.ExternalTurn, error) {
	if d == nil || id == "" || accepted == nil {
		return channelreconcile.ExternalTurn{}, &codexappserver.Error{Kind: codexappserver.Invalid, Op: "turn/start"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.client == nil || d.threadID != id {
		return channelreconcile.ExternalTurn{}, &codexappserver.Error{Kind: codexappserver.Invalid, Op: "turn/start"}
	}
	r, err := d.client.RunTurn(ctx, prompt, func(threadID, turnID string) error {
		if threadID != id {
			return &codexappserver.Error{Kind: codexappserver.Protocol, Op: "turn/start"}
		}
		return accepted(turnID)
	})
	if err != nil {
		var e *codexappserver.Error
		if !errors.As(err, &e) || e.Kind != codexappserver.TurnFailed && e.Kind != codexappserver.Interrupted {
			return channelreconcile.ExternalTurn{}, err
		}
	}
	return channelreconcile.ExternalTurn{ID: r.TurnID, Status: r.Status, Reply: r.Text}, nil
}

// Close kills and reaps the process. It is safe to call more than once.
func (d *Persistent) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
