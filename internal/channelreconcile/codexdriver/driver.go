// Package codexdriver adapts the Codex app-server stdio protocol to the
// provider-neutral channel reconciliation driver.
package codexdriver

import (
	"context"
	"errors"

	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/codexappserver"
)

type Config struct {
	Executable string
	CWD        string
	Model      string // Empty inherits Codex configuration.
}

// Driver uses one private thread per conversation. Each operation owns and
// closes a subprocess; methods serialize on this Driver instance.
type Driver struct {
	cfg  codexappserver.Config
	slot chan struct{}
}

func New(cfg Config) *Driver {
	return &Driver{cfg: codexappserver.Config{Executable: cfg.Executable, CWD: cfg.CWD, Model: cfg.Model}, slot: make(chan struct{}, 1)}
}

func (d *Driver) withClient(ctx context.Context, fn func(*codexappserver.Client) error) error {
	if d == nil || d.slot == nil {
		return &codexappserver.Error{Kind: codexappserver.Invalid, Op: "driver"}
	}
	select {
	case d.slot <- struct{}{}:
	case <-ctx.Done():
		return &codexappserver.Error{Kind: codexappserver.Canceled, Op: "driver"}
	}
	defer func() { <-d.slot }()
	if ctx.Err() != nil {
		return &codexappserver.Error{Kind: codexappserver.Canceled, Op: "driver"}
	}
	c, err := codexappserver.Start(ctx, d.cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	return fn(c)
}

func (d *Driver) OpenThread(ctx context.Context) (string, error) {
	var id string
	err := d.withClient(ctx, func(c *codexappserver.Client) error {
		var err error
		id, err = c.StartThread(ctx)
		return err
	})
	return id, err
}

func (d *Driver) ResumeThread(ctx context.Context, id string) error {
	return d.withClient(ctx, func(c *codexappserver.Client) error {
		_, err := c.ResumeThread(ctx, id)
		return err
	})
}

func (d *Driver) InspectThread(ctx context.Context, id string) ([]channelreconcile.ExternalTurn, error) {
	var turns []channelreconcile.ExternalTurn
	err := d.withClient(ctx, func(c *codexappserver.Client) error {
		h, err := c.ReadThread(ctx, id)
		if err != nil {
			return err
		}
		turns = make([]channelreconcile.ExternalTurn, 0, len(h.Turns))
		for _, t := range h.Turns {
			turns = append(turns, channelreconcile.ExternalTurn{ID: t.ID, Status: t.Status, Reply: t.Reply})
		}
		return nil
	})
	return turns, err
}

func (d *Driver) ReplyForTurn(ctx context.Context, threadID, turnID string) (string, error) {
	if threadID == "" || turnID == "" {
		return "", &codexappserver.Error{Kind: codexappserver.Invalid, Op: "thread/items/list"}
	}
	var reply string
	err := d.withClient(ctx, func(c *codexappserver.Client) error {
		var err error
		reply, err = c.ReadTurnReply(ctx, threadID, turnID)
		return err
	})
	return reply, err
}

func (d *Driver) StartTurn(ctx context.Context, id, prompt string, accepted func(string) error) (channelreconcile.ExternalTurn, error) {
	if accepted == nil {
		return channelreconcile.ExternalTurn{}, &codexappserver.Error{Kind: codexappserver.Invalid, Op: "turn/start"}
	}
	var ext channelreconcile.ExternalTurn
	err := d.withClient(ctx, func(c *codexappserver.Client) error {
		if _, err := c.ResumeThread(ctx, id); err != nil {
			return err
		}
		r, err := c.RunTurn(ctx, prompt, func(_, turnID string) error { return accepted(turnID) })
		if err != nil {
			var e *codexappserver.Error
			if !errors.As(err, &e) || e.Kind != codexappserver.TurnFailed && e.Kind != codexappserver.Interrupted {
				return err
			}
		}
		ext = channelreconcile.ExternalTurn{ID: r.TurnID, Status: r.Status, Reply: r.Text}
		return nil
	})
	return ext, err
}
