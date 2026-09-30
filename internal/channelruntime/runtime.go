// Package channelruntime owns one Agent Deck Slack v2 channel-stream runtime.
package channelruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/rowdriver"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/conductorlock"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
)

type Mode string

const (
	ModeRow    Mode = "row"
	ModeCreate Mode = "create" // compatibility-only; runtime rejects it
	ModeResume Mode = "resume" // compatibility-only; runtime rejects it
)

// Request carries no credential. LoadConfig is called only after the
// cross-process conductor lock has been acquired.
type Request struct {
	Name           string
	ConductorDir   string
	Mode           Mode
	ResumeThreadID string
	LoadConfig     func() (Config, error)
}

// Config binds one Slack route to one immutable Agent Deck row. Tokens are
// never written to the manifest or returned in an error.
type Config struct {
	ConversationID string
	ConductorID    string
	Profile        string
	RowInstanceID  string
	RowBinding     string
	ChannelID      string
	AllowedUserIDs []string
	AppToken       string
	BotToken       string

	// These fields keep older isolated fixtures source-compatible. A nonempty
	// value is rejected and never enters the runtime manifest or driver.
	CodexExecutable string
	CodexCWD        string
	CodexModel      string
}

type Kind string

const (
	KindConfig    Kind = "config"
	KindLease     Kind = "lease"
	KindIdentity  Kind = "identity"
	KindManifest  Kind = "manifest"
	KindStore     Kind = "store"
	KindAgent     Kind = "agent"
	KindUncertain Kind = "uncertain"
	KindRunner    Kind = "runner"
)

type Error struct{ Kind Kind }

func (e *Error) Error() string { return "channelruntime: " + string(e.Kind) }

func KindOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return string(e.Kind)
	}
	return "runtime_failed"
}

// agentDriver keeps isolated legacy fixtures source-compatible. Production
// dependencies never construct or call values stored in this seam.
type agentDriver = any

type rowOperationDriver interface {
	channelreconcile.RowDriver
	io.Closer
}

type dependencies struct {
	acquire   func(string, string) (io.Closer, error)
	identity  func(context.Context, string) (slacknetwork.Identity, error)
	rowDriver func(Config) (rowOperationDriver, error)
	socket    func(string) channelstream.Socket
	sender    func(string) slackgateway.Sender
	run       func(context.Context, *channelstream.Runner) error

	// Compatibility-only seam ignored by run.
	driver func(Config) agentDriver
}

func productionDependencies() dependencies {
	return dependencies{
		acquire: func(name, dir string) (io.Closer, error) { return conductorlock.Acquire(name, dir) },
		identity: func(ctx context.Context, token string) (slacknetwork.Identity, error) {
			return slacknetwork.NewSender(token).VerifyBotIdentity(ctx)
		},
		rowDriver: func(c Config) (rowOperationDriver, error) {
			executable, err := os.Executable()
			if err != nil {
				return nil, err
			}
			return rowdriver.New(rowdriver.Config{
				Executable: executable, Profile: c.Profile,
				SessionID: c.RowInstanceID, RowBinding: c.RowBinding,
			})
		},
		socket: func(token string) channelstream.Socket { return slacknetwork.NewSocketClient(token) },
		sender: func(token string) slackgateway.Sender { return slacknetwork.NewSender(token) },
		run:    func(ctx context.Context, r *channelstream.Runner) error { return r.Run(ctx) },
	}
}

func Run(ctx context.Context, request Request) error {
	return run(ctx, request, productionDependencies())
}

func run(ctx context.Context, request Request, d dependencies) error {
	if ctx == nil || request.Name == "" || request.ConductorDir == "" || d.acquire == nil {
		return &Error{KindConfig}
	}
	lease, err := d.acquire(request.Name, request.ConductorDir)
	if err != nil {
		return &Error{KindLease}
	}
	defer lease.Close()
	if request.LoadConfig == nil || request.Mode != "" && request.Mode != ModeRow || request.ResumeThreadID != "" {
		return &Error{KindConfig}
	}
	cfg, err := request.LoadConfig()
	if err != nil || !validConfig(cfg) || d.identity == nil || d.rowDriver == nil ||
		d.run == nil || d.socket == nil || d.sender == nil {
		return &Error{KindConfig}
	}
	identity, err := d.identity(ctx, cfg.BotToken)
	if err != nil || identity.TeamID == "" || identity.BotUserID == "" {
		return &Error{KindIdentity}
	}
	for _, id := range cfg.AllowedUserIDs {
		if id == identity.BotUserID {
			return &Error{KindConfig}
		}
	}
	paths := pathsFor(request.ConductorDir)
	if err := ensurePrivateDir(filepath.Dir(paths.manifest)); err != nil {
		return &Error{KindManifest}
	}
	manifest, err := loadManifest(paths.manifest)
	if err != nil {
		return &Error{KindManifest}
	}
	if manifest == nil {
		manifest = newManifest(request, cfg, identity)
		if err := saveManifest(paths.manifest, manifest); err != nil {
			return &Error{KindManifest}
		}
	} else if err := manifest.match(request, cfg, identity); err != nil {
		return &Error{KindManifest}
	}
	if err := requireAbsentOrRegular(paths.ledger); err != nil {
		return &Error{KindStore}
	}
	if err := requirePrivateSidecars(paths.ledger); err != nil {
		return &Error{KindStore}
	}
	store, err := channelgateway.Open(paths.ledger)
	if err != nil {
		return &Error{KindStore}
	}
	defer store.Close()
	conversation := channelgateway.Conversation{
		ID: cfg.ConversationID, ChannelID: cfg.ChannelID, ConductorID: cfg.ConductorID,
		RowInstanceID: cfg.RowInstanceID, RowBinding: cfg.RowBinding,
		Mode: channelgateway.ChannelStream, AllowedSenders: cfg.AllowedUserIDs,
	}
	if err := store.CreateConversation(ctx, conversation); err != nil {
		return &Error{KindStore}
	}
	driver, err := d.rowDriver(cfg)
	if err != nil || driver == nil {
		return &Error{KindAgent}
	}
	defer driver.Close()
	runner := &channelstream.Runner{
		Socket: d.socket(cfg.AppToken),
		Handler: slackgateway.Handler{Store: store, Config: slackgateway.Config{
			ConversationID: cfg.ConversationID, TeamID: identity.TeamID,
			ChannelID: cfg.ChannelID, BotUserID: identity.BotUserID,
			AllowedUserIDs: cfg.AllowedUserIDs,
		}},
		Worker: &channelreconcile.Worker{Store: store, RowDriver: driver},
		Delivery: &slackgateway.DeliveryWorker{Store: store, Sender: d.sender(cfg.BotToken),
			ConversationID: cfg.ConversationID, ChannelID: cfg.ChannelID},
	}
	if err := d.run(ctx, runner); err != nil {
		return &Error{KindRunner}
	}
	return nil
}

func validConfig(c Config) bool {
	if c.ConversationID == "" || c.ConductorID == "" || c.Profile == "" || c.RowInstanceID == "" ||
		c.RowBinding == "" || c.ChannelID == "" || c.AppToken == "" || c.BotToken == "" ||
		len(c.AllowedUserIDs) == 0 || c.CodexExecutable != "" || c.CodexCWD != "" || c.CodexModel != "" {
		return false
	}
	seen := make(map[string]bool, len(c.AllowedUserIDs))
	for _, id := range c.AllowedUserIDs {
		if id == "" || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
