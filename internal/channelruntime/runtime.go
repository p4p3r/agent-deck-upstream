// Package channelruntime owns one Agent Deck Slack v2 channel-stream runtime.
package channelruntime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile"
	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/codexdriver"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/conductorlock"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
)

type Mode string

const (
	ModeCreate Mode = "create"
	ModeResume Mode = "resume"
)

// Request carries no credential. LoadConfig is called only after the
// cross-process conductor lock has been acquired.
type Request struct {
	Name           string
	ConductorDir   string // resolved named conductor home
	Mode           Mode
	ResumeThreadID string
	LoadConfig     func() (Config, error)
}

// Config is the explicit, single-conversation binding for one run. Tokens are
// never written to the manifest or returned in an error.
type Config struct {
	ConversationID  string
	ConductorID     string
	ChannelID       string
	AllowedUserIDs  []string
	AppToken        string
	BotToken        string
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

// Error contains a fixed category only; provider details, paths, and message
// bodies are deliberately absent from its public representation.
type Error struct{ Kind Kind }

func (e *Error) Error() string { return "channelruntime: " + string(e.Kind) }

func KindOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return string(e.Kind)
	}
	return "runtime_failed"
}

type agentDriver interface {
	channelreconcile.Driver
	io.Closer
}

type dependencies struct {
	acquire  func(string, string) (io.Closer, error)
	identity func(context.Context, string) (slacknetwork.Identity, error)
	driver   func(Config) agentDriver
	socket   func(string) channelstream.Socket
	sender   func(string) slackgateway.Sender
	run      func(context.Context, *channelstream.Runner) error
}

func productionDependencies() dependencies {
	return dependencies{
		acquire: func(name, dir string) (io.Closer, error) { return conductorlock.Acquire(name, dir) },
		identity: func(ctx context.Context, token string) (slacknetwork.Identity, error) {
			return slacknetwork.NewSender(token).VerifyBotIdentity(ctx)
		},
		driver: func(c Config) agentDriver {
			return codexdriver.NewPersistent(codexdriver.Config{
				Executable: c.CodexExecutable, CWD: c.CodexCWD, Model: c.CodexModel,
			})
		},
		socket: func(token string) channelstream.Socket { return slacknetwork.NewSocketClient(token) },
		sender: func(token string) slackgateway.Sender { return slacknetwork.NewSender(token) },
		run:    func(ctx context.Context, r *channelstream.Runner) error { return r.Run(ctx) },
	}
}

// Run acquires the conductor lock before loading secrets or making any network
// or Codex call. No ingress or outbound work is enabled until both durable
// bootstrap records agree and the manifest reaches ready.
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
	if request.LoadConfig == nil || request.Mode != ModeCreate && request.Mode != ModeResume ||
		request.Mode == ModeCreate && request.ResumeThreadID != "" ||
		request.Mode == ModeResume && request.ResumeThreadID == "" {
		return &Error{KindConfig}
	}
	cfg, err := request.LoadConfig()
	if err != nil || !validConfig(cfg) || d.identity == nil || d.driver == nil || d.run == nil || d.socket == nil || d.sender == nil {
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
		if err := requireAbsent(paths.ledger); err != nil {
			return &Error{KindManifest}
		}
		manifest = newManifest(request, cfg, identity)
		if err := saveManifest(paths.manifest, manifest); err != nil {
			return &Error{KindManifest}
		}
	} else if err := manifest.match(request, cfg, identity); err != nil {
		return &Error{KindManifest}
	}
	if manifest.Phase == phaseReady || manifest.PreviousThreadID != "" {
		if err := requireRegular(paths.ledger); err != nil {
			return &Error{KindStore}
		}
	} else if err := requireAbsentOrRegular(paths.ledger); err != nil {
		return &Error{KindStore}
	}
	if err := requirePrivateSidecars(paths.ledger); err != nil {
		return &Error{KindStore}
	}
	conversation := channelgateway.Conversation{
		ID: cfg.ConversationID, ChannelID: cfg.ChannelID, ConductorID: cfg.ConductorID,
		Mode: channelgateway.ChannelStream, AllowedSenders: cfg.AllowedUserIDs,
	}
	preparedBoundCreate := false
	if manifest.Phase == phasePrepared && manifest.Intent == ModeCreate && manifest.PreviousThreadID == "" &&
		manifest.ThreadID != "" && manifest.BaselineKnown && manifest.Baseline == "" {
		_, err := os.Lstat(paths.ledger)
		preparedBoundCreate = err == nil
	}
	var store *channelgateway.Store
	var readyChain *channelgateway.CursorChain
	if manifest.Phase == phaseReady || manifest.PreviousThreadID != "" || preparedBoundCreate {
		store, err = channelgateway.Open(paths.ledger)
		if err != nil {
			return &Error{KindStore}
		}
		defer store.Close()
		binding, err := store.AgentBinding(ctx, cfg.ConversationID)
		if preparedBoundCreate && errors.Is(err, channelgateway.ErrNotFound) {
			// The ledger file may predate conversation creation. With no
			// durable binding there is no replacement proof; resume exactly.
			binding = channelgateway.Binding{}
			err = nil
			preparedBoundCreate = false
		}
		if err != nil {
			return &Error{KindStore}
		}
		if !preparedBoundCreate && manifest.Phase != phaseReady && manifest.PreviousThreadID == "" {
			// No conversation exists yet; bootstrap the known thread first.
		} else if manifest.Phase == phaseReady {
			if binding.AgentThreadID != manifest.ThreadID {
				return &Error{KindStore}
			}
		} else if preparedBoundCreate {
			if binding.AgentThreadID != manifest.ThreadID || binding.LastExternalTurnID != manifest.Baseline {
				return &Error{KindStore}
			}
		} else if binding.AgentThreadID != manifest.PreviousThreadID &&
			(manifest.ThreadID == "" || binding.AgentThreadID != manifest.ThreadID) {
			return &Error{KindStore}
		}
		if binding.AgentThreadID != "" {
			if err := store.CreateConversation(ctx, conversation); err != nil {
				return &Error{KindStore}
			}
		}
		if manifest.Phase == phaseReady {
			chain, err := store.ValidateCursorChain(ctx, cfg.ConversationID, manifest.ThreadID, manifest.Baseline)
			if err != nil {
				return &Error{KindStore}
			}
			readyChain = &chain
		}
		if binding.AgentThreadID != "" && manifest.Intent == ModeCreate && manifest.Baseline == "" {
			safe, err := store.EmptyCreateReplacementSafe(ctx, cfg.ConversationID, binding.AgentThreadID)
			if err != nil {
				return &Error{KindStore}
			}
			if manifest.PreviousThreadID != "" && !safe {
				return &Error{KindUncertain}
			}
			if safe {
				// Persist replacement intent before thread/start. A crash with
				// an unknown new ID can retry only after the same ledger proof.
				manifest.Phase = phasePrepared
				manifest.PreviousThreadID = binding.AgentThreadID
				manifest.ThreadID = ""
				manifest.Fresh = true
				if err := saveManifest(paths.manifest, manifest); err != nil {
					return &Error{KindManifest}
				}
			}
		}
	}
	// An ambiguous create can have made an external thread whose ID was lost.
	// Its prepared marker is deliberately permanent until operator repair.
	if manifest.ThreadID == "" && manifest.Intent == ModeCreate && !manifest.Fresh {
		return &Error{KindUncertain}
	}
	driver := d.driver(cfg)
	if driver == nil {
		return &Error{KindAgent}
	}
	defer driver.Close()
	if err := bootstrapAgent(ctx, driver, manifest, paths.manifest); err != nil {
		return err
	}
	if readyChain != nil {
		turns, err := driver.InspectThread(ctx, manifest.ThreadID)
		if err != nil || !readyHistoryMatches(manifest.Baseline, *readyChain, turns) {
			return &Error{KindUncertain}
		}
	}
	if store == nil {
		store, err = channelgateway.Open(paths.ledger)
		if err != nil {
			return &Error{KindStore}
		}
		defer store.Close()
	}
	if err := store.CreateConversation(ctx, conversation); err != nil {
		return &Error{KindStore}
	}
	if manifest.Phase != phaseReady {
		if manifest.PreviousThreadID != "" {
			err = store.ReplaceEmptyCreateThread(ctx, cfg.ConversationID, manifest.PreviousThreadID, manifest.ThreadID)
		} else {
			err = store.BindAgentThreadAtCursor(ctx, cfg.ConversationID, manifest.ThreadID, manifest.Baseline)
		}
		if err != nil {
			return &Error{KindStore}
		}
		manifest.Phase = phaseReady
		manifest.PreviousThreadID = ""
		if err := saveManifest(paths.manifest, manifest); err != nil {
			return &Error{KindManifest}
		}
	} else {
		binding, err := store.AgentBinding(ctx, cfg.ConversationID)
		if err != nil || binding.AgentThreadID != manifest.ThreadID {
			return &Error{KindStore}
		}
	}
	runner := &channelstream.Runner{
		Socket: d.socket(cfg.AppToken),
		Handler: slackgateway.Handler{Store: store, Config: slackgateway.Config{
			ConversationID: cfg.ConversationID, TeamID: identity.TeamID,
			ChannelID: cfg.ChannelID, BotUserID: identity.BotUserID,
			AllowedUserIDs: cfg.AllowedUserIDs,
		}},
		Worker: &channelreconcile.Worker{Store: store, Driver: driver},
		Delivery: &slackgateway.DeliveryWorker{Store: store, Sender: d.sender(cfg.BotToken),
			ConversationID: cfg.ConversationID, ChannelID: cfg.ChannelID},
	}
	if err := d.run(ctx, runner); err != nil {
		return &Error{KindRunner}
	}
	return nil
}

func validConfig(c Config) bool {
	if c.ConversationID == "" || c.ConductorID == "" || c.ChannelID == "" || c.AppToken == "" ||
		c.BotToken == "" || c.CodexExecutable == "" || c.CodexCWD == "" || len(c.AllowedUserIDs) == 0 ||
		!filepath.IsAbs(c.CodexCWD) {
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

func bootstrapAgent(ctx context.Context, driver agentDriver, m *manifest, path string) error {
	if m.Phase == phaseReady {
		if err := driver.ResumeThread(ctx, m.ThreadID); err != nil {
			return &Error{KindAgent}
		}
		return nil
	}
	if m.ThreadID == "" {
		id, err := driver.OpenThread(ctx)
		if err != nil || id == "" {
			return &Error{KindUncertain}
		}
		m.ThreadID, m.BaselineKnown = id, true
		if err := saveManifest(path, m); err != nil {
			return &Error{KindUncertain}
		}
	} else if err := driver.ResumeThread(ctx, m.ThreadID); err != nil {
		return &Error{KindAgent}
	}
	turns, err := driver.InspectThread(ctx, m.ThreadID)
	if err != nil {
		return &Error{KindAgent}
	}
	baseline := ""
	seen := make(map[string]bool, len(turns))
	for _, turn := range turns {
		if turn.ID == "" || seen[turn.ID] || !historicalTerminal(turn.Status) {
			return &Error{KindUncertain}
		}
		seen[turn.ID] = true
		baseline = turn.ID
	}
	if m.BaselineKnown && m.Baseline != baseline {
		return &Error{KindUncertain}
	}
	if m.Intent == ModeCreate && baseline != "" {
		return &Error{KindUncertain}
	}
	if !m.BaselineKnown {
		m.Baseline, m.BaselineKnown = baseline, true
		if err := saveManifest(path, m); err != nil {
			return &Error{KindManifest}
		}
	}
	return nil
}

func historicalTerminal(status string) bool {
	switch status {
	case "completed", "failed", "interrupted":
		return true
	default:
		return false
	}
}

func readyHistoryMatches(anchor string, chain channelgateway.CursorChain, turns []channelreconcile.ExternalTurn) bool {
	seen := make(map[string]bool, len(turns))
	start := 0
	if anchor != "" {
		start = -1
	}
	for i, turn := range turns {
		if turn.ID == "" || seen[turn.ID] {
			return false
		}
		seen[turn.ID] = true
		if turn.ID == anchor {
			if !historicalTerminal(turn.Status) {
				return false
			}
			start = i + 1
		}
		if start < 0 && !historicalTerminal(turn.Status) {
			return false
		}
	}
	if start < 0 || len(turns)-start < len(chain.Completed) {
		return false
	}
	for i, id := range chain.Completed {
		if turns[start+i].ID != id || turns[start+i].Status != "completed" {
			return false
		}
	}
	extra := turns[start+len(chain.Completed):]
	if chain.Active == nil || chain.Active.State == channelgateway.Unprepared {
		return len(extra) == 0
	}
	if len(extra) > 1 || len(extra) == 1 && !knownTurnStatus(extra[0].Status) {
		return false
	}
	if chain.Active.ExternalTurnID != "" {
		return len(extra) == 1 && extra[0].ID == chain.Active.ExternalTurnID
	}
	return true
}

func knownTurnStatus(status string) bool {
	return status == "inProgress" || historicalTerminal(status)
}

func canonicalUsers(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}
