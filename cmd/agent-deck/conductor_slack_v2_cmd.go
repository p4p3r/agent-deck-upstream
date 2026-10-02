package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/asheshgoplani/agent-deck/internal/channelruntime"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

var errSlackV2Configuration = errors.New("slack-v2 configuration is unavailable")

var slackV2EnvReference = regexp.MustCompile(`^\$(?:([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\})$`)

type slackV2RunArgs struct {
	name     string
	mode     channelruntime.Mode
	threadID string
}

func isConductorSlackV2Command(args []string) bool {
	return len(args) >= 2 && args[0] == "conductor" && args[1] == "slack-v2"
}

func printConductorSlackV2Help(w io.Writer) {
	fmt.Fprintln(w, "Usage: agent-deck [-p profile] conductor slack-v2 run <name>")
	fmt.Fprintln(w, "Run a conductor whose backend is explicitly set to slack-v2.")
}

func parseConductorSlackV2Run(args []string) (slackV2RunArgs, error) {
	var result slackV2RunArgs
	if len(args) < 2 || args[0] != "run" {
		return result, errors.New("invalid command")
	}
	result.name = args[1]
	if err := session.ValidateConductorName(result.name); err != nil {
		return slackV2RunArgs{}, errors.New("invalid conductor name")
	}
	if len(args) != 2 {
		return slackV2RunArgs{}, errors.New("invalid arguments")
	}
	result.mode = channelruntime.ModeRow
	return result, nil
}

// runConductorSlackV2Command is also the early main dispatch. It must not
// initialize update checks, telemetry, events, tmux, or a real user home when
// only help or argument validation is requested.
func runConductorSlackV2Command(profile string, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printConductorSlackV2Help(stdout)
		return 0
	}
	parsed, err := parseConductorSlackV2Run(args)
	if errors.Is(err, flag.ErrHelp) {
		printConductorSlackV2Help(stdout)
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "slack-v2: invalid command or bootstrap mode")
		printConductorSlackV2Help(stderr)
		return 2
	}
	// Resolving the canonical conductor state directory is necessary to name
	// its singleton lock. No credential fields are accessed until Run acquires
	// that lock and invokes LoadConfig.
	dir, err := session.ConductorNameDir(parsed.name)
	if err != nil {
		fmt.Fprintln(stderr, "slack-v2: configuration")
		return 1
	}
	req := channelruntime.Request{
		Name:         parsed.name,
		ConductorDir: dir,
		Mode:         parsed.mode,
		LoadConfig: func() (channelruntime.Config, error) {
			return loadConductorSlackV2Config(profile, parsed.name, dir, os.LookupEnv)
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := channelruntime.Run(ctx, req); err != nil {
		if ctx.Err() != nil {
			return 0
		}
		fmt.Fprintln(stderr, "slack-v2:", channelruntime.KindOf(err))
		return 1
	}
	return 0
}

// loadConductorSlackV2Config is invoked only by the runtime's LoadConfig callback,
// after it acquires the conductor lock. The lookup is injectable for offline tests.
func loadConductorSlackV2Config(profile, name, dir string, lookupEnv func(string) (string, bool)) (channelruntime.Config, error) {
	backend, err := session.ConductorBackend(name)
	if err != nil || backend != "slack-v2" {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	meta, err := session.LoadConductorMeta(name)
	if err != nil || meta.Name != name || meta.Agent != session.ConductorAgentCodex || meta.Warning != "" {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	// The storage resolver can print an inferred profile's source path on fallback.
	// An inferred mismatch therefore fails closed without that output.
	selectedProfile := session.GetEffectiveProfile(profile)
	if meta.Profile != selectedProfile {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	settings, err := session.ConductorSlackV2Config(name)
	if err != nil {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	for _, value := range []*string{&settings.AppToken, &settings.BotToken, &settings.ChannelID,
		&settings.RowInstanceID, &settings.RowBindingToken} {
		resolved, err := resolveSlackV2Value(*value, lookupEnv)
		if err != nil {
			return channelruntime.Config{}, errSlackV2Configuration
		}
		*value = resolved
	}
	if len(settings.AllowedUserIDs) == 0 {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	seen := make(map[string]bool, len(settings.AllowedUserIDs))
	for i, value := range settings.AllowedUserIDs {
		resolved, err := resolveSlackV2Value(value, lookupEnv)
		if err != nil || seen[resolved] {
			return channelruntime.Config{}, errSlackV2Configuration
		}
		seen[resolved] = true
		settings.AllowedUserIDs[i] = resolved
	}
	bindingID := selectedProfile + "/" + name
	return channelruntime.Config{
		ConversationID: bindingID + "/channel-stream",
		ConductorID:    bindingID,
		Profile:        selectedProfile,
		RowInstanceID:  settings.RowInstanceID,
		RowBinding:     settings.RowBindingToken,
		ChannelID:      settings.ChannelID,
		AllowedUserIDs: settings.AllowedUserIDs,
		AppToken:       settings.AppToken,
		BotToken:       settings.BotToken,
	}, nil
}

// resolveSlackV2Value accepts a literal or one exact environment reference.
// Resolved values are opaque: no partial expansion, recursion, or trimming.
func resolveSlackV2Value(value string, lookupEnv func(string) (string, bool)) (string, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return "", errSlackV2Configuration
	}
	if !strings.Contains(value, "$") {
		return value, nil
	}
	match := slackV2EnvReference.FindStringSubmatch(value)
	if match == nil || lookupEnv == nil {
		return "", errSlackV2Configuration
	}
	name := match[1]
	if name == "" {
		name = match[2]
	}
	resolved, ok := lookupEnv(name)
	if !ok || resolved == "" || strings.TrimSpace(resolved) != resolved {
		return "", errSlackV2Configuration
	}
	return resolved, nil
}
