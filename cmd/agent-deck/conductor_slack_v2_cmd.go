package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/asheshgoplani/agent-deck/internal/channelruntime"
	"github.com/asheshgoplani/agent-deck/internal/channelspool"
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
		return 78
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
		return slackV2FailureExit(err, errors.Is(ctx.Err(), context.Canceled), stderr)
	}
	return 0
}

func slackV2FailureExit(err error, canceled bool, stderr io.Writer) int {
	if canceled {
		return 0
	}
	cause, terminal := channelruntime.Classification(err)
	fmt.Fprintln(stderr, "slack-v2:", channelruntime.KindOf(err)+":"+cause)
	if terminal {
		return 78
	}
	return 75
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
	for _, value := range []*string{&settings.AppToken, &settings.BotToken, &settings.AppID, &settings.TeamID, &settings.ChannelID,
		&settings.RowInstanceID, &settings.RowBindingToken} {
		resolved, err := resolveSlackV2Value(*value, lookupEnv)
		if err != nil {
			return channelruntime.Config{}, errSlackV2Configuration
		}
		*value = resolved
	}
	if settings.ControlSocket != "" {
		resolved, err := resolveSlackV2Value(settings.ControlSocket, lookupEnv)
		if err != nil || !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved || strings.ContainsRune(resolved, '\x00') {
			return channelruntime.Config{}, errSlackV2Configuration
		}
		settings.ControlSocket = resolved
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
	key, err := channelspool.KeyFromEnv()
	if err != nil {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	retention, err := slackV2RetentionPolicy(settings)
	if err != nil {
		return channelruntime.Config{}, errSlackV2Configuration
	}
	config := channelruntime.Config{
		ConversationID: channelspool.AliasFromKey(key, "conversation", bindingID+"/channel-stream"),
		ConductorID:    channelspool.AliasFromKey(key, "conductor", bindingID),
		Profile:        selectedProfile,
		RowInstanceID:  settings.RowInstanceID,
		RowBinding:     settings.RowBindingToken,
		ChannelID:      settings.ChannelID,
		AppID:          settings.AppID,
		TeamID:         settings.TeamID,
		AllowedUserIDs: settings.AllowedUserIDs,
		AppToken:       settings.AppToken,
		BotToken:       settings.BotToken,
		SpoolKey:       key,
		Retention:      retention,
		ControlSocket:  settings.ControlSocket,
	}
	if config.ControlSocket != "" {
		config.BinarySHA256, err = runningBinarySHA256()
		if err != nil {
			return channelruntime.Config{}, errSlackV2Configuration
		}
		config.RowBindingAlias = channelspool.AliasFromKey(key, "rowbinding", config.RowBinding)
		config.ConfigSHA256, err = effectiveSlackV2ConfigSHA256(config)
		if err != nil {
			return channelruntime.Config{}, errSlackV2Configuration
		}
	}
	return config, nil
}

func effectiveSlackV2ConfigSHA256(config channelruntime.Config) (string, error) {
	users := slices.Clone(config.AllowedUserIDs)
	slices.Sort(users)
	delivered, uncertain, metadata := config.Retention.DeliveredContent, config.Retention.UncertainContent, config.Retention.Metadata
	if delivered == 0 {
		delivered = 24 * time.Hour
	}
	if uncertain == 0 {
		uncertain = 7 * 24 * time.Hour
	}
	if metadata == 0 {
		metadata = 90 * 24 * time.Hour
	}
	payload := struct {
		Version          int      `json:"version"`
		ConversationID   string   `json:"conversation_id"`
		ConductorID      string   `json:"conductor_id"`
		Profile          string   `json:"profile"`
		RowInstanceID    string   `json:"row_instance_id"`
		RowBinding       string   `json:"row_binding"`
		AppID            string   `json:"app_id"`
		TeamID           string   `json:"team_id"`
		ChannelID        string   `json:"channel_id"`
		AllowedUserIDs   []string `json:"allowed_user_ids"`
		AppToken         string   `json:"app_token"`
		BotToken         string   `json:"bot_token"`
		SpoolKey         []byte   `json:"spool_key"`
		DeliveredSeconds int64    `json:"retention_delivered_seconds"`
		UncertainSeconds int64    `json:"retention_uncertain_seconds"`
		MetadataSeconds  int64    `json:"retention_metadata_seconds"`
		ControlSocket    string   `json:"control_socket"`
	}{
		Version: 1, ConversationID: config.ConversationID, ConductorID: config.ConductorID, Profile: config.Profile,
		RowInstanceID: config.RowInstanceID, RowBinding: config.RowBinding, AppID: config.AppID, TeamID: config.TeamID,
		ChannelID: config.ChannelID, AllowedUserIDs: users, AppToken: config.AppToken, BotToken: config.BotToken,
		SpoolKey: config.SpoolKey, DeliveredSeconds: int64(delivered / time.Second), UncertainSeconds: int64(uncertain / time.Second),
		MetadataSeconds: int64(metadata / time.Second), ControlSocket: config.ControlSocket,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("agent-deck/slack-v2/effective-config/v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func slackV2RetentionPolicy(settings session.SlackV2ConductorConfig) (channelgateway.RetentionPolicy, error) {
	const maxHours = int64((1<<63 - 1) / int64(time.Hour))
	hours := []int64{settings.RetentionDeliveredHours, settings.RetentionUncertainHours, settings.RetentionMetadataHours}
	for _, n := range hours {
		if n < 0 || n > maxHours {
			return channelgateway.RetentionPolicy{}, errSlackV2Configuration
		}
	}
	policy := channelgateway.RetentionPolicy{
		DeliveredContent: time.Duration(hours[0]) * time.Hour,
		UncertainContent: time.Duration(hours[1]) * time.Hour,
		Metadata:         time.Duration(hours[2]) * time.Hour,
	}
	if !policy.Valid() {
		return channelgateway.RetentionPolicy{}, errSlackV2Configuration
	}
	return policy, nil
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
