package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelruntime"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

func TestConductorSlackV2EarlyDispatch(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{[]string{"conductor", "slack-v2", "run", "sample"}, true},
		{[]string{"conductor", "slack-v2", "--help"}, true},
		{[]string{"conductor", "status"}, false},
		{[]string{"session", "start"}, false},
		{[]string{"conductor"}, false},
	} {
		if got := isConductorSlackV2Command(tc.args); got != tc.want {
			t.Errorf("early dispatch for %v = %v, want %v", tc.args, got, tc.want)
		}
	}
}

func TestParseConductorSlackV2RunUsesRowContract(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		ok   bool
	}{
		{"row", []string{"run", "sample"}, true},
		{"missing name", []string{"run"}, false},
		{"legacy create", []string{"run", "sample", "--create"}, false},
		{"legacy resume", []string{"run", "sample", "--resume", "thread-exact"}, false},
		{"traversal", []string{"run", "../sample"}, false},
		{"extra", []string{"run", "sample", "extra"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConductorSlackV2Run(tc.args)
			if (err == nil) != tc.ok {
				t.Fatalf("error presence = %v, want success %v", err != nil, tc.ok)
			}
			if tc.ok && (got.mode != channelruntime.ModeRow || got.threadID != "" || got.name != "sample") {
				t.Fatalf("parsed mode/id/name = %q/%q/%q", got.mode, got.threadID, got.name)
			}
		})
	}
}

func TestConductorSlackV2HelpDoesNotInitializeRuntime(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := runConductorSlackV2Command("", []string{"--help"}, &out, &errOut); code != 0 {
		t.Fatalf("help exit = %d", code)
	}
	if !strings.Contains(out.String(), "conductor slack-v2 run <name>") ||
		strings.Contains(out.String(), "--create") || strings.Contains(out.String(), "--resume") || errOut.Len() != 0 {
		t.Fatalf("unexpected help output")
	}
}

func TestConductorSlackV2RunRequiresSelectedBackendBeforeNetwork(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	t.Setenv("AGENTDECK_PROFILE", "default")
	session.ClearUserConfigCache()
	t.Cleanup(session.ClearUserConfigCache)
	dir, err := session.ConductorNameDir("sample")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := runConductorSlackV2Command("", []string{"run", "sample"}, &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "config") || out.Len() != 0 {
		t.Fatalf("unselected backend result: code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func slackV2LoaderFixture(t *testing.T, settings session.SlackV2ConductorConfig) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	t.Setenv("AGENTDECK_PROFILE", "default")
	session.ClearUserConfigCache()
	t.Cleanup(session.ClearUserConfigCache)
	if err := session.SaveUserConfig(&session.UserConfig{Conductors: map[string]session.ConductorOverrides{
		"sample": {Backend: session.ConductorBackendSlackV2, SlackV2: settings},
	}}); err != nil {
		t.Fatal("fixture configuration failed")
	}
	if err := session.SaveConductorMeta(&session.ConductorMeta{
		Name: "sample", Agent: session.ConductorAgentCodex, Profile: "default",
	}); err != nil {
		t.Fatal("fixture metadata failed")
	}
	dir, err := session.ConductorNameDir("sample")
	if err != nil {
		t.Fatal("fixture directory failed")
	}
	return dir
}

func slackV2LiteralSettings() session.SlackV2ConductorConfig {
	return session.SlackV2ConductorConfig{
		AppToken: "fixture-app", BotToken: "fixture-bot", ChannelID: "fixture-channel",
		AllowedUserIDs: []string{"fixture-user"}, CodexExecutable: "$CODEX_EXECUTABLE", CodexModel: "${CODEX_MODEL}",
		RowInstanceID: "fixture-row", RowBindingToken: "fixture-binding",
	}
}

func TestLoadConductorSlackV2ConfigValues(t *testing.T) {
	for _, form := range []string{"literal", "unbraced", "braced", "mixed", "nonrecursive"} {
		t.Run(form, func(t *testing.T) {
			settings := slackV2LiteralSettings()
			env := map[string]string{
				"SLACK_APP_TOKEN": "fixture-app", "SLACK_BOT_TOKEN": "fixture-bot",
				"SLACK_DECK_CHANNEL": "fixture-channel", "SLACK_DECK_USER": "fixture-user",
				"_SECOND_USER2": "second-user",
			}
			if form != "literal" {
				settings.AppToken, settings.BotToken = "$SLACK_APP_TOKEN", "$SLACK_BOT_TOKEN"
				settings.ChannelID, settings.AllowedUserIDs = "$SLACK_DECK_CHANNEL", []string{"$SLACK_DECK_USER", "${_SECOND_USER2}"}
			}
			if form == "braced" || form == "mixed" {
				settings.AppToken, settings.AllowedUserIDs[0] = "${SLACK_APP_TOKEN}", "${SLACK_DECK_USER}"
			}
			if form == "braced" {
				settings.BotToken, settings.ChannelID = "${SLACK_BOT_TOKEN}", "${SLACK_DECK_CHANNEL}"
			}
			if form == "nonrecursive" {
				env["SLACK_APP_TOKEN"] = "${UNEXPANDED}"
			}
			dir := slackV2LoaderFixture(t, settings)
			cwd := filepath.Join(dir, "$CWD")
			lookups := 0
			got, err := loadConductorSlackV2Config("default", "sample", cwd, func(name string) (string, bool) {
				lookups++
				value, ok := env[name]
				if !ok {
					t.Fatal("unexpected environment lookup")
				}
				return value, ok
			})
			if err != nil {
				t.Fatal("configuration rejected")
			}
			wantUsers := []string{"fixture-user"}
			wantLookups := 0
			if form != "literal" {
				wantUsers = append(wantUsers, "second-user")
				wantLookups = 5
			}
			if got.AppToken != env["SLACK_APP_TOKEN"] || got.BotToken != env["SLACK_BOT_TOKEN"] ||
				got.ChannelID != env["SLACK_DECK_CHANNEL"] || got.RowInstanceID != settings.RowInstanceID ||
				got.RowBinding != settings.RowBindingToken || !slices.Equal(got.AllowedUserIDs, wantUsers) || lookups != wantLookups {
				t.Fatal("Slack configuration mismatch")
			}
			if got.CodexExecutable != "" || got.CodexModel != "" || got.CodexCWD != "" ||
				got.ConductorID != "default/sample" || got.ConversationID != "default/sample/channel-stream" {
				t.Fatal("runtime configuration mismatch")
			}
			raw, err := session.ConductorSlackV2Config("sample")
			stored := settings
			stored.CodexExecutable, stored.CodexModel = "", ""
			if err != nil || !reflect.DeepEqual(raw, stored) {
				t.Fatal("source configuration changed")
			}
		})
	}
}

func TestLoadConductorSlackV2ConfigRejectsInvalidValues(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"empty", ""}, {"missing", "$UNSET"}, {"missing_braced", "${UNSET}"},
		{"empty_env", "$EMPTY"}, {"padded_env", "${PADDED}"}, {"blank_env", "$BLANK"},
		{"bare_dollar", "$"}, {"numeric_name", "$1NAME"}, {"empty_name", "${}"},
		{"invalid_name", "${BAD-NAME}"}, {"unclosed", "${NAME"}, {"extra_brace", "$NAME}"},
		{"prefix", "prefix$NAME"}, {"suffix", "$NAME/suffix"}, {"braced_suffix", "${NAME}suffix"},
		{"double_dollar", "$$NAME"}, {"shell_default", "${NAME:-default}"},
		{"padded_ref_start", " $NAME"}, {"padded_ref_end", "${NAME} "},
		{"padded_literal_start", " fixture"}, {"padded_literal_end", "fixture\n"}, {"blank", "\t"},
	} {
		for _, field := range []string{"app", "bot", "channel", "user", "row_instance", "row_binding"} {
			t.Run(tc.name+"/"+field, func(t *testing.T) {
				settings := slackV2LiteralSettings()
				switch field {
				case "app":
					settings.AppToken = tc.value
				case "bot":
					settings.BotToken = tc.value
				case "channel":
					settings.ChannelID = tc.value
				case "user":
					settings.AllowedUserIDs[0] = tc.value
				case "row_instance":
					settings.RowInstanceID = tc.value
				case "row_binding":
					settings.RowBindingToken = tc.value
				}
				dir := slackV2LoaderFixture(t, settings)
				got, err := loadConductorSlackV2Config("default", "sample", dir, func(name string) (string, bool) {
					value, ok := map[string]string{"EMPTY": "", "PADDED": " fixture ", "BLANK": "\t"}[name]
					return value, ok
				})
				if !errors.Is(err, errSlackV2Configuration) || err.Error() != "slack-v2 configuration is unavailable" ||
					!reflect.DeepEqual(got, channelruntime.Config{}) {
					t.Fatal("invalid configuration did not fail closed")
				}
			})
		}
	}
}

func TestLoadConductorSlackV2ConfigRejectsDuplicateUsers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		users []string
	}{
		{"empty_allowlist", nil}, {"literal_duplicate", []string{"fixture-user", "fixture-user"}},
		{"reference_duplicate", []string{"$SLACK_DECK_USER", "${SLACK_DECK_USER}"}},
		{"resolved_duplicate", []string{"$SLACK_DECK_USER", "$OTHER_USER"}},
		{"mixed_duplicate", []string{"fixture-user", "$SLACK_DECK_USER"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := slackV2LiteralSettings()
			settings.AllowedUserIDs = tc.users
			dir := slackV2LoaderFixture(t, settings)
			got, err := loadConductorSlackV2Config("default", "sample", dir, func(string) (string, bool) {
				return "fixture-user", true
			})
			if !errors.Is(err, errSlackV2Configuration) || !reflect.DeepEqual(got, channelruntime.Config{}) {
				t.Fatal("invalid allowlist did not fail closed")
			}
		})
	}
}
