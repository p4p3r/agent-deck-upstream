package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelruntime"
	"github.com/asheshgoplani/agent-deck/internal/session"
)

func TestConductorSlackV2RowRunHasNoThreadBootstrapFlags(t *testing.T) {
	parsed, err := parseConductorSlackV2Run([]string{"run", "sample"})
	if err != nil || parsed.name != "sample" || parsed.mode != channelruntime.ModeRow || parsed.threadID != "" {
		t.Fatalf("parsed=%+v err=%v", parsed, err)
	}
	for _, args := range [][]string{
		{"run", "sample", "--create"}, {"run", "sample", "--resume", "thread"}, {"run", "sample", "extra"},
	} {
		if _, err := parseConductorSlackV2Run(args); err == nil {
			t.Fatalf("legacy arguments accepted: %q", args)
		}
	}
	var out bytes.Buffer
	printConductorSlackV2Help(&out)
	if strings.Contains(out.String(), "--create") || strings.Contains(out.String(), "--resume") || strings.Contains(out.String(), "thread") {
		t.Fatalf("help retains direct thread ownership: %q", out.String())
	}
}

func TestLoadConductorSlackV2RowBindingConfig(t *testing.T) {
	settings := session.SlackV2ConductorConfig{
		AppToken: "$APP_TOKEN", BotToken: "$BOT_TOKEN", ChannelID: "$CHANNEL_ID",
		AllowedUserIDs: []string{"$ALLOWED_USER"}, RowInstanceID: "$ROW_ID", RowBindingToken: "$ROW_BINDING",
	}
	dir := slackV2LoaderFixture(t, settings)
	env := map[string]string{
		"APP_TOKEN": "fixture-app", "BOT_TOKEN": "fixture-bot", "CHANNEL_ID": "fixture-channel",
		"ALLOWED_USER": "fixture-user", "ROW_ID": "immutable-row", "ROW_BINDING": "opaque-binding",
	}
	got, err := loadConductorSlackV2Config("default", "sample", dir, func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	})
	want := channelruntime.Config{
		ConversationID: "default/sample/channel-stream", ConductorID: "default/sample", Profile: "default",
		RowInstanceID: "immutable-row", RowBinding: "opaque-binding", ChannelID: "fixture-channel",
		AllowedUserIDs: []string{"fixture-user"}, AppToken: "fixture-app", BotToken: "fixture-bot",
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("config=%+v err=%v want=%+v", got, err, want)
	}
}

func TestLoadConductorSlackV2RejectsMissingRowIdentity(t *testing.T) {
	settings := session.SlackV2ConductorConfig{
		AppToken: "fixture-app", BotToken: "fixture-bot", ChannelID: "fixture-channel",
		AllowedUserIDs: []string{"fixture-user"}, RowInstanceID: "immutable-row",
	}
	dir := slackV2LoaderFixture(t, settings)
	got, err := loadConductorSlackV2Config("default", "sample", dir, func(string) (string, bool) { return "", false })
	if err == nil || !reflect.DeepEqual(got, channelruntime.Config{}) {
		t.Fatalf("missing row binding accepted: config=%+v err=%v", got, err)
	}
}
