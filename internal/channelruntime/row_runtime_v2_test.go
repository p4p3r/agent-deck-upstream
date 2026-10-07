package channelruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelreconcile/rowdriver"
	"github.com/asheshgoplani/agent-deck/internal/channelstream"
	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/asheshgoplani/agent-deck/internal/slacknetwork"
)

type runtimeRowDriver struct{ closed int }

func (*runtimeRowDriver) SubmitRowOperation(context.Context, string, string) (rowdriver.Operation, error) {
	return rowdriver.Operation{}, errors.New("not invoked")
}
func (*runtimeRowDriver) RowOperationStatus(context.Context, string) (rowdriver.Operation, error) {
	return rowdriver.Operation{}, errors.New("not invoked")
}
func (d *runtimeRowDriver) Close() error { d.closed++; return nil }

func rowConfig() Config {
	return Config{
		ConversationID: "conversation", ConductorID: "conductor", Profile: "fixture",
		RowInstanceID: "immutable-row", RowBinding: "opaque-binding", ChannelID: "channel",
		AppID: "app", TeamID: "team", SpoolKey: bytes.Repeat([]byte{0x42}, 32),
		AllowedUserIDs: []string{"allowed-user"}, AppToken: "app-secret", BotToken: "bot-secret",
	}
}

func TestRowRuntimeManifestV2ContainsOnlyRowAndSlackBinding(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "manifest.json")
	cfg := rowConfig()
	identity := struct{ TeamID, BotUserID string }{"team", "bot-user"}
	m := newManifest(Request{Name: "conductor", Mode: ModeRow}, cfg, identity)
	if err := saveManifest(path, m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	wantKeys := map[string]bool{
		"version": true, "profile": true, "row_instance_id": true, "row_binding_token": true,
		"conversation_id": true, "conductor_id": true, "team_id": true, "bot_user_id": true,
		"channel_id": true, "allowed_user_ids": true, "app_id": true,
	}
	if len(fields) != len(wantKeys) {
		t.Fatalf("manifest fields=%v", fields)
	}
	for key := range fields {
		if !wantKeys[key] {
			t.Fatalf("unexpected manifest field %q", key)
		}
	}
	loaded, err := loadManifest(path)
	if err != nil || !reflect.DeepEqual(loaded, m) {
		t.Fatalf("loaded=%+v err=%v want=%+v", loaded, err, m)
	}
	if err := loaded.match(Request{Name: "conductor", Mode: ModeRow}, cfg, identity); err != nil {
		t.Fatal(err)
	}
	configChanges := map[string]func(*Config){
		"profile":      func(c *Config) { c.Profile = "changed" },
		"row":          func(c *Config) { c.RowInstanceID = "changed" },
		"binding":      func(c *Config) { c.RowBinding = "changed" },
		"conversation": func(c *Config) { c.ConversationID = "changed" },
		"conductor":    func(c *Config) { c.ConductorID = "changed" },
		"channel":      func(c *Config) { c.ChannelID = "changed" },
		"allowlist":    func(c *Config) { c.AllowedUserIDs = []string{"changed"} },
	}
	for name, change := range configChanges {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			change(&changed)
			if err := loaded.match(Request{Name: "conductor", Mode: ModeRow}, changed, identity); err == nil {
				t.Fatal("changed manifest binding matched")
			}
		})
	}
	for name, changedIdentity := range map[string]struct{ TeamID, BotUserID string }{
		"team": {TeamID: "changed", BotUserID: identity.BotUserID},
		"bot":  {TeamID: identity.TeamID, BotUserID: "changed"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := loaded.match(Request{Name: "conductor", Mode: ModeRow}, cfg, changedIdentity); err == nil {
				t.Fatal("changed provider identity matched")
			}
		})
	}
}

func TestRowRuntimeRejectsV1AndDuplicateManifest(t *testing.T) {
	for name, data := range map[string]string{
		"v1":        `{"version":1,"phase":"ready","thread_id":"old-thread"}`,
		"duplicate": `{"version":2,"version":2}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadManifest(path); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestRowRuntimeConstructsOnlyRowDriverAfterLockAndIdentity(t *testing.T) {
	cfg := rowConfig()
	dir := filepath.Join(t.TempDir(), "conductor")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	driver := &runtimeRowDriver{}
	locked, identified, legacyConstructed := false, false, false
	d := dependencies{
		acquire: func(_, _ string) (io.Closer, error) {
			locked = true
			closed := new(int)
			return testLock{closed}, nil
		},
		identity: func(context.Context, string) (slacknetwork.Identity, error) {
			if !locked {
				t.Fatal("identity before lock")
			}
			identified = true
			return slacknetwork.Identity{TeamID: "team", BotUserID: "bot-user"}, nil
		},
		rowDriver: func(got Config) (rowOperationDriver, error) {
			if !identified || !reflect.DeepEqual(got, cfg) {
				t.Fatal("row driver created before verified configuration")
			}
			return driver, nil
		},
		driver: func(Config) agentDriver { legacyConstructed = true; return nil },
		socket: func(string) channelstream.Socket { return testSocket{} },
		sender: func(string) slackgateway.Sender { return testSender{} },
		run: func(_ context.Context, runner *channelstream.Runner) error {
			if runner.Worker.RowDriver != driver || runner.Worker.Driver != nil {
				t.Fatal("runtime retained a direct agent driver")
			}
			return nil
		},
	}
	err := run(context.Background(), Request{
		Name: "conductor", ConductorDir: dir, Mode: ModeRow,
		LoadConfig: func() (Config, error) { return cfg, nil },
	}, d)
	if err != nil || legacyConstructed || driver.closed != 1 {
		t.Fatalf("run err=%v legacy=%v closed=%d", err, legacyConstructed, driver.closed)
	}
}
