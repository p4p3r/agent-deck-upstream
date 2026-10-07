package channelruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

func TestManifestV2ArchiveRecoveryIsIdempotentAndAliased(t *testing.T) {
	cfg := rowConfig()
	request := Request{Name: "conductor", ConductorDir: filepath.Join(t.TempDir(), "conductor"), Mode: ModeRow}
	identity := struct{ TeamID, BotUserID string }{"team", "bot-user"}
	paths := pathsFor(request.ConductorDir)
	if err := os.MkdirAll(filepath.Dir(paths.manifest), 0o700); err != nil {
		t.Fatal(err)
	}
	archive := channelgateway.V4ArchiveDir(paths.ledger)
	if err := os.Mkdir(archive, 0o700); err != nil {
		t.Fatal(err)
	}
	old := &manifest{Version: 2, Profile: cfg.Profile, RowInstanceID: cfg.RowInstanceID,
		RowBinding: cfg.RowBinding, ConversationID: cfg.Profile + "/" + request.Name + "/channel-stream",
		ConductorID: cfg.Profile + "/" + request.Name, TeamID: identity.TeamID,
		BotUserID: identity.BotUserID, ChannelID: cfg.ChannelID, AllowedUserIDs: canonicalUsers(cfg.AllowedUserIDs)}
	if err := saveManifest(paths.manifest, old); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(paths.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(paths.manifest, filepath.Join(archive, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	store, err := channelgateway.Open(paths.ledger, cfg.SpoolKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := migrateV4Runtime(request, cfg, identity, paths); err != nil {
			t.Fatal(err)
		}
	}
	archived, err := os.ReadFile(filepath.Join(archive, "manifest.json"))
	if err != nil || !bytes.Equal(before, archived) {
		t.Fatal("v2 manifest evidence changed")
	}
	current, err := loadManifest(paths.manifest)
	if err != nil || current.Version != manifestVersion || current.match(request, cfg, identity) != nil {
		t.Fatal("v3 manifest recovery failed")
	}
	if current.ChannelID == cfg.ChannelID || current.RowBinding == cfg.RowBinding || current.TeamID == cfg.TeamID {
		t.Fatal("manifest contains exact binding")
	}
}
