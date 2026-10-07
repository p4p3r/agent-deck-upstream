package channelgateway

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelspool"
)

func TestPersistedLedgerAndSpoolArtifactsDoNotRevealExactValues(t *testing.T) {
	key := makeSyntheticKey()
	root := t.TempDir()
	path := filepath.Join(root, "gateway.sqlite")
	s, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	const channel = "C-private-exact-channel-marker"
	const user = "U-private-exact-user-marker"
	const event = "E-private-exact-event-marker"
	const message = "M-private-exact-message-marker"
	const prompt = "synthetic-private-prompt-marker"
	const reply = "synthetic-private-reply-marker"
	const provider = "P-private-exact-provider-marker"
	conversation := channelspool.AliasFromKey(key, "conversation", "synthetic-route")
	if err := s.CreateConversation(context.Background(), Conversation{ID: conversation, ConductorID: channelspool.AliasFromKey(key, "conductor", "synthetic-conductor"), ChannelID: channel, RowInstanceID: "synthetic-row", RowBinding: "synthetic-binding", Mode: ChannelStream, AllowedSenders: []string{user}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if result, err := s.Ingest(ctx, Inbound{ConversationID: conversation, EventID: event, MessageID: message, ChannelID: channel, SenderID: user, Body: prompt}); err != nil || result.Disposition != Accepted {
		t.Fatal("ingress failed")
	}
	turn, err := s.NextTurn(ctx, conversation)
	if err != nil || turn == nil {
		t.Fatal("turn missing")
	}
	if err := s.AcceptTurn(ctx, turn.ID, "synthetic-acceptance"); err != nil {
		t.Fatal(err)
	}
	item, err := s.CompleteTurn(ctx, turn.ID, "synthetic-acceptance", reply)
	if err != nil || item == nil {
		t.Fatal("reply missing")
	}
	claim, created, err := s.PrepareDelivery(ctx, item.ID)
	if err != nil || !created {
		t.Fatal("delivery claim missing")
	}
	if err := s.ConfirmDelivery(ctx, item.ID, claim.ID, channel, provider); err != nil {
		t.Fatal(err)
	}
	private := [][]byte{[]byte(channel), []byte(user), []byte(event), []byte(message), []byte(prompt), []byte(reply), []byte(provider), []byte("synthetic-binding")}
	scan := func() (bool, int, int, error) {
		leaked, ledgers, sealed := false, 0, 0
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if filepath.Base(path) == "gateway.sqlite" {
				ledgers++
			}
			if strings.HasPrefix(filepath.Base(path), "inbound-") || strings.HasPrefix(filepath.Base(path), "outbound-") ||
				strings.HasPrefix(filepath.Base(path), "provider-") {
				sealed++
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, marker := range private {
				if bytes.Contains(data, marker) || bytes.Contains([]byte(path), marker) {
					leaked = true
				}
			}
			return nil
		})
		return leaked, ledgers, sealed, err
	}
	checkPrivate := func() {
		leaked, ledgers, sealed, err := scan()
		if err != nil || leaked || ledgers != 1 || sealed < 3 {
			t.Fatal("privacy scan missed artifacts or found exact private data")
		}
	}
	checkPrivate() // include live WAL and SHM before SQLite closes them
	control := filepath.Join(root, "privacy-positive-control")
	if err := os.WriteFile(control, []byte(prompt), 0o600); err != nil {
		t.Fatal(err)
	}
	if leaked, _, _, err := scan(); err != nil || !leaked {
		t.Fatal("privacy scan did not detect the positive control")
	}
	if err := os.Remove(control); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	checkPrivate()
}
