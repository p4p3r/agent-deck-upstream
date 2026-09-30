package channelgateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeV3Fixture(t *testing.T, path, mutation string) []byte {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE channelgateway_schema(version INTEGER NOT NULL)`,
		`INSERT INTO channelgateway_schema(version) VALUES(3)`,
		`CREATE TABLE conversations(id TEXT PRIMARY KEY,next_turn INTEGER NOT NULL,last_external_turn_id TEXT NOT NULL)`,
		`INSERT INTO conversations(id,next_turn,last_external_turn_id) VALUES('conversation',0,'')`,
		`CREATE TABLE inbound_events(id INTEGER)`,
		`CREATE TABLE turns(id INTEGER)`,
		`CREATE TABLE outbox(id INTEGER)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	switch mutation {
	case "inbound":
		_, err = db.Exec(`INSERT INTO inbound_events(id) VALUES(1)`)
	case "turn":
		_, err = db.Exec(`INSERT INTO turns(id) VALUES(1)`)
	case "outbox":
		_, err = db.Exec(`INSERT INTO outbox(id) VALUES(1)`)
	case "cursor":
		_, err = db.Exec(`UPDATE conversations SET last_external_turn_id='cursor'`)
	case "sequence":
		_, err = db.Exec(`UPDATE conversations SET next_turn=1`)
	}
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestGatewayV4ArchivesOnlyPristineV3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	before := writeV3Fixture(t, path, "")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	archive, err := os.ReadFile(path + v3ArchiveSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(archive, before) || sha256.Sum256(archive) != sha256.Sum256(before) {
		t.Fatal("v3 archive bytes changed")
	}
	var version int
	if err := s.db.QueryRow(`SELECT version FROM channelgateway_schema`).Scan(&version); err != nil || version != 4 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestGatewayV4FreshLedgerIsPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("ledger info=%v err=%v", info, err)
	}
}

func TestGatewayV4PristineArchiveCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.db")
	writeV3Fixture(t, path+v3ArchiveSuffix, "")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayV4RefusesEveryNonPristineV3AndUnknownSchema(t *testing.T) {
	for _, mutation := range []string{"inbound", "turn", "outbox", "cursor", "sequence"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "gateway.db")
			before := writeV3Fixture(t, path, mutation)
			if _, err := Open(path); !errors.Is(err, ErrSchema) {
				t.Fatalf("error=%v, want schema refusal", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatalf("refused ledger changed: err=%v", err)
			}
			if _, err := os.Stat(path + v3ArchiveSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("non-pristine ledger was archived")
			}
		})
	}

	path := filepath.Join(t.TempDir(), "unknown.db")
	writeV3Fixture(t, path, "")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`UPDATE channelgateway_schema SET version=99`)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := Open(path); !errors.Is(err, ErrSchema) {
		t.Fatalf("unknown schema error=%v", err)
	}
}

func TestGatewayV4RowIdentityIsImmutable(t *testing.T) {
	s, _ := testStore(t, ChannelStream)
	ctx := context.Background()
	base := Conversation{ID: "conversation", ChannelID: "channel", ConductorID: "conductor",
		RowInstanceID: "row", RowBinding: "binding", Mode: ChannelStream, AllowedSenders: []string{"alice"}}
	if err := s.CreateConversation(ctx, base); err != nil {
		t.Fatal(err)
	}
	base.RowBinding = "changed"
	if err := s.CreateConversation(ctx, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed binding error=%v", err)
	}
	base.RowBinding, base.RowInstanceID = "binding", "different-row"
	if err := s.CreateConversation(ctx, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed row error=%v", err)
	}
}
