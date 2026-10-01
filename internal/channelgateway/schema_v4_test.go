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
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE channelgateway_schema (version INTEGER NOT NULL)`,
		`INSERT INTO channelgateway_schema(version) VALUES(3)`,
		`CREATE TABLE conversations (
			id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, conductor_id TEXT NOT NULL,
			mode TEXT NOT NULL CHECK (mode IN ('channel_stream','thread_segments')),
			next_turn INTEGER NOT NULL DEFAULT 0, active_segment_id TEXT, pending_segment_id TEXT,
			agent_thread_id TEXT NOT NULL DEFAULT '', last_external_turn_id TEXT NOT NULL DEFAULT '')`,
		`CREATE UNIQUE INDEX one_conversation_per_agent_thread ON conversations(agent_thread_id)
			WHERE agent_thread_id != ''`,
		`CREATE TABLE allowed_senders (
			conversation_id TEXT NOT NULL REFERENCES conversations(id), sender_id TEXT NOT NULL,
			PRIMARY KEY (conversation_id, sender_id))`,
		`CREATE TABLE segments (
			id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
			root_thread_id TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('pending','open','superseded')),
			created_event_id TEXT, superseded_by TEXT, UNIQUE (conversation_id, root_thread_id))`,
		`CREATE TABLE inbound_events (
			ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
			conversation_id TEXT NOT NULL REFERENCES conversations(id), event_id TEXT NOT NULL,
			message_id TEXT NOT NULL, thread_id TEXT NOT NULL, segment_id TEXT REFERENCES segments(id),
			body TEXT, disposition TEXT NOT NULL, pointer_thread_id TEXT NOT NULL DEFAULT '', turn_id TEXT,
			UNIQUE (conversation_id, event_id))`,
		`CREATE INDEX inbound_queued ON inbound_events(conversation_id, segment_id, ordinal)
			WHERE disposition='accepted' AND turn_id IS NULL`,
		`CREATE TABLE turns (
			id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
			number INTEGER NOT NULL, event_ordinal INTEGER NOT NULL UNIQUE REFERENCES inbound_events(ordinal),
			segment_id TEXT NOT NULL REFERENCES segments(id), status TEXT NOT NULL CHECK (status IN ('active','completed')),
			acceptance_id TEXT NOT NULL DEFAULT '', attempt_id TEXT NOT NULL DEFAULT '',
			baseline_turn_id TEXT NOT NULL DEFAULT '', external_turn_id TEXT NOT NULL DEFAULT '',
			attempt_state TEXT NOT NULL DEFAULT 'unprepared'
				CHECK (attempt_state IN ('unprepared','prepared','accepted','needs_reconciliation','completed')),
			UNIQUE (conversation_id, number))`,
		`CREATE UNIQUE INDEX one_active_turn ON turns(conversation_id) WHERE status='active'`,
		`CREATE UNIQUE INDEX one_ledger_turn_per_external_turn ON turns(conversation_id,external_turn_id)
			WHERE external_turn_id != ''`,
		`CREATE TABLE outbox (
			ordinal INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
			conversation_id TEXT NOT NULL REFERENCES conversations(id), turn_id TEXT NOT NULL REFERENCES turns(id),
			kind TEXT NOT NULL, thread_id TEXT NOT NULL, body TEXT NOT NULL,
			state TEXT NOT NULL CHECK (state IN ('pending','sending','uncertain','delivered')),
			delivery_attempt_id TEXT NOT NULL DEFAULT '', external_message_id TEXT NOT NULL DEFAULT '',
			CHECK ((state='pending' AND delivery_attempt_id='' AND external_message_id='') OR
				(state IN ('sending','uncertain') AND delivery_attempt_id!='' AND external_message_id='') OR
				(state='delivered' AND delivery_attempt_id!='' AND external_message_id!='')))`,
		`CREATE INDEX outbox_pending ON outbox(conversation_id, ordinal) WHERE state='pending'`,
		`CREATE UNIQUE INDEX outbox_provider_message ON outbox(conversation_id,external_message_id)
			WHERE external_message_id != ''`,
		`INSERT INTO conversations(id,channel_id,conductor_id,mode,active_segment_id)
			VALUES('conversation','channel','conductor','channel_stream','stream:conversation')`,
		`INSERT INTO allowed_senders(conversation_id,sender_id) VALUES('conversation','sender')`,
		`INSERT INTO segments(id,conversation_id,root_thread_id,state)
			VALUES('stream:conversation','conversation','','open')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	switch mutation {
	case "inbound":
		_, err = db.Exec(`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition)
			VALUES('conversation','event','message','','stream:conversation','body','accepted')`)
	case "turn":
		_, err = db.Exec(`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,turn_id)
			VALUES('conversation','event','message','','stream:conversation','body','accepted','turn')`)
		if err == nil {
			_, err = db.Exec(`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status)
				VALUES('turn','conversation',1,1,'stream:conversation','active')`)
		}
	case "outbox":
		_, err = db.Exec(`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,turn_id)
			VALUES('conversation','event','message','','stream:conversation','body','accepted','turn')`)
		if err == nil {
			_, err = db.Exec(`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status)
				VALUES('turn','conversation',1,1,'stream:conversation','completed')`)
		}
		if err == nil {
			_, err = db.Exec(`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,body,state)
				VALUES('outbox','conversation','turn','reply','','reply','pending')`)
		}
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
	if len(data) < 20 || data[18] != 2 || data[19] != 2 {
		t.Fatalf("v3 fixture is not a real WAL ledger: header=%v", data[:min(len(data), 20)])
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("closed v3 fixture retained %s sidecar: %v", suffix, err)
		}
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

func TestGatewayV4InspectionDoesNotCreateWALSidecars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	writeV3Fixture(t, path, "")
	version, pristine, err := inspectGatewaySchema(path)
	if err != nil || version != 3 || !pristine {
		t.Fatalf("inspection: version=%d pristine=%v err=%v", version, pristine, err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspection created %s sidecar: %v", suffix, err)
		}
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

func TestGatewayV4RefusesPreexistingV3SidecarsWithoutInspecting(t *testing.T) {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "gateway.db")
			before := writeV3Fixture(t, path, "")
			if err := os.WriteFile(path+suffix, []byte("preexisting sidecar"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path); !errors.Is(err, ErrSchema) {
				t.Fatalf("error=%v, want schema refusal", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatalf("sidecar refusal changed ledger: err=%v", err)
			}
			if _, err := os.Stat(path + v3ArchiveSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("ledger with sidecar was archived")
			}
		})
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
