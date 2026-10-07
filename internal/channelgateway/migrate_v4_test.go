package channelgateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var v4IDs = map[string]string{
	"T-active":    "00000000-0000-4000-8000-000000000001",
	"T-pending":   "00000000-0000-4000-8000-000000000002",
	"T-uncertain": "00000000-0000-4000-8000-000000000003",
	"T-delivered": "00000000-0000-4000-8000-000000000004",
	"O-pending":   "00000000-0000-4000-8000-000000000011",
	"O-uncertain": "00000000-0000-4000-8000-000000000012",
	"O-delivered": "00000000-0000-4000-8000-000000000013",
	"A-active":    "00000000-0000-4000-8000-000000000021",
	"D-uncertain": "00000000-0000-4000-8000-000000000022",
	"D-delivered": "00000000-0000-4000-8000-000000000023",
	"R-active":    "00000000000000000000000001",
	"R-pending":   "00000000000000000000000002",
	"R-uncertain": "00000000000000000000000003",
	"R-delivered": "00000000000000000000000004",
}

func v4Fixture(t *testing.T) (string, V4Binding, []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	writeV3Fixture(t, path, "")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`ALTER TABLE conversations ADD COLUMN row_instance_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE conversations ADD COLUMN row_binding_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE conversations ADD COLUMN last_row_operation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN row_operation_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN operation_state TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN accepted_codex_session_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN accepted_turn_generation TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN terminal_error TEXT NOT NULL DEFAULT ''`,
		`UPDATE channelgateway_schema SET version=4`,
		`UPDATE conversations SET row_instance_id='row',row_binding_token='binding',next_turn=4`,
		`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition)
		 VALUES('conversation','E-queued','M-queued','','stream:conversation','queued-private-body','accepted')`,
		`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,turn_id)
		 VALUES('conversation','E-active','M-active','','stream:conversation','active-private-body','accepted','T-active')`,
		`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status,attempt_id,attempt_state,row_operation_id,operation_state,accepted_codex_session_id,accepted_turn_generation)
		 VALUES('T-active','conversation',4,2,'stream:conversation','active','A-active','accepted','R-active','accepted','codex-session-secret','generation-secret')`,
		`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,turn_id)
		 VALUES('conversation','E-pending','M-pending','','stream:conversation','pending-private-body','accepted','T-pending')`,
		`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status,attempt_state,row_operation_id,operation_state)
		 VALUES('T-pending','conversation',1,3,'stream:conversation','completed','completed','R-pending','completed')`,
		`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,body,state)
		 VALUES('O-pending','conversation','T-pending','reply','','pending-private-reply','pending')`,
		`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,turn_id)
		 VALUES('conversation','E-uncertain','M-uncertain','','stream:conversation','uncertain-private-body','accepted','T-uncertain')`,
		`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status,attempt_state,row_operation_id,operation_state)
		 VALUES('T-uncertain','conversation',2,4,'stream:conversation','completed','completed','R-uncertain','completed')`,
		`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,body,state,delivery_attempt_id)
		 VALUES('O-uncertain','conversation','T-uncertain','reply','','uncertain-private-reply','uncertain','D-uncertain')`,
		`INSERT INTO inbound_events(conversation_id,event_id,message_id,thread_id,segment_id,body,disposition,turn_id)
		 VALUES('conversation','E-delivered','M-delivered','','stream:conversation','delivered-private-body','accepted','T-delivered')`,
		`INSERT INTO turns(id,conversation_id,number,event_ordinal,segment_id,status,attempt_state,row_operation_id,operation_state)
		 VALUES('T-delivered','conversation',3,5,'stream:conversation','completed','completed','R-delivered','completed')`,
		`INSERT INTO outbox(id,conversation_id,turn_id,kind,thread_id,body,state,delivery_attempt_id,external_message_id)
		 VALUES('O-delivered','conversation','T-delivered','reply','','delivered-private-reply','delivered','D-delivered','provider-ts-secret')`,
	} {
		for old, replacement := range v4IDs {
			stmt = strings.ReplaceAll(stmt, "'"+old+"'", "'"+replacement+"'")
		}
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, V4Binding{OldConversationID: "conversation", OldConductorID: "conductor",
		NewConversation: Conversation{ID: "new-opaque-conversation", ConductorID: "new-opaque-conductor", ChannelID: "channel", RowInstanceID: "row", RowBinding: "binding", Mode: ChannelStream, AllowedSenders: []string{"sender"}}}, old
}

func TestMigrateV4RetainsEveryWorkStateAndOriginalBytes(t *testing.T) {
	path, binding, before := v4Fixture(t)
	if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); err != nil {
		t.Fatal(err)
	}
	archived, err := os.ReadFile(filepath.Join(V4ArchiveDir(path), "gateway.sqlite"))
	if err != nil || sha256.Sum256(archived) != sha256.Sum256(before) {
		t.Fatal("original v4 evidence changed")
	}
	// The raw archive is deliberately scanned as the migration exception.
	if !bytes.Contains(archived, []byte("active-private-body")) ||
		!bytes.Contains(archived, []byte("provider-ts-secret")) {
		t.Fatal("expected body-bearing v4 archive evidence is missing")
	}
	info, err := os.Stat(V4ArchiveDir(path))
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatal("archive is not owner-only")
	}
	s, err := Open(path, makeSyntheticKey())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	turn, err := s.NextTurn(context.Background(), binding.NewConversation.ID)
	if err != nil || turn == nil || turn.ID != v4IDs["T-active"] || turn.Body != "active-private-body" {
		t.Fatal("active turn was not preserved")
	}
	for _, id := range []string{v4IDs["O-pending"], v4IDs["O-uncertain"], v4IDs["O-delivered"]} {
		item, err := s.OutboxRecord(context.Background(), id)
		if err != nil || item.ID != id {
			t.Fatal("terminal delivery state was not preserved")
		}
	}
	item, err := s.OutboxRecord(context.Background(), v4IDs["O-uncertain"])
	if err != nil || item.State != UncertainDelivery {
		t.Fatal("uncertain ownership changed")
	}
	item, err = s.OutboxRecord(context.Background(), v4IDs["O-delivered"])
	if err != nil || item.ExternalMessageID != "provider-ts-secret" {
		t.Fatal("exact provider resolver lost")
	}
	ledger, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"active-private-body", "pending-private-reply", "provider-ts-secret", "codex-session-secret", "generation-secret"} {
		if bytes.Contains(ledger, []byte(private)) {
			t.Fatal("v5 ledger contains plaintext")
		}
	}
}

func TestMigrateV4ResumesAtEveryDurableBoundary(t *testing.T) {
	for _, phase := range []string{"archive_ready", "stage_ready", "retired", "activated"} {
		t.Run(phase, func(t *testing.T) {
			path, binding, before := v4Fixture(t)
			binding.interrupt = func(point string) error {
				if point == phase {
					return errors.New("synthetic interruption")
				}
				return nil
			}
			if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); !errors.Is(err, ErrStorage) {
				t.Fatal("interruption was not reported")
			}
			binding.interrupt = nil
			if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); err != nil {
				t.Fatal(err)
			}
			if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); err != nil {
				t.Fatal("migration was not idempotent")
			}
			archive, err := os.ReadFile(filepath.Join(V4ArchiveDir(path), "gateway.sqlite"))
			if err != nil || !bytes.Equal(archive, before) {
				t.Fatal("v4 evidence bytes changed")
			}
		})
	}
}

func TestMigrateV4ArchivesWALAndSHMByteIdentically(t *testing.T) {
	path, binding, _ := v4Fixture(t)
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if _, err := legacy.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`UPDATE conversations SET next_turn=5`); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		data, err := os.ReadFile(path + suffix)
		if err != nil {
			t.Fatal("fixture sidecar unavailable")
		}
		before[suffix] = data
	}
	if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); err != nil {
		t.Fatal(err)
	}
	for suffix, original := range before {
		archivePath := filepath.Join(V4ArchiveDir(path), "gateway.sqlite"+suffix)
		archive, err := os.ReadFile(archivePath)
		if err != nil || !bytes.Equal(archive, original) {
			t.Fatal("archive sidecar bytes changed")
		}
		info, err := os.Stat(archivePath)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatal("archive sidecar is not owner-only")
		}
	}
}

func TestMigrateV4UnsafeStateLeavesOriginalInPlace(t *testing.T) {
	path, binding, _ := v4Fixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE conversations SET agent_thread_id='unsupported-legacy-thread'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); !errors.Is(err, ErrSchema) {
		t.Fatal("unsupported migration was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("unsafe migration changed old evidence")
	}
	version, _, err := inspectGatewaySchema(path)
	if err != nil || version != 4 {
		t.Fatal("unsafe migration activated a new ledger")
	}
}

func TestMigrateV4ChangedBindingLeavesOriginalInPlace(t *testing.T) {
	path, binding, before := v4Fixture(t)
	binding.NewConversation.RowBinding = "changed-binding"
	if err := MigrateV4(context.Background(), path, binding, makeSyntheticKey()); !errors.Is(err, ErrSchema) {
		t.Fatal("changed migration binding was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("changed binding altered old evidence")
	}
}
