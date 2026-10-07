package channelgateway

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// V4ArchiveDir names the explicit owner-only evidence exception. Its original
// v4 files intentionally remain byte-identical and therefore body-bearing.
func V4ArchiveDir(ledgerPath string) string {
	return filepath.Join(filepath.Dir(ledgerPath), "migration-v4-evidence")
}

type migrationReady struct {
	Stage string `json:"stage"`
}

type V4Binding struct {
	OldConversationID string
	OldConductorID    string
	NewConversation   Conversation
	interrupt         func(string) error // deterministic local crash seam
}

var v4Columns = map[string][]string{
	"conversations":   {"id", "channel_id", "conductor_id", "mode", "next_turn", "active_segment_id", "pending_segment_id", "row_instance_id", "row_binding_token", "last_row_operation_id", "agent_thread_id", "last_external_turn_id"},
	"allowed_senders": {"conversation_id", "sender_id"},
	"segments":        {"id", "conversation_id", "root_thread_id", "state", "created_event_id", "superseded_by"},
	"inbound_events":  {"ordinal", "conversation_id", "event_id", "message_id", "thread_id", "segment_id", "body", "disposition", "pointer_thread_id", "turn_id"},
	"turns":           {"id", "conversation_id", "number", "event_ordinal", "segment_id", "status", "acceptance_id", "attempt_id", "baseline_turn_id", "external_turn_id", "row_operation_id", "operation_state", "accepted_codex_session_id", "accepted_turn_generation", "terminal_error", "attempt_state"},
	"outbox":          {"ordinal", "id", "conversation_id", "turn_id", "kind", "thread_id", "body", "state", "delivery_attempt_id", "external_message_id"},
}

func validV4Columns(table string, columns []string) bool {
	expected, ok := v4Columns[table]
	if !ok || len(expected) != len(columns) {
		return false
	}
	seen := make(map[string]bool, len(columns))
	for _, column := range columns {
		found := false
		for _, known := range expected {
			found = found || known == column
		}
		if !found || seen[column] {
			return false
		}
		seen[column] = true
	}
	return true
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return ErrStorage
	}
	defer d.Close()
	if d.Sync() != nil {
		return ErrStorage
	}
	return nil
}

// A marker is visible only after its complete bytes are durable. A crash
// before rename leaves a harmless private temporary file and permits retry.
func writeMigrationMarker(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".marker-*")
	if err != nil {
		return ErrStorage
	}
	defer os.Remove(f.Name())
	if f.Chmod(0o600) != nil {
		_ = f.Close()
		return ErrStorage
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return ErrStorage
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil || closeErr != nil {
		return ErrStorage
	}
	if os.Rename(f.Name(), path) != nil {
		return ErrStorage
	}
	return syncDirectory(filepath.Dir(path))
}

func privateRegular(path string) (bool, error) {
	i, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !i.Mode().IsRegular() || i.Mode()&os.ModeSymlink != 0 || i.Mode().Perm()&0o022 != 0 {
		return false, ErrSchema
	}
	return true, nil
}

// preserveV4File copies a complete original into the archive before SQLite
// reads the live v4 WAL. A partially written temporary file is never promoted.
func preserveV4File(source, archive string) error {
	present, err := privateRegular(source)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if ready, err := privateRegular(archive); err != nil {
		return err
	} else if ready {
		a, err := hashMigrationFile(source)
		if err != nil {
			return ErrStorage
		}
		b, err := hashMigrationFile(archive)
		if err != nil || a != b {
			return ErrSchema
		}
		return nil
	}
	in, err := os.Open(source)
	if err != nil {
		return ErrStorage
	}
	defer in.Close()
	out, err := os.CreateTemp(filepath.Dir(archive), ".evidence-*")
	if err != nil {
		return ErrStorage
	}
	// A failed or interrupted copy remains inside the private evidence directory.
	if out.Chmod(0o600) != nil {
		_ = out.Close()
		return ErrStorage
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return ErrStorage
	}
	syncErr := out.Sync()
	closeErr := out.Close()
	if syncErr != nil || closeErr != nil {
		return ErrStorage
	}
	if err := os.Rename(out.Name(), archive); err != nil {
		return ErrStorage
	}
	return syncDirectory(filepath.Dir(archive))
}

func hashMigrationFile(path string) ([32]byte, error) {
	var digest [32]byte
	f, err := os.Open(path)
	if err != nil {
		return digest, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return digest, err
	}
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func migrationMapID(oldID, newID, value string) string {
	if value == oldID {
		return newID
	}
	if value == "stream:"+oldID {
		return "stream:" + newID
	}
	return value
}

func migrationString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

func migrationUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}

func migrationSendID(value string) bool {
	if len(value) != 26 {
		return false
	}
	for _, ch := range value {
		if !strings.ContainsRune("0123456789ABCDEFGHJKMNPQRSTVWXYZ", ch) {
			return false
		}
	}
	return true
}

func copyV4Table(ctx context.Context, old *sql.DB, staged *Store, tx *sql.Tx, table string, binding V4Binding) error {
	oldConversation, newConversation := binding.OldConversationID, binding.NewConversation.ID
	rows, err := old.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		return ErrSchema
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return ErrSchema
	}
	if !validV4Columns(table, columns) {
		return ErrSchema
	}
	insertColumns := append([]string(nil), columns...)
	for i := range insertColumns {
		if insertColumns[i] == "body" {
			insertColumns[i] = "content_ref"
		}
	}
	query := "INSERT INTO " + table + "(" + strings.Join(insertColumns, ",") + ") VALUES(" + strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",") + ")"
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if rows.Scan(dest...) != nil {
			return ErrSchema
		}
		fields := map[string]string{}
		for i, name := range columns {
			if value, ok := migrationString(values[i]); ok {
				fields[name] = value
			}
		}
		if table == "conversations" {
			if fields["id"] != oldConversation || fields["conductor_id"] != binding.OldConductorID ||
				fields["channel_id"] != binding.NewConversation.ChannelID ||
				fields["row_instance_id"] != binding.NewConversation.RowInstanceID ||
				fields["row_binding_token"] != binding.NewConversation.RowBinding ||
				fields["mode"] != string(ChannelStream) || fields["agent_thread_id"] != "" || fields["last_external_turn_id"] != "" {
				return ErrSchema
			}
		}
		if table == "allowed_senders" {
			matched := false
			for _, sender := range binding.NewConversation.AllowedSenders {
				matched = matched || sender == fields["sender_id"]
			}
			if !matched || fields["conversation_id"] != oldConversation {
				return ErrSchema
			}
		}
		if table == "segments" && fields["id"] != "stream:"+oldConversation {
			return ErrSchema
		}
		if table == "turns" && (fields["baseline_turn_id"] != "" || fields["external_turn_id"] != "") {
			return ErrSchema
		}
		if table == "turns" && (!migrationUUID(fields["id"]) || fields["acceptance_id"] != "" ||
			fields["attempt_id"] != "" && !migrationUUID(fields["attempt_id"]) ||
			fields["row_operation_id"] != "" && !migrationSendID(fields["row_operation_id"])) {
			return ErrSchema
		}
		if table == "conversations" && fields["last_row_operation_id"] != "" && !migrationSendID(fields["last_row_operation_id"]) {
			return ErrSchema
		}
		if table == "outbox" && (!migrationUUID(fields["id"]) ||
			fields["delivery_attempt_id"] != "" && !migrationUUID(fields["delivery_attempt_id"]) ||
			fields["kind"] != "reply" && fields["kind"] != "status") {
			return ErrSchema
		}
		if table == "turns" && fields["terminal_error"] != "" && !rowTerminalError(RowOperationState(fields["terminal_error"])) {
			return ErrSchema
		}
		for i, name := range columns {
			value, ok := migrationString(values[i])
			if !ok {
				continue
			}
			switch name {
			case "conversation_id":
				values[i] = migrationMapID(oldConversation, newConversation, value)
			case "id":
				if table == "conversations" || table == "segments" {
					values[i] = migrationMapID(oldConversation, newConversation, value)
				}
			case "active_segment_id", "pending_segment_id", "segment_id", "superseded_by":
				values[i] = migrationMapID(oldConversation, newConversation, value)
			case "conductor_id":
				values[i] = binding.NewConversation.ConductorID
			case "channel_id":
				values[i] = staged.alias("channel", value)
				if table == "conversations" && staged.saveBinding(newConversation, value) != nil {
					return ErrStorage
				}
			case "sender_id":
				values[i] = staged.alias("sender", value)
			case "row_instance_id":
				values[i] = staged.alias("row", value)
			case "row_binding_token":
				values[i] = staged.alias("rowbinding", value)
			case "root_thread_id", "thread_id", "pointer_thread_id":
				if staged.saveThreadID(value) != nil {
					return ErrStorage
				}
				values[i] = staged.alias("thread", value)
			case "event_id", "created_event_id":
				values[i] = staged.alias("event", value)
			case "message_id":
				values[i] = staged.alias("message", value)
			case "accepted_codex_session_id":
				values[i] = staged.alias("codexsession", value)
			case "accepted_turn_generation":
				values[i] = staged.alias("codexgeneration", value)
			case "external_message_id":
				values[i] = staged.alias("providermessage", value)
				if value != "" && staged.saveProviderMessage(fields["id"], value) != nil {
					return ErrStorage
				}
			case "body":
				if table == "inbound_events" && fields["disposition"] == string(Accepted) {
					if staged.saveInbound(staged.alias("event", fields["event_id"]), value) != nil {
						return ErrStorage
					}
					values[i] = staged.alias("event", fields["event_id"])
				} else if table == "outbox" && fields["kind"] == "reply" {
					if staged.saveOutbound(fields["id"], value) != nil {
						return ErrStorage
					}
					values[i] = staged.alias("outbox", fields["id"])
				} else {
					values[i] = ""
				}
			}
		}
		if _, err := tx.ExecContext(ctx, query, values...); err != nil {
			return ErrSchema
		}
	}
	if rows.Err() != nil {
		return ErrSchema
	}
	return nil
}

// MigrateV4 stages a body-free v5 ledger and sealed spool before retiring any
// live v4 file. The caller must hold the singleton conductor lock. Unsupported
// legacy routing modes fail closed with all old evidence intact.
func MigrateV4(ctx context.Context, path string, binding V4Binding, key []byte) error {
	oldConversation, newConversation := binding.OldConversationID, binding.NewConversation.ID
	if ctx == nil || path == "" || oldConversation == "" || newConversation == "" ||
		!validConversation(binding.NewConversation) || binding.NewConversation.Mode != ChannelStream || len(key) != 32 || oldConversation == newConversation {
		return ErrInvalid
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return ErrStorage
	}
	archive := V4ArchiveDir(path)
	readyPath := filepath.Join(archive, "stage-ready.json")
	archiveReadyPath := filepath.Join(archive, "archive-ready")
	if err := os.Mkdir(archive, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return ErrStorage
	}
	if info, err := os.Lstat(archive); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return ErrSchema
	}
	var ready migrationReady
	readyPresent, err := privateRegular(readyPath)
	if err != nil {
		return err
	}
	if readyPresent {
		data, err := os.ReadFile(readyPath)
		if err != nil {
			return ErrStorage
		}
		if json.Unmarshal(data, &ready) != nil || filepath.Base(ready.Stage) != ready.Stage || !strings.HasPrefix(ready.Stage, "gateway-v5-stage-") {
			return ErrSchema
		}
	}
	if ready.Stage == "" {
		present, err := privateRegular(path)
		if err != nil || !present {
			return ErrSchema
		}
		archiveReady, err := privateRegular(archiveReadyPath)
		if err != nil {
			return err
		}
		if !archiveReady {
			for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
				if err := preserveV4File(path+suffix, filepath.Join(archive, "gateway.sqlite"+suffix)); err != nil {
					return err
				}
			}
			if writeMigrationMarker(archiveReadyPath, nil) != nil {
				return ErrStorage
			}
		}
		if binding.interrupt != nil && binding.interrupt("archive_ready") != nil {
			return ErrStorage
		}
		old, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=query_only(1)&_pragma=foreign_keys(on)")
		if err != nil {
			return ErrSchema
		}
		var version, maxVersion, schemaRows int
		if old.QueryRowContext(ctx, `SELECT count(*),min(version),max(version) FROM channelgateway_schema`).Scan(&schemaRows, &version, &maxVersion) != nil || schemaRows != 1 || version != 4 || maxVersion != 4 {
			_ = old.Close()
			return ErrSchema
		}
		var extraTables int
		if old.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
			AND name NOT IN ('channelgateway_schema','conversations','allowed_senders','segments','inbound_events','turns','outbox')`).Scan(&extraTables) != nil || extraTables != 0 {
			_ = old.Close()
			return ErrSchema
		}
		stageName := "gateway-v5-stage-" + uuid.NewString() + ".sqlite"
		stagePath := filepath.Join(filepath.Dir(path), stageName)
		staged, err := Open(stagePath, key)
		if err != nil {
			_ = old.Close()
			return err
		}
		tx, err := staged.db.BeginTx(ctx, nil)
		if err == nil {
			for _, table := range []string{"conversations", "allowed_senders", "segments", "inbound_events", "turns", "outbox"} {
				if err = copyV4Table(ctx, old, staged, tx, table, binding); err != nil {
					break
				}
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
		_ = old.Close()
		if err != nil {
			_ = staged.Close()
			return ErrSchema
		}
		var conversations, senders int
		if staged.db.QueryRowContext(ctx, `SELECT count(*) FROM conversations`).Scan(&conversations) != nil || conversations != 1 ||
			staged.db.QueryRowContext(ctx, `SELECT count(*) FROM allowed_senders`).Scan(&senders) != nil ||
			senders != len(binding.NewConversation.AllowedSenders) ||
			staged.CreateConversation(ctx, binding.NewConversation) != nil {
			_ = staged.Close()
			return ErrSchema
		}
		var badReferences int
		if staged.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_foreign_key_check`).Scan(&badReferences) != nil || badReferences != 0 {
			_ = staged.Close()
			return ErrSchema
		}
		stamp := staged.now().Unix()
		for _, stmt := range []string{
			`UPDATE inbound_events SET seen_at=?`,
			`UPDATE turns SET completed_at=? WHERE status='completed'`,
			`UPDATE outbox SET delivered_at=? WHERE state='delivered'`,
			`UPDATE outbox SET uncertain_at=? WHERE state IN ('uncertain','sending')`,
		} {
			if _, err := staged.db.ExecContext(ctx, stmt, stamp); err != nil {
				_ = staged.Close()
				return ErrStorage
			}
		}
		if _, err = staged.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			_ = staged.Close()
			return ErrStorage
		}
		if staged.Close() != nil {
			return ErrStorage
		}
		if _, err := os.Stat(stagePath + "-wal"); err == nil {
			return ErrStorage
		}
		ready = migrationReady{Stage: stageName}
		data, _ := json.Marshal(ready)
		if writeMigrationMarker(readyPath, data) != nil {
			return ErrStorage
		}
		if binding.interrupt != nil && binding.interrupt("stage_ready") != nil {
			return ErrStorage
		}
	}
	stagePath := filepath.Join(filepath.Dir(path), ready.Stage)
	if present, err := privateRegular(stagePath); err != nil {
		return err
	} else if present {
		staged, err := Open(stagePath, key)
		if err != nil {
			return ErrSchema
		}
		var count int
		var integrity string
		valid := staged.db.QueryRowContext(ctx, `SELECT count(*) FROM conversations`).Scan(&count) == nil && count == 1 &&
			staged.db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity) == nil && integrity == "ok" &&
			staged.CreateConversation(ctx, binding.NewConversation) == nil
		if staged.Close() != nil || !valid {
			return ErrSchema
		}
	} else {
		version, _, err := inspectGatewaySchema(path)
		if err != nil || version != schemaVersion {
			return ErrSchema
		}
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		source := path + suffix
		present, err := privateRegular(source)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		if suffix == "" {
			version, _, err := inspectGatewaySchema(path)
			if err == nil && version == schemaVersion {
				break
			}
		}
		if os.Chmod(source, 0o600) != nil {
			return ErrStorage
		}
		if err := os.Rename(source, filepath.Join(archive, "retired-gateway.sqlite"+suffix)); err != nil {
			return ErrStorage
		}
	}
	if syncDirectory(filepath.Dir(path)) != nil || syncDirectory(archive) != nil {
		return ErrStorage
	}
	if binding.interrupt != nil && binding.interrupt("retired") != nil {
		return ErrStorage
	}
	if present, _ := privateRegular(path); !present {
		if err := os.Rename(stagePath, path); err != nil {
			return ErrStorage
		}
		if syncDirectory(filepath.Dir(path)) != nil {
			return ErrStorage
		}
	}
	version, _, err := inspectGatewaySchema(path)
	if err != nil || version != schemaVersion {
		return ErrSchema
	}
	if binding.interrupt != nil && binding.interrupt("activated") != nil {
		return ErrStorage
	}
	return nil
}
