// Package channelgateway is a provider-neutral, durable routing ledger for a
// conductor conversation. It has no transport or agent-driver side effects.
package channelgateway

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"slices"

	_ "modernc.org/sqlite"
)

type Mode string

const (
	ChannelStream  Mode = "channel_stream"
	ThreadSegments Mode = "thread_segments"
)

type Disposition string

const (
	Accepted           Disposition = "accepted"
	Ignored            Disposition = "ignored"
	RejectedPending    Disposition = "rejected_pending"
	RejectedSuperseded Disposition = "rejected_superseded"
)

var (
	ErrInvalid      = errors.New("channelgateway: invalid input")
	ErrUnauthorized = errors.New("channelgateway: unauthorized sender or channel")
	ErrNotFound     = errors.New("channelgateway: record not found")
	ErrConflict     = errors.New("channelgateway: state conflict")
	ErrSchema       = errors.New("channelgateway: incompatible schema")
	ErrStorage      = errors.New("channelgateway: storage failure")
)

// Conversation is an immutable binding. AllowedSenders is an explicit allowlist
// of opaque external sender IDs. A separate conversation is needed to change it.
type Conversation struct {
	ID             string
	ChannelID      string
	ConductorID    string
	RowInstanceID  string
	RowBinding     string
	Mode           Mode
	AllowedSenders []string
}

// Inbound is a transport-normalized message. ThreadID is empty for a top-level
// message. Adapters must filter bot/self events before calling Ingest.
type Inbound struct {
	ConversationID string
	EventID        string
	MessageID      string
	ThreadID       string
	ChannelID      string
	SenderID       string
	Mentioned      bool
	Body           string
}

type IntakeResult struct {
	Disposition     Disposition
	Duplicate       bool
	SegmentID       string
	SegmentState    string
	PointerThreadID string
}

// TurnID is stable across retries and restarts. A driver must reconcile an
// existing AcceptanceID before it starts another external agent attempt.
type Turn struct {
	ID             string
	ConversationID string
	Number         int64
	SegmentID      string
	EventID        string
	MessageID      string
	ThreadID       string
	Body           string
	AcceptanceID   string
	AttemptID      string
	AttemptState   AttemptState
	BaselineTurnID string
	ExternalTurnID string
	RowOperationID string
	OperationState RowOperationState
	CodexSessionID string
	TurnGeneration string
	TerminalError  string
}

type OutboxItem struct {
	ID                string
	ConversationID    string
	TurnID            string
	Kind              string
	ThreadID          string
	Body              string
	State             DeliveryState
	DeliveryAttemptID string
	ExternalMessageID string
}

type Store struct{ db *sql.DB }

const schemaVersion = 4

const v3ArchiveSuffix = ".v3-archive"

// archivePristineV3 performs the only automatic schema transition. A v3
// ledger is archived only when it contains no ingress, turn, cursor, or
// egress evidence. The deterministic archive name makes a crash after rename
// recoverable without ever overwriting predecessor bytes.
func archivePristineV3(path string) error {
	archive := path + v3ArchiveSuffix
	mainInfo, mainErr := os.Lstat(path)
	archiveInfo, archiveErr := os.Lstat(archive)
	if mainErr != nil && !errors.Is(mainErr, os.ErrNotExist) {
		return ErrStorage
	}
	if archiveErr != nil && !errors.Is(archiveErr, os.ErrNotExist) {
		return ErrStorage
	}
	if mainErr != nil {
		if archiveErr == nil {
			if !archiveInfo.Mode().IsRegular() {
				return ErrSchema
			}
			hasSidecars, err := gatewaySidecarsPresent(archive)
			if err != nil {
				return err
			}
			if hasSidecars {
				return ErrSchema
			}
			version, pristine, err := inspectGatewaySchema(archive)
			if err != nil || version != 3 || !pristine {
				return ErrSchema
			}
		}
		return nil
	}
	if !mainInfo.Mode().IsRegular() {
		return ErrStorage
	}
	hasSidecars, err := gatewaySidecarsPresent(path)
	if err != nil {
		return err
	}
	version, pristine, err := inspectGatewaySchema(path)
	if err != nil {
		return ErrSchema
	}
	if version == 0 || version == schemaVersion {
		return nil
	}
	if version != 3 || !pristine || archiveErr == nil || hasSidecars {
		return ErrSchema
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return ErrStorage
	}
	if err := os.Rename(path, archive); err != nil {
		return ErrStorage
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return ErrStorage
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return ErrStorage
	}
	return nil
}

func gatewaySidecarsPresent(path string) (bool, error) {
	present := false
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); err == nil {
			present = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, ErrSchema
		}
	}
	return present, nil
}

func inspectGatewaySchema(path string) (version int, pristine bool, resultErr error) {
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro&immutable=1&_pragma=query_only(1)&_pragma=foreign_keys(on)")
	if err != nil {
		return 0, false, err
	}
	defer db.Close()
	var tables int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
		return 0, false, err
	}
	if tables == 0 {
		return 0, true, nil
	}
	if err := db.QueryRow(`SELECT version FROM channelgateway_schema LIMIT 1`).Scan(&version); err != nil {
		return 0, false, err
	}
	if version != 3 {
		return version, false, nil
	}
	var rowContractColumns int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('conversations')
		WHERE name IN ('row_instance_id','row_binding_token','last_row_operation_id')`).Scan(&rowContractColumns); err != nil {
		return version, false, err
	}
	if rowContractColumns != 0 {
		return version, false, errors.New("schema labeled v3 contains v4 row columns")
	}
	var inbound, turns, outbox, cursor, sequenced int64
	if err := db.QueryRow(`SELECT
		(SELECT count(*) FROM inbound_events),
		(SELECT count(*) FROM turns),
		(SELECT count(*) FROM outbox),
		(SELECT count(*) FROM conversations WHERE last_external_turn_id != ''),
		(SELECT count(*) FROM conversations WHERE next_turn != 0)`).
		Scan(&inbound, &turns, &outbox, &cursor, &sequenced); err != nil {
		return version, false, err
	}
	return version, inbound == 0 && turns == 0 && outbox == 0 && cursor == 0 && sequenced == 0, nil
}

// Open creates a private SQLite file or opens an existing compatible ledger.
// Existing files with an unknown schema are never deleted or migrated.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, ErrInvalid
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, ErrStorage
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, ErrStorage
	}
	if err := archivePristineV3(abs); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, ErrStorage
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return nil, ErrStorage
	}
	if err := f.Close(); err != nil {
		return nil, ErrStorage
	}
	u := url.URL{Scheme: "file", Path: abs}
	db, err := sql.Open("sqlite", u.String()+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, ErrStorage
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.initSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	// journal_mode persists on the file; busy_timeout and foreign_keys are
	// connection-local and therefore belong in the DSN above.
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return nil, ErrStorage
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

type writeTx struct {
	ctx  context.Context
	conn *sql.Conn
}

func (s *Store) write(ctx context.Context, fn func(*writeTx) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return ErrStorage
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return ErrStorage
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	if err := fn(&writeTx{ctx: ctx, conn: conn}); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return ErrStorage
	}
	return nil
}

func (tx *writeTx) exec(query string, args ...any) (sql.Result, error) {
	r, err := tx.conn.ExecContext(tx.ctx, query, args...)
	if err != nil {
		return nil, ErrStorage
	}
	return r, nil
}

func (tx *writeTx) row(query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(tx.ctx, query, args...)
}

func (s *Store) initSchema(ctx context.Context) error {
	return s.write(ctx, func(tx *writeTx) error {
		var tables int
		if err := tx.row(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
			return ErrStorage
		}
		if tables != 0 {
			var version int
			if err := tx.row(`SELECT version FROM channelgateway_schema LIMIT 1`).Scan(&version); err != nil || version != schemaVersion {
				return ErrSchema
			}
			return nil
		}
		for _, stmt := range schemaDDL {
			if _, err := tx.exec(stmt); err != nil {
				return err
			}
		}
		_, err := tx.exec(`INSERT INTO channelgateway_schema(version) VALUES (?)`, schemaVersion)
		return err
	})
}

var schemaDDL = []string{
	`CREATE TABLE channelgateway_schema (version INTEGER NOT NULL)`,
	`CREATE TABLE conversations (
		id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, conductor_id TEXT NOT NULL,
		mode TEXT NOT NULL CHECK (mode IN ('channel_stream','thread_segments')),
		next_turn INTEGER NOT NULL DEFAULT 0, active_segment_id TEXT, pending_segment_id TEXT,
		row_instance_id TEXT NOT NULL, row_binding_token TEXT NOT NULL,
		last_row_operation_id TEXT NOT NULL DEFAULT '',
		agent_thread_id TEXT NOT NULL DEFAULT '', last_external_turn_id TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE UNIQUE INDEX one_conversation_per_agent_thread ON conversations(agent_thread_id)
		WHERE agent_thread_id != ''`,
	`CREATE TABLE allowed_senders (
		conversation_id TEXT NOT NULL REFERENCES conversations(id), sender_id TEXT NOT NULL,
		PRIMARY KEY (conversation_id, sender_id)
	)`,
	`CREATE TABLE segments (
		id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
		root_thread_id TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('pending','open','superseded')),
		created_event_id TEXT, superseded_by TEXT,
		UNIQUE (conversation_id, root_thread_id)
	)`,
	`CREATE TABLE inbound_events (
		ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
		conversation_id TEXT NOT NULL REFERENCES conversations(id), event_id TEXT NOT NULL,
		message_id TEXT NOT NULL, thread_id TEXT NOT NULL, segment_id TEXT REFERENCES segments(id),
		body TEXT, disposition TEXT NOT NULL, pointer_thread_id TEXT NOT NULL DEFAULT '',
		turn_id TEXT,
		UNIQUE (conversation_id, event_id)
	)`,
	`CREATE INDEX inbound_queued ON inbound_events(conversation_id, segment_id, ordinal)
		WHERE disposition='accepted' AND turn_id IS NULL`,
	`CREATE TABLE turns (
		id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
		number INTEGER NOT NULL, event_ordinal INTEGER NOT NULL UNIQUE REFERENCES inbound_events(ordinal),
		segment_id TEXT NOT NULL REFERENCES segments(id), status TEXT NOT NULL CHECK (status IN ('active','completed')),
		acceptance_id TEXT NOT NULL DEFAULT '',
		attempt_id TEXT NOT NULL DEFAULT '', baseline_turn_id TEXT NOT NULL DEFAULT '',
		external_turn_id TEXT NOT NULL DEFAULT '',
		row_operation_id TEXT NOT NULL DEFAULT '', operation_state TEXT NOT NULL DEFAULT '',
		accepted_codex_session_id TEXT NOT NULL DEFAULT '', accepted_turn_generation TEXT NOT NULL DEFAULT '',
		terminal_error TEXT NOT NULL DEFAULT '',
		attempt_state TEXT NOT NULL DEFAULT 'unprepared'
			CHECK (attempt_state IN ('unprepared','prepared','accepted','needs_reconciliation','completed')),
		CHECK (operation_state IN ('','queued','preparing','accepted','completed','refused',
			'binding_changed','expired','indeterminate','result_unavailable')),
		UNIQUE (conversation_id, number)
	)`,
	`CREATE UNIQUE INDEX one_active_turn ON turns(conversation_id) WHERE status='active'`,
	`CREATE UNIQUE INDEX one_ledger_turn_per_external_turn ON turns(conversation_id,external_turn_id)
		WHERE external_turn_id != ''`,
	`CREATE UNIQUE INDEX one_ledger_turn_per_row_operation ON turns(row_operation_id)
		WHERE row_operation_id != ''`,
	`CREATE TABLE outbox (
		ordinal INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
		conversation_id TEXT NOT NULL REFERENCES conversations(id),
		turn_id TEXT NOT NULL REFERENCES turns(id), kind TEXT NOT NULL,
		thread_id TEXT NOT NULL, body TEXT NOT NULL,
		state TEXT NOT NULL CHECK (state IN ('pending','sending','uncertain','delivered')),
		delivery_attempt_id TEXT NOT NULL DEFAULT '',
		external_message_id TEXT NOT NULL DEFAULT '',
		CHECK ((state='pending' AND delivery_attempt_id='' AND external_message_id='') OR
			(state IN ('sending','uncertain') AND delivery_attempt_id!='' AND external_message_id='') OR
			(state='delivered' AND delivery_attempt_id!='' AND external_message_id!=''))
	)`,
	`CREATE INDEX outbox_pending ON outbox(conversation_id, ordinal) WHERE state='pending'`,
	`CREATE UNIQUE INDEX outbox_provider_message ON outbox(conversation_id,external_message_id)
		WHERE external_message_id != ''`,
}

func validConversation(c Conversation) bool {
	if c.ID == "" || c.ChannelID == "" || c.ConductorID == "" || c.RowInstanceID == "" ||
		c.RowBinding == "" || len(c.AllowedSenders) == 0 ||
		(c.Mode != ChannelStream && c.Mode != ThreadSegments) {
		return false
	}
	seen := map[string]bool{}
	for _, sender := range c.AllowedSenders {
		if sender == "" || seen[sender] {
			return false
		}
		seen[sender] = true
	}
	return true
}

// CreateConversation is idempotent for an identical binding and rejects a
// changed binding. This keeps authorization explicit across restarts.
func (s *Store) CreateConversation(ctx context.Context, c Conversation) error {
	if !validConversation(c) {
		return ErrInvalid
	}
	return s.write(ctx, func(tx *writeTx) error {
		var channel, conductor, rowInstance, rowBinding, mode string
		err := tx.row(`SELECT channel_id, conductor_id, row_instance_id, row_binding_token, mode FROM conversations WHERE id=?`, c.ID).
			Scan(&channel, &conductor, &rowInstance, &rowBinding, &mode)
		if err == nil {
			if channel != c.ChannelID || conductor != c.ConductorID || rowInstance != c.RowInstanceID ||
				rowBinding != c.RowBinding || Mode(mode) != c.Mode {
				return ErrConflict
			}
			rows, err := tx.conn.QueryContext(ctx, `SELECT sender_id FROM allowed_senders WHERE conversation_id=? ORDER BY sender_id`, c.ID)
			if err != nil {
				return ErrStorage
			}
			var got []string
			for rows.Next() {
				var v string
				if err := rows.Scan(&v); err != nil {
					_ = rows.Close()
					return ErrStorage
				}
				got = append(got, v)
			}
			err = rows.Err()
			_ = rows.Close()
			if err != nil {
				return ErrStorage
			}
			want := slices.Clone(c.AllowedSenders)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				return ErrConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ErrStorage
		}
		if _, err := tx.exec(`INSERT INTO conversations(id,channel_id,conductor_id,mode,row_instance_id,row_binding_token)
			VALUES(?,?,?,?,?,?)`, c.ID, c.ChannelID, c.ConductorID, c.Mode, c.RowInstanceID, c.RowBinding); err != nil {
			return err
		}
		for _, sender := range c.AllowedSenders {
			if _, err := tx.exec(`INSERT INTO allowed_senders(conversation_id,sender_id) VALUES(?,?)`, c.ID, sender); err != nil {
				return err
			}
		}
		if c.Mode == ChannelStream {
			id := "stream:" + c.ID
			if _, err := tx.exec(`INSERT INTO segments(id,conversation_id,root_thread_id,state) VALUES(?,?,?,'open')`, id, c.ID, ""); err != nil {
				return err
			}
			_, err = tx.exec(`UPDATE conversations SET active_segment_id=? WHERE id=?`, id, c.ID)
			return err
		}
		return nil
	})
}
