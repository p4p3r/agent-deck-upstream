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
}

type OutboxItem struct {
	ID                string
	ConversationID    string
	TurnID            string
	Kind              string
	ThreadID          string
	Body              string
	ExternalMessageID string
}

type Store struct{ db *sql.DB }

const schemaVersion = 2

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
		attempt_state TEXT NOT NULL DEFAULT 'unprepared'
			CHECK (attempt_state IN ('unprepared','prepared','accepted','needs_reconciliation','completed')),
		UNIQUE (conversation_id, number)
	)`,
	`CREATE UNIQUE INDEX one_active_turn ON turns(conversation_id) WHERE status='active'`,
	`CREATE UNIQUE INDEX one_ledger_turn_per_external_turn ON turns(conversation_id,external_turn_id)
		WHERE external_turn_id != ''`,
	`CREATE TABLE outbox (
		ordinal INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
		conversation_id TEXT NOT NULL REFERENCES conversations(id),
		turn_id TEXT NOT NULL REFERENCES turns(id), kind TEXT NOT NULL,
		thread_id TEXT NOT NULL, body TEXT NOT NULL,
		state TEXT NOT NULL CHECK (state IN ('pending','delivered')),
		external_message_id TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX outbox_pending ON outbox(conversation_id, ordinal) WHERE state='pending'`,
}

func validConversation(c Conversation) bool {
	if c.ID == "" || c.ChannelID == "" || c.ConductorID == "" || len(c.AllowedSenders) == 0 ||
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
		var channel, conductor, mode string
		err := tx.row(`SELECT channel_id, conductor_id, mode FROM conversations WHERE id=?`, c.ID).Scan(&channel, &conductor, &mode)
		if err == nil {
			if channel != c.ChannelID || conductor != c.ConductorID || Mode(mode) != c.Mode {
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
		if _, err := tx.exec(`INSERT INTO conversations(id,channel_id,conductor_id,mode) VALUES(?,?,?,?)`, c.ID, c.ChannelID, c.ConductorID, c.Mode); err != nil {
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
