// Package sendqueue is the durable outbox behind `agent-deck session send
// --queue`: one JSON record per send under <profile dir>/sendqueue/, walked
// in order by a detached per-target worker that waits for the target to be
// idle, delivers through the normal `session send` path, and watches the
// native transcript until the message lands. A queued send is never
// dropped silently: it ends as landed, failed with a reason, or settled
// typed/submitted when its outcome could not be proven.
//
// Delivery is at most once. The record moves to typing, durably, before
// anything is typed; a worker that finds a typing record (the previous one
// died mid-send) reconciles it against the child's result file and the
// transcript and never types it again.
package sendqueue

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// States of a queued send, in order. failed is terminal like landed.
const (
	StateQueued    = "queued"    // waiting for the target to be idle
	StateTyping    = "typing"    // handed to `session send`; outcome not known yet
	StateTyped     = "typed"     // typed into the composer, submission not confirmed
	StateSubmitted = "submitted" // submission confirmed by the harness
	StateLanded    = "landed"    // the text is in the transcript (landed_row_id)
	StateFailed    = "failed"    // gave up; reason says why
)

// DefaultRetryBudget is how long a send may wait for a busy target.
const DefaultRetryBudget = 30 * time.Minute

// RetainFinished is how long finished records stay readable by send-status.
const RetainFinished = 7 * 24 * time.Hour

// Correlated operation retention defaults keep request bodies for the
// shortest useful window while retaining small correctness records longer.
const (
	RetainConfirmedBodies  = 24 * time.Hour
	RetainUncertainBodies  = 7 * 24 * time.Hour
	RetainTerminalMetadata = 90 * 24 * time.Hour
)

const (
	RecordSchemaVersion = 1
	maxRecordSize       = 4 << 20

	OperationQueued            = "queued"
	OperationPreparing         = "preparing"
	OperationAccepted          = "accepted"
	OperationCompleted         = "completed"
	OperationRefused           = "refused"
	OperationBindingChanged    = "binding_changed"
	OperationExpired           = "expired"
	OperationIndeterminate     = "indeterminate"
	OperationResultUnavailable = "result_unavailable"
)

// AcceptedTurn is the immutable Codex generation assigned to an operation.
type AcceptedTurn struct {
	ReceiptID      string `json:"receipt_id"`
	InstanceID     string `json:"instance_id"`
	CodexSessionID string `json:"codex_session_id"`
	TurnGeneration string `json:"turn_generation"`
	AcceptedAt     string `json:"accepted_at"`
}

// Completion identifies the exact accepted generation whose result was
// durably stored.
type Completion struct {
	TurnGeneration string `json:"turn_generation"`
	CompletedAt    string `json:"completed_at,omitempty"`
}

// Record is one queued send. It is also the `send-status --json` object.
type Record struct {
	SchemaVersion int      `json:"schema_version,omitempty"`
	SendID        string   `json:"send_id"`
	Verdict       string   `json:"verdict"`
	State         string   `json:"state"`
	Reason        string   `json:"reason"`
	TargetStatus  string   `json:"target_status"`
	SessionID     string   `json:"session_id"`
	SessionTitle  string   `json:"session_title,omitempty"`
	Tool          string   `json:"tool,omitempty"`
	Message       string   `json:"message"`
	Images        []string `json:"images,omitempty"`
	CreatedAt     string   `json:"created_at"`
	UpdatedAt     string   `json:"updated_at"`
	Deadline      string   `json:"deadline"`
	Attempts      int      `json:"attempts"`
	SentAt        string   `json:"sent_at,omitempty"`
	// ChildPID is the `session send` process delivering a typing record;
	// its result lands in ResultPath(dir, send_id).
	ChildPID       int    `json:"child_pid,omitempty"`
	TranscriptPath string `json:"transcript_path,omitempty"`
	TranscriptFrom int64  `json:"transcript_from,omitempty"`
	LandedRowID    string `json:"landed_row_id,omitempty"`
	LandedAt       string `json:"landed_at,omitempty"`
	// ClaudeSessionID is the Claude conversation the send went to: the
	// target's at queue time, then the delivering child's, then the
	// transcript the row landed in (#2397). Empty for other tools.
	ClaudeSessionID string `json:"claude_session_id,omitempty"`
	// Settled marks a typed/submitted send whose text was not found in the
	// transcript within the watch window: it is never typed again.
	Settled bool `json:"settled,omitempty"`

	// Correlated operation fields are absent from legacy queue records.
	IdempotencyKey  string        `json:"idempotency_key,omitempty"`
	RowBindingToken string        `json:"row_binding_token,omitempty"`
	OperationState  string        `json:"operation_state,omitempty"`
	CodexSessionID  string        `json:"codex_session_id,omitempty"`
	HarnessAccount  string        `json:"harness_account,omitempty"`
	HarnessCommand  string        `json:"harness_command,omitempty"`
	HarnessWrapper  string        `json:"harness_wrapper,omitempty"`
	AcceptedTurn    *AcceptedTurn `json:"accepted_turn,omitempty"`
	Completion      *Completion   `json:"completion,omitempty"`
	Content         string        `json:"content,omitempty"`
	OperationError  string        `json:"operation_error,omitempty"`
	RetrySafe       *bool         `json:"retry_safe,omitempty"`
	BodyScrubbedAt  string        `json:"body_scrubbed_at,omitempty"`
}

// Final reports whether the worker is done with the record.
func (r *Record) Final() bool {
	if r != nil && r.SchemaVersion == RecordSchemaVersion {
		switch r.OperationState {
		case OperationCompleted, OperationRefused, OperationBindingChanged, OperationExpired,
			OperationIndeterminate, OperationResultUnavailable:
			return true
		default:
			return false
		}
	}
	return r != nil && (r.State == StateLanded || r.State == StateFailed || r.Settled)
}

// Correlated reports whether this is a versioned row-turn operation.
func (r *Record) Correlated() bool {
	return r != nil && r.SchemaVersion == RecordSchemaVersion
}

// ResultPath is where the delivering `session send` child writes its JSON
// result, so a worker restarted mid-send can still read the outcome.
func ResultPath(dir, id string) string { return filepath.Join(dir, id+".result") }

// Dir is the queue directory of a profile.
func Dir(profileDir string) string { return filepath.Join(profileDir, "sendqueue") }

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewID returns a ULID: 48-bit milliseconds then 80 random bits, Crockford
// base32, so ids sort by creation time.
func NewID(now time.Time) string {
	var b [16]byte
	ms := uint64(now.UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	_, _ = rand.Read(b[6:])
	// 128 bits -> 26 chars, 5 bits each, most significant first.
	out := make([]byte, 26)
	var acc uint64
	bits := uint(2) // leading pad: 26*5 = 130 bits
	idx := 0
	for _, x := range b {
		acc = acc<<8 | uint64(x)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out[idx] = crockford[(acc>>bits)&31]
			idx++
		}
	}
	return string(out[:idx])
}

// NextID returns a new id that sorts after every id NextID handed out
// before in dir, even for callers in the same millisecond or with a clock
// that stepped back: send order is id order.
func NextID(dir string, now time.Time) (string, error) {
	if err := ensurePrivateDir(dir); err != nil {
		return "", err
	}
	path := filepath.Join(dir, ".last-id")
	_, statErr := os.Lstat(path)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return "", statErr
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := validatePrivateFile(f); err != nil {
		return "", fmt.Errorf("sendqueue: unsafe id sequence: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return "", err
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	buf := make([]byte, 64)
	n, _ := f.ReadAt(buf, 0)
	last := strings.TrimSpace(string(buf[:n]))
	id := NewID(now)
	if validID(last) && id <= last {
		id = incrementID(last)
	}
	if err := f.Truncate(0); err != nil {
		return "", err
	}
	if _, err := f.WriteAt([]byte(id), 0); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", err
	}
	if created {
		if err := fsyncDir(dir); err != nil {
			return "", err
		}
	}
	return id, nil
}

// incrementID returns the next id in Crockford base32 order.
func incrementID(id string) string {
	b := []byte(id)
	for i := len(b) - 1; i >= 0; i-- {
		j := strings.IndexByte(crockford, b[i])
		if j < len(crockford)-1 {
			b[i] = crockford[j+1]
			return string(b)
		}
		b[i] = crockford[0]
	}
	return string(b)
}

func validID(id string) bool {
	if len(id) != 26 {
		return false
	}
	for _, c := range id {
		if !strings.ContainsRune(crockford, c) {
			return false
		}
	}
	return true
}

// Save writes and durably publishes a record atomically.
func Save(dir string, r *Record) error {
	if err := ensurePrivateDir(dir); err != nil {
		return err
	}
	if err := validateRecord(r, r.SendID); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if len(b) > maxRecordSize {
		return fmt.Errorf("sendqueue: record exceeds %d bytes", maxRecordSize)
	}
	return publishFile(dir, r.SendID+".json", b)
}

// ErrUnknown is returned for a send id with no record.
var ErrUnknown = errors.New("sendqueue: unknown send id")

// Load reads one record.
func Load(dir, id string) (*Record, error) {
	if !validID(id) {
		return nil, ErrUnknown
	}
	if err := validateExistingPrivateDir(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrUnknown
		}
		return nil, err
	}
	path := filepath.Join(dir, id+".json")
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		return nil, ErrUnknown
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := validatePrivateFile(f); err != nil {
		return nil, fmt.Errorf("sendqueue: unsafe record: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxRecordSize {
		return nil, fmt.Errorf("sendqueue: record exceeds %d bytes", maxRecordSize)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxRecordSize+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxRecordSize {
		return nil, fmt.Errorf("sendqueue: record exceeds %d bytes", maxRecordSize)
	}
	if err := rejectDuplicateJSONKeys(b); err != nil {
		return nil, fmt.Errorf("sendqueue: parse record: %w", err)
	}
	var r Record
	decoder := json.NewDecoder(strings.NewReader(string(b)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("sendqueue: trailing record data")
	}
	if err := validateRecord(&r, id); err != nil {
		return nil, err
	}
	return &r, nil
}

// List returns every record, oldest first; sessionID filters when set.
func List(dir, sessionID string) ([]*Record, error) {
	if err := validateExistingPrivateDir(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []*Record
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		r, err := Load(dir, strings.TrimSuffix(name, ".json"))
		if err != nil {
			return nil, fmt.Errorf("sendqueue: load %s: %w", name, err)
		}
		if sessionID == "" || r.SessionID == sessionID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SendID < out[j].SendID })
	return out, nil
}

// Update loads, mutates and saves a record, stamping updated_at.
func Update(dir, id string, now time.Time, fn func(*Record)) (*Record, error) {
	lock, err := lockFile(dir, ".record-"+id+".lock", false)
	if err != nil {
		return nil, err
	}
	defer lock.Release()
	r, err := Load(dir, id)
	if err != nil {
		return nil, err
	}
	before := *r
	fn(r)
	r.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	if err := validateUpdate(&before, r); err != nil {
		return nil, err
	}
	return r, Save(dir, r)
}

// PendingTargets lists the sessions that still have a record the worker is
// not done with, so a caller can restart their workers (after a reboot).
func PendingTargets(dir string) []string {
	recs, _ := List(dir, "")
	var out []string
	seen := map[string]bool{}
	for _, r := range recs {
		if !r.Final() && !seen[r.SessionID] {
			seen[r.SessionID] = true
			out = append(out, r.SessionID)
		}
	}
	return out
}

// Prune deletes finished records (and their child results) last updated
// before cutoff, so the queue directory does not grow without bound.
func Prune(dir string, cutoff time.Time) {
	recs, _ := List(dir, "")
	for _, r := range recs {
		if r.Correlated() {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, r.UpdatedAt)
		if !r.Final() || err != nil || !t.Before(cutoff) {
			continue
		}
		_ = os.Remove(ResultPath(dir, r.SendID))
		_ = os.Remove(filepath.Join(dir, r.SendID+".message"))
		_ = os.Remove(filepath.Join(dir, r.SendID+".json"))
	}
}

// Lock is a held per-target worker lock.
type Lock struct{ f *os.File }

// TryLock takes the target's worker lock without blocking; ok is false
// when another worker owns the target.
func TryLock(dir, sessionID string) (*Lock, bool, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, false, fmt.Errorf("sendqueue: empty lock identity")
	}
	if err := ensurePrivateDir(dir); err != nil {
		return nil, false, err
	}
	digest := sha256.Sum256([]byte(sessionID))
	name := fmt.Sprintf("target-%x.lock", digest[:])
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := validatePrivateFile(f); err != nil {
		f.Close()
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("sendqueue: lock %s: %w", sessionID, err)
	}
	return &Lock{f: f}, true, nil
}

// Release drops the lock.
func (l *Lock) Release() {
	if l != nil && l.f != nil {
		_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
		l.f.Close()
		l.f = nil
	}
}
