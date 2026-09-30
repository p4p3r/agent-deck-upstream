package sendqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

var (
	// ErrIdempotencyConflict means a key is already bound to different
	// immutable request bytes.
	ErrIdempotencyConflict = errors.New("sendqueue: idempotency key conflict")
)

// CorrelatedRequest is the immutable input used to create or recover one
// durable row-turn operation.
type CorrelatedRequest struct {
	SessionID       string
	IdempotencyKey  string
	RowBindingToken string
	Message         string
	Now             time.Time
	Deadline        time.Time
	InitialState    string
	OperationError  string
	RetrySafe       bool
}

type idempotencyIndex struct {
	Version       int    `json:"version"`
	KeyDigest     string `json:"key_digest"`
	RequestDigest string `json:"request_digest"`
	SendID        string `json:"send_id"`
	CreatedAt     string `json:"created_at"`
}

// EnqueueCorrelated creates one operation for a key or returns the existing
// operation for an identical retry. A key is global to the queue directory,
// so changing the row, binding, or body is always a conflict.
func EnqueueCorrelated(dir string, request CorrelatedRequest) (*Record, bool, error) {
	if err := validateCorrelatedRequest(request); err != nil {
		return nil, false, err
	}
	if err := ensurePrivateDir(dir); err != nil {
		return nil, false, err
	}
	lock, err := lockFile(dir, ".correlated-enqueue.lock", false)
	if err != nil {
		return nil, false, err
	}
	defer lock.Release()

	keyDigest := digestString(request.IdempotencyKey)
	requestDigest := correlatedRequestDigest(request)
	if record, found, err := lookupCorrelatedLocked(dir, request, keyDigest, requestDigest); err != nil || found {
		return record, false, err
	}

	now := request.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	deadline := request.Deadline
	if deadline.IsZero() {
		deadline = now.Add(RetainUncertainBodies)
	}
	id, err := NextID(dir, now)
	if err != nil {
		return nil, false, err
	}
	state := request.InitialState
	if state == "" {
		state = OperationQueued
	}
	record := &Record{
		SchemaVersion: RecordSchemaVersion,
		SendID:        id, SessionID: request.SessionID, IdempotencyKey: request.IdempotencyKey,
		RowBindingToken: request.RowBindingToken, OperationState: state,
		Message: request.Message, CreatedAt: now.Format(time.RFC3339Nano),
		UpdatedAt: now.Format(time.RFC3339Nano), Deadline: deadline.UTC().Format(time.RFC3339Nano),
		State: StateQueued, Verdict: "queued", OperationError: request.OperationError,
	}
	if operationFinal(state) {
		record.State = StateFailed
		record.Verdict = "unknown"
		retrySafe := request.RetrySafe
		record.RetrySafe = &retrySafe
	}
	if err := Save(dir, record); err != nil {
		return nil, false, err
	}
	index := newIdempotencyIndex(keyDigest, requestDigest, id, record.CreatedAt)
	if err := saveIdempotencyIndex(dir, index); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

// LookupCorrelated returns an operation already owned by an identical request
// without creating queue state. It lets response-loss retries survive row
// removal while ensuring an unseen title or id cannot become a target.
func LookupCorrelated(dir string, request CorrelatedRequest) (*Record, bool, error) {
	if err := validateCorrelatedRequest(request); err != nil {
		return nil, false, err
	}
	if err := validateExistingPrivateDir(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	lock, err := lockFile(dir, ".correlated-enqueue.lock", false)
	if err != nil {
		return nil, false, err
	}
	defer lock.Release()
	return lookupCorrelatedLocked(dir, request, digestString(request.IdempotencyKey), correlatedRequestDigest(request))
}

func lookupCorrelatedLocked(dir string, request CorrelatedRequest, keyDigest, requestDigest string) (*Record, bool, error) {
	index, found, err := loadIdempotencyIndex(dir, keyDigest)
	if err != nil {
		return nil, false, err
	}
	if found {
		if index.RequestDigest != requestDigest {
			return nil, false, ErrIdempotencyConflict
		}
		record, loadErr := Load(dir, index.SendID)
		if loadErr == nil {
			return record, true, nil
		}
		if !errors.Is(loadErr, ErrUnknown) {
			return nil, false, loadErr
		}
		// The small index is the correctness tombstone after ordinary terminal
		// metadata expires. It returns the original operation id and never
		// permits the request to be transported again.
		retrySafe := true
		return &Record{
			SchemaVersion: RecordSchemaVersion, SendID: index.SendID,
			SessionID: request.SessionID, IdempotencyKey: request.IdempotencyKey,
			RowBindingToken: request.RowBindingToken, OperationState: OperationExpired,
			OperationError: OperationExpired, RetrySafe: &retrySafe,
			CreatedAt: index.CreatedAt, UpdatedAt: index.CreatedAt,
		}, true, nil
	}

	// A crash can publish the operation before its index. Scan while holding
	// the global enqueue lock, repair the index, and return that same operation.
	records, err := List(dir, "")
	if err != nil {
		return nil, false, err
	}
	for _, record := range records {
		if !record.Correlated() || record.IdempotencyKey != request.IdempotencyKey {
			continue
		}
		if correlatedRecordDigest(record) != requestDigest {
			return nil, false, ErrIdempotencyConflict
		}
		index = newIdempotencyIndex(keyDigest, requestDigest, record.SendID, record.CreatedAt)
		if err := saveIdempotencyIndex(dir, index); err != nil {
			return nil, false, err
		}
		return record, true, nil
	}
	return nil, false, nil
}

func validateCorrelatedRequest(request CorrelatedRequest) error {
	if strings.TrimSpace(request.SessionID) == "" || request.SessionID != strings.TrimSpace(request.SessionID) {
		return fmt.Errorf("sendqueue: invalid immutable session id")
	}
	if err := ValidateOpaque(request.IdempotencyKey); err != nil {
		return fmt.Errorf("sendqueue: invalid idempotency key: %w", err)
	}
	if err := ValidateOpaque(request.RowBindingToken); err != nil {
		return fmt.Errorf("sendqueue: invalid row binding token: %w", err)
	}
	if len(request.Message) > maxRecordSize/2 {
		return fmt.Errorf("sendqueue: message exceeds correlated operation limit")
	}
	state := request.InitialState
	if state == "" {
		state = OperationQueued
	}
	if !validOperationState(state) {
		return fmt.Errorf("sendqueue: invalid initial operation state")
	}
	return nil
}

// ValidateOpaque accepts one bounded, path-independent public token.
func ValidateOpaque(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
		return fmt.Errorf("empty, padded, or oversized value")
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e || r == '/' || r == '\\' {
			return fmt.Errorf("value contains an unsafe character")
		}
	}
	if strings.Contains(value, "..") {
		return fmt.Errorf("value contains a path traversal segment")
	}
	return nil
}

func correlatedRequestDigest(request CorrelatedRequest) string {
	h := sha256.New()
	for _, value := range []string{request.SessionID, request.RowBindingToken, request.Message} {
		fmt.Fprintf(h, "%d:", len(value))
		_, _ = io.WriteString(h, value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func correlatedRecordDigest(record *Record) string {
	return correlatedRequestDigest(CorrelatedRequest{
		SessionID: record.SessionID, RowBindingToken: record.RowBindingToken, Message: record.Message,
	})
}

func digestString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func newIdempotencyIndex(keyDigest, requestDigest, sendID, createdAt string) *idempotencyIndex {
	return &idempotencyIndex{Version: 1, KeyDigest: keyDigest, RequestDigest: requestDigest, SendID: sendID, CreatedAt: createdAt}
}

func idempotencyDir(dir string) string { return filepath.Join(dir, "idempotency") }

func loadIdempotencyIndex(dir, keyDigest string) (*idempotencyIndex, bool, error) {
	indexDir := idempotencyDir(dir)
	if err := validateExistingPrivateDir(indexDir); err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	path := filepath.Join(indexDir, keyDigest+".json")
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	if err := validatePrivateFile(f); err != nil {
		return nil, false, err
	}
	data, err := io.ReadAll(io.LimitReader(f, 16<<10))
	if err != nil {
		return nil, false, err
	}
	if len(data) >= 16<<10 {
		return nil, false, fmt.Errorf("sendqueue: oversized idempotency index")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return nil, false, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var index idempotencyIndex
	if err := decoder.Decode(&index); err != nil {
		return nil, false, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, false, fmt.Errorf("sendqueue: trailing idempotency index data")
	}
	if index.Version != 1 || index.KeyDigest != keyDigest || !validID(index.SendID) || len(index.RequestDigest) != 64 {
		return nil, false, fmt.Errorf("sendqueue: invalid idempotency index")
	}
	return &index, true, nil
}

func saveIdempotencyIndex(dir string, index *idempotencyIndex) error {
	indexDir := idempotencyDir(dir)
	if err := ensurePrivateDir(indexDir); err != nil {
		return err
	}
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return publishFile(indexDir, index.KeyDigest+".json", data)
}

// RetentionPolicy controls body scrubbing and ordinary terminal metadata.
type RetentionPolicy struct {
	ConfirmedBodies  time.Duration
	UncertainBodies  time.Duration
	TerminalMetadata time.Duration
}

// DefaultRetentionPolicy returns the durable row-turn retention defaults.
func DefaultRetentionPolicy() RetentionPolicy {
	return RetentionPolicy{
		ConfirmedBodies:  RetainConfirmedBodies,
		UncertainBodies:  RetainUncertainBodies,
		TerminalMetadata: RetainTerminalMetadata,
	}
}

// PruneWithRetention scrubs request bodies on their configured schedule and
// removes only ordinary terminal metadata. Active and uncertain operations
// are never removed; their idempotency indexes remain as tombstones.
func PruneWithRetention(dir string, now time.Time, policy RetentionPolicy) {
	records, err := List(dir, "")
	if err != nil {
		return
	}
	for _, record := range records {
		if !record.Correlated() {
			continue
		}
		updated, err := time.Parse(time.RFC3339Nano, record.UpdatedAt)
		if err != nil {
			continue
		}
		uncertain := record.OperationState == OperationIndeterminate || record.OperationState == OperationResultUnavailable
		active := !record.Final()
		bodyRetention := policy.ConfirmedBodies
		if uncertain {
			bodyRetention = policy.UncertainBodies
		}
		bodyNoLongerNeeded := record.OperationState == OperationAccepted || record.Final()
		if bodyNoLongerNeeded && record.Message != "" && bodyRetention > 0 && now.Sub(updated) >= bodyRetention {
			_, _ = Update(dir, record.SendID, now, func(current *Record) {
				current.Message = ""
				current.Images = nil
				current.BodyScrubbedAt = now.UTC().Format(time.RFC3339Nano)
			})
		}
		if active || uncertain || policy.TerminalMetadata <= 0 || now.Sub(updated) < policy.TerminalMetadata {
			continue
		}
		_ = os.Remove(ResultPath(dir, record.SendID))
		_ = os.Remove(filepath.Join(dir, record.SendID+".message"))
		_ = os.Remove(filepath.Join(dir, record.SendID+".json"))
		_ = fsyncDir(dir)
	}
}
