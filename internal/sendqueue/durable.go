package sendqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func ensurePrivateDir(dir string) error {
	_, statErr := os.Lstat(dir)
	created := os.IsNotExist(statErr)
	if statErr != nil && !created {
		return statErr
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := validateExistingPrivateDir(dir); err != nil {
		return err
	}
	if created {
		parent := filepath.Dir(dir)
		if parent != dir {
			if err := fsyncDir(parent); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateExistingPrivateDir(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("sendqueue: unsafe directory type")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("sendqueue: directory permissions %o are not private", info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("sendqueue: directory is not owned by the current user")
	}
	return nil
}

func validatePrivateFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("wrong file type")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("permissions %o are not private", info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("file is not owned by the current user")
	}
	return nil
}

func publishFile(dir, name string, data []byte) error {
	tmp, err := os.CreateTemp(dir, "."+name+"-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, name)); err != nil {
		return err
	}
	return fsyncDir(dir)
}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func lockFile(dir, name string, nonblocking bool) (*Lock, error) {
	if err := ensurePrivateDir(dir); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if err := validatePrivateFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	mode := syscall.LOCK_EX
	if nonblocking {
		mode |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(f.Fd()), mode); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f: f}, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("object key is not a string")
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("invalid object terminator")
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("invalid array terminator")
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func validateRecord(record *Record, filenameID string) error {
	if record == nil || !validID(record.SendID) || (filenameID != "" && record.SendID != filenameID) {
		return fmt.Errorf("sendqueue: record identity does not match filename")
	}
	if record.SchemaVersion == 0 {
		if record.IdempotencyKey != "" || record.RowBindingToken != "" || record.OperationState != "" || record.CodexSessionID != "" ||
			record.AcceptedTurn != nil || record.Completion != nil || record.Content != "" || record.OperationError != "" ||
			record.RetrySafe != nil || record.BodyScrubbedAt != "" {
			return fmt.Errorf("sendqueue: correlated fields require a record schema version")
		}
		return nil
	}
	if record.SchemaVersion != RecordSchemaVersion {
		return fmt.Errorf("sendqueue: unsupported record schema version %d", record.SchemaVersion)
	}
	if strings.TrimSpace(record.SessionID) == "" || record.SessionID != strings.TrimSpace(record.SessionID) {
		return fmt.Errorf("sendqueue: invalid correlated session identity")
	}
	if err := ValidateOpaque(record.IdempotencyKey); err != nil {
		return fmt.Errorf("sendqueue: invalid correlated idempotency key: %w", err)
	}
	if err := ValidateOpaque(record.RowBindingToken); err != nil {
		return fmt.Errorf("sendqueue: invalid correlated row binding: %w", err)
	}
	if !validOperationState(record.OperationState) {
		return fmt.Errorf("sendqueue: invalid correlated operation state %q", record.OperationState)
	}
	if record.AcceptedTurn != nil {
		turn := record.AcceptedTurn
		if turn.InstanceID != record.SessionID || turn.ReceiptID == "" || turn.CodexSessionID == "" ||
			turn.TurnGeneration == "" || !strings.HasPrefix(turn.TurnGeneration, turn.CodexSessionID+":") {
			return fmt.Errorf("sendqueue: invalid accepted turn")
		}
	}
	if record.Completion != nil {
		if record.AcceptedTurn == nil || record.Completion.TurnGeneration != record.AcceptedTurn.TurnGeneration {
			return fmt.Errorf("sendqueue: completion generation does not match accepted turn")
		}
	}
	if record.OperationState == OperationCompleted && (record.AcceptedTurn == nil || record.Completion == nil) {
		return fmt.Errorf("sendqueue: completed operation lacks exact generation evidence")
	}
	return nil
}

func validOperationState(state string) bool {
	switch state {
	case OperationQueued, OperationPreparing, OperationAccepted, OperationCompleted,
		OperationRefused, OperationBindingChanged, OperationExpired,
		OperationIndeterminate, OperationResultUnavailable:
		return true
	default:
		return false
	}
}

func operationFinal(state string) bool {
	switch state {
	case OperationCompleted, OperationRefused, OperationBindingChanged, OperationExpired,
		OperationIndeterminate, OperationResultUnavailable:
		return true
	default:
		return false
	}
}

func validateUpdate(before, after *Record) error {
	if before.SchemaVersion == 0 && after.SchemaVersion == 0 {
		return nil
	}
	if before.SchemaVersion != after.SchemaVersion || before.SendID != after.SendID ||
		before.SessionID != after.SessionID || before.IdempotencyKey != after.IdempotencyKey ||
		before.RowBindingToken != after.RowBindingToken {
		return fmt.Errorf("sendqueue: correlated operation identity is immutable")
	}
	if before.CodexSessionID != "" && before.CodexSessionID != after.CodexSessionID {
		return fmt.Errorf("sendqueue: correlated Codex identity is immutable")
	}
	if before.OperationState != OperationQueued && (before.Tool != after.Tool ||
		before.HarnessAccount != after.HarnessAccount || before.HarnessCommand != after.HarnessCommand ||
		before.HarnessWrapper != after.HarnessWrapper) {
		return fmt.Errorf("sendqueue: correlated harness binding is immutable")
	}
	if !operationTransitionAllowed(before.OperationState, after.OperationState) {
		return fmt.Errorf("sendqueue: operation state cannot transition from %s to %s", before.OperationState, after.OperationState)
	}
	if before.AcceptedTurn != nil && !sameAcceptedTurn(before.AcceptedTurn, after.AcceptedTurn) {
		return fmt.Errorf("sendqueue: accepted turn is immutable")
	}
	if before.Completion != nil && !sameCompletion(before.Completion, after.Completion) {
		return fmt.Errorf("sendqueue: completion is immutable")
	}
	return validateRecord(after, after.SendID)
}

func operationTransitionAllowed(from, to string) bool {
	if from == to {
		return true
	}
	if operationFinal(from) {
		return false
	}
	switch from {
	case OperationQueued:
		return to == OperationPreparing || to == OperationRefused || to == OperationBindingChanged || to == OperationExpired
	case OperationPreparing:
		return to == OperationAccepted || to == OperationCompleted || to == OperationRefused || to == OperationIndeterminate || to == OperationBindingChanged || to == OperationExpired
	case OperationAccepted:
		return to == OperationCompleted || to == OperationResultUnavailable
	default:
		return false
	}
}

func sameAcceptedTurn(a, b *AcceptedTurn) bool {
	return a != nil && b != nil && *a == *b
}

func sameCompletion(a, b *Completion) bool {
	return a != nil && b != nil && *a == *b
}
