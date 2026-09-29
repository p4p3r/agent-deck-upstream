// Package conductorlock provides a process-lifetime singleton for a named
// conductor. The lock excludes another owner of the same conductor; it does
// not claim ownership of descendants after the owning process exits.
package conductorlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

var (
	// ErrHeld means another process already owns this named conductor.
	ErrHeld    = errors.New("conductor lock held")
	ErrInvalid = errors.New("invalid conductor lock target")
	ErrIO      = errors.New("conductor lock unavailable")
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Lock holds a private advisory lock until Close. It must not be copied.
type Lock struct {
	mu   sync.Mutex
	file *os.File
}

// Error contains only a category; paths, credentials, and provider data are
// deliberately excluded from its public text.
type Error struct {
	Kind error
}

func (e *Error) Error() string { return e.Kind.Error() }
func (e *Error) Unwrap() error { return e.Kind }

func lockPath(name, conductorDir string) (string, error) {
	if len(name) == 0 || len(name) > 64 || !namePattern.MatchString(name) {
		return "", &Error{Kind: ErrInvalid}
	}
	if !filepath.IsAbs(conductorDir) || conductorDir != filepath.Clean(conductorDir) || filepath.Base(conductorDir) != name {
		return "", &Error{Kind: ErrInvalid}
	}
	canonical, err := filepath.EvalSymlinks(conductorDir)
	if err != nil || canonical != conductorDir {
		return "", &Error{Kind: ErrInvalid}
	}
	info, err := os.Lstat(conductorDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !ownedByCurrentUser(info) {
		return "", &Error{Kind: ErrInvalid}
	}
	privateDir := filepath.Join(conductorDir, ".agent-deck")
	if err := os.Mkdir(privateDir, 0o700); err != nil && !os.IsExist(err) {
		return "", &Error{Kind: ErrIO}
	}
	privateInfo, err := os.Lstat(privateDir)
	if err != nil || !privateInfo.IsDir() || privateInfo.Mode()&os.ModeSymlink != 0 || privateInfo.Mode().Perm() != 0o700 || !ownedByCurrentUser(privateInfo) {
		return "", &Error{Kind: ErrInvalid}
	}
	return filepath.Join(privateDir, "conductor.lock"), nil
}

// Acquire obtains the named conductor's singleton without waiting. V2 runtime
// owners of the same named home must hold it for their process lifetime.
func Acquire(name, conductorDir string) (*Lock, error) {
	path, err := lockPath(name, conductorDir)
	if err != nil {
		return nil, err
	}
	file, err := openAndLock(path)
	if err != nil {
		return nil, err
	}
	return &Lock{file: file}, nil
}

// Close releases the singleton. The lock file remains in place so waiters
// never acquire different inodes after an unlink.
func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	if err != nil {
		return fmt.Errorf("%w", &Error{Kind: ErrIO})
	}
	return nil
}
