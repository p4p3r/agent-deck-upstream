// Package channelspool stores channel content and provider identifiers as
// authenticated, owner-only records outside the routing ledger.
package channelspool

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrKey = errors.New("channelspool: invalid key")
var ErrRecord = errors.New("channelspool: invalid record")
var ErrStorage = errors.New("channelspool: storage failure")

const keyEnv = "SLACK_DECK_SPOOL_KEY"
const recordVersion = "SDSP1"
const maxRecord = 64 << 20
const checkAlias = "0000000000000000000000000000000000000000000000000000000000000000"

// ParseKey requires canonical padded standard base64 for exactly 32 random bytes.
func ParseKey(value string) ([]byte, error) {
	if len(value) != 44 || strings.TrimSpace(value) != value {
		return nil, ErrKey
	}
	key, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(key) != 32 || base64.StdEncoding.EncodeToString(key) != value {
		return nil, ErrKey
	}
	return key, nil
}

func KeyFromEnv() ([]byte, error) {
	return ParseKey(os.Getenv(keyEnv))
}

type Store struct {
	dir         string
	key         []byte
	nonceSource io.Reader
}

func New(dir string, key []byte) (*Store, error) {
	if len(key) != 32 || !filepath.IsAbs(dir) {
		return nil, ErrKey
	}
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, ErrStorage
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrStorage
	}
	tmpDir := filepath.Join(dir, "tmp")
	if err := os.Mkdir(tmpDir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, ErrStorage
	}
	tmpInfo, err := os.Lstat(tmpDir)
	if err != nil || !tmpInfo.IsDir() || tmpInfo.Mode().Perm() != 0o700 || tmpInfo.Mode()&os.ModeSymlink != 0 {
		return nil, ErrStorage
	}
	s := &Store{dir: dir, key: append([]byte(nil), key...), nonceSource: rand.Reader}
	const check = "channelspool-key-check-v1"
	path := s.path("control", checkAlias)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if err := s.WriteImmutable("control", checkAlias, []byte(check)); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, ErrStorage
	}
	got, err := s.Read("control", checkAlias)
	if err != nil || string(got) != check {
		return nil, ErrKey
	}
	return s, nil
}

func (s *Store) derive(label string) []byte {
	h := hmac.New(sha256.New, s.key)
	_, _ = h.Write([]byte("agent-deck/slack-v2/" + label + "/v1"))
	return h.Sum(nil)
}

// Alias is stable across restarts and contains no recoverable provider value.
func (s *Store) Alias(domain, value string) string {
	h := hmac.New(sha256.New, s.derive("alias/"+domain))
	_, _ = h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}

func AliasFromKey(key []byte, domain, value string) string {
	if len(key) != 32 || value == "" {
		return ""
	}
	s := &Store{key: key}
	return s.Alias(domain, value)
}

func validName(domain, alias string) bool {
	if domain == "" || len(domain) > 32 || len(alias) != 64 {
		return false
	}
	for _, c := range domain {
		if c < 'a' || c > 'z' {
			return false
		}
	}
	_, err := hex.DecodeString(alias)
	return err == nil && strings.ToLower(alias) == alias
}

func (s *Store) path(domain, alias string) string { return filepath.Join(s.dir, domain+"-"+alias) }
func (s *Store) tempDir() string                  { return filepath.Join(s.dir, "tmp") }

func (s *Store) aead(domain string) (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.derive("record/" + domain))
	if err != nil {
		return nil, ErrKey
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrKey
	}
	return aead, nil
}

func (s *Store) Seal(domain, alias string, plain []byte) ([]byte, error) {
	if s == nil || !validName(domain, alias) || len(plain) > maxRecord {
		return nil, ErrRecord
	}
	aead, err := s.aead(domain)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(s.nonceSource, nonce); err != nil {
		return nil, ErrStorage
	}
	aad := []byte(recordVersion + "/" + domain + "/" + alias)
	out := append([]byte(recordVersion), nonce...)
	out = aead.Seal(out, nonce, plain, aad)
	return out, nil
}

func (s *Store) OpenRecord(domain, alias string, sealed []byte) ([]byte, error) {
	if s == nil || !validName(domain, alias) {
		return nil, ErrRecord
	}
	aead, err := s.aead(domain)
	if err != nil {
		return nil, err
	}
	minSize := len(recordVersion) + aead.NonceSize() + aead.Overhead()
	if len(sealed) < minSize || len(sealed) > maxRecord+minSize || string(sealed[:len(recordVersion)]) != recordVersion {
		return nil, ErrRecord
	}
	nonce := sealed[len(recordVersion) : len(recordVersion)+aead.NonceSize()]
	aad := []byte(recordVersion + "/" + domain + "/" + alias)
	plain, err := aead.Open(nil, nonce, sealed[len(recordVersion)+aead.NonceSize():], aad)
	if err != nil {
		return nil, ErrRecord
	}
	return plain, nil
}

// Write makes a sealed record durable before callers publish its alias in SQL.
func (s *Store) Write(domain, alias string, plain []byte) error {
	sealed, err := s.Seal(domain, alias, plain)
	if err != nil {
		return err
	}
	path := s.path(domain, alias)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSymlink != 0 {
			return ErrStorage
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrStorage
	}
	f, err := os.CreateTemp(s.tempDir(), ".sealed-*")
	if err != nil {
		return ErrStorage
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return ErrStorage
	}
	if _, err := f.Write(sealed); err != nil {
		f.Close()
		return ErrStorage
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return ErrStorage
	}
	if err := f.Close(); err != nil {
		return ErrStorage
	}
	if err := os.Rename(tmp, path); err != nil {
		return ErrStorage
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return ErrStorage
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return ErrStorage
	}
	return nil
}

// WriteImmutable never changes an existing binding or content record.
func (s *Store) WriteImmutable(domain, alias string, plain []byte) error {
	if s == nil || !validName(domain, alias) {
		return ErrRecord
	}
	if got, err := s.Read(domain, alias); err == nil {
		if hmac.Equal(got, plain) {
			return nil
		}
		return ErrRecord
	} else if _, statErr := os.Lstat(s.path(domain, alias)); statErr == nil {
		return ErrRecord
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return ErrStorage
	}
	sealed, err := s.Seal(domain, alias, plain)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.tempDir(), ".sealed-*")
	if err != nil {
		return ErrStorage
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return ErrStorage
	}
	if _, err := f.Write(sealed); err != nil {
		f.Close()
		return ErrStorage
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return ErrStorage
	}
	if err := f.Close(); err != nil {
		return ErrStorage
	}
	if err := os.Link(tmp, s.path(domain, alias)); err != nil {
		if errors.Is(err, os.ErrExist) {
			got, readErr := s.Read(domain, alias)
			if readErr == nil && hmac.Equal(got, plain) {
				return nil
			}
			return ErrRecord
		}
		return ErrStorage
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return ErrStorage
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return ErrStorage
	}
	return nil
}

func (s *Store) Read(domain, alias string) ([]byte, error) {
	if s == nil || !validName(domain, alias) {
		return nil, ErrRecord
	}
	path := s.path(domain, alias)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxRecord+128 {
		return nil, ErrRecord
	}
	sealed, err := os.ReadFile(path)
	if err != nil {
		return nil, ErrStorage
	}
	return s.OpenRecord(domain, alias, sealed)
}

func (s *Store) Remove(domain, alias string) error {
	if s == nil || !validName(domain, alias) {
		return ErrRecord
	}
	if err := os.Remove(s.path(domain, alias)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ErrStorage
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return ErrStorage
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return ErrStorage
	}
	return nil
}

// CleanupOrphans bounds removal of unpublished ciphertext temp files left by
// an interrupted atomic write. A temp file is never a referenced record.
func (s *Store) CleanupOrphans(before time.Time, limit int) (int, error) {
	if s == nil || limit < 1 || limit > 1024 {
		return 0, ErrRecord
	}
	dir, err := os.Open(s.tempDir())
	if err != nil {
		return 0, ErrStorage
	}
	defer dir.Close()
	entries, err := dir.ReadDir(limit * 8)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, ErrStorage
	}
	removed := 0
	for _, entry := range entries {
		if removed >= limit {
			break
		}
		if !strings.HasPrefix(entry.Name(), ".sealed-") {
			continue
		}
		path := filepath.Join(s.tempDir(), entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			return removed, ErrStorage
		}
		if !info.ModTime().Before(before) {
			continue
		}
		if os.Remove(path) != nil {
			return removed, ErrStorage
		}
		removed++
	}
	if removed != 0 {
		if dir.Sync() != nil {
			return removed, ErrStorage
		}
	}
	return removed, nil
}
