package channelspool

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testKey() []byte { return bytes.Repeat([]byte{0x42}, 32) }

func TestKeyContract(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(testKey())
	if got, err := ParseKey(encoded); err != nil || !bytes.Equal(got, testKey()) {
		t.Fatal("canonical synthetic key rejected")
	}
	for _, value := range []string{"", encoded + "\n", encoded[:43], base64.RawStdEncoding.EncodeToString(testKey()), base64.StdEncoding.EncodeToString(testKey()[:16])} {
		if _, err := ParseKey(value); !errors.Is(err, ErrKey) {
			t.Fatal("invalid key accepted")
		}
	}
}

func TestSealedRecordsAreDistinctPrivateAndAuthenticated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	s, err := New(dir, testKey())
	if err != nil {
		t.Fatal(err)
	}
	const private = "synthetic-private-message-and-provider-id"
	alias := s.Alias("inbound", "synthetic-provider-id")
	first, err := s.Seal("inbound", alias, []byte(private))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Seal("inbound", alias, []byte(private))
	if err != nil || bytes.Equal(first, second) || bytes.Contains(first, []byte(private)) || bytes.Contains(second, []byte(private)) {
		t.Fatal("record nonce or confidentiality failure")
	}
	if err := s.Write("inbound", alias, []byte(private)); err != nil {
		t.Fatal(err)
	}
	sealed, err := os.ReadFile(s.path("inbound", alias))
	if err != nil || bytes.Contains(sealed, []byte(private)) || bytes.Contains([]byte(s.path("inbound", alias)), []byte("synthetic-provider-id")) {
		t.Fatal("spool exposed plaintext")
	}
	info, err := os.Stat(s.path("inbound", alias))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("spool record is not owner-only")
	}
	if got, err := s.Read("inbound", alias); err != nil || string(got) != private {
		t.Fatal("sealed record did not round-trip")
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := s.OpenRecord("inbound", alias, sealed); !errors.Is(err, ErrRecord) {
		t.Fatal("tampered record accepted")
	}
	wrong, err := New(filepath.Join(t.TempDir(), "other"), bytes.Repeat([]byte{0x43}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.OpenRecord("inbound", alias, first); !errors.Is(err, ErrRecord) {
		t.Fatal("wrong key accepted")
	}
	if _, err := New(dir, bytes.Repeat([]byte{0x43}, 32)); !errors.Is(err, ErrKey) {
		t.Fatal("wrong root key opened existing spool")
	}
}

func TestInjectedNoncesAreUniqueAndBoundToRecord(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "spool"), testKey())
	if err != nil {
		t.Fatal(err)
	}
	alias := s.Alias("inbound", "synthetic-event")
	s.nonceSource = bytes.NewReader(append(bytes.Repeat([]byte{0x11}, 12), bytes.Repeat([]byte{0x22}, 12)...))
	first, err := s.Seal("inbound", alias, []byte("synthetic-body"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Seal("inbound", alias, []byte("synthetic-body"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first[5:17], second[5:17]) || bytes.Equal(first, second) {
		t.Fatal("injected record nonces were reused")
	}
	if _, err := s.OpenRecord("outbound", alias, first); !errors.Is(err, ErrRecord) {
		t.Fatal("domain substitution accepted")
	}
	if _, err := s.OpenRecord("inbound", s.Alias("inbound", "other-event"), first); !errors.Is(err, ErrRecord) {
		t.Fatal("alias substitution accepted")
	}
	first[5] ^= 1
	if _, err := s.OpenRecord("inbound", alias, first); !errors.Is(err, ErrRecord) {
		t.Fatal("nonce tampering accepted")
	}
}

func TestBoundedOrphanCleanup(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "spool"), testKey())
	if err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(s.tempDir(), ".sealed-old")
	newer := filepath.Join(s.tempDir(), ".sealed-new")
	for _, path := range []string{old, newer} {
		if err := os.WriteFile(path, []byte("synthetic-orphan-ciphertext"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(1_800_000_000, 0)
	if err := os.Chtimes(old, now.Add(-48*time.Hour), now.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newer, now, now); err != nil {
		t.Fatal(err)
	}
	removed, err := s.CleanupOrphans(now.Add(-24*time.Hour), 1)
	if err != nil || removed != 1 {
		t.Fatal("bounded orphan cleanup failed")
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old orphan survived")
	}
	if _, err := os.Stat(newer); err != nil {
		t.Fatal("new in-flight record was removed")
	}
}
