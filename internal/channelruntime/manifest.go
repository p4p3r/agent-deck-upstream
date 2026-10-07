package channelruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	manifestVersion = 2
	phasePrepared   = "prepared"
	phaseReady      = "ready"
)

type runtimePaths struct {
	manifest string
	ledger   string
}

func pathsFor(conductorDir string) runtimePaths {
	dir := filepath.Join(conductorDir, "slack-v2")
	return runtimePaths{manifest: filepath.Join(dir, "manifest.json"), ledger: filepath.Join(dir, "gateway.sqlite")}
}

// The manifest contains no credential, message body, provider response, Codex
// process setting, or thread identity.
type manifest struct {
	Version        int      `json:"version"`
	Profile        string   `json:"profile"`
	RowInstanceID  string   `json:"row_instance_id"`
	RowBinding     string   `json:"row_binding_token"`
	ConversationID string   `json:"conversation_id"`
	ConductorID    string   `json:"conductor_id"`
	TeamID         string   `json:"team_id"`
	BotUserID      string   `json:"bot_user_id"`
	ChannelID      string   `json:"channel_id"`
	AllowedUserIDs []string `json:"allowed_user_ids"`

	// Compatibility-only fields never serialize into a v2 manifest.
	Phase, ThreadID, PreviousThreadID, Baseline string `json:"-"`
	Intent                                      Mode   `json:"-"`
	BaselineKnown, Fresh                        bool   `json:"-"`
	CodexCWD, CodexExecutable, CodexModel       string `json:"-"`
}

func newManifest(r Request, c Config, identity struct{ TeamID, BotUserID string }) *manifest {
	return &manifest{
		Version: manifestVersion, Profile: c.Profile, RowInstanceID: c.RowInstanceID,
		RowBinding: c.RowBinding, ConversationID: c.ConversationID,
		ConductorID: c.ConductorID, TeamID: identity.TeamID, BotUserID: identity.BotUserID,
		ChannelID: c.ChannelID, AllowedUserIDs: canonicalUsers(c.AllowedUserIDs),
	}
}

func (m *manifest) match(r Request, c Config, identity struct{ TeamID, BotUserID string }) error {
	if m == nil || m.Version != manifestVersion || m.Profile != c.Profile ||
		m.RowInstanceID != c.RowInstanceID || m.RowBinding != c.RowBinding ||
		m.ConversationID != c.ConversationID ||
		m.ConductorID != c.ConductorID || m.TeamID != identity.TeamID || m.BotUserID != identity.BotUserID ||
		m.ChannelID != c.ChannelID ||
		!slices.Equal(m.AllowedUserIDs, canonicalUsers(c.AllowedUserIDs)) {
		return &Error{KindManifest}
	}
	if r.Mode != "" && r.Mode != ModeRow || r.ResumeThreadID != "" {
		return &Error{KindManifest}
	}
	return nil
}

func canonicalUsers(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}

func ensurePrivateDir(dir string) error {
	if err := rejectSymlinkAncestors(dir); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("invalid runtime directory")
	}
	return nil
}

func rejectSymlinkAncestors(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("relative runtime path")
	}
	path = filepath.Clean(path)
	for path != string(os.PathSeparator) {
		info, err := os.Lstat(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return errors.New("symlink in runtime path")
		}
		parent := filepath.Dir(path)
		if parent == path || !strings.HasPrefix(path, parent) {
			break
		}
		path = parent
	}
	return nil
}

func requireAbsent(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		_, err := os.Lstat(candidate)
		if !errors.Is(err, os.ErrNotExist) {
			return errors.New("existing or unreadable runtime file")
		}
	}
	return nil
}

func requireRegular(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return errors.New("missing or invalid runtime file")
	}
	return nil
}

func requireAbsentOrRegular(path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return requireAbsent(path)
	}
	return requireRegular(path)
}

// SQLite may need existing WAL state for crash recovery. SQLite itself may
// create 0644 sidecars, which remain private inside the verified 0700 runtime
// directory. They must still be owned by us and unwritable by other users.
func requirePrivateSidecars(path string) error {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		candidate := path + suffix
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || !ownedSingleLink(info) {
			return errors.New("invalid runtime sidecar")
		}
	}
	return nil
}

func loadManifest(path string) (*manifest, error) {
	if err := requireRegular(path); err != nil {
		if errors.Is(err, os.ErrNotExist) { // retained for custom filesystem seams
			return nil, nil
		}
		if _, statErr := os.Lstat(path); errors.Is(statErr, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 || len(data) > 64<<10 {
		return nil, errors.New("invalid manifest")
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m manifest
	if dec.Decode(&m) != nil {
		return nil, errors.New("invalid manifest")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("invalid manifest")
	}
	return &m, nil
}

func rejectDuplicateKeys(data []byte) error {
	bad := errors.New("invalid manifest")
	d := json.NewDecoder(bytes.NewReader(data))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return bad
	}
	seen := make(map[string]bool)
	for d.More() {
		tok, err := d.Token()
		key, ok := tok.(string)
		if err != nil || !ok || seen[key] {
			return bad
		}
		seen[key] = true
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return bad
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return bad
	}
	if _, err := d.Token(); err != io.EOF {
		return bad
	}
	return nil
}

func saveManifest(path string, m *manifest) error {
	if m == nil {
		return errors.New("missing manifest")
	}
	dir := filepath.Dir(path)
	if err := ensurePrivateDir(dir); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0o600) {
		return errors.New("invalid manifest path")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".manifest-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
