package session

import (
	"testing"
	"time"
)

func persistedRowBindingFixture(t *testing.T) (*Storage, *Instance) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	storage := newTestStorage(t)
	inst := NewInstanceWithTool("row-binding-fixture", t.TempDir(), "codex")
	inst.GroupPath = DefaultGroupPath
	inst.Command = "codex"
	inst.CodexSessionID = "persisted-native-conversation"
	inst.LastStartedAt = time.Unix(1_700_000_000, 0).UTC()
	if err := storage.SaveWithGroups([]*Instance{inst}, nil); err != nil {
		t.Fatalf("SaveWithGroups: %v", err)
	}
	loaded, _, err := storage.LoadWithGroups()
	if err != nil {
		t.Fatalf("LoadWithGroups: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d instances, want 1", len(loaded))
	}
	return storage, loaded[0]
}

func TestRowBindingUsesPersistedSnapshotUntilSave(t *testing.T) {
	_, loaded := persistedRowBindingFixture(t)
	before := RowBindingToken(loaded)

	// Model caller-local status refresh: a one-shot CLI can observe a newer
	// native identity and tmux handle without persisting either value.
	loaded.CodexSessionID = "caller-local-native-conversation"
	loaded.GetTmuxSession().Name += "-caller-local"

	if after := RowBindingToken(loaded); after != before {
		t.Fatalf("caller-local refresh changed persisted row binding: before=%q after=%q", before, after)
	}
}

func TestRowBindingChangesAfterPersistedBindingReload(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Instance)
	}{
		{
			name: "restart",
			mutate: func(inst *Instance) {
				inst.LastStartedAt = inst.LastStartedAt.Add(time.Minute)
				inst.GetTmuxSession().Name += "-restarted"
			},
		},
		{
			name: "harness switch",
			mutate: func(inst *Instance) {
				inst.Tool = "shell"
				inst.Command = "sh"
			},
		},
		{
			name: "native conversation replacement",
			mutate: func(inst *Instance) {
				inst.CodexSessionID = "replacement-native-conversation"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, loaded := persistedRowBindingFixture(t)
			before := RowBindingToken(loaded)
			tt.mutate(loaded)
			if err := storage.SaveWithGroups([]*Instance{loaded}, nil); err != nil {
				t.Fatalf("SaveWithGroups: %v", err)
			}
			reloaded, _, err := storage.LoadWithGroups()
			if err != nil {
				t.Fatalf("LoadWithGroups: %v", err)
			}
			if len(reloaded) != 1 {
				t.Fatalf("reloaded %d instances, want 1", len(reloaded))
			}
			if after := RowBindingToken(reloaded[0]); after == before {
				t.Fatalf("persisted %s did not change row binding %q", tt.name, before)
			}
		})
	}
}

func TestRowBindingUnsavedInstanceUsesCurrentFields(t *testing.T) {
	inst := NewInstanceWithTool("unsaved-row-binding", t.TempDir(), "codex")
	before := RowBindingToken(inst)
	inst.CodexSessionID = "new-unsaved-native-conversation"
	if after := RowBindingToken(inst); after == before {
		t.Fatalf("unsaved instance did not derive binding from current fields: %q", before)
	}
}
