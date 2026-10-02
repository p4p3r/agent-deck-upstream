package tmux

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"al.essio.dev/pkg/shellescape"
)

func TestPaneSnapshot_CompleteViewport(t *testing.T) {
	const geometry = "%7|100|6|2|2|0|0|1|$3|0|12345"
	const raw = "\n\n› Ask Codex to do anything\n\n\x1b[2m  gpt-test · /work/fixture · Context 90% left · Context 10% used\x1b[0m\n  ? for shortcuts · Warning: experimental features enabled\n"
	snapshot, err := parsePaneSnapshot(geometry + "\n" + raw + geometry + "\n")
	if err != nil || snapshot.Raw != raw || snapshot.Geometry.PaneID != "%7" || snapshot.Geometry.Height != 6 || snapshot.Geometry.CursorY != 2 {
		t.Fatalf("full styled viewport: snapshot=%#v err=%v", snapshot, err)
	}
	for _, tc := range []struct {
		name, before, body, after string
	}{
		{"changed cursor", geometry, raw, strings.Replace(geometry, "|2|2|", "|2|1|", 1)},
		{"changed pane", geometry, raw, strings.Replace(geometry, "%7", "%8", 1)},
		{"changed height", geometry, raw, strings.Replace(geometry, "|6|", "|7|", 1)},
		{"missing metadata", "", raw, ""},
		{"unsupported cursor metadata", "%7|100|6|2||0|0|1|$3|0|12345", raw, "%7|100|6|2||0|0|1|$3|0|12345"},
		{"pane in copy mode", "%7|100|6|2|2|0|1|1|$3|0|12345", raw, "%7|100|6|2|2|0|1|1|$3|0|12345"},
		{"dead pane", "%7|100|6|2|2|1|0|1|$3|0|12345", raw, "%7|100|6|2|2|1|0|1|$3|0|12345"},
		{"hidden cursor", "%7|100|6|2|2|0|0|0|$3|0|12345", raw, "%7|100|6|2|2|0|0|0|$3|0|12345"},
		{"cropped rows", geometry, strings.TrimPrefix(raw, "\n"), geometry},
		{"extra rows", geometry, "\n" + raw, geometry},
		{"unsupported escape", geometry, "\x1b[2K" + raw, geometry},
		{"invalid utf8", geometry, "\xff" + raw, geometry},
		{"overwide row", geometry, strings.Replace(raw, "› Ask Codex to do anything", strings.Repeat("x", 101), 1), geometry},
		{"cursor outside viewport", "%7|100|6|100|2|0|0|1|$3|0|12345", raw, "%7|100|6|100|2|0|0|1|$3|0|12345"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parsePaneSnapshot(tc.before + "\n" + tc.body + tc.after + "\n"); err == nil {
				t.Fatal("unreadable or inconsistent viewport must fail closed")
			}
		})
	}
}

func TestPaneSnapshot_AllowsOSC8HyperlinksOnly(t *testing.T) {
	const geometry = "%7|120|6|2|2|0|0|1|$3|0|12345"
	const openST = "\x1b]8;;https://community.openai.com/c/codex/37\x1b\\"
	const closeST = "\x1b]8;;\x1b\\"
	const openBEL = "\x1b]8;id=tip;https://example.test\x07"
	const closeBEL = "\x1b]8;;\x07"

	for _, tc := range []struct {
		name string
		link string
	}{
		{"string terminator", openST + "Codex community forum" + closeST},
		{"bell terminator", openBEL + "linked tip" + closeBEL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := "\n\n› Ask Codex to do anything\n\n  Tip: Visit the " + tc.link + "\n  ? for shortcuts\n"
			snapshot, err := parsePaneSnapshot(geometry + "\n" + raw + geometry + "\n")
			if err != nil {
				t.Fatalf("OSC 8 is width-neutral viewport decoration: %v", err)
			}
			rows, ok := snapshot.Rows()
			if !ok || len(rows) != snapshot.Geometry.Height {
				t.Fatalf("OSC 8 viewport must remain structurally readable: ok=%v rows=%d", ok, len(rows))
			}
		})
	}

	for _, tc := range []struct {
		name, escape string
	}{
		{"unterminated hyperlink", "\x1b]8;;https://example.test"},
		{"window title", "\x1b]0;not-a-link\x07"},
		{"cursor movement", "\x1b[2K"},
	} {
		t.Run("reject "+tc.name, func(t *testing.T) {
			raw := "\n\n› Ask Codex to do anything\n\n  Tip: " + tc.escape + "unsafe\n  ? for shortcuts\n"
			if _, err := parsePaneSnapshot(geometry + "\n" + raw + geometry + "\n"); err == nil {
				t.Fatal("non-OSC-8 or malformed terminal control must fail closed")
			}
		})
	}
}

func TestCapturePaneSnapshot_StyledFixedRows(t *testing.T) {
	const model = "  gpt-test · /work/fixture · Context 90% left · Context 10% used"
	const shortcuts = "  ? for shortcuts · Warning: experimental features enabled"
	const content = "\x1b[2J\x1b[H\x1b[10;1H› Ask Codex to do anything\x1b[12;1H\x1b[2m" + model + "\x1b[13;1H" + shortcuts + "\x1b[0m\x1b[10;3H"
	dir := t.TempDir()
	script := filepath.Join(dir, "fixture.sh")
	if err := os.WriteFile(script, []byte("printf '%s' "+shellescape.Quote(content)+"\nexec sleep 60\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewSession("snapshot-footer-fixture", dir)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := commandRun(s.tmuxCmdContext(ctx, "new-session", "-d", "-s", s.Name, "-x", "100", "-y", "24", "bash", script)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Kill() })
	var snapshot PaneSnapshot
	for {
		var err error
		snapshot, err = s.CapturePaneSnapshot()
		if err == nil && strings.Contains(snapshot.Raw, "Warning: experimental features enabled") {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("sanitized terminal fixture did not render: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	rows, ok := snapshot.Rows()
	if !ok || snapshot.Geometry.CursorX != 2 || snapshot.Geometry.CursorY != 9 || len(rows) != snapshot.Geometry.Height ||
		StripANSI(rows[11]) != model || StripANSI(rows[12]) != shortcuts || !strings.Contains(rows[11], "\x1b[") {
		t.Fatalf("capture must preserve absolute rows, geometry and styling: geometry=%#v row count=%d", snapshot.Geometry, len(rows))
	}
}
