package send

import (
	"strings"

	"github.com/asheshgoplani/agent-deck/internal/tmux"
)

// CodexComposerFrame anchors a clear composer's footer to absolute viewport
// rows. Text-only captures cannot authorize recovery.
type CodexComposerFrame struct {
	geometry   tmux.PaneGeometry
	boundary   int
	footerRows []string
}

// CaptureClearCodexComposerFrame requires a complete viewport and a cursor
// on an empty composer or Codex's native placeholder.
func CaptureClearCodexComposerFrame(c PaneCapture) (CodexComposerFrame, bool) {
	rawRows, ok := codexViewportRows(c)
	if !ok || !strings.HasPrefix(c.Geometry.SessionID, "$") || c.Geometry.ServerPID <= 0 || c.Geometry.AttachedClients < 0 {
		return CodexComposerFrame{}, false
	}
	lines := make([]string, len(rawRows))
	for i, row := range rawRows {
		lines[i] = tmux.StripANSI(row)
	}
	end := len(lines)
	for end > 0 && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	composer := codexComposerLine(lines[:end])
	if composer < 0 || c.Geometry.CursorY != composer || c.Geometry.CursorX < 2 {
		return CodexComposerFrame{}, false
	}
	draft := NormalizePromptText(strings.TrimPrefix(lines[composer], "›"))
	if draft != "" && draft != "Ask Codex to do anything" {
		return CodexComposerFrame{}, false
	}
	footer := composer + 1
	for footer < end && strings.TrimSpace(lines[footer]) == "" {
		footer++
	}
	if footer == composer+1 || footer == end {
		return CodexComposerFrame{}, false
	}
	rows := lines[footer:end]
	for _, row := range rows {
		if !strings.HasPrefix(row, "  ") {
			return CodexComposerFrame{}, false
		}
	}
	boundary := composer + 1
	return CodexComposerFrame{
		geometry:   *c.Geometry,
		boundary:   boundary,
		footerRows: append([]string(nil), rawRows[boundary:]...),
	}, true
}

// Prompt reads only rows above the baseline footer boundary. A footer copy
// inside that range remains draft text, even when it matches every footer byte.
func (f CodexComposerFrame) Prompt(c PaneCapture) (string, bool) {
	rows, ok := f.promptRows(c)
	if !ok {
		return "", false
	}
	return NormalizePromptText(strings.Join(rows, "\n")), true
}

// PromptUnchanged reports whether two complete captures show the same exact
// composer rows inside this frame.
func (f CodexComposerFrame) PromptUnchanged(before, after PaneCapture) bool {
	beforeRows, beforeOK := f.promptRows(before)
	afterRows, afterOK := f.promptRows(after)
	return beforeOK && afterOK && equalCodexPromptRows(beforeRows, afterRows)
}

// PromptAddedOneTrailingNewline requires the second capture to contain the
// exact prior composer plus one blank continuation row.
func (f CodexComposerFrame) PromptAddedOneTrailingNewline(before, after PaneCapture) bool {
	beforeRows, beforeOK := f.promptRows(before)
	afterRows, afterOK := f.promptRows(after)
	if !beforeOK || !afterOK || len(beforeRows) == 0 || len(afterRows) != len(beforeRows)+1 ||
		strings.TrimSpace(beforeRows[len(beforeRows)-1]) == "" || strings.TrimSpace(afterRows[len(afterRows)-1]) != "" {
		return false
	}
	return equalCodexPromptRows(beforeRows, afterRows[:len(beforeRows)])
}

func (f CodexComposerFrame) promptRows(c PaneCapture) ([]string, bool) {
	rows, ok := codexViewportRows(c)
	if !ok || len(f.footerRows) == 0 || c.Geometry.PaneID != f.geometry.PaneID ||
		c.Geometry.SessionID != f.geometry.SessionID ||
		c.Geometry.ServerPID != f.geometry.ServerPID ||
		c.Geometry.Width != f.geometry.Width || c.Geometry.Height != f.geometry.Height {
		return nil, false
	}
	for i, row := range f.footerRows {
		if rows[f.boundary+i] != row {
			return nil, false
		}
	}
	body := make([]string, f.boundary)
	for i, row := range rows[:f.boundary] {
		body[i] = tmux.StripANSI(row)
	}
	composer := codexComposerLine(body)
	if composer < 0 || c.Geometry.CursorY < composer || c.Geometry.CursorY >= f.boundary || c.Geometry.CursorX < 2 {
		return nil, false
	}
	return codexComposerRows(body)
}

func equalCodexPromptRows(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func codexViewportRows(c PaneCapture) ([]string, bool) {
	if !c.OK || c.Geometry == nil {
		return nil, false
	}
	return (tmux.PaneSnapshot{Raw: c.Raw, Geometry: *c.Geometry}).Rows()
}

func codexComposerLine(lines []string) int {
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "› ") || lines[i] == "›" {
			return i
		}
	}
	return -1
}

func codexComposerRows(lines []string) ([]string, bool) {
	composer := codexComposerLine(lines)
	if composer < 0 {
		return nil, false
	}
	body := []string{strings.TrimPrefix(lines[composer], "›")}
	for _, line := range lines[composer+1:] {
		if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, "  ") {
			return nil, false
		}
		body = append(body, line)
	}
	return body, true
}
