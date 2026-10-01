package tmux

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"
)

// PaneGeometry locates visible rows and the input cursor in a stable pane.
type PaneGeometry struct {
	PaneID           string
	SessionID        string
	ServerPID        int
	AttachedClients  int
	Width, Height    int
	CursorX, CursorY int
}

// PaneSnapshot contains every visible row, with ANSI attributes intact.
type PaneSnapshot struct {
	Raw      string
	Geometry PaneGeometry
}

var paneSGR = regexp.MustCompile(`\x1b\[[0-9;:]*m`)

// Rows rejects incomplete viewports, invalid cursors and unsupported escapes.
func (s PaneSnapshot) Rows() ([]string, bool) {
	g := s.Geometry
	if !validTmuxID(g.SessionID, "$") || g.ServerPID <= 0 || g.AttachedClients < 0 ||
		!strings.HasPrefix(g.PaneID, "%") || g.Width < 1 || g.Height < 1 ||
		g.CursorX < 0 || g.CursorX >= g.Width || g.CursorY < 0 || g.CursorY >= g.Height {
		return nil, false
	}
	if _, err := strconv.ParseUint(strings.TrimPrefix(g.PaneID, "%"), 10, 64); err != nil {
		return nil, false
	}
	if !strings.HasSuffix(s.Raw, "\n") || !utf8.ValidString(s.Raw) {
		return nil, false
	}
	rows := strings.Split(strings.TrimSuffix(s.Raw, "\n"), "\n")
	if len(rows) != g.Height {
		return nil, false
	}
	for _, row := range rows {
		plain := paneSGR.ReplaceAllString(row, "")
		if strings.ContainsFunc(plain, unicode.IsControl) || ansi.StringWidth(plain) > g.Width {
			return nil, false
		}
	}
	return rows, true
}

const paneSnapshotFormat = "#{pane_id}|#{pane_width}|#{pane_height}|#{cursor_x}|#{cursor_y}|#{pane_dead}|#{pane_in_mode}|#{cursor_flag}|#{session_id}|#{session_attached}|#{pid}"

// CapturePaneSnapshot bypasses caches and brackets a full viewport capture
// with geometry reads in the same tmux command queue.
func (s *Session) CapturePaneSnapshot() (PaneSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := s.tmuxCmdContext(ctx,
		"display-message", "-p", "-t", "="+s.Name+":", paneSnapshotFormat, ";",
		"capture-pane", "-p", "-e", "-S", "0", "-E", "-", "-t", "="+s.Name+":", ";",
		"display-message", "-p", "-t", "="+s.Name+":", paneSnapshotFormat)
	output, err := commandOutput(cmd)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return PaneSnapshot{}, ErrCaptureTimeout
		}
		if captureGoneFromErr(err) {
			return PaneSnapshot{}, ErrCaptureGone
		}
		return PaneSnapshot{}, fmt.Errorf("capture pane snapshot: %w", err)
	}
	return parsePaneSnapshot(string(output))
}

func parsePaneSnapshot(output string) (PaneSnapshot, error) {
	lines := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
	if len(lines) < 3 || !strings.HasSuffix(output, "\n") || lines[0] != lines[len(lines)-1] {
		return PaneSnapshot{}, fmt.Errorf("pane geometry changed or is missing")
	}
	fields := strings.Split(lines[0], "|")
	if len(fields) != 11 || fields[5] != "0" || fields[6] != "0" || fields[7] != "1" || !validTmuxID(fields[8], "$") {
		return PaneSnapshot{}, fmt.Errorf("pane geometry is unavailable or unsupported")
	}
	attached, err := strconv.Atoi(fields[9])
	if err != nil || attached < 0 {
		return PaneSnapshot{}, fmt.Errorf("unreadable session attachment count")
	}
	serverPID, err := strconv.Atoi(fields[10])
	if err != nil || serverPID <= 0 {
		return PaneSnapshot{}, fmt.Errorf("unreadable tmux server identity")
	}
	values := make([]int, 4)
	for i := range values {
		n, err := strconv.Atoi(fields[i+1])
		if err != nil {
			return PaneSnapshot{}, fmt.Errorf("unreadable pane geometry")
		}
		values[i] = n
	}
	snapshot := PaneSnapshot{
		Raw:      strings.Join(lines[1:len(lines)-1], "\n") + "\n",
		Geometry: PaneGeometry{PaneID: fields[0], SessionID: fields[8], ServerPID: serverPID, AttachedClients: attached, Width: values[0], Height: values[1], CursorX: values[2], CursorY: values[3]},
	}
	if _, ok := snapshot.Rows(); !ok {
		return PaneSnapshot{}, fmt.Errorf("incomplete or unreadable pane viewport")
	}
	return snapshot, nil
}
