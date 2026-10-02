package tmux

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

func validTmuxID(id, prefix string) bool {
	if !strings.HasPrefix(id, prefix) || len(id) == len(prefix) {
		return false
	}
	for _, ch := range id[len(prefix):] {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

// SendKeysAndEnterIfStable proves the target again before typing and pins the
// body and initial Enter to its pane. started means transport may have begun.
func (s *Session) SendKeysAndEnterIfStable(keys string, g PaneGeometry) (started bool, err error) {
	current, err := s.CapturePaneSnapshot()
	if err != nil || s.VimMode || g.AttachedClients < 0 || current.Geometry != g {
		return false, fmt.Errorf("exact target changed or unreadable")
	}
	return true, s.sendKeysAndEnterCheckedToTarget(g.PaneID, keys, nil, nil)
}

func guardedEnterArgs(name string, g PaneGeometry) []string {
	conditions := []string{
		"#{==:#{session_attached}," + strconv.Itoa(g.AttachedClients) + "}",
		"#{==:#{pid}," + strconv.Itoa(g.ServerPID) + "}",
		"#{==:#{session_id}," + g.SessionID + "}",
		"#{==:#{pane_id}," + g.PaneID + "}",
		"#{==:#{pane_dead},0}",
		"#{==:#{pane_in_mode},0}",
		"#{==:#{cursor_flag},1}",
	}
	for _, field := range []struct {
		name  string
		value int
	}{{"pane_width", g.Width}, {"pane_height", g.Height}, {"cursor_x", g.CursorX}, {"cursor_y", g.CursorY}} {
		conditions = append(conditions, "#{==:#{"+field.name+"},"+strconv.Itoa(field.value)+"}")
	}
	condition := conditions[0]
	for _, next := range conditions[1:] {
		condition = "#{&&:" + condition + "," + next + "}"
	}
	return []string{"if-shell", "-F", "-t", "=" + name + ":", condition,
		"send-keys -t " + g.PaneID + " Enter ; display-message -p guarded-enter-sent",
		"display-message -p guarded-enter-withheld"}
}

// SendEnterIfStable evaluates the exact-session guard and sends one Enter in
// the same server command queue. Attachment count is part of the captured
// target identity; observers do not block delivery, but an attachment change
// between capture and submission does.
func (s *Session) SendEnterIfStable(g PaneGeometry) error {
	if s.VimMode || !validTmuxID(g.PaneID, "%") || !validTmuxID(g.SessionID, "$") || g.ServerPID <= 0 || g.AttachedClients < 0 ||
		g.Width < 1 || g.Height < 1 || g.CursorX < 0 || g.CursorX >= g.Width || g.CursorY < 0 || g.CursorY >= g.Height {
		return fmt.Errorf("guarded Enter requires a stable target")
	}
	ctx, cancel := context.WithTimeout(context.Background(), tmuxSendKeysTimeout)
	defer cancel()
	s.invalidateCache()
	output, err := commandOutput(s.tmuxCmdContext(ctx, guardedEnterArgs(s.Name, g)...))
	if err != nil {
		return fmt.Errorf("guarded Enter failed: %w", err)
	}
	if string(output) != "guarded-enter-sent\n" {
		return fmt.Errorf("guarded Enter withheld: target changed or unreadable")
	}
	return nil
}
