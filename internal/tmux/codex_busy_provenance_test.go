package tmux

import "testing"

func TestCodexBusyDetectionRequiresStatusLineShape(t *testing.T) {
	tests := []struct {
		name    string
		content string
		busy    bool
	}{
		{
			name:    "working status line",
			content: "• Working (12s · esc to interrupt)",
			busy:    true,
		},
		{
			name:    "standalone interrupt status line",
			content: "press esc to interrupt",
			busy:    true,
		},
		{
			name:    "ansi working status line",
			content: "\x1b[36m• Working (1m 02s · esc to interrupt)\x1b[0m",
			busy:    true,
		},
		{
			name: "assistant prose is not a status line",
			content: "Completed the audit. The documentation example " +
				"Working (12s · esc to interrupt) is quoted, not live status.",
			busy: false,
		},
		{
			name:    "ordinary prose containing interrupt phrase",
			content: "Use esc to interrupt if a future command hangs.",
			busy:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSession("codex-status-shape", "/tmp")
			s.Command = "codex"
			if got := s.hasBusyIndicator(tc.content); got != tc.busy {
				t.Fatalf("hasBusyIndicator(%q) = %v, want %v", tc.content, got, tc.busy)
			}
		})
	}
}

func TestCodexPromptIsNotMaskedByQuotedBusyProse(t *testing.T) {
	content := "Assistant result: Working (12s · esc to interrupt) was only an example.\n\n› "
	if !NewPromptDetector("codex").HasPrompt(content) {
		t.Fatal("quoted busy prose masked the real Codex prompt")
	}
}
