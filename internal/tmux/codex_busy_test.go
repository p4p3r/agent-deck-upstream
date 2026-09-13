package tmux

import "testing"

func TestCodexPromptBusyDetectionRequiresStatusLineShape(t *testing.T) {
	detector := NewPromptDetector("codex")
	for _, tc := range []struct {
		name    string
		content string
		prompt  bool
	}{
		{
			name: "working footer wins over visible composer",
			content: "Completed output\n\n" +
				"› Add tests\n" +
				"• Working (35s • esc to interrupt) · 92% context left\n",
			prompt: false,
		},
		{
			name:    "standalone interrupt status",
			content: "Running a command\n› Add tests\nesc to interrupt\n",
			prompt:  false,
		},
		{
			name:    "ctrl c working footer wins over visible composer",
			content: "› Add tests\n⠋ Working (35s • ctrl+c to interrupt)\n",
			prompt:  false,
		},
		{
			name:    "block glyph footer wins over visible composer",
			content: "› Add tests\n▌ Working (35s • press esc to interrupt)\n",
			prompt:  false,
		},
		{
			name:    "suffixed standalone footer wins over visible composer",
			content: "› Add tests\n⠋ Esc to interrupt · 12s · 1.2k tokens\n",
			prompt:  false,
		},
		{
			name: "prose mention does not impersonate status",
			content: "The documentation says to use esc to interrupt a turn.\n" +
				"› Add tests\n",
			prompt: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := detector.HasPrompt(tc.content); got != tc.prompt {
				t.Fatalf("HasPrompt() = %v, want %v\n%s", got, tc.prompt, tc.content)
			}
		})
	}
}

func TestHasCodexBusyIndicatorRejectsProse(t *testing.T) {
	if !HasCodexBusyIndicator("\x1b[33m• Working (1m 2s • esc to interrupt) · 90% left\x1b[0m") {
		t.Fatal("colored Codex working footer was not recognized")
	}
	if HasCodexBusyIndicator("The documentation says to use esc to interrupt a turn.") {
		t.Fatal("prose mention impersonated a Codex status line")
	}
	if HasCodexBusyIndicator("esc to interrupt\nold line\nnew line\n› ") {
		t.Fatal("historical status outside the last three lines stayed busy")
	}
	for _, status := range []string{
		"ctrl+c to interrupt",
		"⠋ Working (12s · ctrl+c to interrupt)",
		"▌ Working (12s · press esc to interrupt)",
		"⠋ Esc to interrupt · 12s · 1.2k tokens",
	} {
		if !HasCodexBusyIndicator(status) {
			t.Errorf("Codex status %q was not recognized", status)
		}
	}
}

func TestCodexBusyDetectorIdleAndRecentWindow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		busy    bool
	}{
		{"braille shaped", "› prompt remains visible\n⠋ Working (2s · ctrl+c to interrupt)", true},
		{"block shaped", "› prompt remains visible\n▌ Working (2s · esc to interrupt)", true},
		{"idle prompt", "turn finished\n› ", false},
		{"historical prose", "Working (12s · esc to interrupt) was an old quoted example.\nline 2\nline 3\n› ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSession("codex-recent-window", "/tmp")
			s.Command = "codex"
			if got := s.hasBusyIndicator(tc.content); got != tc.busy {
				t.Fatalf("hasBusyIndicator() = %v, want %v\n%s", got, tc.busy, tc.content)
			}
		})
	}
}

func TestCodexBusyDetectionPreservesExplicitPatternOverrides(t *testing.T) {
	patterns, err := CompilePatterns(&RawPatterns{BusyPatterns: []string{"custom busy marker"}})
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession("custom-codex-status", "/tmp")
	s.Command = "codex"
	s.resolvedPatterns = patterns
	if !s.hasBusyIndicator("custom busy marker") {
		t.Fatal("explicit Codex busy-pattern override was ignored")
	}
}
