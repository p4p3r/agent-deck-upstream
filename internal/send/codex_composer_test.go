package send

import (
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/tmux"
)

func codexFrameCapture(body, footer string) PaneCapture {
	const height = 16
	raw := "› " + strings.ReplaceAll(body, "\n", "\n  ") + "\n\n" + footer + "\n\n"
	padding := height - strings.Count(raw, "\n")
	raw = strings.Repeat("\n", padding) + raw
	return PaneCapture{Raw: raw, OK: true, Geometry: &tmux.PaneGeometry{
		PaneID: "%5", SessionID: "$3", ServerPID: 12345, Width: 100, Height: height, CursorX: 2,
		CursorY: padding + strings.Count(body, "\n"),
	}}
}

func TestCodexComposerFrame_StructuralAttribution(t *testing.T) {
	const message = "Please inspect the synthetic fixture"
	const model = "  gpt-test · /work/fixture · Context 90% left · Context 10% used"
	const shortcuts = "  ? for shortcuts · Warning: experimental features enabled"
	for _, footer := range []string{
		"  ? for shortcuts",
		model,
		model + "\n" + shortcuts,
		"\x1b[2m" + model + "\x1b[0m\n\x1b[2m" + shortcuts + "\x1b[0m",
		"  Tip: Visit the \x1b]8;;https://community.openai.com/c/codex/37\x1b\\Codex community forum\x1b]8;;\x1b\\",
	} {
		t.Run(footer, func(t *testing.T) {
			baseline := codexFrameCapture("Ask Codex to do anything", footer)
			frame, clear := CaptureClearCodexComposerFrame(baseline)
			if !clear {
				t.Fatal("complete clear baseline must identify its footer")
			}
			for _, body := range []string{message, message + "\n\n" + tmux.StripANSI(footer)} {
				capture := codexFrameCapture(body, footer)
				draft, visible := frame.Prompt(capture)
				if !visible || draft != NormalizePromptText(body) {
					t.Fatalf("full draft including footer copies: draft=%q visible=%v", draft, visible)
				}
				presses := 0
				gate := EnterAttribution{Message: message, CodexFrame: frame}
				got := gate.NudgeEnter(enterPresserFunc(func() error { presses++; return nil }), capture, tmux.StripANSI)
				want := body == message
				wantPresses := 0
				if want {
					wantPresses = 1
				}
				if got != want || presses != wantPresses {
					t.Fatalf("footer identity must retain foreign copies: pressed=%v count=%d want=%v", got, presses, want)
				}
			}
			// Identical bytes without a full viewport carry no footer identity.
			partial := Captured("› " + message + "\n\n" + footer + "\n")
			partial.Geometry = baseline.Geometry
			gate := EnterAttribution{Message: message, CodexFrame: frame}
			if _, ok := frame.Prompt(partial); ok || !gate.EnterWouldSubmitForeignDraft(partial, tmux.StripANSI) {
				t.Fatal("copied footer in a partial capture must withhold Enter")
			}
		})
	}
}

func TestCodexComposerFrame_TextOnlyCannotProveFooter(t *testing.T) {
	baseline := codexFrameCapture("Ask Codex to do anything", "  ? for shortcuts")
	baseline.Geometry = nil
	if _, ok := CaptureClearCodexComposerFrame(baseline); ok {
		t.Fatal("footer text cannot establish its own viewport position")
	}
}

func TestCodexComposerFrame_StableAttachedViewer(t *testing.T) {
	baseline := codexFrameCapture("Ask Codex to do anything", "  ? for shortcuts")
	baseline.Geometry.AttachedClients = 1
	frame, clear := CaptureClearCodexComposerFrame(baseline)
	if !clear {
		t.Fatal("an attached observer must not invalidate a clear composer")
	}
	capture := codexFrameCapture("inspect the fixture", "  ? for shortcuts")
	capture.Geometry.AttachedClients = 1
	if draft, visible := frame.Prompt(capture); !visible || draft != "inspect the fixture" {
		t.Fatalf("stable attached observer: draft=%q visible=%v", draft, visible)
	}
	capture.Geometry.AttachedClients = 2
	if draft, visible := frame.Prompt(capture); !visible || draft != "inspect the fixture" {
		t.Fatalf("attachment changes do not alter the composer frame: draft=%q visible=%v", draft, visible)
	}
}

func TestCodexComposerFrame_ExactTrailingNewlineTransition(t *testing.T) {
	const message = "inspect the synthetic fixture"
	const footer = "  ? for shortcuts"
	frame, clear := CaptureClearCodexComposerFrame(codexFrameCapture("Ask Codex to do anything", footer))
	if !clear {
		t.Fatal("fixture must establish a clear composer frame")
	}
	before := codexFrameCapture(message, footer)
	for _, tc := range []struct {
		name      string
		after     PaneCapture
		unchanged bool
		addedOne  bool
	}{
		{"unchanged", codexFrameCapture(message, footer), true, false},
		{"one trailing newline", codexFrameCapture(message+"\n", footer), false, true},
		{"two trailing newlines", codexFrameCapture(message+"\n\n", footer), false, false},
		{"foreign prefix", codexFrameCapture("foreign "+message+"\n", footer), false, false},
		{"foreign suffix", codexFrameCapture(message+" foreign\n", footer), false, false},
		{"foreign line", codexFrameCapture(message+"\nforeign", footer), false, false},
		{"unreadable", PaneCapture{}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := frame.PromptUnchanged(before, tc.after); got != tc.unchanged {
				t.Fatalf("PromptUnchanged()=%v, want %v", got, tc.unchanged)
			}
			if got := frame.PromptAddedOneTrailingNewline(before, tc.after); got != tc.addedOne {
				t.Fatalf("PromptAddedOneTrailingNewline()=%v, want %v", got, tc.addedOne)
			}
		})
	}

	changedGeometry := codexFrameCapture(message+"\n", footer)
	changedGeometry.Geometry.Width++
	if frame.PromptAddedOneTrailingNewline(before, changedGeometry) {
		t.Fatal("changed target geometry must not prove a trailing-newline transition")
	}
	changedSession := codexFrameCapture(message+"\n", footer)
	changedSession.Geometry.SessionID = "$4"
	if frame.PromptAddedOneTrailingNewline(before, changedSession) {
		t.Fatal("changed target session must not prove a trailing-newline transition")
	}
}
