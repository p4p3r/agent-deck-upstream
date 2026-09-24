package tmux

import (
	"bytes"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

func assertPrivateLaunchTransport(t *testing.T, prompt string, calls []*tmuxCall) {
	t.Helper()
	var loads, pastes, enters int
	for n, call := range calls {
		argv := strings.Join(call.argv, "\x00")
		if strings.Contains(argv, prompt) {
			t.Fatalf("prompt appeared in subprocess argv at call %d: %q", n+1, call.argv)
		}
		if strings.Contains(strings.Join(call.env, "\x00"), prompt) {
			t.Fatalf("prompt appeared in subprocess environment at call %d", n+1)
		}
		if len(call.argv) == 0 {
			continue
		}
		if call.argv[0] != "load-buffer" && call.stdin.Len() != 0 {
			t.Fatalf("prompt bytes entered unexpected stdin at call %d: %q", n+1, call.argv)
		}
		switch call.argv[0] {
		case "load-buffer":
			loads++
			if got := call.stdin.String(); got != prompt {
				t.Fatalf("load-buffer stdin = %q, want exact prompt", got)
			}
		case "paste-buffer":
			pastes++
		case "send-keys":
			if hasFlag(call.argv, "-l") {
				t.Fatalf("private launch transport used body-bearing send-keys: %q", call.argv)
			}
			if call.argv[len(call.argv)-1] == "Enter" {
				enters++
			}
		}
	}
	if loads != 1 || pastes != 1 || enters != 1 {
		t.Fatalf("transport counts: load=%d paste=%d enter=%d, want 1/1/1; calls=%v", loads, pastes, enters, calls)
	}
}

func TestSendKeysAndEnterPrivateKeepsEveryPromptOutOfSubprocessArgv(t *testing.T) {
	longMultiline := "first line\n" + strings.Repeat("private long line ", 96) + "\nlast line"
	for _, tc := range []struct {
		name   string
		prompt string
	}{
		{name: "short single line", prompt: "private-short-launch-prompt"},
		{name: "long multiline", prompt: longMultiline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := recordTransport(t)
			s := &Session{Name: "private-launch-pane"}
			if err := s.SendKeysAndEnterPrivate(tc.prompt); err != nil {
				t.Fatal(err)
			}
			assertPrivateLaunchTransport(t, tc.prompt, *calls)
		})
	}
}

func TestSendKeysAndEnterPrivateDoesNotRetryAfterPasteUncertainty(t *testing.T) {
	original := keySenderExec
	var mu sync.Mutex
	var calls []*tmuxCall
	keySenderExec = func(_ string, args ...string) *exec.Cmd {
		call := &tmuxCall{argv: append([]string(nil), args...), stdin: &bytes.Buffer{}}
		mu.Lock()
		calls = append(calls, call)
		mu.Unlock()
		if len(args) > 0 && args[0] == "load-buffer" {
			cmd := exec.Command("cat")
			cmd.Stdout = call.stdin
			call.env = cmd.Environ()
			return cmd
		}
		if len(args) > 0 && args[0] == "paste-buffer" {
			cmd := exec.Command("false")
			call.env = cmd.Environ()
			return cmd
		}
		cmd := exec.Command("true")
		call.env = cmd.Environ()
		return cmd
	}
	t.Cleanup(func() { keySenderExec = original })

	prompt := "private-uncertain-launch-prompt"
	err := (&Session{Name: "private-launch-uncertain"}).SendKeysAndEnterPrivate(prompt)
	if err == nil {
		t.Fatal("expected indeterminate paste error")
	}
	if strings.Contains(err.Error(), prompt) {
		t.Fatalf("prompt appeared in diagnostic: %q", err)
	}

	var loads, pastes, bodySendKeys, enters int
	for n, call := range calls {
		if strings.Contains(strings.Join(call.argv, "\x00"), prompt) {
			t.Fatalf("prompt appeared in subprocess argv at call %d: %q", n+1, call.argv)
		}
		if strings.Contains(strings.Join(call.env, "\x00"), prompt) {
			t.Fatalf("prompt appeared in subprocess environment at call %d", n+1)
		}
		if len(call.argv) == 0 {
			continue
		}
		switch call.argv[0] {
		case "load-buffer":
			loads++
			if got := call.stdin.String(); got != prompt {
				t.Fatalf("load-buffer stdin = %q, want exact prompt", got)
			}
		case "paste-buffer":
			pastes++
		case "send-keys":
			if hasFlag(call.argv, "-l") {
				bodySendKeys++
			}
			if call.argv[len(call.argv)-1] == "Enter" {
				enters++
			}
		}
	}
	if loads != 1 || pastes != 1 || bodySendKeys != 0 || enters != 0 {
		t.Fatalf("uncertain transport retried or submitted: load=%d paste=%d body_send_keys=%d enter=%d calls=%v",
			loads, pastes, bodySendKeys, enters, calls)
	}
}

func TestSendKeysAndEnterPrivateAttemptsEnterOnlyOnceOnEnterUncertainty(t *testing.T) {
	original := keySenderExec
	var mu sync.Mutex
	var calls []*tmuxCall
	keySenderExec = func(_ string, args ...string) *exec.Cmd {
		call := &tmuxCall{argv: append([]string(nil), args...), stdin: &bytes.Buffer{}}
		mu.Lock()
		calls = append(calls, call)
		mu.Unlock()
		if len(args) > 0 && args[0] == "load-buffer" {
			cmd := exec.Command("cat")
			cmd.Stdout = call.stdin
			call.env = cmd.Environ()
			return cmd
		}
		if len(args) > 0 && args[0] == "send-keys" && args[len(args)-1] == "Enter" {
			cmd := exec.Command("false")
			call.env = cmd.Environ()
			return cmd
		}
		cmd := exec.Command("true")
		call.env = cmd.Environ()
		return cmd
	}
	t.Cleanup(func() { keySenderExec = original })

	prompt := "private-enter-uncertain-launch-prompt"
	err := (&Session{Name: "private-launch-enter-uncertain"}).SendKeysAndEnterPrivate(prompt)
	if err == nil {
		t.Fatal("expected indeterminate Enter error")
	}
	if strings.Contains(err.Error(), prompt) {
		t.Fatalf("prompt appeared in diagnostic: %q", err)
	}

	var loads, pastes, bodySendKeys, enters int
	for n, call := range calls {
		if strings.Contains(strings.Join(call.argv, "\x00"), prompt) {
			t.Fatalf("prompt appeared in subprocess argv at call %d: %q", n+1, call.argv)
		}
		if strings.Contains(strings.Join(call.env, "\x00"), prompt) {
			t.Fatalf("prompt appeared in subprocess environment at call %d", n+1)
		}
		if len(call.argv) == 0 {
			continue
		}
		switch call.argv[0] {
		case "load-buffer":
			loads++
			if got := call.stdin.String(); got != prompt {
				t.Fatalf("load-buffer stdin = %q, want exact prompt", got)
			}
		case "paste-buffer":
			pastes++
		case "send-keys":
			if hasFlag(call.argv, "-l") {
				bodySendKeys++
			}
			if call.argv[len(call.argv)-1] == "Enter" {
				enters++
			}
		}
	}
	if loads != 1 || pastes != 1 || bodySendKeys != 0 || enters != 1 {
		t.Fatalf("Enter uncertainty retried transport: load=%d paste=%d body_send_keys=%d enter=%d calls=%v",
			loads, pastes, bodySendKeys, enters, calls)
	}
}
