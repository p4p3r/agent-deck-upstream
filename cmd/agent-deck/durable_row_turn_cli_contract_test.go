package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	syntheticCorrelatedBody = "synthetic correlated body beta-47"
	syntheticRequestKey     = "request-key-beta-47"
)

func addStoppedRow(t *testing.T, home, title, tool string) string {
	t.Helper()
	project := filepath.Join(home, "project-"+title)
	if err := os.MkdirAll(project, 0o700); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runAgentDeck(t, home, "add", "-t", title, "-c", tool, "--no-parent", "--json", project)
	if code != 0 {
		t.Fatalf("add stopped row: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil || response.ID == "" {
		t.Fatalf("decode added row: err=%v stdout=%s", err, stdout)
	}
	return response.ID
}

func showRowJSON(t *testing.T, home, ref string) map[string]any {
	t.Helper()
	stdout, stderr, code := runAgentDeck(t, home, "session", "show", ref, "--json")
	if code != 0 {
		t.Fatalf("session show %q: exit=%d stdout=%s stderr=%s", ref, code, stdout, stderr)
	}
	var response map[string]any
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode session show: %v\n%s", err, stdout)
	}
	return response
}

func rowBindingOrPlaceholder(t *testing.T, home, ref string) string {
	t.Helper()
	response := showRowJSON(t, home, ref)
	token, _ := response["row_binding_token"].(string)
	if token == "" {
		t.Error("session show omitted row_binding_token")
		return "missing-row-binding-token"
	}
	return token
}

func compactCLIOutput(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 240 {
		value = value[:240] + "..."
	}
	return value
}

func submitCorrelated(t *testing.T, home, row, binding, key, body string) (map[string]any, string, string, int) {
	t.Helper()
	stdout, stderr, code := runAgentDeckStdin(
		t, home, body,
		"session", "send", row,
		"--queue",
		"--idempotency-key", key,
		"--expected-row-binding", binding,
		"--message-file", "-",
		"--json",
	)
	var response map[string]any
	_ = json.Unmarshal([]byte(stdout), &response)
	return response, stdout, stderr, code
}

func queueRecordPaths(t *testing.T, home string) []string {
	t.Helper()
	var paths []string
	err := filepath.Walk(home, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || !strings.Contains(path, string(filepath.Separator)+"sendqueue"+string(filepath.Separator)) {
			return nil
		}
		name := info.Name()
		if len(name) == 31 && strings.HasSuffix(name, ".json") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func assertBodyFree(t *testing.T, body string, values ...string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(value, body) {
			t.Fatalf("pre-completion response leaked synthetic body: %s", value)
		}
	}
}

func TestSessionShowPublishesOpaqueRowBindingToken(t *testing.T) {
	home := t.TempDir()
	const title = "binding-token-row"
	id := addStoppedRow(t, home, title, "codex")
	byID := showRowJSON(t, home, id)
	token, _ := byID["row_binding_token"].(string)
	if token == "" {
		t.Fatal("session show omitted row_binding_token")
	}
	byTitle := showRowJSON(t, home, title)
	if byTitle["row_binding_token"] != token {
		t.Fatalf("same immutable row returned different bindings: id=%v title=%v", token, byTitle["row_binding_token"])
	}
	for _, rawIdentity := range []string{id, title, byID["path"].(string), "codex"} {
		if rawIdentity != "" && strings.Contains(token, rawIdentity) {
			t.Fatalf("row binding exposes raw identity %q: %q", rawIdentity, token)
		}
	}
}

func TestSessionShowBindingMatchesCorrelatedAdmissionAcrossCallerRefresh(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not available")
	}
	home := t.TempDir()
	binDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binDir, "codex"), []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatalf("write codex fixture: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	extraEnv := []string{}
	if tmuxDir := os.Getenv("TMUX_TMPDIR"); tmuxDir != "" {
		extraEnv = append(extraEnv, "TMUX_TMPDIR="+tmuxDir)
	}
	run := func(stdin string, args ...string) (string, string, int) {
		t.Helper()
		return runAgentDeckEnv(t, home, stdin, extraEnv, args...)
	}
	id := addStoppedRow(t, home, "caller-refresh-row", "codex")
	stdout, stderr, code := run("", "session", "start", id, "--json")
	if code != 0 {
		t.Fatalf("start row: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}

	stdout, stderr, code = run("", "session", "show", id, "--json")
	if code != 0 {
		t.Fatalf("initial session show: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	var initial map[string]any
	if err := json.Unmarshal([]byte(stdout), &initial); err != nil {
		t.Fatalf("decode initial session show: %v\n%s", err, stdout)
	}
	tmuxName, _ := initial["tmux_session"].(string)
	if tmuxName == "" {
		t.Fatalf("initial session show omitted tmux session: %v", initial)
	}
	t.Cleanup(func() {
		_ = exec.Command("tmux", "kill-session", "-t", tmuxName).Run()
	})

	const callerNativeID = "019f6f45-cd2a-7d21-a36f-20b44ce509f1"
	if output, err := exec.Command("tmux", "set-environment", "-t", tmuxName, "CODEX_SESSION_ID", callerNativeID).CombinedOutput(); err != nil {
		t.Fatalf("set caller-local Codex identity: %v: %s", err, output)
	}
	stdout, stderr, code = run("", "session", "show", id, "--json")
	if code != 0 {
		t.Fatalf("refreshed session show: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	var shown map[string]any
	if err := json.Unmarshal([]byte(stdout), &shown); err != nil {
		t.Fatalf("decode refreshed session show: %v\n%s", err, stdout)
	}
	if shown["codex_session_id"] != callerNativeID {
		t.Fatalf("session show did not observe caller-local Codex identity: %v", shown)
	}
	binding, _ := shown["row_binding_token"].(string)
	if binding == "" {
		t.Fatalf("refreshed session show omitted row binding: %v", shown)
	}

	// Remove the caller-local live state without running agent-deck's stop path,
	// which would intentionally persist the observed native identity.
	if output, err := exec.Command("tmux", "kill-session", "-t", tmuxName).CombinedOutput(); err != nil {
		t.Fatalf("remove caller-local tmux session: %v: %s", err, output)
	}
	response, stdout, stderr, code := submitCorrelated(t, home, id, binding, syntheticRequestKey, syntheticCorrelatedBody)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code != 0 || response["success"] != true || response["row_binding_token"] != binding {
		t.Fatalf("correlated admission disagreed with session show: exit=%d response=%v stdout=%q stderr=%q", code, response, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
}

func TestRowBindingChangesWithHarnessBinding(t *testing.T) {
	home := t.TempDir()
	id := addStoppedRow(t, home, "binding-change-row", "codex")
	before := rowBindingOrPlaceholder(t, home, id)
	stdout, stderr, code := runAgentDeck(t, home, "session", "set", id, "tool", "shell", "--json")
	if code != 0 {
		t.Fatalf("change harness: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	after := rowBindingOrPlaceholder(t, home, id)
	if before == after {
		t.Fatalf("binding did not change across harness replacement: %q", before)
	}
}

func TestCorrelatedQueueSubmitIsIdempotentAndBodyFree(t *testing.T) {
	home := t.TempDir()
	id := addStoppedRow(t, home, "idempotent-row", "codex")
	binding := rowBindingOrPlaceholder(t, home, id)

	first, stdout, stderr, code := submitCorrelated(t, home, id, binding, syntheticRequestKey, syntheticCorrelatedBody)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code != 0 {
		t.Fatalf("first correlated submit: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	firstID, _ := first["send_id"].(string)
	if firstID == "" || first["schema_version"] != float64(1) || first["success"] != true ||
		first["session_id"] != id || first["idempotency_key"] != syntheticRequestKey ||
		first["row_binding_token"] != binding || first["operation_state"] != "queued" {
		t.Fatalf("first correlated submit schema: %v", first)
	}

	// The first response is deliberately ignored before retrying the same request.
	second, stdout, stderr, code := submitCorrelated(t, home, id, binding, syntheticRequestKey, syntheticCorrelatedBody)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code != 0 || second["send_id"] != firstID {
		t.Fatalf("idempotent retry: exit=%d first=%v second=%v stdout=%q stderr=%q", code, first, second, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	if paths := queueRecordPaths(t, home); len(paths) != 1 {
		t.Fatalf("idempotent retry created %d queue records: %v", len(paths), paths)
	}

	stdout, stderr, code = runAgentDeck(t, home, "session", "send-status", firstID, "--json")
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code != 0 {
		t.Fatalf("status lookup: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	var status map[string]any
	if err := json.Unmarshal([]byte(stdout), &status); err != nil {
		t.Fatalf("decode status: %v\n%s", err, stdout)
	}
	for _, key := range []string{"schema_version", "send_id", "session_id", "idempotency_key", "row_binding_token", "operation_state"} {
		if _, ok := status[key]; !ok {
			t.Errorf("status omitted %q: %v", key, status)
		}
	}
	for _, forbidden := range []string{"message", "images", "session_title", "transcript_path", "child_pid", "landed_row_id"} {
		if _, leaked := status[forbidden]; leaked {
			t.Errorf("pre-completion status exposed %q: %v", forbidden, status)
		}
	}
}

func TestCorrelatedQueueKeyConflictsBeforeTransport(t *testing.T) {
	home := t.TempDir()
	firstRow := addStoppedRow(t, home, "conflict-row-a", "codex")
	firstBinding := rowBindingOrPlaceholder(t, home, firstRow)
	first, stdout, stderr, code := submitCorrelated(t, home, firstRow, firstBinding, syntheticRequestKey, syntheticCorrelatedBody)
	if code != 0 || first["send_id"] == nil {
		t.Fatalf("seed correlated submit: exit=%d stdout=%q stderr=%q", code, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}

	secondRow := addStoppedRow(t, home, "conflict-row-b", "codex")
	secondBinding := rowBindingOrPlaceholder(t, home, secondRow)
	cases := []struct {
		name, row, binding, body string
	}{
		{name: "different body", row: firstRow, binding: firstBinding, body: syntheticCorrelatedBody + " changed"},
		{name: "different binding", row: firstRow, binding: firstBinding + "-changed", body: syntheticCorrelatedBody},
		{name: "different row", row: secondRow, binding: secondBinding, body: syntheticCorrelatedBody},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, stdout, stderr, code := submitCorrelated(t, home, tc.row, tc.binding, syntheticRequestKey, tc.body)
			assertBodyFree(t, tc.body, stdout, stderr)
			if code == 0 {
				t.Fatalf("conflicting key reuse succeeded: %v", response)
			}
			if response["success"] != false || response["code"] == nil || response["retry_safe"] != false {
				t.Fatalf("conflict is not a fixed non-retry result: exit=%d response=%v stdout=%q stderr=%q", code, response, compactCLIOutput(stdout), compactCLIOutput(stderr))
			}
		})
	}
	if paths := queueRecordPaths(t, home); len(paths) != 1 {
		t.Fatalf("conflicting retries created queue records: %v", paths)
	}
}

func TestCorrelatedQueueRejectsTitleBeforeDurablePrepare(t *testing.T) {
	home := t.TempDir()
	const title = "title-must-not-route"
	id := addStoppedRow(t, home, title, "codex")
	binding := rowBindingOrPlaceholder(t, home, id)
	response, stdout, stderr, code := submitCorrelated(t, home, title, binding, syntheticRequestKey, syntheticCorrelatedBody)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code == 0 || response["success"] != false || response["code"] == nil {
		t.Fatalf("title resolved a correlated send: exit=%d response=%v stdout=%q stderr=%q", code, response, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	if paths := queueRecordPaths(t, home); len(paths) != 0 {
		t.Fatalf("title rejection durably prepared work: %v", paths)
	}
}

func TestCorrelatedQueueStaleBindingFailsWithoutTransport(t *testing.T) {
	home := t.TempDir()
	id := addStoppedRow(t, home, "stale-binding-row", "codex")
	current := rowBindingOrPlaceholder(t, home, id)
	response, stdout, stderr, code := submitCorrelated(
		t, home, id, current+"-stale", syntheticRequestKey, syntheticCorrelatedBody,
	)
	assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
	if code == 0 || response["success"] != false || response["operation_state"] != "binding_changed" || response["retry_safe"] != true {
		t.Fatalf("stale binding result: exit=%d response=%v stdout=%q stderr=%q", code, response, compactCLIOutput(stdout), compactCLIOutput(stderr))
	}
	if attempts, ok := response["attempts"].(float64); ok && attempts != 0 {
		t.Fatalf("stale binding reached transport: %v", response)
	}
}

func TestCorrelatedQueueOpaqueInputsFailClosed(t *testing.T) {
	home := t.TempDir()
	id := addStoppedRow(t, home, "opaque-input-row", "codex")
	binding := rowBindingOrPlaceholder(t, home, id)
	tests := []struct {
		name, key, token string
	}{
		{name: "empty key", key: "", token: binding},
		{name: "path key", key: "../request", token: binding},
		{name: "oversized key", key: strings.Repeat("k", 8<<10), token: binding},
		{name: "empty binding", key: syntheticRequestKey, token: ""},
		{name: "oversized binding", key: syntheticRequestKey, token: strings.Repeat("b", 8<<10)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, stdout, stderr, code := submitCorrelated(t, home, id, tt.token, tt.key, syntheticCorrelatedBody)
			assertBodyFree(t, syntheticCorrelatedBody, stdout, stderr)
			if code == 0 || response["success"] != false || response["code"] == nil {
				t.Fatalf("invalid opaque input accepted: exit=%d response=%v stdout=%q stderr=%q", code, response, compactCLIOutput(stdout), compactCLIOutput(stderr))
			}
		})
	}
	if paths := queueRecordPaths(t, home); len(paths) != 0 {
		t.Fatalf("invalid opaque inputs created queue records: %v", paths)
	}
}

func TestCorrelatedQueueHelpDocumentsRecoveryContract(t *testing.T) {
	home := t.TempDir()
	stdout, stderr, code := runAgentDeck(t, home, "session", "send", "--help")
	if code != 0 {
		t.Fatalf("send help: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for _, want := range []string{
		"--idempotency-key", "--expected-row-binding", "immutable", "idempotent",
		"accepted", "completed", "indeterminate", "result_unavailable",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("session send help omitted %q", want)
		}
	}
	stdout, stderr, code = runAgentDeck(t, home, "session", "send-status", "--help")
	if code != 0 {
		t.Fatalf("send-status help: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for _, want := range []string{"accepted_turn", "turn_generation", "completed", "result_unavailable"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("send-status help omitted %q", want)
		}
	}
}
