package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/events"
	"github.com/asheshgoplani/agent-deck/internal/recall/query"
	"github.com/asheshgoplani/agent-deck/internal/send"
	"github.com/asheshgoplani/agent-deck/internal/sendqueue"
	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/google/uuid"
)

// imageList is the repeatable --image flag.
type imageList []string

func (l *imageList) String() string     { return strings.Join(*l, ",") }
func (l *imageList) Set(v string) error { *l = append(*l, v); return nil }

var imageExtensions = map[string]bool{".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".webp": true}

// errImagesUnsupported marks a harness that cannot take an image in a
// running session; the CLI exits 2 for it.
var errImagesUnsupported = errors.New("images not supported")

// attachImages copies images next to the session and returns the message
// with the harness's image reference appended. Claude Code and Gemini CLI
// read `@path` from the composer. Codex takes images only at launch (-i), so
// a running Codex session refuses them, as does any other harness.
func attachImages(inst *session.Instance, message string, images []string, now time.Time) (string, []string, error) {
	if len(images) == 0 {
		return message, nil, nil
	}
	switch {
	case session.IsClaudeCompatible(inst.Tool), inst.Tool == "gemini":
	case session.IsCodexCompatible(inst.Tool):
		return "", nil, fmt.Errorf("%w for codex in a running session (Codex accepts images only at launch with -i)", errImagesUnsupported)
	default:
		return "", nil, fmt.Errorf("%w for %s", errImagesUnsupported, inst.Tool)
	}
	dir := filepath.Join(inst.EffectiveWorkingDir(), ".agentdeck-images")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, fmt.Errorf("image dir: %w", err)
	}
	// Keep the copies out of the user's git status.
	ignore := filepath.Join(dir, ".gitignore")
	if _, err := os.Stat(ignore); os.IsNotExist(err) {
		_ = os.WriteFile(ignore, []byte("*\n"), 0o644)
	}
	var saved []string
	refs := []string{strings.TrimSpace(message)}
	for i, src := range images {
		if !imageExtensions[strings.ToLower(filepath.Ext(src))] {
			return "", nil, fmt.Errorf("%s: not an image (png, jpg, jpeg, gif, webp)", src)
		}
		in, err := os.Open(src)
		if err != nil {
			return "", nil, err
		}
		info, err := in.Stat()
		if err != nil || !info.Mode().IsRegular() {
			in.Close()
			return "", nil, fmt.Errorf("%s: not a regular file", src)
		}
		// The composer reads @path up to the first space: the copy's name
		// never has one.
		name := strings.Join(strings.Fields(filepath.Base(src)), "-")
		dst := filepath.Join(dir, fmt.Sprintf("%d-%d-%s", now.UnixMilli(), i, name))
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, err = io.Copy(out, in)
			if cerr := out.Close(); err == nil {
				err = cerr
			}
		}
		in.Close()
		if err != nil {
			return "", nil, fmt.Errorf("copy %s: %w", src, err)
		}
		saved = append(saved, dst)
		refs = append(refs, "@"+dst)
	}
	return strings.TrimSpace(strings.Join(refs, " ")), saved, nil
}

// profileArgs prefixes -p <profile> when a profile was chosen explicitly.
func profileArgs(profile string, args ...string) []string {
	if profile == "" {
		return args
	}
	return append([]string{"-p", profile}, args...)
}

func sendQueueDir(storage *session.Storage) string {
	return sendqueue.Dir(filepath.Dir(storage.Path()))
}

// publishSendState mirrors a queued send's state on the bus so a client
// following `events follow --kind session.send` never polls send-status.
func publishSendState(profile string, r *sendqueue.Record) {
	events.PublishProfile(profile, "session.send", r.SessionID, map[string]string{"send_id": r.SendID, "state": r.State, "verdict": r.Verdict, "reason": r.Reason})
}

// queueSend records the send and hands it to the target's worker. It never
// types anything itself; it returns at once.
func queueSend(profile string, storage *session.Storage, inst *session.Instance, message string, images []string, out *CLIOutput) {
	now := time.Now()
	dir := sendQueueDir(storage)
	status := "unknown"
	id, err := sendqueue.NextID(dir, now)
	if err != nil {
		out.Error(fmt.Sprintf("cannot queue send: %v", err), ErrCodeInvalidOperation)
		os.Exit(1)
	}
	rec := &sendqueue.Record{
		SendID: id, State: sendqueue.StateQueued, Verdict: "queued", TargetStatus: status,
		SessionID: inst.ID, SessionTitle: inst.Title, Tool: inst.Tool, Message: message, Images: images,
		CreatedAt: now.UTC().Format(time.RFC3339Nano), UpdatedAt: now.UTC().Format(time.RFC3339Nano),
		Deadline: now.Add(sendqueue.DefaultRetryBudget).UTC().Format(time.RFC3339Nano),
	}
	if session.IsClaudeCompatible(inst.Tool) {
		rec.ClaudeSessionID = inst.ClaudeSessionID
	}
	if !inst.Exists() {
		rec.State, rec.Reason, rec.Verdict = sendqueue.StateFailed, "target not running", "unknown"
	}
	if err := sendqueue.Save(dir, rec); err != nil {
		out.Error(fmt.Sprintf("cannot queue send: %v", err), ErrCodeInvalidOperation)
		os.Exit(1)
	}
	publishSendState(profile, rec)
	if rec.State == sendqueue.StateFailed {
		out.ErrorWithData(fmt.Sprintf("send %s failed: %s", rec.SendID, rec.Reason), ErrCodeDeliveryFailed, queuedSendFields(rec))
		os.Exit(1)
	}
	if err := spawnSendWorker(profile, inst.ID); err != nil {
		// The record stays queued; the next --queue or send-status for
		// this target starts a worker again.
		fmt.Fprintf(os.Stderr, "Warning: could not start the delivery worker yet: %v\n", err)
	}
	out.Success(fmt.Sprintf("Queued %s for '%s' (%s)", rec.SendID, inst.Title, status), queuedSendFields(rec))
}

func queueCorrelatedSend(profile string, storage *session.Storage, instances []*session.Instance, sessionRef, message, idempotencyKey, expectedBinding string, out *CLIOutput) {
	if sendqueue.ValidateOpaque(idempotencyKey) != nil || sendqueue.ValidateOpaque(expectedBinding) != nil {
		out.ErrorWithData("invalid correlated queue identity", "invalid_correlated_request", map[string]interface{}{"retry_safe": false})
		os.Exit(2)
	}
	if !validInstanceID.MatchString(sessionRef) || strings.Contains(sessionRef, "..") {
		out.ErrorWithData("correlated sends require an immutable session id", sendqueue.OperationRefused, map[string]interface{}{"operation_state": sendqueue.OperationRefused, "retry_safe": true})
		os.Exit(2)
	}
	inst := instanceByID(instances, sessionRef)

	initialState := sendqueue.OperationQueued
	operationError := ""
	retrySafe := false
	switch {
	case inst == nil:
		initialState, operationError, retrySafe = sendqueue.OperationRefused, sendqueue.OperationRefused, true
	case expectedBinding != session.RowBindingToken(inst):
		initialState, operationError, retrySafe = sendqueue.OperationBindingChanged, sendqueue.OperationBindingChanged, true
	case !session.IsCodexCompatible(inst.Tool):
		initialState, operationError, retrySafe = sendqueue.OperationRefused, sendqueue.OperationRefused, true
	}
	request := sendqueue.CorrelatedRequest{
		SessionID: sessionRef, IdempotencyKey: idempotencyKey, RowBindingToken: expectedBinding,
		Message: message, Now: time.Now(), InitialState: initialState,
		OperationError: operationError, RetrySafe: retrySafe,
	}
	var record *sendqueue.Record
	var created bool
	var err error
	if inst == nil {
		var found bool
		record, found, err = sendqueue.LookupCorrelated(sendQueueDir(storage), request)
		if err == nil && !found {
			out.ErrorWithData("immutable session id was not found", sendqueue.OperationRefused, map[string]interface{}{"operation_state": sendqueue.OperationRefused, "retry_safe": true})
			os.Exit(2)
		}
	} else {
		record, created, err = sendqueue.EnqueueCorrelated(sendQueueDir(storage), request)
	}
	if errors.Is(err, sendqueue.ErrIdempotencyConflict) {
		out.ErrorWithData("idempotency key is already bound to a different request", "idempotency_conflict", map[string]interface{}{"retry_safe": false})
		os.Exit(1)
	}
	if err != nil {
		out.ErrorWithData("cannot durably queue correlated send", "durability_failure", map[string]interface{}{"retry_safe": false})
		os.Exit(1)
	}
	fields := correlatedRecordFields(record)
	if record.Final() {
		code := record.OperationError
		if code == "" {
			code = record.OperationState
		}
		out.ErrorWithData("correlated operation ended before transport", code, fields)
		os.Exit(1)
	}
	if created && inst != nil && inst.Exists() {
		if err := spawnSendWorker(profile, inst.ID); err != nil {
			fmt.Fprintln(os.Stderr, "Warning: correlated operation is durable but its worker has not started yet")
		}
	}
	fields["success"] = true
	out.Success(fmt.Sprintf("Queued correlated operation %s", record.SendID), fields)
}

// queuedSendFields is the immediate --json reply for a queued send: the
// record plus the documented sync-send keys (success, delivery, submitted,
// confirmation), so a reader written against the synchronous reply keeps
// working. A queued send has been accepted, not yet submitted, and its
// confirmation is unknown until send-status reports; a record that failed
// at once reports the failure the same way the sync path does.
func queuedSendFields(rec *sendqueue.Record) map[string]interface{} {
	fields := recordFields(rec)
	fields["submitted"] = false
	if rec.State == sendqueue.StateFailed {
		fields["success"], fields["delivery"], fields["confirmation"] = false, deliveryPaneGone, send.ConfirmationFailed
		return fields
	}
	fields["success"], fields["delivery"], fields["confirmation"] = true, deliveryQueued, send.ConfirmationUnknown
	return fields
}

func recordFields(r *sendqueue.Record) map[string]interface{} {
	if r.Correlated() {
		return correlatedRecordFields(r)
	}
	b, _ := json.Marshal(r)
	var m map[string]interface{}
	_ = json.Unmarshal(b, &m)
	return m
}

func correlatedRecordFields(record *sendqueue.Record) map[string]interface{} {
	fields := map[string]interface{}{
		"schema_version":    record.SchemaVersion,
		"send_id":           record.SendID,
		"session_id":        record.SessionID,
		"idempotency_key":   record.IdempotencyKey,
		"row_binding_token": record.RowBindingToken,
		"operation_state":   record.OperationState,
	}
	if record.Attempts > 0 || record.Final() {
		fields["attempts"] = record.Attempts
	}
	if record.AcceptedTurn != nil {
		fields["accepted_turn"] = map[string]interface{}{
			"receipt_id": record.AcceptedTurn.ReceiptID, "instance_id": record.AcceptedTurn.InstanceID,
			"codex_session_id": record.AcceptedTurn.CodexSessionID,
			"turn_generation":  record.AcceptedTurn.TurnGeneration, "accepted_at": record.AcceptedTurn.AcceptedAt,
		}
	}
	if record.OperationState == sendqueue.OperationCompleted && record.Completion != nil {
		completion := map[string]interface{}{"turn_generation": record.Completion.TurnGeneration}
		if record.Completion.CompletedAt != "" {
			completion["completed_at"] = record.Completion.CompletedAt
		}
		fields["completion"] = completion
		fields["content"] = record.Content
	}
	if record.Final() && record.OperationState != sendqueue.OperationCompleted {
		code := record.OperationError
		if code == "" {
			code = record.OperationState
		}
		fields["code"] = code
		retrySafe := false
		if record.RetrySafe != nil {
			retrySafe = *record.RetrySafe
		}
		fields["retry_safe"] = retrySafe
	}
	return fields
}

func persistCorrelatedAccepted(storage *session.Storage, operationID string, receipt *codexAcceptedTurnReceipt) error {
	if storage == nil || receipt == nil {
		return fmt.Errorf("correlated acceptance persistence is unavailable")
	}
	_, err := sendqueue.Update(sendQueueDir(storage), operationID, time.Now(), func(record *sendqueue.Record) {
		record.OperationState = sendqueue.OperationAccepted
		record.State = sendqueue.StateSubmitted
		record.Verdict = "delivered"
		record.CodexSessionID = receipt.CodexSessionID
		record.AcceptedTurn = &sendqueue.AcceptedTurn{
			ReceiptID: receipt.ReceiptID, InstanceID: receipt.InstanceID,
			CodexSessionID: receipt.CodexSessionID, TurnGeneration: receipt.TurnGeneration,
			AcceptedAt: receipt.AcceptedAt,
		}
	})
	return err
}

func persistCorrelatedCompletion(storage *session.Storage, operationID string, receipt *codexAcceptedTurnReceipt, response *session.ResponseOutput) error {
	if storage == nil || receipt == nil || response == nil || response.CodexTurnGeneration != receipt.TurnGeneration {
		return fmt.Errorf("correlated completion does not match its accepted generation")
	}
	_, err := sendqueue.Update(sendQueueDir(storage), operationID, time.Now(), func(record *sendqueue.Record) {
		record.OperationState = sendqueue.OperationCompleted
		record.State = sendqueue.StateLanded
		record.Verdict = "delivered"
		record.Completion = &sendqueue.Completion{
			TurnGeneration: receipt.TurnGeneration,
			CompletedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		}
		record.Content = response.Content
	})
	return err
}

func validateCorrelatedWorkerBinding(storage *session.Storage, operationID string, instance *session.Instance) error {
	if storage == nil || instance == nil {
		return fmt.Errorf("correlated worker binding is unavailable")
	}
	dir := sendQueueDir(storage)
	record, err := sendqueue.Load(dir, operationID)
	if err != nil {
		return err
	}
	if !record.Correlated() || record.SessionID != instance.ID || record.OperationState != sendqueue.OperationPreparing {
		return fmt.Errorf("correlated worker does not own the prepared operation")
	}
	if session.RowBindingToken(instance) == record.RowBindingToken {
		return nil
	}
	retrySafe := true
	_, updateErr := sendqueue.Update(dir, operationID, time.Now(), func(current *sendqueue.Record) {
		current.OperationState = sendqueue.OperationBindingChanged
		current.OperationError = sendqueue.OperationBindingChanged
		current.State = sendqueue.StateFailed
		current.Verdict = "unknown"
		current.ChildPID = 0
		current.RetrySafe = &retrySafe
	})
	if updateErr != nil {
		return updateErr
	}
	return fmt.Errorf("row binding changed before transport")
}

// spawnSendWorker starts a detached worker for the target. A second worker
// for the same target exits at once on the target lock. sessionID comes from
// storage or an on-disk queue record, so it is checked against the same
// instance-id guard the hook handler uses before it reaches argv.
func spawnSendWorker(profile, sessionID string) error {
	if !validInstanceID.MatchString(sessionID) || strings.Contains(sessionID, "..") {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, profileArgs(profile, "session", "send-worker", "--target", sessionID)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}

// kickPendingSendWorkers starts a worker for every target in the queue
// directory dir (or only sessionID) that still has an unfinished queued
// send, e.g. after a reboot killed the old workers. A live worker keeps its
// target lock, so the extra one exits at once.
func kickPendingSendWorkers(profile, dir, sessionID string) {
	for _, target := range sendqueue.PendingTargets(dir) {
		if sessionID == "" || target == sessionID {
			_ = spawnSendWorker(profile, target)
		}
	}
}

// handleSessionSendStatus implements `agent-deck session send-status <send-id> --json`.
func handleSessionSendStatus(profile string, args []string) {
	fs := flag.NewFlagSet("session send-status", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	jsonOutput := fs.Bool("json", false, "Output as JSON")
	fs.Usage = func() {
		fmt.Println("Usage: agent-deck session send-status <send-id> [--json]")
		fmt.Println()
		fmt.Println("State of a `session send --queue` message: queued, typing, typed, submitted, landed or failed,")
		fmt.Println("with reason, target_status, attempts, and landed_row_id/landed_at once the text is")
		fmt.Println("in the transcript (the row id recall timeline/follow use). Exit 0 when found, 2 for an unknown id.")
		fmt.Println()
		fmt.Println("landed is reported only on transcript evidence (a user row, or a queued message absorbed")
		fmt.Println("into the turn). failed means nothing was typed, so resending is safe. A send whose outcome")
		fmt.Println("could not be proven settles as typed/submitted (settled: true) and is never typed again.")
		fmt.Println()
		fmt.Println("Correlated operations expose accepted_turn with its exact turn_generation, then completed")
		fmt.Println("with exact content. Missing exact evidence ends as result_unavailable; earlier states are body-free.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(normalizeArgs(fs, args)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(2)
	}
	out := NewCLIOutput(*jsonOutput, false)
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}
	storage, err := session.NewStorageWithProfile(profile)
	if err != nil {
		out.Error(err.Error(), ErrCodeInvalidOperation)
		os.Exit(1)
	}
	defer storage.Close()
	dir := sendQueueDir(storage)
	rec, err := sendqueue.Load(dir, fs.Arg(0))
	if err != nil {
		out.Error(fmt.Sprintf("send %s: %v", fs.Arg(0), err), ErrCodeNotFound)
		os.Exit(2)
	}
	if !rec.Final() {
		// A worker that died (reboot, kill) is restarted by any status read.
		_ = spawnSendWorker(profile, rec.SessionID)
	}
	out.Success(fmt.Sprintf("%s: %s %s", rec.SendID, rec.State, rec.Reason), recordFields(rec))
}

// envDuration reads a positive duration from the environment, so tests can
// tune the worker's timing; def otherwise.
func envDuration(name string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(name)); err == nil && d > 0 {
		return d
	}
	return def
}

// sendWorkerPoll is how often the worker rechecks a busy target.
func sendWorkerPoll() time.Duration {
	return envDuration("AGENTDECK_SEND_WORKER_POLL", time.Second)
}

// sendLandWindow is how long a delivered send is watched for in the
// transcript before it is settled without a landed row.
func sendLandWindow() time.Duration {
	return envDuration("AGENTDECK_SEND_LAND_WINDOW", 2*time.Minute)
}

// handleSessionSendWorker is the hidden detached worker: it owns delivery
// for one target, walks its queued records oldest first, and exits when
// none are left.
func handleSessionSendWorker(profile string, args []string) {
	fs := flag.NewFlagSet("session send-worker", flag.ContinueOnError)
	target := fs.String("target", "", "deck session id")
	watch := fs.String("watch", "", "send id whose transcript is being watched")
	if err := fs.Parse(args); err != nil || (*target == "" && *watch == "") {
		os.Exit(2)
	}
	storage, err := session.NewStorageWithProfile(profile)
	if err != nil {
		os.Exit(1)
	}
	dir := sendQueueDir(storage)
	storage.Close()
	if *watch != "" {
		watchQueuedSend(profile, dir, *watch)
		return
	}
	sendqueue.Prune(dir, time.Now().Add(-sendqueue.RetainFinished))
	sendqueue.PruneWithRetention(dir, time.Now(), sendqueue.DefaultRetentionPolicy())
	for {
		lock, ok, err := sendqueue.TryLock(dir, *target)
		if err != nil || !ok {
			return // another worker owns this target
		}
		for {
			rec := nextPending(dir, *target)
			if rec == nil {
				break
			}
			deliverQueuedAsync(profile, dir, rec)
		}
		startPendingWatchers(profile, dir, *target)
		lock.Release()
		// A send queued while this worker was finishing: pick it up.
		if nextPending(dir, *target) == nil {
			return
		}
	}
}

func nextPending(dir, target string) *sendqueue.Record {
	recs, _ := sendqueue.List(dir, target)
	for _, r := range recs {
		if r.Correlated() && !r.Final() {
			return r
		}
		if r.State == sendqueue.StateQueued || r.State == sendqueue.StateTyping {
			return r
		}
	}
	return nil
}

// Transcript confirmation is independent of typing. Watching one send must
// not block later messages from entering a busy Claude harness's own queue.
func startPendingWatchers(profile, dir, target string) {
	recs, _ := sendqueue.List(dir, target)
	for _, r := range recs {
		if !r.Final() && (r.State == sendqueue.StateTyped || r.State == sendqueue.StateSubmitted) {
			if err := spawnSendWatcher(profile, r.SendID); err != nil {
				watchQueuedSend(profile, dir, r.SendID)
			}
		}
	}
}

func spawnSendWatcher(profile, sendID string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, profileArgs(profile, "session", "send-worker", "--watch", sendID)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}

func watchQueuedSend(profile, dir, sendID string) {
	lock, ok, err := sendqueue.TryLock(dir, "watch-"+sendID)
	if err != nil || !ok {
		return
	}
	defer lock.Release()
	rec, err := sendqueue.Load(dir, sendID)
	if err != nil || rec.Final() || (rec.State != sendqueue.StateTyped && rec.State != sendqueue.StateSubmitted) {
		return
	}
	set := func(fn func(*sendqueue.Record)) error {
		r, err := sendqueue.Update(dir, sendID, time.Now(), fn)
		if err != nil {
			return err
		}
		if r.State != rec.State || r.Reason != rec.Reason || r.Verdict != rec.Verdict {
			publishSendState(profile, r)
		}
		*rec = *r
		return nil
	}
	watchLanded(profile, rec, set)
}

// childOutcome is what a `session send` child's result says happened.
type childOutcome int

const (
	childTyped     childOutcome = iota // exit 0: typed, submission not confirmed
	childSubmitted                     // exit 0 with confirmed submission
	childNotSent                       // refused before typing: safe to retry
	childUnknown                       // anything else: may have been typed
)

// notSentDeliveries are the `session send` outcomes that guarantee nothing
// was typed, so the worker may try again once the target settles.
var notSentDeliveries = map[string]bool{
	deliveryTargetBusy:        true,
	deliveryComposerBlocked:   true,
	deliveryAcceptanceRefused: true,
	deliveryLineTooLong:       true,
}

// classifyChild reads a `session send --json` result. Only a refusal that
// guarantees nothing was typed may be retried. Every other failure — an open
// menu, a readiness timeout, no_evidence, a crash — may have typed the text,
// so it is never reported failed (a client that resends on failed would
// double the message): it settles as typed and the transcript decides.
func classifyChild(result map[string]interface{}, code int) (childOutcome, string) {
	delivery, _ := result["delivery"].(string)
	success, _ := result["success"].(bool)
	if code == 0 && (success || len(result) == 0) {
		if submitted, _ := result["submitted"].(bool); submitted || result["confirmation"] == "confirmed" {
			return childSubmitted, ""
		}
		return childTyped, ""
	}
	if notSentDeliveries[delivery] {
		return childNotSent, delivery
	}
	reason, _ := result["error"].(string)
	if reason == "" {
		reason = fmt.Sprintf("session send exited %d", code)
	}
	if delivery != "" {
		reason = delivery + ": " + reason
	}
	return childUnknown, reason
}

// sendChild starts the child that types one queued message; tests replace it.
var sendChild = startChildSend

// deliverQueued drives one record to landed, failed (only when nothing was
// typed), or settled typed/submitted.
func deliverQueued(profile, dir string, rec *sendqueue.Record) {
	deliverQueuedMode(profile, dir, rec, true)
}

func deliverQueuedAsync(profile, dir string, rec *sendqueue.Record) {
	deliverQueuedMode(profile, dir, rec, false)
}

func deliverQueuedMode(profile, dir string, rec *sendqueue.Record, watch bool) {
	if rec.Correlated() {
		deliverCorrelated(profile, dir, rec)
		return
	}
	poll := sendWorkerPoll()
	deadline, _ := time.Parse(time.RFC3339Nano, rec.Deadline)
	set := func(fn func(*sendqueue.Record)) error {
		r, err := sendqueue.Update(dir, rec.SendID, time.Now(), fn)
		if err != nil {
			return err
		}
		if r.State != rec.State || r.Reason != rec.Reason || r.Verdict != rec.Verdict {
			publishSendState(profile, r)
		}
		*rec = *r
		return nil
	}
	fail := func(reason string) {
		_ = set(func(r *sendqueue.Record) { r.State, r.Reason, r.Verdict = sendqueue.StateFailed, reason, "unknown" })
	}
	pastDeadline := func() bool { return !deadline.IsZero() && time.Now().After(deadline) }
	if rec.State == sendqueue.StateTyping {
		// The previous worker died while its child was delivering.
		reconcileTyping(dir, rec, set)
	}
	for rec.State == sendqueue.StateQueued {
		_, instances, _, err := loadSessionData(profile)
		if err != nil {
			fail("cannot load sessions: " + err.Error())
			return
		}
		inst := instanceByID(instances, rec.SessionID)
		if inst == nil {
			fail("target removed")
			return
		}
		if !inst.Exists() {
			fail("target not running")
			return
		}
		status, _ := fetchHookDrivenStatus(profile, inst.ID)
		if shouldWaitForIdle(inst.Tool, status) {
			if pastDeadline() {
				fail("target stayed busy past the retry budget")
				return
			}
			_ = set(func(r *sendqueue.Record) { r.TargetStatus = status })
			time.Sleep(poll)
			continue
		}
		path := session.LiveTranscriptPath(inst, instances)
		var from int64
		if info, err := os.Stat(path); err == nil {
			from = info.Size()
		}
		if !typeQueued(profile, dir, rec, status, path, from, set) {
			fail("cannot record the send before typing it")
			return
		}
		if rec.State == sendqueue.StateQueued {
			// Refused before typing: safe to try again once the target settles.
			if pastDeadline() {
				fail("not delivered before the retry budget ran out: " + strings.TrimPrefix(rec.Reason, "retrying: "))
				return
			}
			time.Sleep(poll)
		}
	}
	if rec.Final() {
		return
	}
	if watch {
		watchLanded(profile, rec, set)
	}
}

var correlatedSendChild = startCorrelatedChildSend

func deliverCorrelated(profile, dir string, record *sendqueue.Record) {
	set := func(update func(*sendqueue.Record)) error {
		next, err := sendqueue.Update(dir, record.SendID, time.Now(), update)
		if err == nil {
			*record = *next
		}
		return err
	}
	deadline, _ := time.Parse(time.RFC3339Nano, record.Deadline)
	if record.OperationState == sendqueue.OperationPreparing {
		reconcileCorrelatedPreparing(profile, dir, record, set)
	}
	if record.OperationState == sendqueue.OperationAccepted {
		completeCorrelatedOperation(profile, record, deadline, set)
		return
	}
	if record.OperationState != sendqueue.OperationQueued {
		return
	}

	for record.OperationState == sendqueue.OperationQueued {
		_, instances, _, err := loadSessionData(profile)
		if err != nil {
			setCorrelatedTerminal(record, sendqueue.OperationRefused, true, set)
			return
		}
		instance := instanceByID(instances, record.SessionID)
		if instance == nil || !session.IsCodexCompatible(instance.Tool) {
			setCorrelatedTerminal(record, sendqueue.OperationRefused, true, set)
			return
		}
		if session.RowBindingToken(instance) != record.RowBindingToken {
			setCorrelatedTerminal(record, sendqueue.OperationBindingChanged, true, set)
			return
		}
		if !instance.Exists() {
			if !deadline.IsZero() && time.Now().After(deadline) {
				setCorrelatedTerminal(record, sendqueue.OperationExpired, true, set)
			}
			return
		}
		status, _ := fetchHookDrivenStatus(profile, instance.ID)
		if shouldWaitForIdle(instance.Tool, status) {
			if !deadline.IsZero() && time.Now().After(deadline) {
				setCorrelatedTerminal(record, sendqueue.OperationExpired, true, set)
				return
			}
			time.Sleep(sendWorkerPoll())
			continue
		}
		if session.RowBindingToken(instance) != record.RowBindingToken {
			setCorrelatedTerminal(record, sendqueue.OperationBindingChanged, true, set)
			return
		}
		resultPath := sendqueue.ResultPath(dir, record.SendID)
		_ = os.Remove(resultPath)
		if err := set(func(current *sendqueue.Record) {
			current.OperationState = sendqueue.OperationPreparing
			current.State = sendqueue.StateTyping
			current.Reason = ""
			current.Attempts++
			current.TargetStatus = status
			current.CodexSessionID = instance.CodexSessionID
			current.Tool = instance.Tool
			current.HarnessAccount = instance.Account
			current.HarnessCommand = instance.Command
			current.HarnessWrapper = instance.Wrapper
			current.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
			current.ChildPID = 0
		}); err != nil {
			return
		}
		pid, wait, err := correlatedSendChild(profile, record.SessionID, record.SendID, record.Message, resultPath, deadline)
		if err != nil {
			setCorrelatedTerminal(record, sendqueue.OperationRefused, true, set)
			return
		}
		_ = set(func(current *sendqueue.Record) { current.ChildPID = pid })
		code := wait()
		if latest, err := sendqueue.Load(dir, record.SendID); err == nil {
			*record = *latest
		}
		if record.Final() {
			return
		}
		applyChildResult(record, readChildResult(resultPath), code, true, set)
		if record.OperationState == sendqueue.OperationAccepted {
			completeCorrelatedOperation(profile, record, deadline, set)
		}
		return
	}
}

func setCorrelatedTerminal(record *sendqueue.Record, state string, retrySafe bool, set func(func(*sendqueue.Record)) error) {
	_ = set(func(current *sendqueue.Record) {
		current.OperationState = state
		current.OperationError = state
		current.State = sendqueue.StateFailed
		current.Verdict = "unknown"
		current.ChildPID = 0
		current.RetrySafe = new(bool)
		*current.RetrySafe = retrySafe
	})
}

func reconcileCorrelatedPreparing(profile, dir string, record *sendqueue.Record, set func(func(*sendqueue.Record)) error) {
	resultPath := sendqueue.ResultPath(dir, record.SendID)
	end := time.Now().Add(sendChildWaitMax())
	for {
		if result, ok := parseChildResult(resultPath); ok {
			code := 1
			if success, _ := result["success"].(bool); success {
				code = 0
			}
			applyChildResult(record, result, code, true, set)
			return
		}
		if !processAlive(record.ChildPID) || time.Now().After(end) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	_, instances, _, err := loadSessionData(profile)
	if err != nil {
		setCorrelatedTerminal(record, sendqueue.OperationIndeterminate, false, set)
		return
	}
	instance := instanceByID(instances, record.SessionID)
	if instance == nil || record.CodexSessionID == "" {
		setCorrelatedTerminal(record, sendqueue.OperationIndeterminate, false, set)
		return
	}
	bound := instance
	bound.CodexSessionID = record.CodexSessionID
	bound.Tool, bound.Account = record.Tool, record.HarnessAccount
	bound.Command, bound.Wrapper = record.HarnessCommand, record.HarnessWrapper
	generation, err := bound.LatestCodexTurnGeneration()
	if err != nil {
		setCorrelatedTerminal(record, sendqueue.OperationIndeterminate, false, set)
		return
	}
	generation, marker, err := session.RecoverCorrelatedCodexSubmissionMarker(record.SessionID, record.CodexSessionID, record.SendID, generation)
	if err != nil {
		setCorrelatedTerminal(record, sendqueue.OperationIndeterminate, false, set)
		return
	}
	accepted := &sendqueue.AcceptedTurn{
		ReceiptID: uuid.NewString(), InstanceID: record.SessionID, CodexSessionID: record.CodexSessionID,
		TurnGeneration: generation, AcceptedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := set(func(current *sendqueue.Record) {
		current.OperationState = sendqueue.OperationAccepted
		current.State = sendqueue.StateSubmitted
		current.Verdict = "delivered"
		current.ChildPID = 0
		current.AcceptedTurn = accepted
	}); err != nil {
		return
	}
	_ = session.ClearCodexSubmissionMarker(marker)
}

func completeCorrelatedOperation(profile string, record *sendqueue.Record, deadline time.Time, set func(func(*sendqueue.Record)) error) {
	if record.AcceptedTurn == nil {
		setCorrelatedTerminal(record, sendqueue.OperationResultUnavailable, false, set)
		return
	}
	_, instances, _, err := loadSessionData(profile)
	if err != nil {
		setCorrelatedTerminal(record, sendqueue.OperationResultUnavailable, false, set)
		return
	}
	instance := instanceByID(instances, record.SessionID)
	if instance == nil {
		setCorrelatedTerminal(record, sendqueue.OperationResultUnavailable, false, set)
		return
	}
	bound := instance
	bound.CodexSessionID = record.AcceptedTurn.CodexSessionID
	bound.Tool, bound.Account = record.Tool, record.HarnessAccount
	bound.Command, bound.Wrapper = record.HarnessCommand, record.HarnessWrapper
	if deadline.IsZero() {
		deadline = time.Now().Add(sendqueue.RetainUncertainBodies)
	}
	response, err := waitForCodexTurnOutput(bound, record.AcceptedTurn.TurnGeneration, deadline)
	if err != nil {
		setCorrelatedTerminal(record, sendqueue.OperationResultUnavailable, false, set)
		return
	}
	_ = set(func(current *sendqueue.Record) {
		current.OperationState = sendqueue.OperationCompleted
		current.State = sendqueue.StateLanded
		current.Verdict = "delivered"
		current.ChildPID = 0
		current.Completion = &sendqueue.Completion{
			TurnGeneration: current.AcceptedTurn.TurnGeneration,
			CompletedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		}
		current.Content = response.Content
	})
}

func shouldWaitForIdle(tool, status string) bool {
	return status == "" || status == "unknown" || status == "starting" || (status == "running" && !session.AcceptsInputWhileBusy(tool))
}

// typeQueued hands one queued record to a `session send` child. The record
// is written as typing (attempts, sent_at, transcript offset) BEFORE the
// child starts, so a worker that dies at any point after this leaves a
// record no later worker types again. It returns false only when that
// write failed, in which case nothing was typed.
func typeQueued(profile, dir string, rec *sendqueue.Record, status, path string, from int64, set func(func(*sendqueue.Record)) error) bool {
	result := sendqueue.ResultPath(dir, rec.SendID)
	_ = os.Remove(result)
	if err := set(func(r *sendqueue.Record) {
		r.State, r.Reason, r.ChildPID = sendqueue.StateTyping, "", 0
		r.Attempts++
		r.TargetStatus, r.TranscriptPath, r.TranscriptFrom = status, path, from
		r.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
	}); err != nil {
		return false
	}
	pid, wait, err := sendChild(profile, rec.SessionID, rec.Message, result)
	if err != nil {
		// The child never started, so nothing was typed.
		_ = set(func(r *sendqueue.Record) {
			r.State, r.Reason = sendqueue.StateQueued, "retrying: cannot start session send: "+err.Error()
		})
		return true
	}
	_ = set(func(r *sendqueue.Record) { r.ChildPID = pid })
	code := wait()
	applyChildResult(rec, readChildResult(result), code, true, set)
	return true
}

// applyChildResult moves a typing record on from its child's result.
func applyChildResult(rec *sendqueue.Record, result map[string]interface{}, code int, haveResult bool, set func(func(*sendqueue.Record)) error) {
	if rec.Correlated() {
		applyCorrelatedChildResult(rec, result, code, haveResult, set)
		return
	}
	outcome, reason := childUnknown, "worker restarted mid-send and the child left no result"
	if haveResult {
		outcome, reason = classifyChild(result, code)
	}
	_ = set(func(r *sendqueue.Record) {
		r.ChildPID = 0
		if id, _ := result["claude_session_id"].(string); id != "" {
			r.ClaudeSessionID = id
		}
		switch outcome {
		case childSubmitted:
			r.State, r.Reason, r.Verdict = sendqueue.StateSubmitted, "", "delivered"
		case childTyped:
			r.State, r.Reason = sendqueue.StateTyped, ""
			if result["delivery"] == deliveryQueued || result["delivery"] == deliveryDelivered {
				r.Verdict = "delivered"
			} else {
				r.Verdict = "unknown"
			}
		case childNotSent:
			r.State, r.Reason, r.Verdict = sendqueue.StateQueued, "retrying: "+reason, "queued"
		default:
			r.State, r.Reason, r.Verdict = sendqueue.StateTyped, "outcome unknown ("+reason+"); not retyped, watching the transcript", "unknown"
		}
	})
}

func applyCorrelatedChildResult(record *sendqueue.Record, result map[string]interface{}, code int, haveResult bool, set func(func(*sendqueue.Record)) error) {
	if haveResult {
		if accepted := acceptedTurnFromResult(result); accepted != nil {
			if accepted.InstanceID == record.SessionID && (record.CodexSessionID == "" || accepted.CodexSessionID == record.CodexSessionID) {
				if err := set(func(current *sendqueue.Record) {
					current.OperationState = sendqueue.OperationAccepted
					current.State = sendqueue.StateSubmitted
					current.Verdict = "delivered"
					current.ChildPID = 0
					current.CodexSessionID = accepted.CodexSessionID
					current.AcceptedTurn = accepted
				}); err == nil {
					return
				}
			}
		}
	}
	delivery, _ := result["delivery"].(string)
	if haveResult && (delivery == deliveryTargetBusy || delivery == deliveryComposerBlocked || delivery == deliveryLineTooLong || delivery == deliveryAcceptanceRefused) {
		setCorrelatedTerminal(record, sendqueue.OperationRefused, true, set)
		return
	}
	setCorrelatedTerminal(record, sendqueue.OperationIndeterminate, false, set)
}

func acceptedTurnFromResult(result map[string]interface{}) *sendqueue.AcceptedTurn {
	raw, ok := result["accepted_turn"]
	if !ok {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var accepted sendqueue.AcceptedTurn
	if err := json.Unmarshal(data, &accepted); err != nil || accepted.ReceiptID == "" || accepted.InstanceID == "" ||
		accepted.CodexSessionID == "" || accepted.TurnGeneration == "" ||
		!strings.HasPrefix(accepted.TurnGeneration, accepted.CodexSessionID+":") {
		return nil
	}
	return &accepted
}

// reconcileTyping settles a record a dead worker left in typing. The
// child may still be running (it outlives the worker): wait for it, then
// use its result file. Without a result the outcome is unknown and the
// record goes to the transcript watch; it is never typed again.
func reconcileTyping(dir string, rec *sendqueue.Record, set func(func(*sendqueue.Record)) error) {
	resultPath := sendqueue.ResultPath(dir, rec.SendID)
	end := time.Now().Add(sendChildWaitMax())
	for {
		if result, ok := parseChildResult(resultPath); ok {
			code := 1
			if success, _ := result["success"].(bool); success {
				code = 0
			}
			applyChildResult(rec, result, code, true, set)
			return
		}
		if !processAlive(rec.ChildPID) || time.Now().After(end) {
			applyChildResult(rec, nil, 0, false, set)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// watchLanded waits for a typed or submitted record's text to land in the
// transcript, then for the target to take the turn up, so the next queued
// message is not typed into this one's turn. Without evidence it settles
// typed/submitted with a reason: never landed, and never typed again.
func watchLanded(profile string, rec *sendqueue.Record, set func(func(*sendqueue.Record)) error) {
	harness := rowsHarness(rec.Tool)
	if !query.SupportsDirectRows(harness) {
		_ = set(func(r *sendqueue.Record) {
			r.Settled = true
			if r.Reason == "" {
				r.Reason = "no transcript to confirm landing for " + r.Tool
			}
		})
		return
	}
	sentAt, _ := time.Parse(time.RFC3339Nano, rec.SentAt)
	landBy := time.Now().Add(sendLandWindow())
	for time.Now().Before(landBy) {
		if rec.TranscriptPath == "" {
			if p := liveTranscriptForID(profile, rec.SessionID); p != "" {
				_ = set(func(r *sendqueue.Record) { r.TranscriptPath = p })
			}
		}
		if rec.TranscriptPath != "" {
			if id, ts, ok := query.FindLanded(context.Background(), harness, rec.TranscriptPath, rec.TranscriptFrom, rec.Message, sentAt); ok {
				_ = set(func(r *sendqueue.Record) {
					r.State, r.Reason, r.Verdict, r.LandedRowID, r.LandedAt = sendqueue.StateLanded, "", "delivered", id, ts
					if sid := claudeSessionIDFromTranscript(harness, r.TranscriptPath); sid != "" {
						r.ClaudeSessionID = sid
					}
				})
				waitTurnStarted(profile, rec.SessionID, 5*time.Second)
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	_ = set(func(r *sendqueue.Record) {
		if r.Reason == "" {
			r.Reason = "not seen in the transcript within " + sendLandWindow().String()
		}
		r.Settled = true
	})
}

// claudeSessionIDFromTranscript is the Claude conversation a transcript
// holds: Claude names each transcript <session id>.jsonl.
func claudeSessionIDFromTranscript(harness, path string) string {
	if harness != "claude" || filepath.Ext(path) != ".jsonl" {
		return ""
	}
	return strings.TrimSuffix(filepath.Base(path), ".jsonl")
}

// sendChildWaitMax bounds how long a restarted worker waits for the child
// a dead worker left running.
func sendChildWaitMax() time.Duration {
	return envDuration("AGENTDECK_SEND_CHILD_WAIT", 15*time.Minute)
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// parseChildResult reads a child's complete JSON result; a partial write
// does not parse.
func parseChildResult(path string) (map[string]interface{}, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	result := map[string]interface{}{}
	if json.Unmarshal(bytes.TrimSpace(b), &result) != nil {
		return nil, false
	}
	return result, true
}

func readChildResult(path string) map[string]interface{} {
	result, _ := parseChildResult(path)
	if result == nil {
		result = map[string]interface{}{}
	}
	return result
}

func instanceByID(instances []*session.Instance, id string) *session.Instance {
	for _, i := range instances {
		if i.ID == id {
			return i
		}
	}
	return nil
}

func liveTranscriptForID(profile, id string) string {
	_, instances, _, err := loadSessionData(profile)
	if err != nil {
		return ""
	}
	if inst := instanceByID(instances, id); inst != nil {
		return session.LiveTranscriptPath(inst, instances)
	}
	return ""
}

func waitTurnStarted(profile, id string, max time.Duration) {
	end := time.Now().Add(max)
	for time.Now().Before(end) {
		if s, _ := fetchHookDrivenStatus(profile, id); s == "running" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// startChildSend delivers through the normal `session send` path (readiness
// wait, composer guard, submit verification) in a child process. Its input
// and JSON result are files, not pipes: the child outlives a worker that
// dies, reads the whole message regardless, and the next worker reads the
// outcome from resultPath.
func startChildSend(profile, id, message, resultPath string) (int, func() int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, nil, err
	}
	msgPath := strings.TrimSuffix(resultPath, ".result") + ".message"
	if err := os.WriteFile(msgPath, []byte(message), 0o600); err != nil {
		return 0, nil, err
	}
	out, err := os.OpenFile(resultPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, nil, err
	}
	cmd := exec.Command(exe, profileArgs(profile, "session", "send", id, "--message-file", msgPath, "--json", "--queue-worker")...)
	cmd.Stdout = out
	if err := cmd.Start(); err != nil {
		out.Close()
		return 0, nil, err
	}
	wait := func() int {
		err := cmd.Wait()
		out.Close()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		if err != nil {
			return 1
		}
		return 0
	}
	return cmd.Process.Pid, wait, nil
}

func startCorrelatedChildSend(profile, id, operationID, message, resultPath string, deadline time.Time) (int, func() int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, nil, err
	}
	msgPath := strings.TrimSuffix(resultPath, ".result") + ".message"
	if err := os.WriteFile(msgPath, []byte(message), 0o600); err != nil {
		return 0, nil, err
	}
	out, err := os.OpenFile(resultPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, nil, err
	}
	timeout := time.Until(deadline)
	if deadline.IsZero() || timeout <= 0 {
		timeout = sendqueue.RetainUncertainBodies
	}
	args := profileArgs(profile, "session", "send", id, "--message-file", msgPath, "--json", "--wait", "--queue-worker", "--correlated-operation", operationID, "--timeout", timeout.String())
	cmd := exec.Command(exe, args...)
	cmd.Stdout = out
	if err := cmd.Start(); err != nil {
		_ = out.Close()
		return 0, nil, err
	}
	wait := func() int {
		err := cmd.Wait()
		_ = out.Close()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		if err != nil {
			return 1
		}
		return 0
	}
	return cmd.Process.Pid, wait, nil
}

// deliveryFrames turns queued sends of one session into follow delivery
// frames: every state change after the first scan, plus the current state
// of sends still in flight at start.
func deliveryFrames(profile string, storage *session.Storage, sessionID string) func() []query.RowFrame {
	if storage == nil || sessionID == "" {
		return nil
	}
	dir := sendQueueDir(storage)
	kickPendingSendWorkers(profile, dir, sessionID)
	seen := map[string]string{}
	first := true
	return func() []query.RowFrame {
		recs, _ := sendqueue.List(dir, sessionID)
		var out []query.RowFrame
		for _, r := range recs {
			prev, known := seen[r.SendID]
			seen[r.SendID] = r.State + ":" + r.Verdict
			if (first && !r.Final()) || (!first && (!known || prev != seen[r.SendID])) {
				out = append(out, query.RowFrame{Frame: "delivery", Reason: r.Reason, Delivery: &query.Delivery{SendID: r.SendID, State: r.State, Verdict: r.Verdict}})
			}
		}
		first = false
		return out
	}
}
