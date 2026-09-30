package slackgateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/stretchr/testify/require"
)

const (
	testConversation = "conversation"
	testChannel      = "C-bound"
	testTeam         = "T-bound"
	testUser         = "U-allowed"
	testBot          = "U-bot"
)

func slackStore(t *testing.T, mode channelgateway.Mode) (*channelgateway.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "slack-gateway.db")
	s, err := channelgateway.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.CreateConversation(context.Background(), channelgateway.Conversation{
		ID: testConversation, ChannelID: testChannel, ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding",
		Mode: mode, AllowedSenders: []string{testUser},
	}))
	return s, path
}

func slackHandler(s *channelgateway.Store) Handler {
	return Handler{Store: s, Config: Config{
		ConversationID: testConversation, TeamID: testTeam, ChannelID: testChannel,
		BotUserID: testBot, AllowedUserIDs: []string{testUser},
	}}
}

func eventEnvelope(envelopeID, eventID, body string) map[string]any {
	return map[string]any{
		"type": "events_api", "envelope_id": envelopeID, "accepts_response_payload": false,
		"payload": map[string]any{
			"type": "event_callback", "team_id": testTeam, "event_id": eventID,
			"event": map[string]any{
				"type": "message", "channel": testChannel, "user": testUser,
				"ts": "1700000000.000001", "text": body,
			},
		},
	}
}

func encodeEnvelope(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func envelopeFields(v map[string]any) (map[string]any, map[string]any) {
	p := v["payload"].(map[string]any)
	return p, p["event"].(map[string]any)
}

func ackEnvelopeID(t *testing.T, raw []byte) string {
	t.Helper()
	var a struct {
		EnvelopeID string `json:"envelope_id"`
	}
	require.NoError(t, json.Unmarshal(raw, &a))
	return a.EnvelopeID
}

func TestHandleCommitsBeforeAckAndAckFailureRedeliveryDeduplicates(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	h := slackHandler(s)
	ctx := context.Background()
	secret := "private-message-and-credential-marker-1"
	first := encodeEnvelope(t, eventEnvelope("envelope-1", "event-1", secret))
	ackCalls := 0
	_, err := h.Handle(ctx, first, func(_ context.Context, ack []byte) error {
		ackCalls++
		require.Equal(t, "envelope-1", ackEnvelopeID(t, ack))
		// The durable event is visible before the transport acknowledgment.
		turn, e := s.NextTurn(ctx, testConversation)
		require.NoError(t, e)
		require.NotNil(t, turn)
		require.Equal(t, secret, turn.Body)
		return errors.New("ack failed with private credential-marker-2")
	})
	require.Error(t, err)
	require.Equal(t, 1, ackCalls)
	require.NotContains(t, err.Error(), secret)
	require.NotContains(t, err.Error(), "credential-marker-2")

	retry := encodeEnvelope(t, eventEnvelope("envelope-2", "event-1", "modified-on-retry"))
	result, err := h.Handle(ctx, retry, func(_ context.Context, ack []byte) error {
		ackCalls++
		require.Equal(t, "envelope-2", ackEnvelopeID(t, ack))
		return nil
	})
	require.NoError(t, err)
	require.True(t, result.Intake.Duplicate)
	require.Equal(t, 2, ackCalls)
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.Equal(t, secret, turn.Body)
}

func TestHandleOnlyIngestsAndNeverReservesOrStartsAgentTurn(t *testing.T) {
	s, path := slackStore(t, channelgateway.ChannelStream)
	h := slackHandler(s)
	_, err := h.Handle(context.Background(), encodeEnvelope(t, eventEnvelope("envelope", "event", "private prompt")),
		func(context.Context, []byte) error { return nil })
	require.NoError(t, err)
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	var turnCount int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM turns`).Scan(&turnCount))
	require.Zero(t, turnCount)
	binding, err := s.AgentBinding(context.Background(), testConversation)
	require.NoError(t, err)
	require.Empty(t, binding.AgentThreadID)
	turn, err := s.NextTurn(context.Background(), testConversation)
	require.NoError(t, err)
	require.NotNil(t, turn)
	require.Equal(t, channelgateway.Unprepared, turn.AttemptState)
	require.Empty(t, turn.AcceptanceID)
}

func TestHandleValidIneligibleEventsAreAckedWithoutIntake(t *testing.T) {
	cases := map[string]func(map[string]any){
		"wrong team":    func(v map[string]any) { p, _ := envelopeFields(v); p["team_id"] = "T-other" },
		"wrong channel": func(v map[string]any) { _, e := envelopeFields(v); e["channel"] = "C-other" },
		"wrong user":    func(v map[string]any) { _, e := envelopeFields(v); e["user"] = "U-other" },
		"self":          func(v map[string]any) { _, e := envelopeFields(v); e["user"] = testBot },
		"bot":           func(v map[string]any) { _, e := envelopeFields(v); e["bot_id"] = "B-bot" },
		"bot profile":   func(v map[string]any) { _, e := envelopeFields(v); e["bot_profile"] = map[string]any{"id": "B-bot"} },
		"files with text": func(v map[string]any) {
			_, e := envelopeFields(v)
			e["files"] = []any{map[string]any{"id": "F-private"}}
		},
		"attachment text": func(v map[string]any) {
			_, e := envelopeFields(v)
			e["attachments"] = []any{map[string]any{"text": "private attachment"}}
		},
		"subtype":           func(v map[string]any) { _, e := envelopeFields(v); e["subtype"] = "message_changed" },
		"thread reply":      func(v map[string]any) { _, e := envelopeFields(v); e["thread_ts"] = "1699999999.000001" },
		"unsupported event": func(v map[string]any) { _, e := envelopeFields(v); e["type"] = "reaction_added" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s, _ := slackStore(t, channelgateway.ChannelStream)
			h := slackHandler(s)
			v := eventEnvelope("envelope-filtered", "event-filtered", "private body")
			mutate(v)
			acks := 0
			result, err := h.Handle(context.Background(), encodeEnvelope(t, v), func(_ context.Context, ack []byte) error {
				acks++
				require.Equal(t, "envelope-filtered", ackEnvelopeID(t, ack))
				return nil
			})
			require.NoError(t, err)
			require.True(t, result.Ignored)
			require.Equal(t, 1, acks)
			turn, err := s.NextTurn(context.Background(), testConversation)
			require.NoError(t, err)
			require.Nil(t, turn)
		})
	}
}

func TestHandleMalformedAndOversizeNeverAckOrIntake(t *testing.T) {
	base := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	missingAccepts := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	delete(missingAccepts, "accepts_response_payload")
	stringAccepts := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	stringAccepts["accepts_response_payload"] = "false"
	trueAccepts := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	trueAccepts["accepts_response_payload"] = true
	unsupportedMissingPayload := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	unsupportedMissingPayload["type"] = "slash_commands"
	delete(unsupportedMissingPayload, "payload")
	unsupportedInvalidPayload := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	unsupportedInvalidPayload["type"], unsupportedInvalidPayload["payload"] = "slash_commands", "invalid"
	missingTeam := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	p, _ := envelopeFields(missingTeam)
	delete(p, "team_id")
	contradictoryTeam := eventEnvelope("envelope-bad", "event-bad", "private credential-marker")
	_, event := envelopeFields(contradictoryTeam)
	event["team"] = "T-other"
	cases := map[string][]byte{
		"invalid JSON":                []byte(`{"type":`),
		"missing accepts flag":        encodeEnvelope(t, missingAccepts),
		"string accepts flag":         encodeEnvelope(t, stringAccepts),
		"true accepts flag":           encodeEnvelope(t, trueAccepts),
		"unsupported missing payload": encodeEnvelope(t, unsupportedMissingPayload),
		"unsupported invalid payload": encodeEnvelope(t, unsupportedInvalidPayload),
		"missing team":                encodeEnvelope(t, missingTeam),
		"contradictory team":          encodeEnvelope(t, contradictoryTeam),
		"duplicate envelope ID":       []byte(`{"type":"events_api","envelope_id":"envelope-bad","envelope_id":"other","payload":{}}`),
		"duplicate event ID":          []byte(`{"type":"events_api","envelope_id":"envelope-bad","payload":{"type":"event_callback","team_id":"T-bound","event_id":"event-bad","event_id":"other","event":{}}}`),
		"duplicate sender":            []byte(`{"type":"events_api","envelope_id":"envelope-bad","payload":{"type":"event_callback","team_id":"T-bound","event_id":"event-bad","event":{"type":"message","channel":"C-bound","user":"U-allowed","user":"U-other","ts":"1700000000.000001","text":"private credential-marker"}}}`),
		"envelope too large":          []byte(strings.Repeat("x", MaxEnvelopeBytes+1)),
	}
	largeBody := eventEnvelope("envelope-bad", "event-bad", strings.Repeat("x", MaxBodyBytes+1))
	cases["body too large"] = encodeEnvelope(t, largeBody)
	_, e := envelopeFields(base)
	delete(e, "ts")
	cases["missing timestamp"] = encodeEnvelope(t, base)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			s, _ := slackStore(t, channelgateway.ChannelStream)
			h := slackHandler(s)
			acks := 0
			_, err := h.Handle(context.Background(), raw, func(context.Context, []byte) error {
				acks++
				return nil
			})
			require.Error(t, err)
			require.Zero(t, acks)
			require.NotContains(t, err.Error(), "private credential-marker")
			turn, err := s.NextTurn(context.Background(), testConversation)
			require.NoError(t, err)
			require.Nil(t, turn)
		})
	}
}

func TestHandleUnsupportedOuterTypeWithValidPayloadIsAckedOnly(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	v := eventEnvelope("envelope-other", "event-other", "private credential-marker")
	v["type"] = "slash_commands"
	acks := 0
	h := slackHandler(s)
	r, err := h.Handle(context.Background(), encodeEnvelope(t, v), func(_ context.Context, ack []byte) error {
		acks++
		require.Equal(t, "envelope-other", ackEnvelopeID(t, ack))
		return nil
	})
	require.NoError(t, err)
	require.True(t, r.Ignored)
	require.Equal(t, 1, acks)
	turn, err := s.NextTurn(context.Background(), testConversation)
	require.NoError(t, err)
	require.Nil(t, turn)
}

func TestHandleStorageFailureAndIncompatibleModeNeverAck(t *testing.T) {
	ctx := context.Background()
	raw := encodeEnvelope(t, eventEnvelope("envelope", "event", "secret credential-marker"))
	for _, mode := range []channelgateway.Mode{channelgateway.ChannelStream, channelgateway.ThreadSegments} {
		t.Run(string(mode), func(t *testing.T) {
			s, _ := slackStore(t, mode)
			if mode == channelgateway.ChannelStream {
				require.NoError(t, s.Close())
			}
			h := slackHandler(s)
			acks := 0
			_, err := h.Handle(ctx, raw, func(context.Context, []byte) error {
				acks++
				return nil
			})
			require.Error(t, err)
			require.Zero(t, acks)
			require.NotContains(t, err.Error(), "secret credential-marker")
		})
	}
}

func TestHandleIngestAuthorizationFailureNeverAck(t *testing.T) {
	ctx := context.Background()
	s, err := channelgateway.Open(filepath.Join(t.TempDir(), "disallowed.db"))
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.CreateConversation(ctx, channelgateway.Conversation{
		ID: testConversation, ChannelID: testChannel, ConductorID: "conductor", RowInstanceID: "row", RowBinding: "binding",
		Mode: channelgateway.ChannelStream, AllowedSenders: []string{"U-other"},
	}))
	h := slackHandler(s) // The adapter allows U-allowed, but the ledger does not.
	acks := 0
	_, err = h.Handle(ctx, encodeEnvelope(t, eventEnvelope("envelope", "event", "private credential-marker")),
		func(context.Context, []byte) error { acks++; return nil })
	require.ErrorIs(t, err, channelgateway.ErrUnauthorized)
	require.Zero(t, acks)
	require.NotContains(t, err.Error(), "credential-marker")
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.Nil(t, turn)
}

type fakeSender struct {
	mu      sync.Mutex
	calls   int
	channel string
	text    string
	onPost  func()
	result  PostResult
	err     error
}

func (f *fakeSender) PostTopLevel(_ context.Context, channelID, body string) (PostResult, error) {
	f.mu.Lock()
	f.calls++
	f.channel, f.text = channelID, body
	onPost, result, err := f.onPost, f.result, f.err
	f.mu.Unlock()
	if onPost != nil {
		onPost()
	}
	return result, err
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func pendingSlackItem(t *testing.T) (*channelgateway.Store, string, *channelgateway.OutboxItem) {
	return pendingSlackItemWithBody(t, "private reply")
}

func pendingSlackItemWithBody(t *testing.T, body string) (*channelgateway.Store, string, *channelgateway.OutboxItem) {
	t.Helper()
	s, path := slackStore(t, channelgateway.ChannelStream)
	ctx := context.Background()
	_, err := s.Ingest(ctx, channelgateway.Inbound{
		ConversationID: testConversation, EventID: "event", MessageID: "message", ChannelID: testChannel,
		SenderID: testUser, Body: "private prompt",
	})
	require.NoError(t, err)
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NotNil(t, turn)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "legacy-accepted"))
	item, err := s.CompleteTurn(ctx, turn.ID, "legacy-accepted", body)
	require.NoError(t, err)
	require.NotNil(t, item)
	return s, path, item
}

func TestDeliverOnePreparesBeforeTopLevelPostAndConfirmsExactResponse(t *testing.T) {
	s, _, item := pendingSlackItem(t)
	ctx := context.Background()
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "1700000000.000002"}}
	sender.onPost = func() {
		a, created, err := s.PrepareDelivery(ctx, item.ID)
		require.NoError(t, err)
		require.False(t, created)
		require.NotEmpty(t, a.ID)
		require.Equal(t, channelgateway.SendingDelivery, a.State)
	}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	_, err := w.DeliverOne(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, 1, sender.count())
	require.Equal(t, testChannel, sender.channel)
	require.Equal(t, "private reply", sender.text)
	items, err := s.PendingOutbox(ctx, testConversation, 10)
	require.NoError(t, err)
	require.Empty(t, items)
	_, err = w.DeliverOne(ctx, item.ID)
	require.Equal(t, 1, sender.count())
}

func TestDeliverOneRejectsThreadSegmentWithoutClaimOrPost(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, channelgateway.Inbound{
		ConversationID: testConversation, EventID: "thread-event", MessageID: "root-ts",
		ChannelID: testChannel, SenderID: testUser, Mentioned: true, Body: "private prompt",
	})
	require.NoError(t, err)
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NotNil(t, turn)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "legacy-accepted"))
	item, err := s.CompleteTurn(ctx, turn.ID, "legacy-accepted", "private reply")
	require.NoError(t, err)
	require.Equal(t, "root-ts", item.ThreadID)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	_, err = w.DeliverOne(ctx, item.ID)
	require.Error(t, err)
	require.Zero(t, sender.count())
	items, err := s.PendingOutbox(ctx, testConversation, 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, channelgateway.PendingDelivery, items[0].State)
}

func TestDrainRecoveryNeverResendsPreparedOrAmbiguousSend(t *testing.T) {
	for _, sent := range []bool{false, true} {
		t.Run(map[bool]string{false: "crash before post", true: "crash after post"}[sent], func(t *testing.T) {
			s, path, item := pendingSlackItem(t)
			ctx := context.Background()
			a, created, err := s.PrepareDelivery(ctx, item.ID)
			require.NoError(t, err)
			require.True(t, created)
			if sent {
				// The provider may have accepted this request; the response was lost.
				fake := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "provider-ts"}}
				_, err := fake.PostTopLevel(ctx, testChannel, item.Body)
				require.NoError(t, err)
				require.Equal(t, 1, fake.count())
			}
			require.NoError(t, s.Close())
			reopened, err := channelgateway.Open(path)
			require.NoError(t, err)
			defer reopened.Close()
			sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "new-ts"}}
			w := DeliveryWorker{Store: reopened, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
			_, err = w.DrainPending(ctx, 10)
			require.NoError(t, err)
			require.Zero(t, sender.count())
			recovered, created, err := reopened.PrepareDelivery(ctx, item.ID)
			require.NoError(t, err)
			require.False(t, created)
			require.Equal(t, a.ID, recovered.ID)
			require.Equal(t, channelgateway.UncertainDelivery, recovered.State)
		})
	}
}

func TestDeliverOneAmbiguousProviderOutcomesBecomeUncertain(t *testing.T) {
	cases := map[string]struct {
		result PostResult
		err    error
	}{
		"sender error":  {err: errors.New("provider leaked private reply credential-marker")},
		"not ok":        {result: PostResult{OK: false, ChannelID: testChannel, TS: "1700000000.000002"}},
		"wrong channel": {result: PostResult{OK: true, ChannelID: "C-other", TS: "1700000000.000002"}},
		"missing ts":    {result: PostResult{OK: true, ChannelID: testChannel}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, item := pendingSlackItem(t)
			sender := &fakeSender{result: tc.result, err: tc.err}
			w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
			_, err := w.DeliverOne(context.Background(), item.ID)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private reply")
			require.NotContains(t, err.Error(), "credential-marker")
			a, created, err := s.PrepareDelivery(context.Background(), item.ID)
			require.NoError(t, err)
			require.False(t, created)
			require.Equal(t, channelgateway.UncertainDelivery, a.State)
			_, _ = w.DeliverOne(context.Background(), item.ID)
			require.Equal(t, 1, sender.count())
		})
	}
}

func TestDeliverOneOversizeUnicodeReplyNeverPostsOrRetries(t *testing.T) {
	ctx := context.Background()
	s, _, item := pendingSlackItemWithBody(t, strings.Repeat("界", 40001))
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "provider-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	_, err := w.DeliverOne(ctx, item.ID)
	require.ErrorIs(t, err, ErrPostResult)
	require.Zero(t, sender.count())
	require.NotContains(t, err.Error(), "界")
	a, created, err := s.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, channelgateway.UncertainDelivery, a.State)
	_, err = w.DrainPending(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, sender.count())

	// The limit is characters, not UTF-8 bytes: 40,000 multibyte runes fit.
	s2, _, item2 := pendingSlackItemWithBody(t, strings.Repeat("界", 40000))
	sender2 := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "provider-ts"}}
	w2 := DeliveryWorker{Store: s2, Sender: sender2, ConversationID: testConversation, ChannelID: testChannel}
	_, err = w2.DeliverOne(ctx, item2.ID)
	require.NoError(t, err)
	require.Equal(t, 1, sender2.count())
}

func TestConcurrentDrainMarksInFlightUncertainButExactLateConfirmWins(t *testing.T) {
	s, path, item := pendingSlackItem(t)
	other, err := channelgateway.Open(path)
	require.NoError(t, err)
	defer other.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "1700000000.000002"}}
	sender.onPost = func() {
		close(started)
		<-release
	}
	ctx := context.Background()
	w1 := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	w2 := DeliveryWorker{Store: other, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	finished := make(chan error, 1)
	go func() { _, e := w1.DeliverOne(ctx, item.ID); finished <- e }()
	<-started
	_, err = w2.DrainPending(ctx, 10)
	require.NoError(t, err)
	close(release)
	require.NoError(t, <-finished)
	require.Equal(t, 1, sender.count())
	a, created, err := other.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, channelgateway.DeliveredDelivery, a.State)
	require.NoError(t, other.MarkDeliveryUncertain(ctx, item.ID, a.ID))
	a, _, err = other.PrepareDelivery(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.DeliveredDelivery, a.State)
}
