package slackgateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/stretchr/testify/require"
)

// deliver runs one envelope through the handler and returns the intake.
func deliver(t *testing.T, h Handler, v map[string]any) HandleResult {
	t.Helper()
	acks := 0
	result, err := h.Handle(context.Background(), encodeEnvelope(t, v), func(context.Context, []byte) error {
		acks++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, acks, "every deterministic outcome is acked exactly once")
	return result
}

func threadEnvelope(envelopeID, eventID, body, ts, threadTS string) map[string]any {
	v := eventEnvelope(envelopeID, eventID, body)
	_, e := envelopeFields(v)
	e["ts"] = ts
	if threadTS != "" {
		e["thread_ts"] = threadTS
	}
	return v
}

func mention(body string) string { return "<@" + testBot + "> " + body }

// A shared channel gives every app in it a copy of every message, so an
// unaddressed top-level message must not start anything.
func TestThreadSegmentsIgnoresUnaddressedTopLevel(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	result := deliver(t, h, threadEnvelope("e1", "ev1", "status update for someone else", "1700000000.000001", ""))
	require.True(t, result.Ignored)
	require.Equal(t, channelgateway.Ignored, result.Intake.Disposition)
	turn, err := s.NextTurn(context.Background(), testConversation)
	require.NoError(t, err)
	require.Nil(t, turn)
}

// The mention is an address, paid once. Everything after it is ordinary text
// in a thread this conversation now owns.
func TestThreadSegmentsMentionOpensAndThreadContinuesWithout(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)

	opened := deliver(t, h, threadEnvelope("e1", "ev1", mention("start the sweep"), "1700000000.000001", ""))
	require.False(t, opened.Ignored)
	require.Equal(t, channelgateway.Accepted, opened.Intake.Disposition)
	require.NotEmpty(t, opened.Intake.SegmentID)

	followUp := deliver(t, h, threadEnvelope("e2", "ev2", "and skip the packs repo", "1700000000.000002", "1700000000.000001"))
	require.False(t, followUp.Ignored, "a reply in a bound thread needs no mention")
	require.Equal(t, channelgateway.Accepted, followUp.Intake.Disposition)
	require.Equal(t, opened.Intake.SegmentID, followUp.Intake.SegmentID)

	first, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Contains(t, first.Body, "start the sweep")
}

// Another conductor's thread is silent here until it is addressed, which is
// what lets four conductors share one channel.
func TestThreadSegmentsIgnoresForeignThreadUntilMentioned(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)

	foreign := deliver(t, h, threadEnvelope("e1", "ev1", "box 1 please continue", "1700000000.000002", "1700000000.000001"))
	require.True(t, foreign.Ignored, "an unknown thread is another conductor's until addressed")

	pulled := deliver(t, h, threadEnvelope("e2", "ev2", mention("what does MANDO say about this?"), "1700000000.000003", "1700000000.000001"))
	require.False(t, pulled.Ignored, "an explicit mention reaches into any thread")
	require.Equal(t, channelgateway.Accepted, pulled.Intake.Disposition)

	after := deliver(t, h, threadEnvelope("e3", "ev3", "and the packs repo too", "1700000000.000004", "1700000000.000001"))
	require.False(t, after.Ignored, "the thread is bound here once it has been addressed")
	require.Equal(t, pulled.Intake.SegmentID, after.Intake.SegmentID)
}

// Slack stamps thread_ts on a parent once it has replies. That parent is still
// a top-level message and must not look like a reply to itself.
func TestThreadSegmentsNormalizesSelfReferentialThreadTS(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	result := deliver(t, h, threadEnvelope("e1", "ev1", "no address here", "1700000000.000001", "1700000000.000001"))
	require.True(t, result.Ignored, "a self-referential thread_ts is still a top-level message")
}

// The mention is matched on the bot's own ID, never on a display name a human
// typed, so a lookalike is not an address.
func TestMentionsMatchesOnlyTheBotID(t *testing.T) {
	require.True(t, mentions("<@U-bot> go", "U-bot"))
	require.True(t, mentions("hello <@U-bot>", "U-bot"))
	require.False(t, mentions("hello @U-bot", "U-bot"))
	require.False(t, mentions("<@U-bottom> go", "U-bot"))
	require.False(t, mentions("<@U-other> go", "U-bot"))
	require.False(t, mentions("<@U-bot> go", ""))
}

// A third thread arrives while one segment is active and one is pending. The
// gateway parks it, and silence would be indistinguishable from a dead
// channel, so the operator is told where the work actually is.
func TestParkedMessageYieldsAPointerIntoItsOwnThread(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)

	first := deliver(t, h, threadEnvelope("e1", "ev1", mention("first"), "1700000000.000001", ""))
	require.Equal(t, channelgateway.Accepted, first.Intake.Disposition)
	second := deliver(t, h, threadEnvelope("e2", "ev2", mention("second"), "1700000000.000002", ""))
	require.Equal(t, channelgateway.Accepted, second.Intake.Disposition)

	third := deliver(t, h, threadEnvelope("e3", "ev3", mention("third"), "1700000000.000003", ""))
	require.Equal(t, channelgateway.RejectedPending, third.Intake.Disposition)
	require.True(t, third.Pointer.Wanted())
	require.Equal(t, "1700000000.000003", third.Pointer.ThreadTS, "the answer belongs in the thread that was parked")
	require.Equal(t, "1700000000.000002", third.Pointer.PointerThreadTS, "it points at the thread that is waiting")
	require.False(t, third.Pointer.Superseded)
}

// Slack retries an event whose ack was lost. The gateway recognises the
// duplicate, and a second pointer must not appear in the channel.
func TestDuplicateParkedEventPointsOnlyOnce(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("first"), "1700000000.000001", ""))
	deliver(t, h, threadEnvelope("e2", "ev2", mention("second"), "1700000000.000002", ""))

	parked := threadEnvelope("e3", "ev3", mention("third"), "1700000000.000003", "")
	first := deliver(t, h, parked)
	require.True(t, first.Pointer.Wanted())
	retry := deliver(t, h, parked)
	require.True(t, retry.Intake.Duplicate)
	require.False(t, retry.Pointer.Wanted(), "a retried event must not post a second pointer")
}

// An accepted message has nothing to point at.
func TestAcceptedMessageHasNoPointer(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	accepted := deliver(t, h, threadEnvelope("e1", "ev1", mention("go"), "1700000000.000001", ""))
	require.Equal(t, channelgateway.Accepted, accepted.Intake.Disposition)
	require.False(t, accepted.Pointer.Wanted())
}

// The pointer is fixed text plus a link. It never carries operator content or
// conductor output, and a missing permalink costs the link, not the pointer.
func TestPostPointerBodyIsFixedTextAndOptionalLink(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		pointer  Pointer
		linkErr  error
		contains string
		link     bool
	}{
		{name: "busy with link", pointer: Pointer{ThreadTS: "ts-parked", PointerThreadTS: "ts-active"}, contains: pointerBusy, link: true},
		{name: "superseded with link", pointer: Pointer{ThreadTS: "ts-parked", PointerThreadTS: "ts-active", Superseded: true}, contains: pointerSuperseded, link: true},
		{name: "link unavailable", pointer: Pointer{ThreadTS: "ts-parked", PointerThreadTS: "ts-active"}, linkErr: ErrPost, contains: pointerBusy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := slackStore(t, channelgateway.ThreadSegments)
			sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "posted"}, permalinkErr: tc.linkErr}
			w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
			require.NoError(t, w.PostPointer(ctx, tc.pointer))
			require.Equal(t, "ts-parked", sender.threadTS)
			require.Contains(t, sender.text, tc.contains)
			require.NotContains(t, sender.text, "private")
			if tc.link {
				require.Contains(t, sender.text, "https://example.slack.com/archives/"+testChannel+"/pts-active")
			} else {
				require.NotContains(t, sender.text, "https://")
			}
		})
	}
}

// A flat channel has no segments, so it can never park anything, and a pointer
// there would be posted in a thread the conversation does not use.
func TestPostPointerRefusedOnChannelStream(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "posted"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	require.Error(t, w.PostPointer(context.Background(), Pointer{ThreadTS: "ts-parked", PointerThreadTS: "ts-active"}))
	require.Zero(t, sender.count())
}

// The operator's own message is marked as heard. Without it, a message sent
// into a busy or broken conductor looks identical.
func TestAcceptedEventReportsTheMessageToMark(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	accepted := deliver(t, h, threadEnvelope("e1", "ev1", mention("go"), "1700000000.000001", ""))
	require.Equal(t, "1700000000.000001", accepted.AcceptedMessageTS)

	retry := deliver(t, h, threadEnvelope("e1", "ev1", mention("go"), "1700000000.000001", ""))
	require.True(t, retry.Intake.Duplicate)
	require.Empty(t, retry.AcceptedMessageTS, "a retried event was already marked")

	ignored := deliver(t, h, threadEnvelope("e2", "ev2", "no address", "1700000000.000002", ""))
	require.Empty(t, ignored.AcceptedMessageTS)
}

// A delivered reply marks the message that asked for it; the fixed-failure
// status item marks failure instead.
func TestDeliveryMarksTheTurnThatAsked(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("do the thing"), "1700000000.000001", ""))

	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NotNil(t, turn)
	require.Equal(t, "1700000000.000001", turn.MessageID, "the exact message is recoverable after intake")
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "accepted-1"))
	item, err := s.CompleteTurn(ctx, turn.ID, "accepted-1", "private reply")
	require.NoError(t, err)

	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	_, err = w.DeliverOne(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, []string{testChannel + "/1700000000.000001/" + ReactionDone}, sender.reactions)
	require.Equal(t, []string{testChannel + "/1700000000.000001/" + ReactionAccepted}, sender.removed,
		"a message shows the state it is in, not every state it has been in")
}

// A reaction is advisory everywhere: an app without the scope yet, or a
// message that cannot be marked, must not fail the delivery that succeeded.
func TestMarkFailureNeverFailsDelivery(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("do the thing"), "1700000000.000001", ""))
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "accepted-1"))
	item, err := s.CompleteTurn(ctx, turn.ID, "accepted-1", "private reply")
	require.NoError(t, err)

	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}, reactionErr: ErrPost}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	r, err := w.DeliverOne(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.DeliveredDelivery, r.State)
}

// A turn migrated from a ledger written before message sealing has no exact
// timestamp. That costs the mark, never the turn.
func TestUnsealedMessageIsNotMarkedAndIsNotAnError(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	w.markTurn(ctx, "turn-that-does-not-exist", ReactionDone)
	require.Empty(t, sender.reactions)
}

// The thread shows it is working from acceptance until its answer is out. The
// closing "active" is mandatory: the indicator does not clear when the app
// posts, so a missing one leaves the thread spinning forever.
func TestSessionIndicatorOpensOnAcceptAndClosesOnDelivery(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	accepted := deliver(t, h, threadEnvelope("e1", "ev1", mention("sweep the packs repo"), "1700000000.000001", ""))
	require.Equal(t, "1700000000.000001", accepted.AcceptedThreadTS)
	require.Equal(t, "sweep the packs repo", accepted.AcceptedTitle)

	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	w.Session(ctx, accepted.AcceptedThreadTS, SessionWorking, accepted.AcceptedTitle, accepted.AcceptedUserID)

	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "accepted-1"))
	item, err := s.CompleteTurn(ctx, turn.ID, "accepted-1", "private reply")
	require.NoError(t, err)
	_, err = w.DeliverOne(ctx, item.ID)
	require.NoError(t, err)

	require.Equal(t, []string{
		testChannel + "/1700000000.000001/" + SessionWorking + "/sweep the packs repo/" + testUser,
		testChannel + "/1700000000.000001/" + SessionIdle + "//",
	}, sender.sessions)
}

// A flat channel has no thread to drive, so the indicator is never attempted.
func TestSessionIndicatorSkippedOnChannelStream(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	w.Session(context.Background(), "1700000000.000001", SessionWorking, "title", testUser)
	require.Empty(t, sender.sessions)
}

// A title names the thread after the message that opened it, without the
// mention markup and without a second line.
func TestSessionTitleIsTheFirstLineWithoutMentions(t *testing.T) {
	require.Equal(t, "sweep the packs repo", sessionTitle("<@U-bot> sweep the packs repo"))
	require.Equal(t, "first line", sessionTitle("<@U-bot> first line\nsecond line"))
	require.Equal(t, "ask box 4 and box 2", sessionTitle("ask <@U-a> box 4 and <@U-b> box 2"))
	require.Equal(t, "", sessionTitle("<@U-bot>"))
	require.LessOrEqual(t, len([]rune(sessionTitle(strings.Repeat("x", 400)))), 120)
}

// An app that is not declared as an agent, or a workspace without the feature,
// has no indicator and must not have a failed turn either.
func TestSessionFailureNeverFailsDelivery(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("go"), "1700000000.000001", ""))
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "accepted-1"))
	item, err := s.CompleteTurn(ctx, turn.ID, "accepted-1", "private reply")
	require.NoError(t, err)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}, sessionErr: ErrPost}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	r, err := w.DeliverOne(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, channelgateway.DeliveredDelivery, r.State)
}

// Intake and delivery race by construction. A turn finishing must not clear an
// indicator that a newer turn in the same thread still owns.
func TestIndicatorSurvivesAnOlderTurnFinishing(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("first"), "1700000000.000001", ""))
	deliver(t, h, threadEnvelope("e2", "ev2", "second", "1700000000.000002", "1700000000.000001"))

	first, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NoError(t, s.AcceptTurn(ctx, first.ID, "accepted-1"))
	item, err := s.CompleteTurn(ctx, first.ID, "accepted-1", "first reply")
	require.NoError(t, err)

	// The second message is now the active turn in the same segment.
	second, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)

	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	_, err = w.DeliverOne(ctx, item.ID)
	require.NoError(t, err)
	require.Empty(t, sender.sessions, "the thread is still working; its indicator must stay")
}

// One advisory failure suppresses advisory calls briefly, so an app without
// the scope costs a call a minute rather than one per turn. Replies are never
// suppressed.
func TestAdvisoryFailureIsSuppressedButRepliesAreNot(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("go"), "1700000000.000001", ""))
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "accepted-1"))
	item, err := s.CompleteTurn(ctx, turn.ID, "accepted-1", "private reply")
	require.NoError(t, err)

	now := time.Unix(1700000000, 0)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "post-ts"}, reactionErr: ErrPost}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel,
		Clock: func() time.Time { return now }}

	w.Mark(ctx, "1700000000.000001", ReactionAccepted)
	require.Len(t, sender.reactions, 1)
	w.Mark(ctx, "1700000000.000001", ReactionAccepted)
	require.Len(t, sender.reactions, 1, "suppressed inside the window")

	r, err := w.DeliverOne(ctx, item.ID)
	require.NoError(t, err, "a reply is never suppressed by an advisory failure")
	require.Equal(t, channelgateway.DeliveredDelivery, r.State)

	now = now.Add(2 * time.Minute)
	w.Mark(ctx, "1700000000.000001", ReactionAccepted)
	require.Len(t, sender.reactions, 2, "allowed again after the window")
}
