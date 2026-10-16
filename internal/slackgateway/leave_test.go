package slackgateway

import (
	"context"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
	"github.com/stretchr/testify/require"
)

// The incident this exists for: deck2 and deck4 were cross-mentioned inside
// box 1's thread on 2026-10-14, took ownership of it, and then answered every
// later reply -- including the ones telling them to stop.
func TestACrossMentionDoesNotSubscribeToSomeoneElsesThread(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)

	addressed := deliver(t, h, threadEnvelope("e1", "ev1", mention("what do you think?"),
		"1700000000.000002", "1700000000.000001"))
	require.Equal(t, channelgateway.Accepted, addressed.Intake.Disposition)
	require.Equal(t, "guest", addressed.Intake.SegmentOwner)

	quiet := deliver(t, h, threadEnvelope("e2", "ev2", "no, ignore that, I meant box 1",
		"1700000000.000003", "1700000000.000001"))
	require.True(t, quiet.Ignored, "an unmentioned reply in someone else's thread is not ours")

	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.NotNil(t, turn, "the addressed message is answered")
	require.NoError(t, s.AcceptTurn(ctx, turn.ID, "a1"))
	_, err = s.CompleteTurn(ctx, turn.ID, "a1", "here is what I think")
	require.NoError(t, err)
	next, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.Nil(t, next, "and nothing else in that thread is")
}

func TestLeaveIsObeyedByTheGatewayAndNeverReachesTheAgent(t *testing.T) {
	ctx := context.Background()
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)

	opened := deliver(t, h, threadEnvelope("e1", "ev1", mention("start the sweep"), "1700000000.000001", ""))
	require.Equal(t, channelgateway.Accepted, opened.Intake.Disposition)

	left := deliver(t, h, threadEnvelope("e2", "ev2", mention("leave"),
		"1700000000.000002", "1700000000.000001"))
	require.True(t, left.Ignored, "a control is never a turn")
	require.True(t, left.Control.Wanted())
	require.Equal(t, channelgateway.ControlLeave, left.Control.Kind)
	require.Equal(t, 1, left.Control.Closed)
	require.Equal(t, "1700000000.000001", left.Control.ThreadTS)

	after := deliver(t, h, threadEnvelope("e3", "ev3", "are you still there?",
		"1700000000.000003", "1700000000.000001"))
	require.True(t, after.Ignored)

	// Leaving stops the thread, including a message that was queued in it and
	// not yet answered. A turn already in flight finishes -- there is no
	// honest way to unsay something the conductor has already been given --
	// but nothing new is taken from a thread the operator has left.
	turn, err := s.NextTurn(ctx, testConversation)
	require.NoError(t, err)
	require.Nil(t, turn, "the queued message in the left thread is dropped")
}

func TestLeaveAllClosesEveryThreadAndIsAnswered(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("one"), "1700000000.000001", ""))
	deliver(t, h, threadEnvelope("e2", "ev2", mention("two"), "1700000000.000010", "1700000000.000009"))

	all := deliver(t, h, threadEnvelope("e3", "ev3", mention("leave all"), "1700000000.000020", ""))
	require.Equal(t, channelgateway.ControlLeaveAll, all.Control.Kind)
	require.Equal(t, 2, all.Control.Closed)

	after := deliver(t, h, threadEnvelope("e4", "ev4", "still following?",
		"1700000000.000021", "1700000000.000001"))
	require.True(t, after.Ignored)
}

func TestLeaveWithNothingFollowedIsStillAnswered(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	result := deliver(t, h, threadEnvelope("e1", "ev1", mention("leave"),
		"1700000000.000002", "1700000000.000001"))
	require.True(t, result.Control.Wanted(), "silence is what the operator is trying to escape")
	require.Equal(t, 0, result.Control.Closed)
}

// Only a message that is exactly the address plus the word is a control.
// Anything else is ordinary text for the agent, so an instruction written in
// a sentence keeps its normal meaning.
func TestOnlyTheBareWordIsAControl(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ThreadSegments)
	h := slackHandler(s)
	deliver(t, h, threadEnvelope("e1", "ev1", mention("start"), "1700000000.000001", ""))

	for i, body := range []string{
		mention("leave the packs repo alone"),
		mention("please leave"),
		mention("leaving"),
		mention("leave all the tests running"),
	} {
		result := deliver(t, h, threadEnvelope(
			"env"+string(rune('a'+i)), "evt"+string(rune('a'+i)), body,
			"170000000"+string(rune('1'+i))+".000004", "1700000000.000001"))
		require.False(t, result.Control.Wanted(), body+" is text, not a control")
		require.Equal(t, channelgateway.Accepted, result.Intake.Disposition)
	}
}

// Two bots addressed in one message must each read it as their own control.
func TestAControlAddressedToSeveralBotsIsOneForEach(t *testing.T) {
	require.Equal(t, channelgateway.ControlLeave,
		control("<@"+testBot+"> <@U0OTHERBOT> leave", testBot))
	require.Equal(t, channelgateway.ControlLeave,
		control("<@U0OTHERBOT> <@"+testBot+"> leave", testBot))
	require.Equal(t, channelgateway.ControlNone,
		control("<@U0OTHERBOT> leave", testBot), "a control needs this bot's own address")
	require.Equal(t, channelgateway.ControlLeaveAll,
		control("<@"+testBot+">   LEAVE   ALL  ", testBot))
}

// The answer is fixed text and says which of the four outcomes happened. An
// operator who types "leave" and hears nothing cannot tell a bot that obeyed
// from one that is about to answer again, which is the whole complaint.
func TestPostControlBodyNamesTheOutcome(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		control  Control
		contains string
	}{
		{name: "left one", control: Control{ThreadTS: "ts", Kind: channelgateway.ControlLeave, Closed: 1}, contains: controlLeft},
		{name: "nothing to leave", control: Control{ThreadTS: "ts", Kind: channelgateway.ControlLeave}, contains: controlNotHere},
		{name: "left all", control: Control{ThreadTS: "ts", Kind: channelgateway.ControlLeaveAll, Closed: 3}, contains: controlLeftAll},
		{name: "none open", control: Control{ThreadTS: "ts", Kind: channelgateway.ControlLeaveAll}, contains: controlNoneOpen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := slackStore(t, channelgateway.ThreadSegments)
			sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "posted"}}
			w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
			require.NoError(t, w.PostControl(ctx, tc.control))
			require.Equal(t, "ts", sender.threadTS)
			require.Equal(t, tc.contains, sender.text)
		})
	}
}

// A flat channel follows no threads, so it can never leave one.
func TestPostControlRefusedOnChannelStream(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "posted"}}
	w := DeliveryWorker{Store: s, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	require.Error(t, w.PostControl(context.Background(),
		Control{ThreadTS: "ts", Kind: channelgateway.ControlLeave, Closed: 1}))
	require.Zero(t, sender.count())
}
