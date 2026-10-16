package channelgateway

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A mention inside a thread this conversation did not open answers that
// message and nothing else. On 2026-10-14 two boxes were cross-mentioned in a
// third box's thread, each took ownership of it, and then answered every later
// reply -- including the two telling them to stop.
func TestACrossMentionAnswersOnceAndOwnsNothing(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()

	guest, err := s.Ingest(ctx, inbound("cross-1", "m1", "someone-elses-thread", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, guest.Disposition)
	require.Equal(t, "guest", guest.SegmentOwner)
	require.Equal(t, "open", guest.SegmentState)

	// The conversation still owns nothing: no active segment was taken.
	quiet, err := s.Ingest(ctx, inbound("cross-2", "m2", "someone-elses-thread", false))
	require.NoError(t, err)
	require.Equal(t, Ignored, quiet.Disposition, "an unmentioned reply in a guest thread is not ours")

	// Addressed again, it answers again.
	again, err := s.Ingest(ctx, inbound("cross-3", "m3", "someone-elses-thread", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, again.Disposition)
	require.Equal(t, guest.SegmentID, again.SegmentID, "the same guest segment is reused")
}

// The owner's own thread is unchanged: it follows unmentioned replies.
func TestAnOpenedThreadStillFollowsUnmentionedReplies(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	opened, err := s.Ingest(ctx, inbound("root", "root-ts", "", true))
	require.NoError(t, err)
	require.Equal(t, "open", opened.SegmentState)
	reply, err := s.Ingest(ctx, inbound("reply", "m2", "root-ts", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, reply.Disposition)
	require.Equal(t, opened.SegmentID, reply.SegmentID)
}

// A guest segment must not displace the thread the conversation is working in.
func TestAGuestThreadDoesNotTakeOverTheActiveOne(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	owned, err := s.Ingest(ctx, inbound("root", "root-ts", "", true))
	require.NoError(t, err)
	_, err = s.Ingest(ctx, inbound("guest", "m2", "other-thread", true))
	require.NoError(t, err)
	reply, err := s.Ingest(ctx, inbound("reply", "m3", "root-ts", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, reply.Disposition, "the owned thread keeps working")
	require.Equal(t, owned.SegmentID, reply.SegmentID)
}

func TestLeaveStopsAnOwnedThreadAndIsReported(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	opened, err := s.Ingest(ctx, inbound("root", "root-ts", "", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, opened.Disposition)

	leave := inbound("leave-1", "m2", "root-ts", true)
	leave.Control = ControlLeave
	left, err := s.Ingest(ctx, leave)
	require.NoError(t, err)
	require.Equal(t, Ignored, left.Disposition, "a control is never a turn")
	require.Equal(t, ControlResult{Control: ControlLeave, Closed: 1}, left.ControlResult)

	after, err := s.Ingest(ctx, inbound("after", "m3", "root-ts", false))
	require.NoError(t, err)
	require.Equal(t, Ignored, after.Disposition, "the thread is no longer followed")

	// Addressed directly in the same thread it is answered once, as a guest.
	addressed, err := s.Ingest(ctx, inbound("addressed", "m4", "root-ts", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, addressed.Disposition)
	require.Equal(t, "guest", addressed.SegmentOwner, "picked up again as a guest, not as the owner")
}

// Closing the active segment must promote the pending one, or the bot answers
// nothing anywhere afterwards.
func TestLeavingTheActiveThreadPromotesThePendingOne(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("first", "first-ts", "", true))
	require.NoError(t, err)
	second, err := s.Ingest(ctx, inbound("second", "second-ts", "", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, second.Disposition)
	require.Equal(t, "pending", second.SegmentState)

	leave := inbound("leave-1", "m3", "first-ts", true)
	leave.Control = ControlLeave
	left, err := s.Ingest(ctx, leave)
	require.NoError(t, err)
	require.Equal(t, 1, left.ControlResult.Closed)

	// The promoted thread now answers an unmentioned reply, which only the
	// active segment does.
	reply, err := s.Ingest(ctx, inbound("reply", "m4", "second-ts", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, reply.Disposition)
	require.Equal(t, second.SegmentID, reply.SegmentID)
}

func TestLeaveAllClosesEveryFollowedThread(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("first", "first-ts", "", true))
	require.NoError(t, err)
	// The guest is entered while the conversation is free; a mention arriving
	// while a pending thread is already waiting is parked instead, which the
	// next test covers.
	_, err = s.Ingest(ctx, inbound("guest", "m3", "third-thread", true))
	require.NoError(t, err)
	_, err = s.Ingest(ctx, inbound("second", "second-ts", "", true))
	require.NoError(t, err)

	leave := inbound("leave-all", "m4", "", true)
	leave.Control = ControlLeaveAll
	left, err := s.Ingest(ctx, leave)
	require.NoError(t, err)
	require.Equal(t, 3, left.ControlResult.Closed, "two owned and one guest")

	for _, thread := range []string{"first-ts", "second-ts", "third-thread"} {
		r, err := s.Ingest(ctx, inbound("after-"+thread, "m-"+thread, thread, false))
		require.NoError(t, err)
		require.Equal(t, Ignored, r.Disposition, thread+" is still being followed")
	}
}

func TestLeaveWithNothingToLeaveIsReportedNotAnError(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	leave := inbound("leave-1", "m1", "a-thread-nobody-owns", true)
	leave.Control = ControlLeave
	left, err := s.Ingest(ctx, leave)
	require.NoError(t, err)
	require.Equal(t, ControlResult{Control: ControlLeave, Closed: 0}, left.ControlResult)

	top := inbound("leave-2", "m2", "", true)
	top.Control = ControlLeave
	left, err = s.Ingest(ctx, top)
	require.NoError(t, err)
	require.Equal(t, 0, left.ControlResult.Closed, "a top-level leave names no thread")
}

// A Slack retry of the control must not run it twice, and must not be
// answered twice either.
func TestAReplayedLeaveIsADuplicate(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("root", "root-ts", "", true))
	require.NoError(t, err)
	leave := inbound("leave-1", "m2", "root-ts", true)
	leave.Control = ControlLeave
	first, err := s.Ingest(ctx, leave)
	require.NoError(t, err)
	require.Equal(t, 1, first.ControlResult.Closed)
	replay, err := s.Ingest(ctx, leave)
	require.NoError(t, err)
	require.True(t, replay.Duplicate)
	require.Equal(t, ControlNone, replay.ControlResult.Control)
}

// A reopened thread is a fresh top-level mention, not a reply in the closed one.
func TestAFreshTopLevelMentionOpensAgain(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("root", "root-ts", "", true))
	require.NoError(t, err)
	leave := inbound("leave-1", "m2", "root-ts", true)
	leave.Control = ControlLeave
	_, err = s.Ingest(ctx, leave)
	require.NoError(t, err)

	reopened, err := s.Ingest(ctx, inbound("new-root", "new-ts", "", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, reopened.Disposition)
	require.Equal(t, "open", reopened.SegmentState)
	reply, err := s.Ingest(ctx, inbound("new-reply", "m3", "new-ts", false))
	require.NoError(t, err)
	require.Equal(t, Accepted, reply.Disposition)
}

// Being addressed in someone else's thread while busy claims no slot: it is
// answered in its turn, and the thread the operator is working in is not
// parked by a question somebody asked elsewhere.
func TestACrossMentionWhileBusyClaimsNoSlot(t *testing.T) {
	s, _ := testStore(t, ThreadSegments)
	ctx := context.Background()
	_, err := s.Ingest(ctx, inbound("first", "first-ts", "", true))
	require.NoError(t, err)
	_, err = s.Ingest(ctx, inbound("second", "second-ts", "", true))
	require.NoError(t, err)

	guest, err := s.Ingest(ctx, inbound("cross", "m3", "third-thread", true))
	require.NoError(t, err)
	require.Equal(t, Accepted, guest.Disposition, "being asked a question is answered, not parked")
	require.Equal(t, "guest", guest.SegmentOwner)

	// It took neither slot: the owner's threads are untouched.
	reply, err := s.Ingest(ctx, inbound("owner-reply", "m4", "first-ts", false))
	require.NoError(t, err)
	require.Equal(t, RejectedPending, reply.Disposition, "parked by the pending successor, not by the guest")
}
