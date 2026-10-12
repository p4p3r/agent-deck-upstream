package slackgateway

import (
	"context"
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

// A conductor that cannot see where a message came from has no way to tell an
// operator's Slack message from text pasted into its pane, and refuses to act
// on it. The gateway has already authorized the sender by this point, so the
// line is provenance for a reader rather than a credential.
func TestInboundBodyCarriesProvenance(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	raw := encodeEnvelope(t, eventEnvelope("prov-envelope", "prov-event", "ship it"))
	if _, err := slackHandler(s).Handle(context.Background(), raw,
		func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("handle: %v", err)
	}
	turn, err := s.NextTurn(context.Background(), testConversation)
	if err != nil || turn == nil {
		t.Fatalf("next turn: %v", err)
	}
	line, text, found := strings.Cut(turn.Body, "\n")
	if !found {
		t.Fatalf("body carries no provenance line: %q", turn.Body)
	}
	if text != "ship it" {
		t.Fatalf("message text was altered: %q", text)
	}
	for _, want := range []string{"[slack ", "channel=", "sender=", "ts="} {
		if !strings.Contains(line, want) {
			t.Fatalf("provenance line %q lacks %q", line, want)
		}
	}
	if len(line)+1 > MaxProvenanceBytes {
		t.Fatalf("provenance line exceeds its reserved headroom: %d", len(line))
	}
}

// A full-size message must still be accepted: the store reserves headroom for
// the provenance line rather than taking it out of the user's allowance.
func TestProvenanceDoesNotShrinkTheMessageAllowance(t *testing.T) {
	s, _ := slackStore(t, channelgateway.ChannelStream)
	body := strings.Repeat("a", MaxBodyBytes)
	raw := encodeEnvelope(t, eventEnvelope("full-envelope", "full-event", body))
	result, err := slackHandler(s).Handle(context.Background(), raw,
		func(context.Context, []byte) error { return nil })
	if err != nil || result.Intake.Disposition != channelgateway.Accepted {
		t.Fatalf("a %d byte message was rejected: err=%v disposition=%q",
			MaxBodyBytes, err, result.Intake.Disposition)
	}
}
