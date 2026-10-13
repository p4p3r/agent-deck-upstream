package slackgateway

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// A conductor quotes what it is given, so addressing markup reaches replies.
// Posted verbatim it notifies people, which is an action rather than a report.
func TestReplyNeutralizesAddressingMarkup(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{"<!channel> everyone look", "&lt;!channel> everyone look"},
		{"ping <@U123> please", "ping &lt;@U123> please"},
		{"see <#C456|general>", "see &lt;#C456|general>"},
		{"<!here>", "&lt;!here>"},
		{"a < b and 3<4", "a < b and 3<4"},
		{"https://example.com/<x>", "https://example.com/<x>"},
	} {
		require.Equal(t, tc.out, replyBody(tc.in))
	}
}

// A reply too long for one message is shortened with the loss stated. Dropping
// it is indistinguishable from a conductor that never answered.
func TestOverlongReplyIsShortenedNotDropped(t *testing.T) {
	body := replyBody(strings.Repeat("x", MaxPostCharacters*2))
	require.LessOrEqual(t, utf8.RuneCountInString(body), MaxPostCharacters)
	require.Contains(t, body, "truncated")
	require.True(t, strings.HasPrefix(body, "xxx"))
}

// Shortening counts runes, not bytes: Slack's limit is characters, and cutting
// bytes would also split one.
func TestShorteningCountsRunesAndKeepsValidUTF8(t *testing.T) {
	body := replyBody(strings.Repeat("é€🙂", MaxPostCharacters))
	require.LessOrEqual(t, utf8.RuneCountInString(body), MaxPostCharacters)
	require.True(t, utf8.ValidString(body))
}

// A reply that fits is posted exactly as written.
func TestOrdinaryReplyIsUnchanged(t *testing.T) {
	body := "**bold**, `code`, and a list:\n- one\n- two\n"
	require.Equal(t, body, replyBody(body))
}
