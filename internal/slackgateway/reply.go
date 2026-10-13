package slackgateway

import (
	"strings"
	"unicode/utf8"
)

// Slack's own cap on one message. A conductor's report can exceed it, and a
// reply that is dropped for being long is indistinguishable from a conductor
// that never answered.
const MaxPostCharacters = 40000

// truncationNotice is appended when a reply does not fit. It states the loss
// rather than hiding it: a silently shortened report is worse than a visibly
// shortened one, because only one of them can be chased.
const truncationNotice = "\n\n[truncated: the reply did not fit in one Slack message]"

// neutralizeMentions disarms Slack's addressing markup in text this gateway did
// not write.
//
// A conductor quotes what it is given, so "<!channel>" or "<@U...>" can reach a
// reply by way of an operator's own message. Posted verbatim they notify people
// -- in a channel shared by four conductors, potentially everyone -- which is
// an action, not a report. Escaping the opening bracket leaves the text
// readable and makes it inert.
func neutralizeMentions(text string) string {
	var out strings.Builder
	out.Grow(len(text))
	for i := 0; i < len(text); i++ {
		if text[i] == '<' && i+1 < len(text) {
			switch text[i+1] {
			case '@', '#', '!':
				out.WriteString("&lt;")
				continue
			}
		}
		out.WriteByte(text[i])
	}
	return out.String()
}

// replyBody prepares conductor output for posting: inert, and within Slack's
// limit. The returned string is always postable.
func replyBody(body string) string {
	body = neutralizeMentions(body)
	if utf8.RuneCountInString(body) <= MaxPostCharacters {
		return body
	}
	keep := MaxPostCharacters - utf8.RuneCountInString(truncationNotice)
	runes := []rune(body)
	if keep < 0 {
		keep = 0
	}
	return strings.TrimRight(string(runes[:keep]), " \t\r\n") + truncationNotice
}
