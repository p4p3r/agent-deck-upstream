// Package slackgateway is the narrow, offline Slack Socket Mode and posting
// boundary for a dedicated channel. It owns no websocket or HTTP client.
package slackgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

const (
	MaxEnvelopeBytes = 256 << 10
	MaxBodyBytes     = 32 << 10
	// Headroom the store reserves for the provenance line below.
	MaxProvenanceBytes = 1 << 10
	maxJSONDepth       = 32
)

var (
	ErrEnvelope = errors.New("slackgateway: invalid socket envelope")
	ErrConfig   = errors.New("slackgateway: invalid channel binding")
	ErrAck      = errors.New("slackgateway: acknowledgment failed")
)

// Config contains opaque Slack IDs, never display names or credentials.
type Config struct {
	ConversationID string
	AppID          string
	TeamID         string
	ChannelID      string
	BotUserID      string
	AllowedUserIDs []string
}

type Handler struct {
	Store  *channelgateway.Store
	Config Config
}

type HandleResult struct {
	Intake  channelgateway.IntakeResult
	Ignored bool
	// AcceptedMessageTS is the operator's own message, to be marked as heard.
	// Set only for a freshly accepted event: a duplicate was already marked.
	AcceptedMessageTS string
	// AcceptedThreadTS is the thread that message belongs to, and
	// AcceptedTitle a short name for it. Both drive the thread's agent-session
	// indicator; the title is honoured only when the session is created.
	AcceptedThreadTS string
	AcceptedTitle    string
	// Pointer is set when the gateway parked a message rather than acting on
	// it. Silence would be indistinguishable from a dead channel, which is the
	// failure this whole fork exists to stop, so the caller answers in the
	// thread the operator wrote in.
	Pointer Pointer
}

// Pointer addresses one advisory reply: where to post it, and which thread the
// operator should be reading instead.
type Pointer struct {
	ThreadTS        string
	PointerThreadTS string
	Superseded      bool
}

func (p Pointer) Wanted() bool { return p.ThreadTS != "" && p.PointerThreadTS != "" }

func validConfig(c Config) bool {
	if c.ConversationID == "" || c.AppID == "" || c.TeamID == "" || c.ChannelID == "" || c.BotUserID == "" || len(c.AllowedUserIDs) == 0 {
		return false
	}
	seen := make(map[string]bool, len(c.AllowedUserIDs))
	for _, id := range c.AllowedUserIDs {
		if id == "" || id == c.BotUserID || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

// checkValue validates every JSON object recursively. encoding/json otherwise
// accepts duplicate keys, allowing different consumers to see different IDs.
func checkValue(d *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return ErrEnvelope
	}
	t, err := d.Token()
	if err != nil {
		return ErrEnvelope
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return ErrEnvelope
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return ErrEnvelope
			}
			seen[name] = true
			if err := checkValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrEnvelope
		}
	case '[':
		for d.More() {
			if err := checkValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrEnvelope
		}
	default:
		return ErrEnvelope
	}
	return nil
}

func object(raw json.RawMessage) (map[string]json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return nil, ErrEnvelope
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return nil, ErrEnvelope
	}
	return obj, nil
}

func field(obj map[string]json.RawMessage, key string, required bool) (string, bool, error) {
	raw, present := obj[key]
	if !present {
		if required {
			return "", false, ErrEnvelope
		}
		return "", false, nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || bytes.Equal(raw, []byte("null")) || value == "" {
		return "", true, ErrEnvelope
	}
	return value, true, nil
}

func optionalTeam(obj map[string]json.RawMessage, key string, teams *[]string) error {
	team, present, err := field(obj, key, false)
	if err != nil {
		return err
	}
	if present {
		*teams = append(*teams, team)
	}
	return nil
}

func populatedArray(obj map[string]json.RawMessage, key string) (bool, error) {
	raw, present := obj[key]
	if !present {
		return false, nil
	}
	if raw = bytes.TrimSpace(raw); len(raw) == 0 || raw[0] != '[' {
		return false, ErrEnvelope
	}
	var elements []json.RawMessage
	if json.Unmarshal(raw, &elements) != nil {
		return false, ErrEnvelope
	}
	return len(elements) != 0, nil
}

type parsed struct {
	envelopeID string
	appID      string
	eventID    string
	teamID     string
	channelID  string
	userID     string
	messageID  string
	threadID   string
	text       string
	ignored    bool
}

func parse(raw []byte) (parsed, error) {
	var p parsed
	if len(raw) == 0 || len(raw) > MaxEnvelopeBytes || !utf8.Valid(raw) {
		return p, ErrEnvelope
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := checkValue(d, 0); err != nil {
		return p, err
	}
	if _, err := d.Token(); err != io.EOF {
		return p, ErrEnvelope
	}
	outer, err := object(raw)
	if err != nil {
		return p, err
	}
	if p.envelopeID, _, err = field(outer, "envelope_id", true); err != nil {
		return p, err
	}
	typ, _, err := field(outer, "type", true)
	if err != nil {
		return p, err
	}
	rawAccepts, present := outer["accepts_response_payload"]
	var accepts bool
	if !present || bytes.Equal(bytes.TrimSpace(rawAccepts), []byte("null")) || json.Unmarshal(rawAccepts, &accepts) != nil {
		return p, ErrEnvelope
	}
	payload, err := object(outer["payload"])
	if err != nil {
		return p, err
	}
	if typ != "events_api" {
		p.ignored = true
		return p, nil
	}
	if accepts {
		return p, ErrEnvelope
	}
	callbackType, _, err := field(payload, "type", true)
	if err != nil {
		return p, err
	}
	var teams []string
	for _, pair := range []struct {
		obj map[string]json.RawMessage
		key string
	}{{outer, "team_id"}, {payload, "team_id"}, {payload, "context_team_id"}} {
		if err := optionalTeam(pair.obj, pair.key, &teams); err != nil {
			return p, err
		}
	}
	if callbackType != "event_callback" {
		p.ignored = true
		return p, nil
	}
	if p.eventID, _, err = field(payload, "event_id", true); err != nil {
		return p, err
	}
	if p.appID, _, err = field(payload, "api_app_id", true); err != nil {
		return p, err
	}
	// A callback must identify its workspace somewhere. Every represented
	// team ID, including authorization entries, must agree.
	event, err := object(payload["event"])
	if err != nil {
		return p, err
	}
	for _, key := range []string{"team", "team_id"} {
		if err := optionalTeam(event, key, &teams); err != nil {
			return p, err
		}
	}
	if rawAuth, present := payload["authorizations"]; present {
		var authorizations []json.RawMessage
		if json.Unmarshal(rawAuth, &authorizations) != nil || bytes.Equal(rawAuth, []byte("null")) {
			return p, ErrEnvelope
		}
		for _, rawAuthorization := range authorizations {
			authorization, err := object(rawAuthorization)
			if err != nil {
				return p, err
			}
			if err := optionalTeam(authorization, "team_id", &teams); err != nil {
				return p, err
			}
		}
	}
	if len(teams) == 0 {
		return p, ErrEnvelope
	}
	p.teamID = teams[0]
	for _, team := range teams[1:] {
		if team != p.teamID {
			return p, ErrEnvelope
		}
	}
	eventType, _, err := field(event, "type", true)
	if err != nil {
		return p, err
	}
	if eventType != "message" {
		p.ignored = true
		return p, nil
	}
	channelType, _, err := field(event, "channel_type", true)
	if err != nil {
		return p, err
	}
	if channelType != "group" {
		p.ignored = true
		return p, nil
	}
	// An edit, a deletion or another app's message is never an instruction.
	// thread_ts is no longer in this list: a reply inside a thread is the
	// ordinary way to continue a conversation, and the gateway decides whether
	// this one belongs to a segment it owns.
	for _, key := range []string{"subtype", "bot_id", "bot_profile"} {
		if _, present := event[key]; present {
			p.ignored = true
			return p, nil
		}
	}
	if p.channelID, _, err = field(event, "channel", true); err != nil {
		return p, err
	}
	if p.userID, _, err = field(event, "user", true); err != nil {
		return p, err
	}
	if p.messageID, _, err = field(event, "ts", true); err != nil {
		return p, err
	}
	// Slack sets thread_ts on a reply and, on a parent that has replies, to the
	// parent's own ts. The latter is still a top-level message, so it is
	// normalized away rather than treated as a reply to itself.
	if p.threadID, _, err = field(event, "thread_ts", false); err != nil {
		return p, err
	}
	if p.threadID == p.messageID {
		p.threadID = ""
	}
	for _, key := range []string{"attachments", "files"} {
		populated, err := populatedArray(event, key)
		if err != nil {
			return p, err
		}
		if populated || event[key] != nil {
			p.ignored = true
			return p, nil
		}
	}
	rawText, present := event["text"]
	if !present || json.Unmarshal(rawText, &p.text) != nil || bytes.Equal(rawText, []byte("null")) || len(p.text) > MaxBodyBytes {
		return p, ErrEnvelope
	}
	return p, nil
}

// replyThread is the thread an answer to this message belongs in: the thread it
// arrived in, or the message itself when it opened one.
func replyThread(p parsed) string {
	if p.threadID != "" {
		return p.threadID
	}
	return p.messageID
}

// sessionTitle names a thread after the message that opened it.
//
// The text is the operator's own message, posted back into the channel it came
// from, so this discloses nothing new. Mentions are stripped because "<@U...>"
// is markup rather than words, the first line is enough to recognise a thread
// in a list, and Slack caps a title at 200 characters.
func sessionTitle(text string) string {
	line := text
	if cut := strings.IndexAny(line, "\r\n"); cut >= 0 {
		line = line[:cut]
	}
	for {
		open := strings.Index(line, "<@")
		if open < 0 {
			break
		}
		close := strings.Index(line[open:], ">")
		if close < 0 {
			break
		}
		line = line[:open] + line[open+close+1:]
	}
	// Removing markup leaves the gaps it occupied, so the words are rejoined
	// rather than trimmed at the ends only.
	line = strings.Join(strings.Fields(line), " ")
	const maxTitle = 120
	if utf8.RuneCountInString(line) > maxTitle {
		runes := []rune(line)
		line = strings.TrimSpace(string(runes[:maxTitle]))
	}
	return line
}

// mentions reports whether a message addresses this bot.
//
// Slack renders a mention as the literal "<@U…>", with the bot's own user ID
// and no display name, so this is an exact search rather than a parse of the
// text a human typed. A channel shared by several conductors needs it: every
// app in that channel receives every message, so without an address each one
// would answer. The gateway then decides what an address means -- in
// thread_segments mode a mention opens a conversation, and a reply inside a
// segment this conversation already owns needs none.
func mentions(text, botUserID string) bool {
	if botUserID == "" {
		return false
	}
	return strings.Contains(text, "<@"+botUserID+">")
}

// provenance prefixes an inbound body with the origin a conductor needs to
// attribute it. Kept to one line so it costs nothing in a prompt, and always
// present so its absence is meaningful rather than ambiguous.
func provenance(p parsed) string {
	line := "[slack channel=" + p.channelID + " sender=" + p.userID + " ts=" + p.messageID + "]\n"
	if len(line) > MaxProvenanceBytes {
		// Identifiers are short and fixed-shape; an oversized one is a
		// malformed envelope, and a body is never truncated to fit a header.
		return "[slack]\n"
	}
	return line
}

// Handle processes one already-authenticated Socket Mode frame. The caller's
// ack callback is invoked only after a deterministic filter or committed
// gateway intake. On malformed input or storage failure it is never invoked.
// Ack failures do not roll back a committed event; Slack may retry it with a
// new envelope ID, which the gateway deduplicates by event_id.
func (h Handler) Handle(ctx context.Context, raw []byte, ack func(context.Context, []byte) error) (HandleResult, error) {
	var result HandleResult
	if h.Store == nil || ack == nil || !validConfig(h.Config) {
		return result, ErrConfig
	}
	p, err := parse(raw)
	if err != nil {
		return result, err
	}
	allowed := false
	for _, id := range h.Config.AllowedUserIDs {
		if id == p.userID {
			allowed = true
			break
		}
	}
	if p.ignored || p.appID != h.Config.AppID || p.teamID != h.Config.TeamID || p.channelID != h.Config.ChannelID || p.userID == h.Config.BotUserID || !allowed {
		result.Ignored = true
	} else {
		mode, boundChannel, err := h.Store.ConversationRoute(ctx, h.Config.ConversationID)
		if err != nil {
			return result, err
		}
		if mode != channelgateway.ChannelStream && mode != channelgateway.ThreadSegments {
			return result, ErrConfig
		}
		if boundChannel != h.Config.ChannelID {
			return result, ErrConfig
		}
		// The agent is told where this came from. Everything reaching this
		// point has already passed the app, team, channel, bot and allowlist
		// checks above, so the header is provenance for a reader, not a
		// credential: a conductor that cannot see it has no way to tell an
		// operator's Slack message from text pasted into its pane, and
		// correctly refuses to treat it as instruction. The gateway aliases
		// these identifiers before storing them, so they are composed here,
		// where the real ones still exist.
		result.Intake, err = h.Store.Ingest(ctx, channelgateway.Inbound{
			ConversationID: h.Config.ConversationID, EventID: p.eventID, MessageID: p.messageID,
			ThreadID: p.threadID, ChannelID: p.channelID, SenderID: p.userID,
			Mentioned: mentions(p.text, h.Config.BotUserID), Body: provenance(p) + p.text,
		})
		if err != nil {
			return result, err
		}
		// A stored event the gateway declined to act on is as ignored to a
		// caller as one this boundary filtered: both mean no turn. The
		// disposition stays in result.Intake for anything that needs the
		// distinction.
		if result.Intake.Disposition == channelgateway.Ignored {
			result.Ignored = true
		}
		if !result.Intake.Duplicate && result.Intake.Disposition == channelgateway.Accepted {
			result.AcceptedMessageTS = p.messageID
			result.AcceptedThreadTS = replyThread(p)
			result.AcceptedTitle = sessionTitle(p.text)
		}
		// A duplicate is Slack retrying an event this gateway already answered,
		// so its pointer was already posted and must not be posted twice.
		if !result.Intake.Duplicate && result.Intake.PointerThreadID != "" {
			switch result.Intake.Disposition {
			case channelgateway.RejectedPending:
				result.Pointer = Pointer{ThreadTS: replyThread(p), PointerThreadTS: result.Intake.PointerThreadID}
			case channelgateway.RejectedSuperseded:
				result.Pointer = Pointer{ThreadTS: replyThread(p), PointerThreadTS: result.Intake.PointerThreadID, Superseded: true}
			}
		}
	}
	ackBody, _ := json.Marshal(struct {
		EnvelopeID string `json:"envelope_id"`
	}{p.envelopeID})
	if err := ack(ctx, ackBody); err != nil {
		return result, ErrAck
	}
	return result, nil
}
