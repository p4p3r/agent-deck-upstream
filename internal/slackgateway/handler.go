// Package slackgateway is the narrow, offline Slack Socket Mode and posting
// boundary for a dedicated channel. It owns no websocket or HTTP client.
package slackgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

const (
	MaxEnvelopeBytes = 256 << 10
	MaxBodyBytes     = 64 << 10
	maxJSONDepth     = 32
)

var (
	ErrEnvelope = errors.New("slackgateway: invalid socket envelope")
	ErrConfig   = errors.New("slackgateway: invalid channel binding")
	ErrAck      = errors.New("slackgateway: acknowledgment failed")
)

// Config contains opaque Slack IDs, never display names or credentials.
type Config struct {
	ConversationID string
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
}

func validConfig(c Config) bool {
	if c.ConversationID == "" || c.TeamID == "" || c.ChannelID == "" || c.BotUserID == "" || len(c.AllowedUserIDs) == 0 {
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
	eventID    string
	teamID     string
	channelID  string
	userID     string
	messageID  string
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
	for _, key := range []string{"subtype", "bot_id", "bot_profile", "thread_ts"} {
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
	for _, key := range []string{"attachments", "files"} {
		populated, err := populatedArray(event, key)
		if err != nil {
			return p, err
		}
		if populated {
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
	if p.ignored || p.teamID != h.Config.TeamID || p.channelID != h.Config.ChannelID || p.userID == h.Config.BotUserID || !allowed {
		result.Ignored = true
	} else {
		mode, boundChannel, err := h.Store.ConversationRoute(ctx, h.Config.ConversationID)
		if err != nil {
			return result, err
		}
		if mode != channelgateway.ChannelStream || boundChannel != h.Config.ChannelID {
			return result, ErrConfig
		}
		result.Intake, err = h.Store.Ingest(ctx, channelgateway.Inbound{
			ConversationID: h.Config.ConversationID, EventID: p.eventID, MessageID: p.messageID,
			ChannelID: p.channelID, SenderID: p.userID, Body: p.text,
		})
		if err != nil {
			return result, err
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
