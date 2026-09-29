// Package slacknetwork provides the credential-bearing Slack network boundary
// for the offline slackgateway. It owns no configuration or reconnect loop.
package slacknetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
)

const (
	connectionsOpenURL = "https://slack.com/api/apps.connections.open"
	postMessageURL     = "https://slack.com/api/chat.postMessage"
	maxResponseBytes   = 64 << 10
	httpTimeout        = 10 * time.Second
)

var (
	ErrConfig       = errors.New("slacknetwork: invalid configuration")
	ErrOpen         = errors.New("slacknetwork: connection open failed")
	ErrPost         = errors.New("slacknetwork: post outcome uncertain")
	ErrProtocol     = errors.New("slacknetwork: invalid socket protocol")
	ErrDisconnected = errors.New("slacknetwork: socket disconnected")
	ErrReconnect    = errors.New("slacknetwork: new socket connection required")
	ErrCanceled     = errors.New("slacknetwork: operation canceled")
	ErrCallback     = errors.New("slacknetwork: envelope callback failed")
	ErrAck          = errors.New("slacknetwork: acknowledgment failed")
)

// Sender implements slackgateway.Sender. Only a bot token is held here; it
// cannot open a Socket Mode connection.
type Sender struct {
	botToken   string
	httpClient *http.Client // same-package local-fake test seam
	postURL    string       // same-package local-fake test seam
}

var _ slackgateway.Sender = (*Sender)(nil)

func NewSender(botToken string) *Sender {
	return &Sender{botToken: botToken, postURL: postMessageURL}
}

func validSecret(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if r <= ' ' || r >= 127 {
			return false
		}
	}
	return true
}

func validOpaqueID(id string) bool {
	if id == "" || len(id) > 128 || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if r <= ' ' || r == 127 {
			return false
		}
	}
	return true
}

// postJSON makes a single request. Redirects are disabled even if a caller
// injected a permissive client; a bearer token must never follow Location.
// Neither request errors nor response bodies are allowed into public errors.
func postJSON(ctx context.Context, client *http.Client, endpoint, token string, body []byte) (map[string]json.RawMessage, error) {
	if !validSecret(token) {
		return nil, ErrConfig
	}
	if client == nil {
		client = &http.Client{}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copyClient.Timeout == 0 || copyClient.Timeout > httpTimeout {
		copyClient.Timeout = httpTimeout
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, ErrProtocol
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := copyClient.Do(req)
	if err != nil {
		return nil, ErrProtocol
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, ErrProtocol
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxResponseBytes || !utf8.Valid(data) {
		return nil, ErrProtocol
	}
	return decodeObject(data)
}

// decodeObject rejects duplicate top-level keys and trailing JSON. This keeps
// the security-critical ok/url/channel/ts fields unambiguous.
func decodeObject(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, ErrProtocol
	}
	obj := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, ErrProtocol
		}
		name, ok := key.(string)
		if !ok {
			return nil, ErrProtocol
		}
		if _, duplicate := obj[name]; duplicate {
			return nil, ErrProtocol
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return nil, ErrProtocol
		}
		obj[name] = raw
	}
	if tok, err = d.Token(); err != nil || tok != json.Delim('}') {
		return nil, ErrProtocol
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrProtocol
	}
	return obj, nil
}

func boolField(obj map[string]json.RawMessage, key string) bool {
	return bytes.Equal(bytes.TrimSpace(obj[key]), []byte("true"))
}

func stringField(obj map[string]json.RawMessage, key string) string {
	var value string
	if len(obj[key]) == 0 || json.Unmarshal(obj[key], &value) != nil {
		return ""
	}
	return value
}

// PostTopLevel sends exactly channel and text as JSON; there is deliberately
// no thread parameter. Any uncertain HTTP outcome returns ErrPost without
// exposing provider text, the token, or the attempted message.
func (s *Sender) PostTopLevel(ctx context.Context, channel, message string) (slackgateway.PostResult, error) {
	var zero slackgateway.PostResult
	if s == nil || !validSecret(s.botToken) || !validOpaqueID(channel) || !utf8.ValidString(message) || utf8.RuneCountInString(message) > slackgateway.MaxPostCharacters {
		return zero, ErrConfig
	}
	endpoint := s.postURL
	if endpoint == "" {
		endpoint = postMessageURL
	}
	body, _ := json.Marshal(struct {
		Channel string `json:"channel"`
		Text    string `json:"text"`
	}{channel, message})
	obj, err := postJSON(ctx, s.httpClient, endpoint, s.botToken, body)
	if err != nil || !boolField(obj, "ok") {
		return zero, ErrPost
	}
	actualChannel, ts := stringField(obj, "channel"), stringField(obj, "ts")
	if actualChannel != channel || !validOpaqueID(ts) {
		return zero, ErrPost
	}
	return slackgateway.PostResult{OK: true, ChannelID: actualChannel, TS: ts}, nil
}

func safeSocketURL(raw string) bool {
	if len(raw) == 0 || len(raw) > 4096 || strings.ContainsAny(raw, "\r\n\t") {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "wss" || u.Opaque != "" || u.User != nil || u.Fragment != "" || u.RawFragment != "" || u.Path != "/link/" || u.RawPath != "" || u.RawQuery == "" {
		return false
	}
	host := u.Hostname()
	if host != "wss.slack.com" {
		if !strings.HasPrefix(host, "wss-") || !strings.HasSuffix(host, ".slack.com") {
			return false
		}
		label := strings.TrimSuffix(strings.TrimPrefix(host, "wss-"), ".slack.com")
		if label == "" || len(label) > 59 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
				return false
			}
		}
	}
	if port := u.Port(); port != "" && port != "443" {
		return false
	}
	values, err := url.ParseQuery(u.RawQuery)
	return err == nil && len(values["ticket"]) == 1 && values.Get("ticket") != ""
}
