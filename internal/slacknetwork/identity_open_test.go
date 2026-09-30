package slacknetwork

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestVerifyBotIdentityUsesFixedAuthEndpointAndBotToken(t *testing.T) {
	var calls int
	sender := NewSender(testBotToken)
	sender.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodPost || r.URL.String() != "https://slack.com/api/auth.test" {
			t.Errorf("auth.test method/endpoint: %s %s", r.Method, r.URL)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testBotToken {
			t.Errorf("auth.test Authorization mismatch: %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(r.URL.String(), testBotToken) || strings.Contains(r.URL.String(), testAppToken) ||
			strings.Contains(string(body), testBotToken) || strings.Contains(string(body), testAppToken) ||
			strings.Contains(r.Header.Get("Authorization"), testAppToken) {
			t.Error("credential sent to wrong auth.test field")
		}
		if len(body) > 0 && r.Header.Get("Content-Type") == "" {
			t.Error("auth.test body omitted Content-Type")
		}
		return jsonResponse(`{"ok":true,"team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot","team":"provider-private"}`), nil
	})}
	got, err := sender.VerifyBotIdentity(context.Background())
	if err != nil || calls != 1 || got != (Identity{TeamID: "T-bound", BotUserID: "U-bot"}) {
		t.Fatalf("identity=%+v calls=%d error=%v", got, calls, err)
	}
}

func TestVerifyBotIdentityRejectsAmbiguousResponsesAndRedirect(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		header http.Header
	}{
		{"missing-team", 200, `{"ok":true,"user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"missing-user", 200, `{"ok":true,"team_id":"T-bound","bot_id":"B-bot"}`, nil},
		{"missing-bot", 200, `{"ok":true,"team_id":"T-bound","user_id":"U-bot"}`, nil},
		{"empty-bot", 200, `{"ok":true,"team_id":"T-bound","user_id":"U-bot","bot_id":""}`, nil},
		{"wrong-team-type", 200, `{"ok":true,"team_id":9,"user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"wrong-user-type", 200, `{"ok":true,"team_id":"T-bound","user_id":9,"bot_id":"B-bot"}`, nil},
		{"wrong-bot-type", 200, `{"ok":true,"team_id":"T-bound","user_id":"U-bot","bot_id":9}`, nil},
		{"wrong-ok-type", 200, `{"ok":"true","team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"duplicate-team", 200, `{"ok":true,"team_id":"T-bound","team_id":"T-other","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"duplicate-ok", 200, `{"ok":true,"ok":false,"team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"malformed", 200, `{"ok":`, nil},
		{"oversize", 200, `{"ok":true,"team_id":"T-bound","user_id":"U-bot","bot_id":"` + strings.Repeat("x", 70<<10) + `"}`, nil},
		{"provider-auth", 200, `{"ok":false,"error":"invalid_auth","detail":"` + testProvider + `"}`, nil},
		{"provider-unknown", 200, `{"ok":false,"error":"` + testProvider + `"}`, nil},
		{"true-with-provider-error", 200, `{"ok":true,"error":"invalid_auth","team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"true-with-empty-error", 200, `{"ok":true,"error":"","team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"true-with-null-error", 200, `{"ok":true,"error":null,"team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"created-not-success", 201, `{"ok":true,"team_id":"T-bound","user_id":"U-bot","bot_id":"B-bot"}`, nil},
		{"no-content-not-success", 204, "", nil},
		{"non-2xx", 503, testProvider, nil},
		{"redirect", 302, "", http.Header{"Location": {"https://other.example/steal"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			sender := NewSender(testBotToken)
			sender.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return fakeResponse(tc.status, tc.body, tc.header), nil
			})}
			identity, err := sender.VerifyBotIdentity(context.Background())
			if !errors.Is(err, ErrIdentity) || calls != 1 || identity != (Identity{}) {
				t.Fatalf("identity=%+v calls=%d error=%v; want private identity failure", identity, calls, err)
			}
			noSecrets(t, err)
			if strings.Contains(err.Error(), "invalid_auth") {
				t.Error("provider code leaked")
			}
		})
	}
}

func TestVerifyBotIdentityExplicitCancellationIsPrivate(t *testing.T) {
	entered := make(chan struct{})
	sender := NewSender(testBotToken)
	sender.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := sender.VerifyBotIdentity(ctx); done <- err }()
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		cancel()
		t.Fatal("auth.test did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("canceled identity error=%v", err)
		}
		noSecrets(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("canceled auth.test did not return")
	}
}

func TestOpenClassifiesTransportHTTPProviderAndProtocol(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		transportErr error
		want         error
	}{
		{"transport", 0, "", errors.New(testProvider + testTicket), ErrOpenTransient},
		{"timeout", 0, "", context.DeadlineExceeded, ErrOpenTransient},
		{"rate-limit-http", 429, "", nil, ErrOpenTransient},
		{"server-error", 503, "", nil, ErrOpenTransient},
		{"provider-rate-limit", 200, `{"ok":false,"error":"ratelimited"}`, nil, ErrOpenTransient},
		{"provider-internal", 200, `{"ok":false,"error":"internal_error"}`, nil, ErrOpenTransient},
		{"provider-unavailable", 200, `{"ok":false,"error":"service_unavailable"}`, nil, ErrOpenTransient},
		{"provider-team-migration", 200, `{"ok":false,"error":"team_added_to_org"}`, nil, ErrOpenTransient},
		{"http-unauthorized", 401, "", nil, ErrOpenAuth},
		{"http-forbidden", 403, "", nil, ErrOpenAuth},
		{"provider-invalid-auth", 200, `{"ok":false,"error":"invalid_auth"}`, nil, ErrOpenAuth},
		{"provider-revoked", 200, `{"ok":false,"error":"token_revoked"}`, nil, ErrOpenAuth},
		{"provider-missing-scope", 200, `{"ok":false,"error":"missing_scope"}`, nil, ErrOpenConfig},
		{"unknown-provider", 200, `{"ok":false,"error":"` + testProvider + `"}`, nil, ErrOpenUnknown},
		{"missing-url", 200, `{"ok":true}`, nil, ErrOpenProtocol},
		{"unsafe-url", 200, `{"ok":true,"url":"wss://evil.example/link/?ticket=` + testTicket + `"}`, nil, ErrOpenProtocol},
		{"malformed", 200, `{"ok":`, nil, ErrOpenProtocol},
		{"duplicate-ok", 200, `{"ok":true,"ok":false,"url":"` + testWSSURL + `"}`, nil, ErrOpenProtocol},
		{"true-with-provider-error", 200, `{"ok":true,"error":"invalid_auth","url":"` + testWSSURL + `"}`, nil, ErrOpenProtocol},
		{"true-with-empty-error", 200, `{"ok":true,"error":"","url":"` + testWSSURL + `"}`, nil, ErrOpenProtocol},
		{"true-with-null-error", 200, `{"ok":true,"error":null,"url":"` + testWSSURL + `"}`, nil, ErrOpenProtocol},
		{"created-not-success", 201, `{"ok":true,"url":"` + testWSSURL + `"}`, nil, ErrOpenProtocol},
		{"no-content-not-success", 204, "", nil, ErrOpenProtocol},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			client := NewSocketClient(testAppToken)
			client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return fakeResponse(tc.status, tc.body, nil), nil
			})}
			url, err := client.ticket(context.Background())
			if !errors.Is(err, tc.want) || !errors.Is(err, ErrOpen) || url != "" || calls != 1 {
				t.Fatalf("ticket=%q calls=%d error=%v; want %v + ErrOpen", url, calls, err, tc.want)
			}
			noSecrets(t, err)
			for _, private := range []string{"invalid_auth", "token_revoked", "missing_scope", "ratelimited", "internal_error", "service_unavailable"} {
				if strings.Contains(err.Error(), private) {
					t.Errorf("provider code %q leaked", private)
				}
			}
		})
	}
}

func TestOpenExplicitCancellationDoesNotBecomeTransient(t *testing.T) {
	entered := make(chan struct{})
	client := NewSocketClient(testAppToken)
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.ticket(ctx); done <- err }()
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		cancel()
		t.Fatal("apps.connections.open did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCanceled) || errors.Is(err, ErrOpenTransient) {
			t.Fatalf("canceled open error=%v", err)
		}
		noSecrets(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("canceled open did not return")
	}
}

func TestOpenContradictorySuccessNeverDialsTicket(t *testing.T) {
	client := NewSocketClient(testAppToken)
	client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(`{"ok":true,"error":"invalid_auth","url":"` + testWSSURL + `"}`), nil
	})}
	var dials int
	client.dialer = &websocket.Dialer{NetDialContext: func(context.Context, string, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected ticket dial")
	}}
	err := client.Run(context.Background(), func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	if !errors.Is(err, ErrOpenProtocol) || dials != 0 {
		t.Fatalf("contradictory open: dials=%d error=%v", dials, err)
	}
	noSecrets(t, err)
}
