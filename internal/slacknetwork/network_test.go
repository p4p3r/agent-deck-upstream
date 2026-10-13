package slacknetwork

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/gorilla/websocket"
)

const (
	testAppToken = "app-marker-private"
	testBotToken = "bot-marker-private"
	testTicket   = "ticket-marker-private"
	testBody     = "body-marker-private"
	testProvider = "provider-marker-private"
	testWSSURL   = "wss://wss.slack.com/link/?ticket=" + testTicket
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fakeResponse(status int, body string, header http.Header) *http.Response {
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

func jsonResponse(body string) *http.Response {
	return fakeResponse(http.StatusOK, body, http.Header{"Content-Type": {"application/json"}})
}

func noSecrets(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	for _, secret := range []string{testAppToken, testBotToken, testTicket, testBody, testProvider} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error disclosed private test marker %q: %v", secret, err)
		}
	}
}

func TestOpenRequestFixedEndpointAndTokenSeparation(t *testing.T) {
	var calls int
	client := NewSocketClient(testAppToken)
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "https://slack.com/api/apps.connections.open" || r.Method != http.MethodPost {
			t.Errorf("unexpected open endpoint/method: %s %s", r.Method, r.URL)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testAppToken {
			t.Errorf("open Authorization = %q", got)
		}
		if strings.Contains(r.URL.String(), testAppToken) || strings.Contains(r.URL.String(), testBotToken) {
			t.Error("credential appeared in open URL")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), testAppToken) || strings.Contains(string(body), testBotToken) {
			t.Error("credential appeared in open body")
		}
		if len(body) > 0 && r.Header.Get("Content-Type") == "" {
			t.Error("open body omitted Content-Type")
		}
		return jsonResponse(`{"ok":true,"url":"wss://evil.example/link/?ticket=` + testTicket + `"}`), nil
	})}
	err := client.Run(context.Background(), func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	if err == nil || calls != 1 {
		t.Fatalf("open with unsafe ticket: calls=%d error=%v", calls, err)
	}
	noSecrets(t, err)
}

func TestOpenRejectsUnsafeResponsesAndDoesNotRedirect(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		header http.Header
	}{
		{"not-ok", 200, `{"ok":false,"error":"` + testProvider + `"}`, nil},
		{"malformed", 200, `{`, nil},
		{"missing-url", 200, `{"ok":true}`, nil},
		{"oversize", 200, `{"ok":true,"url":"` + strings.Repeat("x", 70<<10) + `"}`, nil},
		{"non-2xx", 503, testProvider, nil},
		{"redirect", 302, "", http.Header{"Location": {"https://other.example/steal"}}},
		{"userinfo", 200, `{"ok":true,"url":"wss://user@wss.slack.com/link/?ticket=` + testTicket + `"}`, nil},
		{"fragment", 200, `{"ok":true,"url":"wss://wss.slack.com/link/?ticket=` + testTicket + `#frag"}`, nil},
		{"host-suffix", 200, `{"ok":true,"url":"wss://wss.slack.com.evil.example/link/?ticket=` + testTicket + `"}`, nil},
		{"unsafe-port", 200, `{"ok":true,"url":"wss://wss.slack.com:444/link/?ticket=` + testTicket + `"}`, nil},
		{"insecure-scheme", 200, `{"ok":true,"url":"ws://wss.slack.com/link/?ticket=` + testTicket + `"}`, nil},
		{"missing-ticket", 200, `{"ok":true,"url":"wss://wss.slack.com/link/"}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			client := NewSocketClient(testAppToken)
			client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return fakeResponse(tc.status, tc.body, tc.header), nil
			})}
			err := client.Run(context.Background(), func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
			if err == nil || calls != 1 {
				t.Fatalf("unsafe open response: calls=%d error=%v", calls, err)
			}
			noSecrets(t, err)
		})
	}
}

func keysOf(obj map[string]json.RawMessage) []string {
	names := make([]string, 0, len(obj))
	for name := range obj {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestPostTopLevelWireAndTokenSeparation(t *testing.T) {
	var calls int
	sender := NewSender(testBotToken)
	sender.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "https://slack.com/api/chat.postMessage" || r.Method != http.MethodPost {
			t.Errorf("unexpected post endpoint/method: %s %s", r.Method, r.URL)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testBotToken {
			t.Errorf("post Authorization = %q", got)
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("post Content-Type = %q", ct)
		}
		if strings.Contains(r.URL.String(), testBotToken) || strings.Contains(r.URL.String(), testAppToken) {
			t.Error("credential appeared in post URL")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(body, &obj); err != nil {
			t.Fatalf("post body: %v", err)
		}
		// channel, text and a markdown block. The text field stays as the
		// plain fallback, and a top-level post still carries no thread.
		if len(obj) != 3 || obj["thread_ts"] != nil || obj["blocks"] == nil {
			t.Errorf("post JSON keys = %v; expected channel, text and blocks", keysOf(obj))
		}
		var channel, text string
		_ = json.Unmarshal(obj["channel"], &channel)
		_ = json.Unmarshal(obj["text"], &text)
		if channel != "C-bound" || text != testBody {
			t.Errorf("post channel/text mismatch: %q %q", channel, text)
		}
		if strings.Contains(string(body), testAppToken) || strings.Contains(string(body), testBotToken) {
			t.Error("credential appeared in post body")
		}
		return jsonResponse(`{"ok":true,"channel":"C-bound","ts":"123.456"}`), nil
	})}
	got, err := sender.PostTopLevel(context.Background(), "C-bound", testBody)
	if err != nil || calls != 1 || got != (slackgateway.PostResult{OK: true, ChannelID: "C-bound", TS: "123.456"}) {
		t.Fatalf("post: result=%+v calls=%d error=%v", got, calls, err)
	}
}

func TestPostRejectsAmbiguousResponsesAndDoesNotRedirect(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		header http.Header
	}{
		{"not-ok", 200, `{"ok":false,"error":"` + testProvider + `"}`, nil},
		{"malformed", 200, `{`, nil},
		{"oversize", 200, `{"ok":true,"channel":"C-bound","ts":"` + strings.Repeat("x", 70<<10) + `"}`, nil},
		{"non-2xx", 503, testProvider, nil},
		{"redirect", 302, "", http.Header{"Location": {"https://other.example/steal"}}},
		{"wrong-channel", 200, `{"ok":true,"channel":"C-other","ts":"123.456"}`, nil},
		{"missing-ts", 200, `{"ok":true,"channel":"C-bound"}`, nil},
		{"empty-ts", 200, `{"ok":true,"channel":"C-bound","ts":""}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			sender := NewSender(testBotToken)
			sender.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return fakeResponse(tc.status, tc.body, tc.header), nil
			})}
			got, err := sender.PostTopLevel(context.Background(), "C-bound", testBody)
			if err == nil || calls != 1 || got.OK {
				t.Fatalf("ambiguous post: result=%+v calls=%d error=%v", got, calls, err)
			}
			noSecrets(t, err)
		})
	}
}

func TestPostTransportErrorIsSanitized(t *testing.T) {
	sender := NewSender(testBotToken)
	sender.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New(testProvider + testBotToken + testBody)
	})}
	_, err := sender.PostTopLevel(context.Background(), "C-bound", testBody)
	if err == nil {
		t.Fatal("expected transport error")
	}
	noSecrets(t, err)
}

// localWSS generates a certificate for the production-allowed host, then routes
// its TCP dial to a loopback TLS server. The tested ticket URL remains a real
// wss.slack.com URL, so URL validation is not weakened for tests.
func localWSS(t *testing.T, handler http.Handler) (*websocket.Dialer, *httptest.Server) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		DNSNames: []string{"wss.slack.com"}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	dialer := &websocket.Dialer{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", server.Listener.Addr().String())
		},
	}
	return dialer, server
}

func socketClientWithTicket(dialer *websocket.Dialer, ticketURL string) *SocketClient {
	c := NewSocketClient(testAppToken)
	c.dialer = dialer
	c.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"ok": true, "url": ticketURL})
		return jsonResponse(string(body)), nil
	})}
	return c
}

func TestSocketHelloEventAckOnceAndDisconnect(t *testing.T) {
	var callbackCalls atomic.Int32
	serverDone := make(chan error, 1)
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/link/" || r.URL.Query().Get("ticket") != testTicket || r.Header.Get("Authorization") != "" {
			serverDone <- errors.New("unsafe websocket handshake")
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`)); err != nil {
			serverDone <- err
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"events_api","envelope_id":"E1","accepts_response_payload":false,"payload":{}}`)); err != nil {
			serverDone <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		typ, ack, err := conn.ReadMessage()
		if err != nil || typ != websocket.TextMessage || string(ack) != `{"envelope_id":"E1"}` {
			serverDone <- errors.New("first ack not exact")
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		_, _, err = conn.ReadMessage()
		if err == nil {
			serverDone <- errors.New("second ack was written")
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"disconnect","reason":"refresh_requested"}`)); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err := client.Run(ctx, func(ctx context.Context, raw []byte, ack func(context.Context, []byte) error) error {
		callbackCalls.Add(1)
		if !strings.Contains(string(raw), `"envelope_id":"E1"`) {
			t.Errorf("unexpected frame: %s", raw)
		}
		if err := ack(ctx, []byte(`{"envelope_id":"E1"}`)); err != nil {
			return err
		}
		if err := ack(ctx, []byte(`{"envelope_id":"E1"}`)); err == nil {
			t.Error("second ack unexpectedly succeeded")
		}
		return nil
	})
	if !errors.Is(err, ErrReconnect) || callbackCalls.Load() != 1 {
		t.Fatalf("run: callbacks=%d error=%v", callbackCalls.Load(), err)
	}
	noSecrets(t, err)
	if serverErr := <-serverDone; serverErr != nil {
		t.Fatal(serverErr)
	}
}

func TestSocketInvalidFramesAndDeadPeer(t *testing.T) {
	cases := []struct {
		name      string
		frameType int
		frame     string
		want      error
	}{
		{"event-before-hello", websocket.TextMessage, `{"type":"events_api","envelope_id":"E1"}`, ErrProtocol},
		{"malformed", websocket.TextMessage, `{`, ErrProtocol},
		{"binary", websocket.BinaryMessage, `{"type":"hello"}`, ErrProtocol},
		{"oversize", websocket.TextMessage, strings.Repeat("x", slackgateway.MaxEnvelopeBytes+1), ErrProtocol},
		{"dead-peer", websocket.TextMessage, `{"type":"hello"}`, ErrReconnect},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.WriteMessage(tc.frameType, []byte(tc.frame))
			}))
			client := socketClientWithTicket(dialer, testWSSURL)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			var calls int
			err := client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error {
				calls++
				return nil
			})
			if !errors.Is(err, tc.want) || calls != 0 {
				t.Fatalf("calls=%d error=%v; want %v", calls, err, tc.want)
			}
			noSecrets(t, err)
		})
	}
}

func TestSocketCancellationClosesConnection(t *testing.T) {
	connected := make(chan struct{})
	closed := make(chan struct{})
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			close(closed)
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		close(connected)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _, _ = conn.ReadMessage()
		close(closed)
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	}()
	select {
	case <-connected:
	case <-time.After(4 * time.Second):
		cancel()
		t.Fatal("websocket did not connect")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("canceled run returned nil")
		}
		noSecrets(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("canceled run did not return")
	}
	select {
	case <-closed:
	case <-time.After(4 * time.Second):
		t.Fatal("canceled run did not close connection")
	}
}

func TestSocketDuplicateFramesAreDeliveredAndAckedSeparately(t *testing.T) {
	serverDone := make(chan error, 1)
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`)); err != nil {
			serverDone <- err
			return
		}
		frame := []byte(`{"type":"events_api","envelope_id":"E-duplicate","payload":{}}`)
		for i := 0; i < 2; i++ {
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				serverDone <- err
				return
			}
			typ, ack, err := conn.ReadMessage()
			if err != nil || typ != websocket.TextMessage || string(ack) != `{"envelope_id":"E-duplicate"}` {
				serverDone <- errors.New("duplicate envelope not independently acknowledged")
				return
			}
		}
		serverDone <- conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"disconnect","reason":"link_disabled"}`))
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	var calls int
	err := client.Run(ctx, func(ctx context.Context, raw []byte, ack func(context.Context, []byte) error) error {
		calls++
		return ack(ctx, []byte(`{"envelope_id":"E-duplicate"}`))
	})
	if !errors.Is(err, ErrDisconnected) || errors.Is(err, ErrReconnect) || calls != 2 {
		t.Fatalf("duplicate delivery: calls=%d error=%v", calls, err)
	}
	if serverErr := <-serverDone; serverErr != nil {
		t.Fatal(serverErr)
	}
}

func TestSocketConcurrentAckOnlyWritesOneFrame(t *testing.T) {
	serverDone := make(chan error, 1)
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"events_api","envelope_id":"E-race","payload":{}}`))
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		typ, frame, err := conn.ReadMessage()
		if err != nil || typ != websocket.TextMessage || string(frame) != `{"envelope_id":"E-race"}` {
			serverDone <- errors.New("expected one exact concurrent ack")
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		if _, _, err := conn.ReadMessage(); err == nil {
			serverDone <- errors.New("concurrent ack wrote a second frame")
			return
		}
		serverDone <- conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"disconnect","reason":"warning"}`))
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	var calls int
	err := client.Run(ctx, func(ctx context.Context, raw []byte, ack func(context.Context, []byte) error) error {
		calls++
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- ack(ctx, []byte(`{"envelope_id":"E-race"}`))
			}()
		}
		wg.Wait()
		close(results)
		var success, rejected int
		for result := range results {
			if result == nil {
				success++
			} else if errors.Is(result, ErrAck) {
				rejected++
			} else {
				return result
			}
		}
		if success != 1 || rejected != 1 {
			t.Errorf("concurrent ack: success=%d rejected=%d", success, rejected)
		}
		return nil
	})
	if !errors.Is(err, ErrReconnect) || calls != 1 {
		t.Fatalf("run: calls=%d error=%v", calls, err)
	}
	if serverErr := <-serverDone; serverErr != nil {
		t.Fatal(serverErr)
	}
}

func TestSocketCallbackAndAckFailuresArePrivate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		callback func(context.Context, func(context.Context, []byte) error) error
	}{
		{"callback", func(context.Context, func(context.Context, []byte) error) error {
			return errors.New(testProvider + testBody)
		}},
		{"ack", func(ctx context.Context, ack func(context.Context, []byte) error) error {
			err := ack(ctx, []byte("{"))
			if !errors.Is(err, ErrAck) {
				return errors.New("invalid ack body did not fail")
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverDone := make(chan error, 1)
			dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					serverDone <- err
					return
				}
				defer conn.Close()
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"events_api","envelope_id":"E-fail","payload":{}}`))
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, _, err := conn.ReadMessage(); err == nil {
					serverDone <- errors.New("failed callback or ack wrote a frame")
					return
				}
				serverDone <- nil
			}))
			client := socketClientWithTicket(dialer, testWSSURL)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			err := client.Run(ctx, func(ctx context.Context, raw []byte, ack func(context.Context, []byte) error) error {
				return tc.callback(ctx, ack)
			})
			if !errors.Is(err, ErrCallback) {
				t.Fatalf("failed callback: %v", err)
			}
			noSecrets(t, err)
			if serverErr := <-serverDone; serverErr != nil {
				t.Fatal(serverErr)
			}
		})
	}
}

func TestSocketDisconnectBeforeHelloIsReconnectable(t *testing.T) {
	// An over-limit app is disconnected before hello: too_many_websockets is
	// the first frame. Reading that as a protocol violation made the conductor
	// exit fatally, and with Restart=on-failure it looped, each attempt taking
	// another connection slot it was already being refused for.
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"disconnect","reason":"too_many_websockets"}`))
		time.Sleep(50 * time.Millisecond)
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	var disconnect *DisconnectError
	if !errors.As(err, &disconnect) || string(disconnect.Reason) != "too_many_websockets" {
		t.Fatalf("a pre-hello disconnect must carry its reason, got %v", err)
	}
	if !errors.Is(err, ErrReconnect) {
		t.Fatalf("a pre-hello disconnect must invite a reconnect, got %v", err)
	}
}

func TestSocketUnknownDisconnectReasonIsReconnectable(t *testing.T) {
	// too_many_websockets is backpressure Slack documents and sends. Treating
	// it as terminal stopped a production conductor for 100 minutes: the
	// process exited, the unit is Restart=no, and nothing retried.
	for _, reason := range []string{"too_many_websockets", "something_new"} {
		dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
			_ = conn.WriteMessage(websocket.TextMessage,
				[]byte(`{"type":"disconnect","reason":"`+reason+`"}`))
			time.Sleep(50 * time.Millisecond)
		}))
		client := socketClientWithTicket(dialer, testWSSURL)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
		cancel()
		var disconnect *DisconnectError
		if !errors.As(err, &disconnect) {
			t.Fatalf("%s: expected a DisconnectError carrying the reason, got %v", reason, err)
		}
		if string(disconnect.Reason) != reason {
			t.Fatalf("%s: reason was discarded, got %q", reason, disconnect.Reason)
		}
		if !errors.Is(err, ErrReconnect) {
			t.Fatalf("%s: must invite a reconnect", reason)
		}
	}
}

func TestSocketLinkDisabledStaysTerminal(t *testing.T) {
	// The one reason no retry can fix.
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"disconnect","reason":"link_disabled"}`))
		time.Sleep(50 * time.Millisecond)
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	if errors.Is(err, ErrReconnect) {
		t.Fatalf("link_disabled must not invite a reconnect, got %v", err)
	}
}

func TestSocketSilentServerTripsDeadman(t *testing.T) {
	// The production failure: the server answers our PINGs, so the read
	// deadline is refreshed forever, while it delivers no events at all. The
	// connection must not be allowed to survive that.
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		// Answer every PING, send none of our own, deliver nothing.
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	client.pingEvery = 10 * time.Millisecond
	client.readExpiry = 2 * time.Second
	client.serverPingExpiry = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err := client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	if !errors.Is(err, ErrReconnect) {
		t.Fatalf("silent server: expected ErrReconnect, got %v after %s", err, time.Since(start))
	}
	if time.Since(start) > time.Second {
		t.Fatalf("silent server: deadman took %s, too slow", time.Since(start))
	}
}

func TestSocketServerPingKeepsConnectionAlive(t *testing.T) {
	// The converse: a server that PINGs us is being serviced, and an idle
	// channel must not be torn down just because nobody is talking.
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
		deadline := time.Now().Add(400 * time.Millisecond)
		for time.Now().Before(deadline) {
			if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)) != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	client.pingEvery = 10 * time.Millisecond
	client.readExpiry = 2 * time.Second
	client.serverPingExpiry = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_ = client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("server pings should have kept an idle connection alive, torn down after %s", elapsed)
	}
}

func TestSocketDeadPeerHeartbeat(t *testing.T) {
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		// No reads means no pong; the client must leave via its read expiry.
		time.Sleep(350 * time.Millisecond)
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	client.pingEvery = 20 * time.Millisecond
	client.readExpiry = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	if !errors.Is(err, ErrReconnect) || time.Since(start) > time.Second {
		t.Fatalf("dead peer: elapsed=%s error=%v", time.Since(start), err)
	}
}
