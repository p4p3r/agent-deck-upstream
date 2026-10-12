package slacknetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/asheshgoplani/agent-deck/internal/slackgateway"
	"github.com/gorilla/websocket"
)

const (
	maxAckBytes       = 64 << 10
	writeTimeout      = 5 * time.Second
	dialTimeout       = 10 * time.Second
	defaultPingEvery  = 20 * time.Second
	defaultReadExpiry = 60 * time.Second
	// Slack sends a WebSocket PING to every Socket Mode client at least every
	// 30 seconds. Silence from the server for twice that is a connection it is
	// no longer servicing, whatever the transport believes.
	defaultServerPingExpiry = 60 * time.Second
)

// EnvelopeHandler receives one authenticated Socket Mode events_api frame.
// Its ack function writes the exact supplied JSON at most once, and is valid
// only during the callback. The offline gateway should be called here so it
// can commit intake before requesting acknowledgment.
type EnvelopeHandler func(context.Context, []byte, func(context.Context, []byte) error) error

type DisconnectReason string

const (
	DisconnectWarning          DisconnectReason = "warning"
	DisconnectRefreshRequested DisconnectReason = "refresh_requested"
	DisconnectLinkDisabled     DisconnectReason = "link_disabled"
)

// DisconnectError is a classified Socket Mode control message. Slack sends
// these routinely and expects a client to reopen, so every reason invites a
// reconnect except link_disabled, which is an authorization decision no retry
// can change. Reasons we do not recognise -- too_many_websockets is one Slack
// documents and sends -- are backpressure, not protocol violations: collapsing
// them into a terminal error stopped a conductor permanently in production.
// It never includes provider fields.
type DisconnectError struct{ Reason DisconnectReason }

func (*DisconnectError) Error() string { return ErrDisconnected.Error() }

func (e *DisconnectError) Is(target error) bool {
	if target == ErrDisconnected {
		return true
	}
	return target == ErrReconnect && e.Reason != DisconnectLinkDisabled
}

// SocketClient holds only an app-level token. A Run call opens one temporary
// ticket and serves one WebSocket; the caller owns reconnect policy.
type SocketClient struct {
	appToken   string
	httpClient *http.Client      // same-package local-fake test seam
	openURL    string            // same-package local-fake test seam
	dialer     *websocket.Dialer // same-package local-fake test seam

	pingEvery  time.Duration // same-package timer test seam
	readExpiry time.Duration // same-package timer test seam
	// Deadline on PINGs arriving FROM Slack, as opposed to replies to ours.
	serverPingExpiry time.Duration // same-package timer test seam
}

func NewSocketClient(appToken string) *SocketClient {
	return &SocketClient{appToken: appToken, openURL: connectionsOpenURL}
}

func (c *SocketClient) ticket(ctx context.Context) (string, error) {
	if c == nil || !validSecret(c.appToken) {
		return "", ErrConfig
	}
	endpoint := c.openURL
	if endpoint == "" {
		endpoint = connectionsOpenURL
	}
	status, obj, err := requestLimitedJSON(ctx, c.httpClient, endpoint, c.appToken)
	if err != nil {
		switch {
		case errors.Is(err, ErrCanceled):
			return "", ErrCanceled
		case errors.Is(err, errAPITransport):
			return "", ErrOpenTransient
		default:
			return "", ErrOpenProtocol
		}
	}
	if status == http.StatusTooManyRequests || status >= 500 && status <= 599 {
		return "", ErrOpenTransient
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return "", ErrOpenAuth
	}
	if status != http.StatusOK {
		if status >= 400 && status < 500 {
			return "", ErrOpenConfig
		}
		return "", ErrOpenProtocol
	}
	if bytes.Equal(bytes.TrimSpace(obj["ok"]), []byte("false")) {
		switch stringField(obj, "error") {
		case "ratelimited", "internal_error", "service_unavailable", "team_added_to_org":
			return "", ErrOpenTransient
		case "invalid_auth", "not_authed", "token_expired", "token_revoked", "account_inactive":
			return "", ErrOpenAuth
		case "missing_scope", "not_allowed_token_type", "forbidden_team", "team_access_not_granted",
			"no_permission", "access_denied", "accesslimited", "ekm_access_denied", "enterprise_is_restricted":
			return "", ErrOpenConfig
		case "":
			return "", ErrOpenProtocol
		default:
			return "", ErrOpenUnknown
		}
	}
	if !boolField(obj, "ok") {
		return "", ErrOpenProtocol
	}
	if _, present := obj["error"]; present {
		return "", ErrOpenProtocol
	}
	url := stringField(obj, "url")
	if !safeSocketURL(url) {
		return "", ErrOpenProtocol
	}
	return url, nil
}

func (c *SocketClient) timings() (time.Duration, time.Duration, time.Duration) {
	ping, expiry, serverPing := c.pingEvery, c.readExpiry, c.serverPingExpiry
	if ping <= 0 {
		ping = defaultPingEvery
	}
	if expiry <= 0 {
		expiry = defaultReadExpiry
	}
	if serverPing <= 0 {
		serverPing = defaultServerPingExpiry
	}
	return ping, expiry, serverPing
}

// Run obtains a ticket and runs one connection. It does not retry a failed
// open, ambiguous callback, or failed ack. All outward errors are classified,
// never wrapped network errors that could contain the credential-bearing URL.
func (c *SocketClient) Run(ctx context.Context, handler EnvelopeHandler) error {
	if c == nil || handler == nil || !validSecret(c.appToken) {
		return ErrConfig
	}
	if ctx.Err() != nil {
		return ErrCanceled
	}
	ticket, err := c.ticket(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ErrCanceled
		}
		return err
	}
	dialer := c.dialer
	if dialer == nil {
		dialer = &websocket.Dialer{HandshakeTimeout: dialTimeout}
	}
	conn, response, err := dialer.DialContext(ctx, ticket, nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return ErrCanceled
		}
		return ErrReconnect
	}
	// Slack keeps counting a connection it was never told about. Dropping the
	// TCP socket leaves the server holding a session it still considers live
	// and still routes events to, and an event is delivered to exactly one of
	// an app's connections: every abandoned socket is a share of inbound
	// messages silently lost. One restart cycle left three connections open
	// for one running process. Close is announced on every exit path, not just
	// shutdown, because a reconnect abandons a socket exactly as a stop does.
	var closeOnce sync.Once
	shutdown := func() {
		closeOnce.Do(func() {
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(writeTimeout))
			_ = conn.Close()
		})
	}
	defer shutdown()

	pingEvery, readExpiry, serverPingExpiry := c.timings()
	conn.SetReadLimit(slackgateway.MaxEnvelopeBytes)
	if conn.SetReadDeadline(time.Now().Add(readExpiry)) != nil {
		return ErrReconnect
	}

	var writeMu sync.Mutex
	writeControl := func(frameType int, payload []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteControl(frameType, payload, time.Now().Add(writeTimeout))
	}
	// A PING from Slack is the only evidence that the server is still
	// servicing this connection. Record it, and treat it as liveness.
	var lastServerPing atomic.Int64
	lastServerPing.Store(time.Now().UnixNano())
	conn.SetPingHandler(func(message string) error {
		lastServerPing.Store(time.Now().UnixNano())
		_ = conn.SetReadDeadline(time.Now().Add(readExpiry))
		return writeControl(websocket.PongMessage, []byte(message))
	})
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readExpiry))
	})

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			shutdown()
		case <-done:
		}
	}()
	go func() {
		ticker := time.NewTicker(pingEvery)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if writeControl(websocket.PingMessage, nil) != nil {
					shutdown()
					return
				}
			}
		}
	}()
	// The deadman. Our own PING/PONG exchange cannot prove the connection is
	// being serviced: a server that answers pings while delivering no events
	// keeps the read deadline refreshed forever, which is exactly how a silent
	// Socket Mode connection survived 30 minutes in production while every
	// message sent to it was lost. Only Slack's own PING proves delivery is
	// still attached to this socket.
	go func() {
		ticker := time.NewTicker(serverPingExpiry / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				last := time.Unix(0, lastServerPing.Load())
				if now.Sub(last) > serverPingExpiry {
					shutdown()
					return
				}
			}
		}
	}()

	seenHello := false
	for {
		frameType, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ErrCanceled
			}
			if errors.Is(err, websocket.ErrReadLimit) {
				return ErrProtocol
			}
			return ErrReconnect
		}
		if frameType != websocket.TextMessage || len(raw) == 0 || len(raw) > slackgateway.MaxEnvelopeBytes || !utf8.Valid(raw) {
			return ErrProtocol
		}
		obj, err := decodeObject(raw)
		if err != nil {
			return ErrProtocol
		}
		typ := stringField(obj, "type")
		switch typ {
		case "hello":
			if seenHello {
				return ErrProtocol
			}
			seenHello = true
		case "disconnect":
			if !seenHello {
				return ErrProtocol
			}
			reason := DisconnectReason(stringField(obj, "reason"))
			// Every reason is carried through. The runner decides which are
			// terminal; the transport does not get to discard the reason.
			return &DisconnectError{Reason: reason}
		default:
			if !seenHello || typ == "" || stringField(obj, "envelope_id") == "" {
				return ErrProtocol
			}
			ack := newAck(conn, &writeMu)
			if err := handler(ctx, raw, ack.call); err != nil {
				ack.close()
				return ErrCallback
			}
			ack.close()
		}
	}
}

type ackWriter struct {
	conn    *websocket.Conn
	writeMu *sync.Mutex
	mu      sync.Mutex
	active  bool
	used    bool
}

func newAck(conn *websocket.Conn, writeMu *sync.Mutex) *ackWriter {
	return &ackWriter{conn: conn, writeMu: writeMu, active: true}
}

func (a *ackWriter) close() {
	a.mu.Lock()
	a.active = false
	a.mu.Unlock()
}

func (a *ackWriter) call(ctx context.Context, raw []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.active || a.used {
		return ErrAck
	}
	a.used = true // before any fallible write: retries are never safe
	if ctx == nil || ctx.Err() != nil || len(raw) == 0 || len(raw) > maxAckBytes || !json.Valid(raw) {
		return ErrAck
	}
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	deadline := time.Now().Add(writeTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := a.conn.SetWriteDeadline(deadline); err != nil {
		return ErrAck
	}
	if err := a.conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		return ErrAck
	}
	return nil
}

var _ interface{ Is(error) bool } = (*DisconnectError)(nil)
