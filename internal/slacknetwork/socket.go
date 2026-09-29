package slacknetwork

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
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

// DisconnectError is a classified Socket Mode control message. A warning or
// refresh request asks the caller to establish a new session; link_disabled
// does not invite an automatic reconnect. It never includes provider fields.
type DisconnectError struct{ Reason DisconnectReason }

func (*DisconnectError) Error() string { return ErrDisconnected.Error() }

func (e *DisconnectError) Is(target error) bool {
	return target == ErrDisconnected || (target == ErrReconnect && (e.Reason == DisconnectWarning || e.Reason == DisconnectRefreshRequested))
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
	obj, err := postJSON(ctx, c.httpClient, endpoint, c.appToken, []byte("{}"))
	if err != nil || !boolField(obj, "ok") {
		return "", ErrOpen
	}
	url := stringField(obj, "url")
	if !safeSocketURL(url) {
		return "", ErrOpen
	}
	return url, nil
}

func (c *SocketClient) timings() (time.Duration, time.Duration) {
	ping, expiry := c.pingEvery, c.readExpiry
	if ping <= 0 {
		ping = defaultPingEvery
	}
	if expiry <= 0 {
		expiry = defaultReadExpiry
	}
	return ping, expiry
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
	defer conn.Close()

	pingEvery, readExpiry := c.timings()
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
	conn.SetPingHandler(func(message string) error {
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
			conn.Close()
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
					conn.Close()
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
			switch reason {
			case DisconnectWarning, DisconnectRefreshRequested, DisconnectLinkDisabled:
				return &DisconnectError{Reason: reason}
			default:
				return ErrDisconnected
			}
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
