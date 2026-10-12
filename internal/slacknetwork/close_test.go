package slacknetwork

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Slack routes an event to exactly one of an app's open connections, so a
// socket abandoned without a close frame keeps taking a share of inbound
// messages and losing them. One restart cycle left three connections open for
// one running process.
func TestSocketAnnouncesCloseOnShutdown(t *testing.T) {
	var sawClose atomic.Bool
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					sawClose.Store(true)
				}
				return
			}
		}
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()
	_ = client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	for i := 0; i < 40 && !sawClose.Load(); i++ {
		time.Sleep(25 * time.Millisecond)
	}
	if !sawClose.Load() {
		t.Fatal("shutdown dropped the socket without announcing a close")
	}
}

// A reconnect abandons a socket exactly as a stop does, so it must announce
// the close too: otherwise every rotation adds a connection Slack still routes
// events to.
func TestSocketAnnouncesCloseWhenServerDisconnects(t *testing.T) {
	var sawClose atomic.Bool
	dialer, _ := localWSS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"disconnect","reason":"refresh_requested"}`))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					sawClose.Store(true)
				}
				return
			}
		}
	}))
	client := socketClientWithTicket(dialer, testWSSURL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Run(ctx, func(context.Context, []byte, func(context.Context, []byte) error) error { return nil })
	for i := 0; i < 40 && !sawClose.Load(); i++ {
		time.Sleep(25 * time.Millisecond)
	}
	if !sawClose.Load() {
		t.Fatal("reconnect abandoned the socket without announcing a close")
	}
}
