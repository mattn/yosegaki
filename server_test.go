package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type testClient struct {
	t    *testing.T
	c    *websocket.Conn
	msgs chan map[string]any
}

func dial(t *testing.T, url string, hello map[string]any) *testClient {
	t.Helper()
	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	tc := &testClient{t: t, c: c, msgs: make(chan map[string]any, 100)}
	go func() {
		defer close(tc.msgs)
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				return
			}
			var m map[string]any
			json.Unmarshal(b, &m)
			tc.msgs <- m
		}
	}()
	t.Cleanup(func() { c.CloseNow() })
	tc.send(hello)
	return tc
}

func (tc *testClient) send(m map[string]any) {
	b, _ := json.Marshal(m)
	if err := tc.c.Write(context.Background(), websocket.MessageText, b); err != nil {
		tc.t.Fatal(err)
	}
}

func (tc *testClient) expect(typ string) map[string]any {
	tc.t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case m, ok := <-tc.msgs:
			if !ok {
				tc.t.Fatalf("connection closed while waiting for %s", typ)
			}
			if m["type"] == typ {
				return m
			}
		case <-timeout:
			tc.t.Fatalf("timeout waiting for %s", typ)
		}
	}
}

func newTestServer(t *testing.T, ping time.Duration) string {
	s := newServer()
	s.pingInterval = ping
	ts := httptest.NewServer(httpHandler(s))
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
}

func TestPrivateSession(t *testing.T) {
	url := newTestServer(t, pingInterval)
	host := dial(t, url, map[string]any{"type": "hello", "create": true, "name": "host", "text": "abc"})
	init := host.expect("init")
	id := init["session"].(string)

	guest := dial(t, url, map[string]any{"type": "hello", "session": id, "name": "guest"})
	guest.expect("pending")
	req := host.expect("request")

	// A pending guest must not be able to edit.
	host.send(map[string]any{"type": "set_role", "client": req["client"], "role": "viewer"})
	g := guest.expect("init")
	if g["text"] != "abc" || g["role"] != "viewer" {
		t.Fatalf("unexpected init: %v", g)
	}
	guest.send(map[string]any{"type": "op", "rev": 0, "op": []any{"x", 3}})
	if e := guest.expect("error"); !strings.Contains(e["error"].(string), "not allowed") {
		t.Fatalf("viewer edit not rejected: %v", e)
	}
}

func TestPing(t *testing.T) {
	url := newTestServer(t, 20*time.Millisecond)
	host := dial(t, url, map[string]any{"type": "hello", "create": true, "name": "host", "text": ""})
	host.expect("init")
	// Several pings go by; a client that answers them stays connected.
	time.Sleep(200 * time.Millisecond)
	host.send(map[string]any{"type": "op", "rev": 0, "op": []any{"hi"}})
	host.expect("ack")
}
