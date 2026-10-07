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
	c.SetReadLimit(maxMessageSize)
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

func newLimitedServer(t *testing.T, s *server) string {
	ts := httptest.NewServer(httpHandler(s))
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
}

func waitUsed(t *testing.T, s *server, want int64) {
	t.Helper()
	for range 100 {
		if s.used.Load() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("used = %d, want %d", s.used.Load(), want)
}

func TestMemoryBudget(t *testing.T) {
	s := newServer()
	s.maxMemory = 100 << 10
	url := newLimitedServer(t, s)

	host := dial(t, url, map[string]any{"type": "hello", "create": true, "name": "host", "text": strings.Repeat("a", 40<<10)})
	host.expect("init")
	// The init message has left the queue; only the document is held.
	waitUsed(t, s, 40<<10)
	other := dial(t, url, map[string]any{"type": "hello", "create": true, "name": "x", "text": strings.Repeat("b", 70<<10)})
	if e := other.expect("error"); e["error"] != errFull.Error() {
		t.Fatalf("second session not refused: %v", e)
	}

	// Growing past the budget drops the editor instead of the server.
	host.send(map[string]any{"type": "op", "rev": 0, "op": []any{40 << 10, strings.Repeat("c", 40<<10)}})
	if e := host.expect("error"); e["error"] != errFull.Error() {
		t.Fatalf("growth not refused: %v", e)
	}
	// Everything is given back once the session is gone.
	waitUsed(t, s, 0)
}

func TestHistoryIsTrimmed(t *testing.T) {
	s := newServer()
	url := newLimitedServer(t, s)
	host := dial(t, url, map[string]any{"type": "hello", "create": true, "name": "host", "text": ""})
	host.expect("init")
	big := strings.Repeat("x", 300<<10)
	for rev := range 12 {
		// Insert then delete, so the document stays small but history grows.
		if rev%2 == 0 {
			host.send(map[string]any{"type": "op", "rev": rev, "op": []any{big}})
		} else {
			host.send(map[string]any{"type": "op", "rev": rev, "op": []any{-(300 << 10)}})
		}
		host.expect("ack")
	}
	s.mu.Lock()
	var ss *session
	for _, v := range s.sessions {
		ss = v
	}
	s.mu.Unlock()
	ss.mu.Lock()
	hb := ss.histBytes
	ss.mu.Unlock()
	if hb > maxHistoryBytes {
		t.Fatalf("history holds %d bytes", hb)
	}
	host.c.Close(websocket.StatusNormalClosure, "")
	waitUsed(t, s, 0)
}

func TestConnectionsPerIP(t *testing.T) {
	s := newServer()
	s.realIPHeader = "X-Real-IP"
	ts := httptest.NewServer(httpHandler(s))
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	opts := func(ip string) *websocket.DialOptions {
		return &websocket.DialOptions{HTTPHeader: map[string][]string{"X-Real-IP": {ip}}}
	}
	ctx := context.Background()
	for i := range maxConnsPerIP {
		c, _, err := websocket.Dial(ctx, url, opts("192.0.2.1"))
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		defer c.CloseNow()
	}
	if _, resp, err := websocket.Dial(ctx, url, opts("192.0.2.1")); err == nil || resp.StatusCode != 429 {
		t.Fatalf("connection over the limit was accepted: %v", err)
	}
	c, _, err := websocket.Dial(ctx, url, opts("192.0.2.2"))
	if err != nil {
		t.Fatalf("another IP was refused: %v", err)
	}
	c.CloseNow()
}

func TestCreateRate(t *testing.T) {
	var l ipLimiter
	l.creates = map[string][]time.Time{}
	now := time.Now()
	for range createsPerIP {
		if !l.allowCreate("a", now) {
			t.Fatal("refused too early")
		}
	}
	if l.allowCreate("a", now) {
		t.Fatal("rate not limited")
	}
	if !l.allowCreate("b", now) {
		t.Fatal("other IP limited")
	}
	if !l.allowCreate("a", now.Add(createWindow)) {
		t.Fatal("window does not slide")
	}
}

func TestSanitize(t *testing.T) {
	got := sanitize(" a\x1b[31mb\u009bc‮d⁦e ", 32)
	if got != "a[31mbcde" {
		t.Fatalf("got %q", got)
	}
}
