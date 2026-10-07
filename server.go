package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base32"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/mattn/yosegaki/ot"
)

const (
	maxMessageSize  = 8 << 20
	maxDocumentSize = 4 << 20
	maxClients      = 32
	maxPending      = 16
	maxSessions     = 1000
	maxHistory      = 1000
	writeTimeout    = 10 * time.Second
	helloTimeout    = 10 * time.Second
	sendQueueSize   = 256
)

// Proxies such as Cloudflare drop WebSockets idle for about 100 seconds.
const pingInterval = 30 * time.Second

const (
	roleHost    = "host"
	roleEditor  = "editor"
	roleViewer  = "viewer"
	rolePending = "pending"
)

type inMessage struct {
	Type     string        `json:"type"`
	Session  string        `json:"session"`
	Create   bool          `json:"create"`
	Public   bool          `json:"public"`
	Title    string        `json:"title"`
	Name     string        `json:"name"`
	Filetype string        `json:"filetype"`
	Text     string        `json:"text"`
	Rev      int           `json:"rev"`
	Op       *ot.Operation `json:"op"`
	Pos      int           `json:"pos"`
	Client   int           `json:"client"`
	Role     string        `json:"role"`
}

type peer struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
	Pos  int    `json:"pos"`
}

type client struct {
	id      int
	name    string
	role    string
	pos     int
	baseRev int
	send    chan []byte
	conn    *websocket.Conn
}

func (c *client) push(b []byte) {
	select {
	case c.send <- b:
	default:
		// Too slow to keep up; the reader goroutine cleans up.
		c.conn.CloseNow()
	}
}

func (c *client) canEdit() bool {
	return c.role == roleHost || c.role == roleEditor
}

type session struct {
	id       string
	public   bool
	title    string
	created  time.Time
	mu       sync.Mutex
	closed   bool
	doc      string
	docLen   int
	filetype string
	rev      int
	histBase int
	history  []*ot.Operation
	host     *client
	clients  map[int]*client
	nextID   int
}

type server struct {
	mu           sync.Mutex
	sessions     map[string]*session
	pingInterval time.Duration
}

func newServer() *server {
	return &server{sessions: map[string]*session{}, pingInterval: pingInterval}
}

func newSessionID() string {
	var b [10]byte
	rand.Read(b[:])
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

func encode(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func errorMessage(msg string) []byte {
	return encode(map[string]any{"type": "error", "error": msg})
}

func sanitize(s string, limit int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, strings.TrimSpace(s))
	if utf8.RuneCountInString(s) > limit {
		s = string([]rune(s)[:limit])
	}
	return s
}

func (s *server) create(m *inMessage) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessions) >= maxSessions {
		return nil, errors.New("too many sessions")
	}
	ss := &session{
		id:       newSessionID(),
		public:   m.Public,
		title:    sanitize(m.Title, 64),
		created:  time.Now(),
		doc:      m.Text,
		docLen:   utf8.RuneCountInString(m.Text),
		filetype: sanitize(m.Filetype, 32),
		clients:  map[int]*client{},
	}
	s.sessions[ss.id] = ss
	return ss, nil
}

func (s *server) lookup(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *server) remove(ss *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, ss.id)
}

func (ss *session) peers() []peer {
	peers := []peer{}
	for _, o := range ss.clients {
		if o.role != rolePending {
			peers = append(peers, peer{ID: o.id, Name: o.name, Role: o.role, Pos: o.pos})
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	return peers
}

func (ss *session) broadcast(b []byte, except *client) {
	for _, o := range ss.clients {
		if o != except && o.role != rolePending {
			o.push(b)
		}
	}
}

func (ss *session) sendInit(cl *client) {
	cl.push(encode(map[string]any{
		"type":     "init",
		"session":  ss.id,
		"public":   ss.public,
		"title":    ss.title,
		"id":       cl.id,
		"role":     cl.role,
		"rev":      ss.rev,
		"text":     ss.doc,
		"filetype": ss.filetype,
		"peers":    ss.peers(),
	}))
}

// admit lets cl in with the given role. The caller must hold ss.mu.
func (ss *session) admit(cl *client, role string) {
	cl.role = role
	cl.baseRev = ss.rev
	ss.broadcast(encode(map[string]any{"type": "join", "client": cl.id, "name": cl.name, "role": role}), cl)
	ss.sendInit(cl)
}

func (ss *session) join(c *websocket.Conn, name string, host bool) (*client, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.closed {
		return nil, errors.New("session is closed")
	}
	pending := 0
	for _, o := range ss.clients {
		if o.role == rolePending {
			pending++
		}
	}
	if len(ss.clients)-pending >= maxClients {
		return nil, errors.New("session is full")
	}
	ss.nextID++
	cl := &client{
		id:   ss.nextID,
		name: name,
		send: make(chan []byte, sendQueueSize),
		conn: c,
	}
	switch {
	case host:
		ss.host = cl
		ss.clients[cl.id] = cl
		ss.admit(cl, roleHost)
	case ss.public:
		ss.clients[cl.id] = cl
		ss.admit(cl, roleViewer)
	default:
		if pending >= maxPending {
			return nil, errors.New("too many pending requests")
		}
		cl.role = rolePending
		ss.clients[cl.id] = cl
		cl.push(encode(map[string]any{"type": "pending", "session": ss.id, "title": ss.title}))
		ss.host.push(encode(map[string]any{"type": "request", "client": cl.id, "name": cl.name, "want": "join"}))
	}
	return cl, nil
}

// closeLater gives the writer goroutine a moment to flush the last message.
func closeLater(c *client, reason string) {
	time.AfterFunc(time.Second, func() { c.conn.Close(websocket.StatusNormalClosure, reason) })
}

func (ss *session) leave(s *server, cl *client) {
	ss.mu.Lock()
	delete(ss.clients, cl.id)
	close(cl.send)
	if ss.closed {
		ss.mu.Unlock()
		return
	}
	if cl == ss.host {
		// Without the host nobody can approve guests, so the session ends.
		ss.closed = true
		for _, o := range ss.clients {
			o.push(encode(map[string]any{"type": "closed", "reason": "host left"}))
			closeLater(o, "host left")
		}
		ss.mu.Unlock()
		s.remove(ss)
		log.Printf("session %s closed", ss.id)
		return
	}
	if cl.role == rolePending {
		ss.host.push(encode(map[string]any{"type": "cancel", "client": cl.id, "name": cl.name}))
	} else {
		ss.broadcast(encode(map[string]any{"type": "leave", "client": cl.id, "name": cl.name}), nil)
	}
	ss.mu.Unlock()
}

func (ss *session) operation(cl *client, rev int, op *ot.Operation) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if !cl.canEdit() {
		return errors.New("you are not allowed to edit")
	}
	if op == nil {
		return errors.New("missing op")
	}
	if rev < ss.histBase || rev > ss.rev {
		return errors.New("revision out of range")
	}
	var err error
	for _, h := range ss.history[rev-ss.histBase:] {
		if op, _, err = ot.Transform(op, h); err != nil {
			return err
		}
	}
	if op.BaseLen != ss.docLen {
		return errors.New("operation does not match the document")
	}
	if op.TargetLen > maxDocumentSize {
		return errors.New("document too large")
	}
	doc, err := op.Apply(ss.doc)
	if err != nil {
		return err
	}
	ss.doc = doc
	ss.docLen = op.TargetLen
	ss.rev++
	ss.history = append(ss.history, op)
	cl.baseRev = rev

	cl.push(encode(map[string]any{"type": "ack", "rev": ss.rev}))
	for _, o := range ss.clients {
		o.pos = op.TransformIndex(o.pos)
	}
	ss.broadcast(encode(map[string]any{"type": "op", "rev": ss.rev, "op": op, "client": cl.id}), cl)

	if len(ss.history) > maxHistory {
		base := ss.rev
		for _, o := range ss.clients {
			if o.canEdit() {
				base = min(base, o.baseRev)
			}
		}
		// Keep enough history for edits that are still in flight.
		base = max(base, ss.rev-maxHistory)
		ss.history = ss.history[base-ss.histBase:]
		ss.histBase = base
	}
	return nil
}

func (ss *session) cursor(cl *client, pos int) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if cl.role == rolePending {
		return
	}
	cl.pos = max(0, min(pos, ss.docLen))
	ss.broadcast(encode(map[string]any{"type": "cursor", "client": cl.id, "name": cl.name, "pos": cl.pos}), cl)
}

func (ss *session) requestEdit(cl *client) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if cl.role != roleViewer {
		return
	}
	ss.host.push(encode(map[string]any{"type": "request", "client": cl.id, "name": cl.name, "want": "edit"}))
}

// setRole is how the host answers requests and changes permissions.
// role is editor, viewer or deny.
func (ss *session) setRole(from *client, id int, role string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if from != ss.host {
		return errors.New("only the host can change roles")
	}
	cl := ss.clients[id]
	if cl == nil || cl == ss.host {
		return errors.New("no such guest")
	}
	switch role {
	case roleEditor, roleViewer:
		if cl.role == rolePending {
			ss.admit(cl, role)
			return nil
		}
		cl.role = role
		cl.baseRev = ss.rev
		ss.broadcast(encode(map[string]any{"type": "role", "client": cl.id, "name": cl.name, "role": role}), nil)
	case "deny":
		if cl.role == rolePending {
			cl.push(encode(map[string]any{"type": "closed", "reason": "denied by host"}))
			closeLater(cl, "denied")
		} else {
			cl.push(encode(map[string]any{"type": "denied"}))
		}
	default:
		return errors.New("unknown role")
	}
	return nil
}

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(maxMessageSize)
	ctx := r.Context()

	hctx, cancel := context.WithTimeout(ctx, helloTimeout)
	_, b, err := c.Read(hctx)
	cancel()
	if err != nil {
		return
	}
	var hello inMessage
	if err := json.Unmarshal(b, &hello); err != nil || hello.Type != "hello" {
		c.Write(ctx, websocket.MessageText, errorMessage("expected hello"))
		return
	}
	name := sanitize(hello.Name, 32)
	if name == "" {
		name = "anonymous"
	}

	var ss *session
	if hello.Create {
		if len(hello.Text) > maxDocumentSize {
			c.Write(ctx, websocket.MessageText, errorMessage("document too large"))
			return
		}
		if ss, err = s.create(&hello); err != nil {
			c.Write(ctx, websocket.MessageText, errorMessage(err.Error()))
			return
		}
		log.Printf("session %s created (public=%v)", ss.id, ss.public)
	} else if ss = s.lookup(hello.Session); ss == nil {
		c.Write(ctx, websocket.MessageText, errorMessage("no such session"))
		return
	}

	cl, err := ss.join(c, name, hello.Create)
	if err != nil {
		c.Write(ctx, websocket.MessageText, errorMessage(err.Error()))
		return
	}
	go func() {
		t := time.NewTicker(s.pingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pctx, cancel := context.WithTimeout(ctx, writeTimeout)
				err := c.Ping(pctx)
				cancel()
				if err != nil {
					c.CloseNow()
					return
				}
			}
		}
	}()

	written := make(chan struct{})
	defer func() {
		ss.leave(s, cl)
		// Let the writer flush what is queued, such as the reason for
		// closing, before the connection is torn down.
		select {
		case <-written:
		case <-time.After(writeTimeout):
		}
	}()
	go func() {
		defer close(written)
		for b := range cl.send {
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				c.CloseNow()
				return
			}
		}
	}()

	for {
		_, b, err := c.Read(ctx)
		if err != nil {
			return
		}
		var m inMessage
		if err := json.Unmarshal(b, &m); err != nil {
			cl.push(errorMessage("invalid message"))
			continue
		}
		switch m.Type {
		case "op":
			if err := ss.operation(cl, m.Rev, m.Op); err != nil {
				// The client cannot recover from a rejected edit, so drop it.
				cl.push(errorMessage(err.Error()))
				return
			}
		case "cursor":
			ss.cursor(cl, m.Pos)
		case "request_edit":
			ss.requestEdit(cl)
		case "set_role":
			if err := ss.setRole(cl, m.Client, m.Role); err != nil {
				cl.push(errorMessage(err.Error()))
			}
		}
	}
}

func (s *server) handleSessions(w http.ResponseWriter, r *http.Request) {
	type item struct {
		ID      string    `json:"id"`
		Title   string    `json:"title"`
		Host    string    `json:"host"`
		People  int       `json:"people"`
		Created time.Time `json:"created"`
	}
	list := []item{}
	s.mu.Lock()
	for _, ss := range s.sessions {
		ss.mu.Lock()
		if ss.public && !ss.closed && ss.host != nil {
			list = append(list, item{ss.id, ss.title, ss.host.name, len(ss.peers()), ss.created})
		}
		ss.mu.Unlock()
	}
	s.mu.Unlock()
	sort.Slice(list, func(i, j int) bool { return list[i].Created.After(list[j].Created) })
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

func serve(addr string) error {
	log.Printf("listening on %s", addr)
	return http.ListenAndServe(addr, httpHandler(newServer()))
}

//go:embed web
var webFS embed.FS

func httpHandler(s *server) http.Handler {
	static, _ := fs.Sub(webFS, "web")
	files := http.FileServerFS(static)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/sessions", s.handleSessions)
	mux.Handle("/static/", http.StripPrefix("/static/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "" || strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		files.ServeHTTP(w, r)
	})))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFileFS(w, r, static, "index.html")
	})
	return securityHeaders(mux)
}

func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hd := w.Header()
		hd.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("Referrer-Policy", "no-referrer")
		h.ServeHTTP(w, r)
	})
}
