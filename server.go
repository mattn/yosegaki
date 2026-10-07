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
	"net"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/mattn/yosegaki/ot"
)

const (
	maxMessageSize  = 2 << 20
	maxDocumentSize = 1 << 20
	maxClients      = 32
	maxPending      = 16
	maxSessions     = 1000
	maxHistory      = 1000
	maxHistoryBytes = 2 << 20
	maxQueueBytes   = 4 << 20
	maxConns        = 256
	maxConnsPerIP   = 16
	createsPerIP    = 10 // per createWindow
	createWindow    = time.Minute
	writeTimeout    = 10 * time.Second
	helloTimeout    = 10 * time.Second
	sendQueueSize   = 256
)

// Memory the server may spend on documents, history and send queues.
const defaultMaxMemory = 96 << 20

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
	queued  atomic.Int64
	conn    *websocket.Conn
	srv     *server
	ip      string
}

func (c *client) push(b []byte) {
	n := int64(len(b))
	if c.queued.Load()+n > maxQueueBytes || !c.srv.reserve(n) {
		// Too slow to keep up, or the server is out of memory; the reader
		// goroutine cleans up.
		c.conn.CloseNow()
		return
	}
	select {
	case c.send <- b:
		c.queued.Add(n)
	default:
		c.srv.release(n)
		c.conn.CloseNow()
	}
}

// sent is called by the writer once b has left the queue.
func (c *client) sent(b []byte) {
	c.queued.Add(-int64(len(b)))
	c.srv.release(int64(len(b)))
}

func (c *client) canEdit() bool {
	return c.role == roleHost || c.role == roleEditor
}

type session struct {
	id        string
	public    bool
	title     string
	created   time.Time
	mu        sync.Mutex
	closed    bool
	doc       string
	docLen    int
	filetype  string
	rev       int
	histBase  int
	history   []*ot.Operation
	histBytes int
	host      *client
	clients   map[int]*client
	nextID    int
}

// size is what a session holds in memory. The caller must hold ss.mu.
func (ss *session) size() int64 {
	return int64(len(ss.doc) + ss.histBytes)
}

type server struct {
	mu           sync.Mutex
	sessions     map[string]*session
	pingInterval time.Duration
	maxMemory    int64
	used         atomic.Int64
	// realIPHeader names a header set by a trusted proxy, such as
	// CF-Connecting-IP. Empty means the peer address is used.
	realIPHeader string
	limiter      ipLimiter
}

func newServer() *server {
	return &server{
		sessions:     map[string]*session{},
		pingInterval: pingInterval,
		maxMemory:    defaultMaxMemory,
		limiter:      ipLimiter{conns: map[string]int{}, creates: map[string][]time.Time{}},
	}
}

var errFull = errors.New("server is busy, try again later")

func (s *server) reserve(n int64) bool {
	for {
		cur := s.used.Load()
		if cur+n > s.maxMemory {
			return false
		}
		if s.used.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (s *server) release(n int64) {
	s.used.Add(-n)
}

func (s *server) clientIP(r *http.Request) string {
	if s.realIPHeader != "" {
		if v := r.Header.Get(s.realIPHeader); v != "" {
			return strings.TrimSpace(strings.Split(v, ",")[0])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type ipLimiter struct {
	mu      sync.Mutex
	total   int
	conns   map[string]int
	creates map[string][]time.Time
}

func (l *ipLimiter) acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= maxConns || l.conns[ip] >= maxConnsPerIP {
		return false
	}
	l.total++
	l.conns[ip]++
	return true
}

func (l *ipLimiter) release(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	if l.conns[ip]--; l.conns[ip] <= 0 {
		delete(l.conns, ip)
	}
}

func (l *ipLimiter) allowCreate(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	recent := func(ts []time.Time) []time.Time {
		return slices.DeleteFunc(ts, func(t time.Time) bool { return now.Sub(t) >= createWindow })
	}
	if len(l.creates) > 10000 {
		for k, ts := range l.creates {
			if ts = recent(ts); len(ts) == 0 {
				delete(l.creates, k)
			} else {
				l.creates[k] = ts
			}
		}
	}
	ts := recent(l.creates[ip])
	if len(ts) >= createsPerIP {
		l.creates[ip] = ts
		return false
	}
	l.creates[ip] = append(ts, now)
	return true
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

// sanitize makes a label safe to show in a terminal: no control characters
// (C0, DEL and C1) and no bidi controls that could reorder what is shown.
func sanitize(s string, limit int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) {
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
	if len(s.sessions) >= maxSessions || !s.reserve(int64(len(m.Text))) {
		return nil, errFull
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

func (s *server) remove(ss *session, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, ss.id)
	s.release(size)
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

func (ss *session) join(srv *server, c *websocket.Conn, ip, name string, host bool) (*client, error) {
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
		srv:  srv,
		ip:   ip,
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
		size := ss.size()
		ss.mu.Unlock()
		s.remove(ss, size)
		log.Printf("session %s: closed, host %s left", ss.id, cl.ip)
		return
	}
	log.Printf("session %s: %s left as #%d", ss.id, cl.ip, cl.id)
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
	if ss.closed {
		return errors.New("session is closed")
	}
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
	doc, err := op.Apply(ss.doc)
	if err != nil {
		return err
	}
	if len(doc) > maxDocumentSize {
		return errors.New("document too large")
	}
	grow := int64(len(doc)-len(ss.doc)) + int64(op.Size())
	if grow > 0 && !cl.srv.reserve(grow) {
		return errFull
	} else if grow < 0 {
		cl.srv.release(-grow)
	}
	ss.doc = doc
	ss.docLen = op.TargetLen
	ss.rev++
	ss.history = append(ss.history, op)
	ss.histBytes += op.Size()
	cl.baseRev = rev

	cl.push(encode(map[string]any{"type": "ack", "rev": ss.rev}))
	for _, o := range ss.clients {
		o.pos = op.TransformIndex(o.pos)
	}
	ss.broadcast(encode(map[string]any{"type": "op", "rev": ss.rev, "op": op, "client": cl.id}), cl)

	// Old history is only needed to transform edits that are still in
	// flight. Clients that fall this far behind get an error and drop.
	n, freed := 0, 0
	for n < len(ss.history)-1 && (len(ss.history)-n > maxHistory || ss.histBytes-freed > maxHistoryBytes) {
		freed += ss.history[n].Size()
		n++
	}
	if n > 0 {
		ss.history = slices.Clone(ss.history[n:])
		ss.histBase += n
		ss.histBytes -= freed
		cl.srv.release(int64(freed))
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
	ip := s.clientIP(r)
	if !s.limiter.acquire(ip) {
		log.Printf("%s: too many connections", ip)
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	defer s.limiter.release(ip)
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
		if !s.limiter.allowCreate(ip, time.Now()) {
			log.Printf("%s: too many sessions created", ip)
			c.Write(ctx, websocket.MessageText, errorMessage("too many sessions created, try again later"))
			return
		}
		if ss, err = s.create(&hello); err != nil {
			log.Printf("%s: cannot create a session: %v", ip, err)
			c.Write(ctx, websocket.MessageText, errorMessage(err.Error()))
			return
		}
		log.Printf("session %s: created by %s (public=%v, %d bytes)", ss.id, ip, ss.public, len(hello.Text))
	} else if ss = s.lookup(hello.Session); ss == nil {
		log.Printf("%s: no such session %q", ip, sanitize(hello.Session, 32))
		c.Write(ctx, websocket.MessageText, errorMessage("no such session"))
		return
	}

	cl, err := ss.join(s, c, ip, name, hello.Create)
	if err != nil {
		log.Printf("session %s: %s rejected: %v", ss.id, ip, err)
		c.Write(ctx, websocket.MessageText, errorMessage(err.Error()))
		return
	}
	log.Printf("session %s: %s joined as #%d %q (%s)", ss.id, ip, cl.id, cl.name, cl.role)
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
		failed := false
		for b := range cl.send {
			if !failed {
				wctx, cancel := context.WithTimeout(ctx, writeTimeout)
				if err := c.Write(wctx, websocket.MessageText, b); err != nil {
					c.CloseNow()
					failed = true
				}
				cancel()
			}
			// Keep draining so every queued byte is given back.
			cl.sent(b)
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
				log.Printf("session %s: dropped %s #%d: %v", ss.id, ip, cl.id, err)
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

func serve(addr, realIPHeader string, maxMemory int64) error {
	s := newServer()
	s.realIPHeader = realIPHeader
	s.maxMemory = maxMemory
	log.Printf("listening on %s", addr)
	return http.ListenAndServe(addr, httpHandler(s))
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
		hd.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'; "+
			// Cloudflare Web Analytics, injected by the proxy.
			"script-src 'self' https://static.cloudflareinsights.com; connect-src 'self' https://cloudflareinsights.com")
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("Referrer-Policy", "no-referrer")
		h.ServeHTTP(w, r)
	})
}
