package mqtt

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotConnected  = errors.New("mqtt: client not connected")
	ErrNotSubscribed = errors.New("mqtt: client has no matching subscription")
)

// Handler gets the decoded traffic. payload is only valid for the duration of
// the OnPublish call, copy it if you need it later. For QoS 1 the PUBACK goes
// out only when OnPublish returns nil, so return nil once the data is durable.
type Handler interface {
	OnConnect(clientID string)
	OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte) error
	OnDisconnect(clientID string)
}

type Config struct {
	Addr           string
	MaxConns       int
	MaxPacket      int
	ConnectTimeout time.Duration
	MaxIdle        time.Duration // cap on keepalive*1.5, and the limit when a client sends keepalive 0
	WriteTimeout   time.Duration
	// Auth can be nil to let everyone in
	Auth   func(clientID, username string, password []byte) bool
	Logger *slog.Logger
}

func (c *Config) defaults() {
	if c.MaxConns <= 0 {
		c.MaxConns = 50000
	}
	if c.MaxPacket <= 0 {
		c.MaxPacket = 64 << 10
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.MaxIdle <= 0 {
		c.MaxIdle = 5 * time.Minute
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

type Stats struct {
	Active     int64
	Accepted   uint64
	Rejected   uint64 // over MaxConns, bad auth, bad connect
	PacketsIn  uint64
	BytesIn    uint64
	Publishes  uint64
	ProtoError uint64
	Takeovers  uint64
}

type Server struct {
	cfg Config
	h   Handler
	lg  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	sessions map[string]*session
	ln       net.Listener
	wg       sync.WaitGroup

	active                                    atomic.Int64
	accepted, rejected, pktsIn, bytesIn, pubs atomic.Uint64
	protoErrs, takeovers                      atomic.Uint64
}

func NewServer(cfg Config, h Handler) *Server {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{cfg: cfg, h: h, lg: cfg.Logger.With("component", "mqtt"), ctx: ctx, cancel: cancel, sessions: make(map[string]*session)}
}

func (s *Server) Stats() Stats {
	return Stats{
		Active: s.active.Load(), Accepted: s.accepted.Load(), Rejected: s.rejected.Load(),
		PacketsIn: s.pktsIn.Load(), BytesIn: s.bytesIn.Load(), Publishes: s.pubs.Load(),
		ProtoError: s.protoErrs.Load(), Takeovers: s.takeovers.Load(),
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve blocks until the listener is closed (Shutdown does that).
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	var backoff time.Duration
	for {
		c, err := ln.Accept()
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() { // e.g. out of fds, don't spin
				backoff = min(max(backoff*2, 5*time.Millisecond), time.Second)
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0

		if s.active.Add(1) > int64(s.cfg.MaxConns) {
			s.active.Add(-1)
			s.rejected.Add(1)
			c.Close()
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.active.Add(-1)
			s.handle(c)
		}()
	}
}

func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Shutdown stops accepting, drops every connection and waits for handlers.
func (s *Server) Shutdown(ctx context.Context) error {
	s.cancel()
	s.mu.Lock()
	if s.ln != nil {
		s.ln.Close()
	}
	for _, ss := range s.sessions {
		ss.conn.Close()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type session struct {
	srv  *Server
	conn net.Conn
	id   string

	wmu sync.Mutex

	smu  sync.RWMutex
	subs map[string]struct{}
}

func (ss *session) write(b []byte) error {
	ss.wmu.Lock()
	defer ss.wmu.Unlock()
	ss.conn.SetWriteDeadline(time.Now().Add(ss.srv.cfg.WriteTimeout))
	_, err := ss.conn.Write(b)
	return err
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	// small buffer: 10k+ idle conns shouldn't cost 4k each
	br := bufio.NewReaderSize(c, 512)
	var buf []byte

	c.SetReadDeadline(time.Now().Add(s.cfg.ConnectTimeout))
	hdr, body, err := readPacket(br, s.cfg.MaxPacket, &buf)
	if err != nil || hdr>>4 != typeConnect || hdr&0x0f != 0 {
		s.rejected.Add(1)
		return
	}
	cp, code, err := parseConnect(body)
	if err != nil {
		s.rejected.Add(1)
		if code != 0 {
			c.Write([]byte{typeConnack << 4, 2, 0, code})
		}
		return
	}
	if cp.clientID == "" {
		s.rejected.Add(1)
		c.Write([]byte{typeConnack << 4, 2, 0, connBadClientID})
		return
	}
	if s.cfg.Auth != nil && !s.cfg.Auth(cp.clientID, cp.username, cp.password) {
		s.rejected.Add(1)
		c.Write([]byte{typeConnack << 4, 2, 0, connBadCreds})
		return
	}

	ss := &session{srv: s, conn: c, id: cp.clientID, subs: map[string]struct{}{}}
	if !s.register(ss) {
		c.Write([]byte{typeConnack << 4, 2, 0, connUnavailable})
		return
	}
	defer s.unregister(ss)

	if err := ss.write([]byte{typeConnack << 4, 2, 0, connAccepted}); err != nil {
		return
	}
	s.accepted.Add(1)
	s.h.OnConnect(ss.id)
	defer s.h.OnDisconnect(ss.id)

	idle := s.cfg.MaxIdle
	if cp.keepAlive > 0 {
		idle = min(time.Duration(cp.keepAlive)*time.Second*3/2, s.cfg.MaxIdle)
	}

	for {
		c.SetReadDeadline(time.Now().Add(idle))
		hdr, body, err := readPacket(br, s.cfg.MaxPacket, &buf)
		if err != nil {
			if errors.Is(err, ErrMalformed) || errors.Is(err, ErrTooLarge) {
				s.protoErrs.Add(1)
			}
			return
		}
		s.pktsIn.Add(1)
		s.bytesIn.Add(uint64(len(body) + 2))

		switch hdr >> 4 {
		case typePublish:
			p, err := parsePublish(hdr, body)
			if err != nil {
				s.protoErrs.Add(1)
				return
			}
			if p.qos == 2 {
				s.protoErrs.Add(1) // we don't do the 4-way handshake
				return
			}
			s.pubs.Add(1)
			if err := s.h.OnPublish(s.ctx, ss.id, p.topic, p.payload, p.qos); err != nil {
				s.lg.Debug("publish rejected", "client", ss.id, "topic", p.topic, "err", err)
				continue // no ack, a QoS1 client will retry
			}
			if p.qos == 1 {
				if ss.write([]byte{typePuback << 4, 2, byte(p.id >> 8), byte(p.id)}) != nil {
					return
				}
			}

		case typePingreq:
			if ss.write([]byte{typePingresp << 4, 0}) != nil {
				return
			}

		case typeSubscribe:
			if hdr&0x0f != 2 {
				s.protoErrs.Add(1)
				return
			}
			sp, err := parseSubscribe(body)
			if err != nil {
				s.protoErrs.Add(1)
				return
			}
			ack := []byte{typeSuback << 4, byte(2 + len(sp.filters)), byte(sp.id >> 8), byte(sp.id)}
			ss.smu.Lock()
			for _, f := range sp.filters {
				ss.subs[f] = struct{}{}
				ack = append(ack, 0)
			}
			ss.smu.Unlock()
			if ss.write(ack) != nil {
				return
			}

		case typeUnsubscribe:
			id, fs, err := parseUnsubscribe(body)
			if err != nil {
				s.protoErrs.Add(1)
				return
			}
			ss.smu.Lock()
			for _, f := range fs {
				delete(ss.subs, f)
			}
			ss.smu.Unlock()
			if ss.write([]byte{typeUnsuback << 4, 2, byte(id >> 8), byte(id)}) != nil {
				return
			}

		case typeDisconnect:
			return

		case typePuback:
			// we only send QoS0 downlink, nothing to match

		default:
			s.protoErrs.Add(1)
			return
		}
	}
}

// returns false if the server is shutting down
func (s *Server) register(ss *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return false
	}
	if old, ok := s.sessions[ss.id]; ok {
		// same client id reconnecting: spec says kick the old one
		s.takeovers.Add(1)
		old.conn.Close()
	}
	s.sessions[ss.id] = ss
	return true
}

func (s *Server) unregister(ss *session) {
	s.mu.Lock()
	if s.sessions[ss.id] == ss {
		delete(s.sessions, ss.id)
	}
	s.mu.Unlock()
}

func (s *Server) Connected(clientID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.sessions[clientID]
	return ok
}

// Publish sends a QoS0 message down to one client, if it subscribed to a
// matching filter.
func (s *Server) Publish(clientID, topic string, payload []byte) error {
	s.mu.RLock()
	ss := s.sessions[clientID]
	s.mu.RUnlock()
	if ss == nil {
		return ErrNotConnected
	}
	ss.smu.RLock()
	ok := false
	for f := range ss.subs {
		if topicMatch(f, topic) {
			ok = true
			break
		}
	}
	ss.smu.RUnlock()
	if !ok {
		return ErrNotSubscribed
	}
	return ss.write(buildPublish(topic, payload, 0, 0))
}
