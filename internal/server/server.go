// Package server provides a bounded, concurrent TCP chat server.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"tcp-chat-server/internal/protocol"
)

var (
	ErrServerClosed   = errors.New("server is closed")
	ErrAlreadyServing = errors.New("server has already been started")
)

const MaxNameBytes = 64

// Config's zero values select defaults. All numeric settings must be positive
// when explicitly provided. Queue sizes count frames, not bytes.
type Config struct {
	AuthTimeout        time.Duration
	IdleTimeout        time.Duration
	WriteTimeout       time.Duration
	SendQueueSize      int
	BroadcastQueueSize int
	MaxConnections     int
	Logger             *slog.Logger
}

type broadcast struct {
	sender *session
	frame  protocol.Frame
}

// Server may be served once. Shutdown and ClientCount are concurrency safe.
// mu protects lifecycle state, connection tracking, and the name registry.
type Server struct {
	cfg          Config
	mu           sync.RWMutex
	started      bool
	closing      bool
	listener     net.Listener
	connections  map[*session]struct{}
	clients      map[string]*session
	broadcasts   chan broadcast
	stopping     chan struct{}
	done         chan struct{}
	shutdownOnce sync.Once
	readers      sync.WaitGroup
	writers      sync.WaitGroup
	hub          sync.WaitGroup
}

func New(cfg Config) (*Server, error) {
	if cfg.AuthTimeout < 0 || cfg.IdleTimeout < 0 || cfg.WriteTimeout < 0 ||
		cfg.SendQueueSize < 0 || cfg.BroadcastQueueSize < 0 || cfg.MaxConnections < 0 {
		return nil, errors.New("server settings must not be negative")
	}
	if cfg.AuthTimeout == 0 {
		cfg.AuthTimeout = 10 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.SendQueueSize == 0 {
		cfg.SendQueueSize = 128
	}
	if cfg.BroadcastQueueSize == 0 {
		cfg.BroadcastQueueSize = 256
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{
		cfg: cfg, connections: make(map[*session]struct{}),
		clients:    make(map[string]*session),
		broadcasts: make(chan broadcast, cfg.BroadcastQueueSize),
		stopping:   make(chan struct{}), done: make(chan struct{}),
	}, nil
}

// Serve takes ownership of listener on its first successful invocation.
// It returns when accepting stops; call Shutdown to wait for queued writes.
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("nil listener")
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrServerClosed
	}
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyServing
	}
	s.started, s.listener = true, listener
	s.hub.Add(1)
	go s.broadcastLoop()
	s.mu.Unlock()

	var delay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.isStopping() {
				return ErrServerClosed
			}
			if temporary, ok := err.(interface{ Temporary() bool }); ok && temporary.Temporary() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
				}
				if delay > time.Second {
					delay = time.Second
				}
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-s.stopping:
					timer.Stop()
					return ErrServerClosed
				}
				continue
			}
			s.beginShutdown()
			return fmt.Errorf("accept: %w", err)
		}
		delay = 0
		s.mu.Lock()
		if s.closing || len(s.connections) >= s.cfg.MaxConnections {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		c := &session{conn: conn, outbound: make(chan protocol.Frame, s.cfg.SendQueueSize)}
		s.connections[c] = struct{}{}
		// Adds are protected by the same lock as the shutdown transition, so
		// no Add can race the shutdown Wait after admission has stopped.
		s.readers.Add(1)
		s.writers.Add(1)
		go s.writeLoop(c)
		go s.readLoop(c)
		s.mu.Unlock()
	}
}

func (s *Server) ClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// Shutdown stops admission and reads, drains accepted broadcasts and outbound
// queues, then closes connections. On context expiry it force-closes all
// remaining connections and returns ctx.Err(). It is safe to call repeatedly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.beginShutdown()
	select {
	case <-s.done:
		return nil
	default:
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		for _, c := range s.connectionSnapshot() {
			s.terminate(c)
		}
		return ctx.Err()
	}
}

func (s *Server) beginShutdown() {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		close(s.stopping)
		listener := s.listener
		s.mu.Unlock()
		if listener != nil {
			_ = listener.Close()
		}
		for _, c := range s.connectionSnapshot() {
			// Wake blocked readers while keeping the write half available.
			_ = c.conn.SetReadDeadline(time.Now())
		}
		go func() {
			s.readers.Wait()
			close(s.broadcasts) // Every producer has exited.
			s.hub.Wait()
			for _, c := range s.connectionSnapshot() {
				c.seal()
			}
			s.writers.Wait()
			close(s.done)
		}()
	})
}

func (s *Server) connectionSnapshot() []*session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*session, 0, len(s.connections))
	for c := range s.connections {
		result = append(result, c)
	}
	return result
}

func (s *Server) isStopping() bool {
	select {
	case <-s.stopping:
		return true
	default:
		return false
	}
}

func (s *Server) readFrame(c *session, timeout time.Duration) (protocol.Frame, error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return protocol.Frame{}, ErrServerClosed
	}
	// Serialize deadline updates with the shutdown transition: a new read
	// cannot overwrite the deadline used to interrupt shutdown reads.
	err := c.conn.SetReadDeadline(time.Now().Add(timeout))
	s.mu.Unlock()
	if err != nil {
		return protocol.Frame{}, err
	}
	return protocol.ReadFrame(c.conn)
}

func (s *Server) readLoop(c *session) {
	defer s.readers.Done()
	defer func() {
		s.mu.Lock()
		closing := s.closing
		if !closing {
			s.unregisterLocked(c)
		}
		s.mu.Unlock()
		if !closing {
			c.seal()
		}
		// During shutdown retain recipients until all accepted broadcasts
		// have been dispatched. The shutdown coordinator then seals queues.
	}()

	frame, err := s.readFrame(c, s.cfg.AuthTimeout)
	if err != nil {
		s.readError(c, err)
		return
	}
	if frame.Type != protocol.TypeAuth || !validName(frame.Payload) {
		s.sendError(c, "first frame must be Auth with a valid name (1-64 UTF-8 bytes)")
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	name := string(frame.Payload)
	if _, exists := s.clients[name]; exists {
		s.mu.Unlock()
		s.sendError(c, "name is already in use")
		return
	}
	c.name = name
	// Enqueue acknowledgement before publishing the session to the hub.
	c.enqueue(protocol.Frame{Type: protocol.TypeAuth, Payload: frame.Payload})
	s.clients[name] = c
	s.mu.Unlock()

	for {
		frame, err = s.readFrame(c, s.cfg.IdleTimeout)
		if err != nil {
			s.readError(c, err)
			return
		}
		switch frame.Type {
		case protocol.TypeMessage:
			payload, err := encodeBroadcast(c.name, frame.Payload)
			if err != nil {
				s.sendError(c, err.Error())
				return
			}
			select {
			case s.broadcasts <- broadcast{sender: c, frame: protocol.Frame{Type: protocol.TypeMessage, Payload: payload}}:
			case <-s.stopping:
				return
			}
		case protocol.TypePing:
			// Pong: mirror the keepalive payload back to the originator.
			if !c.enqueue(protocol.Frame{Type: protocol.TypePing, Payload: frame.Payload}) {
				s.terminate(c)
				return
			}
		case protocol.TypeDisconnect:
			if len(frame.Payload) != 0 {
				s.sendError(c, "Disconnect payload must be empty")
			}
			return
		default:
			s.sendError(c, "expected Message, Ping, or Disconnect after Auth")
			return
		}
	}
}

func validName(payload []byte) bool {
	name := string(payload)
	if len(payload) == 0 || len(payload) > MaxNameBytes || !utf8.Valid(payload) || strings.TrimSpace(name) != name {
		return false
	}
	return !strings.ContainsFunc(name, unicode.IsControl)
}

func encodeBroadcast(name string, payload []byte) ([]byte, error) {
	if !utf8.Valid(payload) {
		return nil, errors.New("Message must contain valid UTF-8 JSON")
	}
	var request struct {
		Text string `json:"text"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Text == "" {
		return nil, errors.New("Message must be a JSON object with a nonempty text field")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("Message must contain exactly one JSON object")
	}
	encoded, err := json.Marshal(protocol.Message{From: name, Text: request.Text})
	if err != nil {
		return nil, err
	}
	if len(encoded) > protocol.MaxPayloadSize {
		return nil, errors.New("Message exceeds frame limit after adding sender metadata")
	}
	return encoded, nil
}

func (s *Server) readError(c *session, err error) {
	if s.isStopping() || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return
	}
	s.sendError(c, "invalid, incomplete, or timed-out frame")
}

func (s *Server) sendError(c *session, message string) {
	if !c.enqueue(protocol.Frame{Type: protocol.TypeError, Payload: []byte(message)}) {
		s.terminate(c)
	}
}

func (s *Server) broadcastLoop() {
	defer s.hub.Done()
	for message := range s.broadcasts {
		s.mu.RLock()
		recipients := make([]*session, 0, len(s.clients))
		for _, c := range s.clients {
			if c != message.sender {
				recipients = append(recipients, c)
			}
		}
		s.mu.RUnlock()
		for _, c := range recipients {
			if !c.enqueue(message.frame) {
				s.cfg.Logger.Debug("disconnecting unavailable recipient", "remote", c.conn.RemoteAddr())
				s.terminate(c)
			}
		}
	}
}

func (s *Server) writeLoop(c *session) {
	defer s.writers.Done()
	defer func() {
		_ = c.conn.Close()
		c.seal()
		s.mu.Lock()
		s.unregisterLocked(c)
		delete(s.connections, c)
		s.mu.Unlock()
	}()
	for frame := range c.outbound {
		if err := c.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
			return
		}
		if err := protocol.WriteFrame(c.conn, frame); err != nil {
			s.cfg.Logger.Debug("client write failed", "remote", c.conn.RemoteAddr(), "error", err)
			return
		}
	}
}

func (s *Server) unregisterLocked(c *session) {
	if s.clients[c.name] == c {
		delete(s.clients, c.name)
	}
}

func (s *Server) terminate(c *session) {
	s.mu.Lock()
	s.unregisterLocked(c)
	s.mu.Unlock()
	_ = c.conn.Close()
	c.seal()
}
