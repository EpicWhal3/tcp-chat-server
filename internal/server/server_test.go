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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tcp-chat-server/internal/client"
	"tcp-chat-server/internal/protocol"
)

func startServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return startWithListener(t, cfg, listener), listener.Addr().String()
}

func startWithListener(t *testing.T, cfg Config, listener net.Listener) *Server {
	t.Helper()
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(cfg)
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- s.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoErrorf(t, s.Shutdown(ctx), "shutdown")
		_ = listener.Close()
		select {
		case err := <-served:
			assert.ErrorIsf(t, err, ErrServerClosed, "Serve")
		case <-ctx.Done():
			t.Error("Serve did not exit")
		}
	})
	waitFor(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.started
	})
	return s
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func dial(t *testing.T, address string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	return conn
}

func writeFrame(t *testing.T, conn net.Conn, typ protocol.Type, payload string) {
	t.Helper()
	require.NoError(t, protocol.WriteFrame(conn, protocol.Frame{Type: typ, Payload: []byte(payload)}))
}

func readFrame(t *testing.T, conn net.Conn, typ protocol.Type) protocol.Frame {
	t.Helper()
	frame, err := protocol.ReadFrame(conn)
	require.NoError(t, err)
	require.Equalf(t, typ, frame.Type, "frame type (payload %s)", frame.Payload)
	return frame
}

func auth(t *testing.T, address, name string) net.Conn {
	t.Helper()
	conn := dial(t, address)
	writeFrame(t, conn, protocol.TypeAuth, name)
	assert.Equal(t, name, string(readFrame(t, conn, protocol.TypeAuth).Payload), "Auth acknowledgement")
	return conn
}

func TestBroadcast(t *testing.T) {
	s, address := startServer(t, Config{})
	alice := auth(t, address, "alice")
	bob := auth(t, address, "bob")
	carol := auth(t, address, "carol")
	writeFrame(t, alice, protocol.TypeMessage, `{"text":"Привет 🌍"}`)
	for _, conn := range []net.Conn{bob, carol} {
		var message protocol.Message
		require.NoError(t, json.Unmarshal(readFrame(t, conn, protocol.TypeMessage).Payload, &message))
		assert.Equal(t, "alice", message.From)
		assert.Equal(t, "Привет 🌍", message.Text)
	}
	require.NoError(t, alice.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
	_, err := protocol.ReadFrame(alice)
	var netErr net.Error
	require.ErrorAsf(t, err, &netErr, "sender must not receive its own broadcast")
	assert.True(t, netErr.Timeout())
	writeFrame(t, alice, protocol.TypeDisconnect, "")
	waitFor(t, func() bool { return s.ClientCount() == 2 })
}

func TestPingPong(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"keepalive payload", "keepalive"},
		{"empty payload", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, address := startServer(t, Config{})
			conn := auth(t, address, "pinger")
			writeFrame(t, conn, protocol.TypePing, tt.payload)
			assert.Equal(t, tt.payload, string(readFrame(t, conn, protocol.TypePing).Payload), "pong must mirror ping")
		})
	}
}

func TestFragmentedAndCoalescedTCPFrames(t *testing.T) {
	_, address := startServer(t, Config{})
	bob := auth(t, address, "bob")
	alice := dial(t, address)
	var wire bytes.Buffer
	require.NoError(t, protocol.WriteFrame(&wire, protocol.Frame{Type: protocol.TypeAuth, Payload: []byte("alice")}))
	for _, b := range wire.Bytes() {
		_, err := alice.Write([]byte{b})
		require.NoError(t, err)
	}
	readFrame(t, alice, protocol.TypeAuth)
	wire.Reset()
	for _, text := range []string{"one", "two"} {
		payload, _ := json.Marshal(protocol.Message{Text: text})
		require.NoError(t, protocol.WriteFrame(&wire, protocol.Frame{Type: protocol.TypeMessage, Payload: payload}))
	}
	_, err := io.Copy(alice, &wire)
	require.NoError(t, err)
	for _, want := range []string{"one", "two"} {
		var message protocol.Message
		require.NoError(t, json.Unmarshal(readFrame(t, bob, protocol.TypeMessage).Payload, &message))
		assert.Equal(t, want, message.Text)
	}
}

func TestRejectedFrames(t *testing.T) {
	_, address := startServer(t, Config{})
	_ = auth(t, address, "taken")
	tests := []struct {
		name    string
		authed  bool
		typ     protocol.Type
		payload string
	}{
		{"message before auth", false, protocol.TypeMessage, `{"text":"hi"}`},
		{"empty name", false, protocol.TypeAuth, ""},
		{"duplicate name", false, protocol.TypeAuth, "taken"},
		{"long name", false, protocol.TypeAuth, strings.Repeat("x", MaxNameBytes+1)},
		{"invalid name UTF-8", false, protocol.TypeAuth, "\xff"},
		{"control in name", false, protocol.TypeAuth, "a\nb"},
		{"whitespace name", false, protocol.TypeAuth, " alice"},
		{"repeated auth", true, protocol.TypeAuth, "again"},
		{"invalid JSON", true, protocol.TypeMessage, "{"},
		{"invalid UTF-8", true, protocol.TypeMessage, "\xff"},
		{"empty text", true, protocol.TypeMessage, `{"text":""}`},
		{"spoof sender", true, protocol.TypeMessage, `{"text":"hi","from":"admin"}`},
		{"trailing JSON", true, protocol.TypeMessage, `{"text":"hi"}{}`},
		{"bad disconnect", true, protocol.TypeDisconnect, "bye"},
		{"client error frame", true, protocol.TypeError, "error"},
		{"expanded payload", true, protocol.TypeMessage, `{"text":"` + strings.Repeat("x", protocol.MaxPayloadSize-11) + `"}`},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var conn net.Conn
			if tt.authed {
				conn = auth(t, address, fmt.Sprintf("user%d", i))
			} else {
				conn = dial(t, address)
			}
			writeFrame(t, conn, tt.typ, tt.payload)
			readFrame(t, conn, protocol.TypeError)
			_, err := protocol.ReadFrame(conn)
			require.ErrorIsf(t, err, io.EOF, "expected close after Error")
		})
	}
}

func TestConcurrentBroadcastAndClient(t *testing.T) {
	_, address := startServer(t, Config{SendQueueSize: 256})
	const count, messages = 4, 30
	clients := make([]*client.Client, count)
	for i := range clients {
		c, err := client.Dial(context.Background(), address, fmt.Sprintf("user%d", i))
		require.NoError(t, err)
		clients[i] = c
		t.Cleanup(func() { _ = c.Close() })
	}
	results := make(chan error, count*2)
	for i, c := range clients {
		go func(c *client.Client) {
			for j := 0; j < messages; j++ {
				if err := c.SendMessage(strconv.Itoa(j)); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(c)
		go func(i int, c *client.Client) {
			next := make(map[string]int)
			for j := 0; j < (count-1)*messages; j++ {
				message, err := c.Receive()
				if err != nil {
					results <- err
					return
				}
				if message.From == fmt.Sprintf("user%d", i) || message.Text != strconv.Itoa(next[message.From]) {
					results <- fmt.Errorf("wrong sender or per-sender order: %+v", message)
					return
				}
				next[message.From]++
			}
			if len(next) != count-1 {
				results <- fmt.Errorf("expected %d senders, got %d", count-1, len(next))
				return
			}
			results <- nil
		}(i, c)
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for i := 0; i < count*2; i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-timer.C:
			t.Fatal("concurrent clients did not complete")
		}
	}
}

func TestClientPingDoesNotDisturbReceive(t *testing.T) {
	_, address := startServer(t, Config{})
	pinger, err := client.Dial(context.Background(), address, "pinger")
	require.NoError(t, err)
	t.Cleanup(func() { _ = pinger.Close() })
	sender := auth(t, address, "sender")
	require.NoError(t, pinger.Ping())
	writeFrame(t, sender, protocol.TypeMessage, `{"text":"after-ping"}`)
	message, err := pinger.Receive()
	require.NoError(t, err)
	assert.Equal(t, "sender", message.From)
	assert.Equal(t, "after-ping", message.Text)
}

func TestAuthTimeoutAndConnectionLimit(t *testing.T) {
	s, address := startServer(t, Config{AuthTimeout: 100 * time.Millisecond, MaxConnections: 1})
	idle := dial(t, address)
	waitFor(t, func() bool { return len(s.connectionSnapshot()) == 1 })
	excess := dial(t, address)
	_, err := protocol.ReadFrame(excess)
	require.Error(t, err, "connection limit was not enforced")
	var netErr net.Error
	assert.False(t, errors.As(err, &netErr) && netErr.Timeout(), "excess connection was not closed promptly")
	readFrame(t, idle, protocol.TypeError)
	_, err = protocol.ReadFrame(idle)
	require.ErrorIsf(t, err, io.EOF, "idle unauthenticated client was not closed")
}

func TestShutdownDrainsAcceptedBroadcasts(t *testing.T) {
	s, address := startServer(t, Config{SendQueueSize: 128})
	alice := auth(t, address, "alice")
	bob := auth(t, address, "bob")
	idle := dial(t, address)
	waitFor(t, func() bool { return len(s.connectionSnapshot()) == 3 })
	s.mu.RLock()
	sender := s.clients["alice"]
	s.mu.RUnlock()
	// Enqueue at the acceptance boundary deterministically: a successful
	// client TCP write alone does not prove the server accepted the message.
	const count = 100
	for i := 0; i < count; i++ {
		payload, _ := json.Marshal(protocol.Message{From: "alice", Text: strconv.Itoa(i)})
		s.broadcasts <- broadcast{sender: sender, frame: protocol.Frame{Type: protocol.TypeMessage, Payload: payload}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
	for i := 0; i < count; i++ {
		var message protocol.Message
		require.NoError(t, json.Unmarshal(readFrame(t, bob, protocol.TypeMessage).Payload, &message))
		require.Equalf(t, strconv.Itoa(i), message.Text, "lost or reordered queued message %d", i)
	}
	for _, conn := range []net.Conn{alice, bob, idle} {
		_, err := protocol.ReadFrame(conn)
		require.ErrorIsf(t, err, io.EOF, "connection not closed after drain")
	}
	assert.Zero(t, s.ClientCount(), "sessions leaked after shutdown")
	assert.NoError(t, s.Shutdown(ctx), "repeated shutdown")
}

func TestShutdownBeforeServe(t *testing.T) {
	s, _ := New(Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	require.ErrorIs(t, s.Serve(listener), ErrServerClosed, "Serve after shutdown")
}

func TestSessionQueueBoundedAndConcurrentSeal(t *testing.T) {
	c := &session{outbound: make(chan protocol.Frame, 1)}
	frame := protocol.Frame{Type: protocol.TypeDisconnect}
	assert.True(t, c.enqueue(frame), "first enqueue must succeed")
	assert.False(t, c.enqueue(frame), "queue capacity not enforced")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.enqueue(frame)
			c.seal()
		}()
	}
	wg.Wait()
	assert.False(t, c.enqueue(frame), "sealed queue accepted a frame")
	assert.Len(t, c.outbound, 1, "sealing discarded a queued frame")
}

// net.Pipe provides deterministic backpressure, independent of OS socket
// buffer sizes. The main integration tests above use actual TCP listeners.
type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{connections: make(chan net.Conn, 8), closed: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{} }

func (l *pipeListener) connect(t *testing.T, name string) net.Conn {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	l.connections <- serverConn
	t.Cleanup(func() { _ = clientConn.Close() })
	_ = clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	writeFrame(t, clientConn, protocol.TypeAuth, name)
	readFrame(t, clientConn, protocol.TypeAuth)
	return clientConn
}

func TestSlowRecipientDoesNotBlockBroadcast(t *testing.T) {
	listener := newPipeListener()
	s := startWithListener(t, Config{SendQueueSize: 1}, listener)
	alice := listener.connect(t, "alice")
	bob := listener.connect(t, "bob")
	slow := listener.connect(t, "slow")
	for i := 0; i < 6; i++ {
		writeFrame(t, alice, protocol.TypeMessage, fmt.Sprintf(`{"text":"%d"}`, i))
		var message protocol.Message
		require.NoError(t, json.Unmarshal(readFrame(t, bob, protocol.TypeMessage).Payload, &message))
		assert.Equal(t, strconv.Itoa(i), message.Text)
	}
	waitFor(t, func() bool { return s.ClientCount() == 2 })
	_, err := protocol.ReadFrame(slow)
	require.ErrorIsf(t, err, io.EOF, "slow recipient was not disconnected")
}

func TestShutdownDeadlineInterruptsBlockedWriter(t *testing.T) {
	listener := newPipeListener()
	s := startWithListener(t, Config{WriteTimeout: time.Minute}, listener)
	conn := listener.connect(t, "blocked")
	s.broadcasts <- broadcast{frame: protocol.Frame{Type: protocol.TypeMessage, Payload: []byte(`{"from":"x","text":"pending"}`)}}
	// Consume only the header; the writer must block on the payload.
	var header [protocol.HeaderSize]byte
	_, err := io.ReadFull(conn, header[:])
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.Shutdown(ctx), context.DeadlineExceeded, "expected forced shutdown")
	finalCtx, finalCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalCancel()
	require.NoError(t, s.Shutdown(finalCtx), "goroutines did not exit after force close")
	assert.Zero(t, len(s.connectionSnapshot()), "connections leaked after forced shutdown")
	assert.Zero(t, s.ClientCount())
}

func TestWriteTimeoutDisconnectsBlockedRecipient(t *testing.T) {
	listener := newPipeListener()
	s := startWithListener(t, Config{WriteTimeout: 50 * time.Millisecond}, listener)
	conn := listener.connect(t, "blocked")
	s.broadcasts <- broadcast{frame: protocol.Frame{Type: protocol.TypeMessage, Payload: []byte(`{"from":"x","text":"pending"}`)}}
	waitFor(t, func() bool { return s.ClientCount() == 0 })
	_, err := protocol.ReadFrame(conn)
	require.ErrorIsf(t, err, io.EOF, "blocked writer was not closed")
}

func TestConcurrentWritesFromOneClient(t *testing.T) {
	_, address := startServer(t, Config{SendQueueSize: 128})
	sender, err := client.Dial(context.Background(), address, "sender")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sender.Close() })
	receiver := auth(t, address, "receiver")
	const workers, messages = 4, 15
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			for j := 0; j < messages; j++ {
				if err := sender.SendMessage(fmt.Sprintf("%d:%d", worker, j)); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(i)
	}
	seen := make(map[string]bool)
	for i := 0; i < workers*messages; i++ {
		var message protocol.Message
		require.NoError(t, json.Unmarshal(readFrame(t, receiver, protocol.TypeMessage).Payload, &message))
		require.Equalf(t, "sender", message.From, "unexpected sender")
		require.Falsef(t, seen[message.Text], "repeated message %q", message.Text)
		seen[message.Text] = true
	}
	for i := 0; i < workers; i++ {
		require.NoError(t, <-results)
	}
	require.NoError(t, sender.Disconnect())
	require.NoError(t, sender.Close(), "repeated close")
}
