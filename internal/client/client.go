// Package client provides a small concurrent client for the binary chat protocol.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
	"unicode/utf8"

	"tcp-chat-server/internal/protocol"
)

const operationTimeout = 5 * time.Second

// Client serializes full frame writes and reads independently. Close interrupts
// pending I/O. A failed read or write should be followed by Close.
type Client struct {
	conn      net.Conn
	readMu    sync.Mutex
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Dial connects and waits for the server's Auth acknowledgement. The context
// controls connection and authentication only; Close controls the session.
func Dial(ctx context.Context, address, name string) (*Client, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	fail := func(err error) (*Client, error) {
		_ = conn.Close()
		return nil, err
	}
	deadline := time.Now().Add(operationTimeout)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fail(err)
	}
	if err := protocol.WriteFrame(conn, protocol.Frame{Type: protocol.TypeAuth, Payload: []byte(name)}); err != nil {
		return fail(err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return fail(err)
	}
	if frame.Type == protocol.TypeError {
		return fail(fmt.Errorf("authentication: %s", frame.Payload))
	}
	if frame.Type != protocol.TypeAuth || string(frame.Payload) != name {
		return fail(errors.New("invalid authentication acknowledgement"))
	}
	if !stop() {
		return fail(ctx.Err())
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return fail(err)
	}
	return &Client{conn: conn}, nil
}

// SendMessage completes a socket write; it is not a delivery acknowledgement.
func (c *Client) SendMessage(text string) error {
	if text == "" || !utf8.ValidString(text) {
		return errors.New("message text must be nonempty UTF-8")
	}
	payload, err := json.Marshal(protocol.Message{Text: text})
	if err != nil {
		return err
	}
	return c.write(protocol.Frame{Type: protocol.TypeMessage, Payload: payload})
}

// Ping sends a keepalive Ping frame. The matching Pong is consumed by Receive.
func (c *Client) Ping() error {
	return c.write(protocol.Frame{Type: protocol.TypePing})
}

// Receive blocks for the next broadcast. Ping/Pong keepalive frames are
// consumed transparently. Close unblocks it. Callers should finish the session
// on any error, including a server Error frame.
func (c *Client) Receive() (protocol.Message, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		frame, err := protocol.ReadFrame(c.conn)
		if err != nil {
			return protocol.Message{}, err
		}
		switch frame.Type {
		case protocol.TypePing:
			continue
		case protocol.TypeMessage:
			var message protocol.Message
			if !utf8.Valid(frame.Payload) {
				return message, errors.New("invalid message UTF-8")
			}
			if err := json.Unmarshal(frame.Payload, &message); err != nil {
				return message, err
			}
			if message.From == "" || message.Text == "" {
				return message, errors.New("invalid broadcast message")
			}
			return message, nil
		case protocol.TypeError:
			return protocol.Message{}, fmt.Errorf("server: %s", frame.Payload)
		case protocol.TypeDisconnect:
			return protocol.Message{}, io.EOF
		default:
			return protocol.Message{}, errors.New("unexpected frame after authentication")
		}
	}
}

func (c *Client) write(frame protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(operationTimeout)); err != nil {
		return err
	}
	return protocol.WriteFrame(c.conn, frame)
}

// Disconnect sends a protocol Disconnect and closes the connection.
func (c *Client) Disconnect() error {
	err := c.write(protocol.Frame{Type: protocol.TypeDisconnect})
	closeErr := c.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.conn.Close() })
	return c.closeErr
}
