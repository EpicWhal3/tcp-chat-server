// Package protocol implements the chat server's length-prefixed wire format.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Type byte

const (
	TypeAuth       Type = 0x01
	TypeMessage    Type = 0x02
	TypeDisconnect Type = 0x03
	TypeError      Type = 0x04
	TypePing       Type = 0x05
	HeaderSize          = 3
	MaxPayloadSize      = 1<<16 - 1
)

var (
	ErrPayloadTooLarge = errors.New("payload exceeds 65535 bytes")
	ErrUnknownType     = errors.New("unknown frame type")
)

// String renders the frame type for logs and error messages.
func (t Type) String() string {
	switch t {
	case TypeAuth:
		return "Auth"
	case TypeMessage:
		return "Message"
	case TypeDisconnect:
		return "Disconnect"
	case TypeError:
		return "Error"
	case TypePing:
		return "Ping"
	default:
		return fmt.Sprintf("Type(0x%02x)", byte(t))
	}
}

// Frame owns Payload after reading. Callers must not mutate it during writing.
type Frame struct {
	Type    Type
	Payload []byte
}

// Message is the JSON body of a chat message. Clients omit From; the server
// supplies it from the authenticated session when forwarding the message.
type Message struct {
	From string `json:"from,omitempty"`
	Text string `json:"text"`
}

func validType(t Type) bool {
	return t >= TypeAuth && t <= TypePing
}

// ReadFrame consumes exactly one frame, regardless of TCP read boundaries.
// EOF before any header byte means a clean end of stream. Truncated headers or
// payloads return io.ErrUnexpectedEOF. Unknown types make the stream unusable.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	t := Type(header[0])
	if !validType(t) {
		return Frame{}, fmt.Errorf("%w: 0x%02x", ErrUnknownType, header[0])
	}
	payload := make([]byte, int(binary.BigEndian.Uint16(header[1:])))
	if len(payload) > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return Frame{}, err
		}
	}
	return Frame{Type: t, Payload: payload}, nil
}

// WriteFrame writes one complete frame, handling short writes. Concurrent
// callers sharing a writer must serialize entire calls, not individual writes.
func WriteFrame(w io.Writer, frame Frame) error {
	if !validType(frame.Type) {
		return fmt.Errorf("%w: 0x%02x", ErrUnknownType, frame.Type)
	}
	if len(frame.Payload) > MaxPayloadSize {
		return ErrPayloadTooLarge
	}
	var header [HeaderSize]byte
	header[0] = byte(frame.Type)
	binary.BigEndian.PutUint16(header[1:], uint16(len(frame.Payload)))
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, frame.Payload)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
