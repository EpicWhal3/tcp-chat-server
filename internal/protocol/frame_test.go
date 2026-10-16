package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrameRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{"auth", Frame{TypeAuth, []byte("alice")}},
		{"message", Frame{TypeMessage, []byte(`{"text":"hi"}`)}},
		{"message UTF-8", Frame{TypeMessage, []byte(`{"text":"Привет 🌍"}`)}},
		{"ping payload", Frame{TypePing, []byte("keepalive")}},
		{"ping empty", Frame{TypePing, nil}},
		{"disconnect empty", Frame{TypeDisconnect, nil}},
		{"error", Frame{TypeError, []byte("bad request")}},
		{"maximum payload", Frame{TypeMessage, bytes.Repeat([]byte{'x'}, MaxPayloadSize)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wire bytes.Buffer
			require.NoError(t, WriteFrame(&wire, tt.frame))
			require.Equal(t, HeaderSize+len(tt.frame.Payload), wire.Len(), "encoded length")

			// OneByteReader forces every read to return a single byte, proving
			// the decoder survives arbitrarily fragmented TCP segments.
			got, err := ReadFrame(iotest.OneByteReader(&wire))
			require.NoError(t, err)
			require.Equal(t, tt.frame.Type, got.Type, "frame type")
			require.Truef(t, bytes.Equal(tt.frame.Payload, got.Payload),
				"payload mismatch: want %q, got %q", tt.frame.Payload, got.Payload)
		})
	}
}

func TestFrameWireFormat(t *testing.T) {
	tests := []struct {
		name   string
		frame  Frame
		expect []byte
	}{
		{"multi-byte length", Frame{TypeMessage, bytes.Repeat([]byte{'x'}, 258)}, append([]byte{0x02, 0x01, 0x02}, bytes.Repeat([]byte{'x'}, 258)...)},
		{"ping empty", Frame{TypePing, nil}, []byte{0x05, 0x00, 0x00}},
		{"auth single byte", Frame{TypeAuth, []byte("a")}, []byte{0x01, 0x00, 0x01, 'a'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wire bytes.Buffer
			require.NoError(t, WriteFrame(&wire, tt.frame))
			require.Equal(t, tt.expect, wire.Bytes(), "wire bytes must be big-endian")
		})
	}
}

func TestReadFrameErrors(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
		want error
	}{
		{"clean EOF", nil, io.EOF},
		{"one header byte", []byte{1}, io.ErrUnexpectedEOF},
		{"two header bytes", []byte{1, 0}, io.ErrUnexpectedEOF},
		{"missing payload", []byte{1, 0, 2}, io.ErrUnexpectedEOF},
		{"partial payload", []byte{1, 0, 2, 'a'}, io.ErrUnexpectedEOF},
		{"unknown type below range", []byte{0x00, 0, 0}, ErrUnknownType},
		{"unknown type above range", []byte{0xFF, 0, 0}, ErrUnknownType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(tt.wire))
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestWriteFrameValidation(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
		want  error
	}{
		{"overflow", Frame{TypeMessage, make([]byte, MaxPayloadSize+1)}, ErrPayloadTooLarge},
		{"unknown type", Frame{0, nil}, ErrUnknownType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wire bytes.Buffer
			require.ErrorIs(t, WriteFrame(&wire, tt.frame), tt.want)
			require.Zero(t, wire.Len(), "invalid frame must not emit partial bytes")
		})
	}
}

type shortWriter struct{ bytes.Buffer }

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return w.Buffer.Write(p)
}

type writerFunc func([]byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }

func TestWriteFrameShortWrites(t *testing.T) {
	var wire shortWriter
	require.NoError(t, WriteFrame(&wire, Frame{TypeAuth, []byte("abc")}))
	require.Equal(t, []byte{1, 0, 3, 'a', 'b', 'c'}, wire.Bytes(), "short writes lost bytes")

	sentinel := errors.New("write failed")
	tests := []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{"no progress", writerFunc(func([]byte) (int, error) { return 0, nil }), io.ErrShortWrite},
		{"invalid count", writerFunc(func(p []byte) (int, error) { return len(p) + 1, nil }), io.ErrShortWrite},
		{"I/O failure", writerFunc(func([]byte) (int, error) { return 0, sentinel }), sentinel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, WriteFrame(tt.writer, Frame{TypeAuth, []byte("abc")}), tt.want)
		})
	}
}

func TestReadCoalescedFrames(t *testing.T) {
	var wire bytes.Buffer
	for _, frame := range []Frame{{TypeAuth, []byte("a")}, {TypePing, nil}, {TypeDisconnect, nil}} {
		require.NoError(t, WriteFrame(&wire, frame))
	}
	for _, want := range []Type{TypeAuth, TypePing, TypeDisconnect} {
		got, err := ReadFrame(&wire)
		require.NoError(t, err)
		assert.Equal(t, want, got.Type)
	}
	_, err := ReadFrame(&wire)
	require.ErrorIs(t, err, io.EOF)
}

func TestTypeString(t *testing.T) {
	tests := []struct {
		name string
		typ  Type
		want string
	}{
		{"auth", TypeAuth, "Auth"},
		{"message", TypeMessage, "Message"},
		{"disconnect", TypeDisconnect, "Disconnect"},
		{"error", TypeError, "Error"},
		{"ping", TypePing, "Ping"},
		{"unknown", Type(0x42), "Type(0x42)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.typ.String())
		})
	}
}

func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{1, 0, 1, 'a'})
	f.Add([]byte{3, 0, 0})
	f.Add([]byte{5, 0, 0})
	f.Add([]byte{2, 0xFF, 0xFF})
	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := ReadFrame(bytes.NewReader(data))
		if err != nil {
			return
		}
		var wire bytes.Buffer
		require.NoError(t, WriteFrame(&wire, frame))
		require.True(t, bytes.Equal(wire.Bytes(), data[:HeaderSize+len(frame.Payload)]),
			"decoder and encoder disagree on wire format")
	})
}
