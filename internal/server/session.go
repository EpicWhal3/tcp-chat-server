package server

import (
	"net"
	"sync"

	"tcp-chat-server/internal/protocol"
)

type session struct {
	conn     net.Conn
	name     string // Set once, under Server.mu, before registration.
	outbound chan protocol.Frame
	mu       sync.Mutex
	sealed   bool
}

// enqueue never blocks on a slow writer. mu makes sending and sealing atomic
// with respect to each other, preventing send-on-closed-channel panics.
func (c *session) enqueue(frame protocol.Frame) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed {
		return false
	}
	select {
	case c.outbound <- frame:
		return true
	default:
		return false
	}
}

func (c *session) seal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sealed {
		c.sealed = true
		close(c.outbound)
	}
}
