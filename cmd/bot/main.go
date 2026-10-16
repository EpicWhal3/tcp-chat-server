// Command bot is a small demonstration client. It authenticates, greets the
// room, answers a couple of commands, keeps the connection alive with periodic
// pings, and disconnects cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"

	"fmt"
	"io"
	"os"

	"os/signal"
	"strings"
	"syscall"

	"time"

	"tcp-chat-server/internal/client"
)

func main() {
	os.Exit(run())
}

func run() int {
	address := flag.String("addr", "127.0.0.1:9000", "TCP server address")
	name := flag.String("name", "demo-bot", "bot nickname")
	greeting := flag.String("greeting", "demo-bot online. Try: !echo <text> or !ping", "message sent after joining; empty disables it")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.Dial(ctx, *address, *name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	if *greeting != "" {
		if err := c.SendMessage(*greeting); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	messages := make(chan error, 1)
	go func() {
		for {
			message, err := c.Receive()
			if err != nil {
				messages <- err
				return
			}
			fmt.Fprintf(os.Stderr, "%s: %s\n", message.From, message.Text)
			reply := respond(message.Text)
			if reply == "" {
				continue
			}
			if err := c.SendMessage(reply); err != nil {
				messages <- err
				return
			}
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = c.Disconnect()
			return 0
		case err := <-messages:
			if errors.Is(err, io.EOF) {
				fmt.Fprintln(os.Stderr, "server closed the connection")
				return 0
			}
			fmt.Fprintln(os.Stderr, err)
			return 1
		case <-ticker.C:
			if err := c.Ping(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
		}
	}
}

// respond returns the bot reply for a received message, or "" to stay silent.
func respond(text string) string {
	switch {
	case text == "!ping":
		return "pong"
	case strings.HasPrefix(text, "!echo "):
		return strings.TrimPrefix(text, "!echo ")
	default:
		return ""
	}
}
