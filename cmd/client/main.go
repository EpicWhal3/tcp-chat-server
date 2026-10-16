package main

import (
	"bufio"
	"context"
	"errors"

	"flag"
	"fmt"
	"io"

	"os"
	"os/signal"
	"syscall"

	"tcp-chat-server/internal/client"
	"tcp-chat-server/internal/protocol"
)

func main() {
	os.Exit(run())
}

func run() int {
	address := flag.String("addr", "127.0.0.1:9000", "TCP server address")
	name := flag.String("name", "", "unique client name (required)")
	flag.Parse()
	if *name == "" {
		fmt.Fprintln(os.Stderr, "provide -name")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := client.Dial(ctx, *address, *name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.Close()
	fmt.Fprintln(os.Stderr, "Connected. Type messages; /quit disconnects.")
	readErr := make(chan error, 1)
	go func() {
		for {
			message, err := c.Receive()
			if err != nil {
				readErr <- err
				return
			}
			fmt.Printf("%s: %s\n", message.From, message.Text)
		}
	}()
	lines := make(chan string)
	inputErr := make(chan error, 1)
	inputCtx, cancelInput := context.WithCancel(ctx)
	defer cancelInput()
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), protocol.MaxPayloadSize)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-inputCtx.Done():
				return
			}
		}
		inputErr <- scanner.Err()
	}()
	for {
		select {
		case <-ctx.Done():
			_ = c.Disconnect()
			return 0
		case err := <-readErr:
			if !errors.Is(err, io.EOF) {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			fmt.Fprintln(os.Stderr, "Server closed the connection.")
			return 0
		case line, ok := <-lines:
			if !ok {
				if err := <-inputErr; err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 1
				}
				_ = c.Disconnect()
				return 0
			}
			if line == "/quit" {
				_ = c.Disconnect()
				return 0
			}
			if line == "" {
				continue
			}
			if err := c.SendMessage(line); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
		}
	}
}
