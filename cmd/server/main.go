package main

import (
	"context"
	"errors"
	"log/slog"

	"net"
	"os"
	"os/signal"

	"syscall"

	"tcp-chat-server/internal/server"
)

func main() {
	os.Exit(serve())
}

// serve owns the full server lifecycle: flag parsing, healthcheck short-circuit,
// listen, signal-driven graceful shutdown, and draining.
func serve() int {
	opts := parseFlags()
	runHealthcheck(opts.healthcheck, opts.address)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	opts.config.Logger = logger
	if opts.shutdownTimeout <= 0 {
		logger.Error("shutdown-timeout must be positive")
		return 1
	}
	srv, err := server.New(opts.config)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return 1
	}
	listener, err := net.Listen("tcp", opts.address)
	if err != nil {
		logger.Error("listen failed", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()
	logger.Info("TCP chat server listening", "address", listener.Addr().String())
	var result error
	serveReturned := false
	select {
	case <-ctx.Done():
	case result = <-serveErr:
		serveReturned = true
	}
	// Restore default signal handling so a second interrupt can terminate.
	stop()
	logger.Info("draining server")
	drainCtx, cancel := context.WithTimeout(context.Background(), opts.shutdownTimeout)
	defer cancel()
	err = srv.Shutdown(drainCtx)
	// Covers a signal arriving before Serve takes ownership of the listener.
	_ = listener.Close()
	if !serveReturned {
		result = <-serveErr
	}
	if err != nil {
		logger.Error("shutdown forced; pending messages may be lost", "error", err)
		return 1
	}
	if result != nil && !errors.Is(result, server.ErrServerClosed) {
		logger.Error("server stopped unexpectedly", "error", result)
		return 1
	}
	logger.Info("server stopped")
	return 0
}
