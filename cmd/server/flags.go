package main

import (
	"flag"
	"time"

	"tcp-chat-server/internal/server"
)

// options holds the fully parsed command-line configuration.
type options struct {
	address         string
	healthcheck     bool
	shutdownTimeout time.Duration
	config          server.Config
}

// parseFlags registers and parses every command-line flag.
func parseFlags() options {
	var opts options
	flag.StringVar(&opts.address, "addr", "127.0.0.1:9000", "TCP listen address")
	flag.BoolVar(&opts.healthcheck, "healthcheck", false, "dial -addr once, exit 0 if the TCP port is open")
	flag.DurationVar(&opts.shutdownTimeout, "shutdown-timeout", 15*time.Second, "maximum time to drain pending writes")
	flag.IntVar(&opts.config.MaxConnections, "max-connections", 1024, "maximum concurrent TCP connections")
	flag.IntVar(&opts.config.SendQueueSize, "send-queue", 128, "outbound frames per connection")
	flag.IntVar(&opts.config.BroadcastQueueSize, "broadcast-queue", 256, "pending broadcasts")
	flag.DurationVar(&opts.config.AuthTimeout, "auth-timeout", 10*time.Second, "time allowed for an Auth frame")
	flag.DurationVar(&opts.config.IdleTimeout, "idle-timeout", 5*time.Minute, "time allowed to read each subsequent frame")
	flag.DurationVar(&opts.config.WriteTimeout, "write-timeout", 5*time.Second, "time allowed to write each frame")
	flag.Parse()
	return opts
}
