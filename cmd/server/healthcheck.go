package main

import (
	"fmt"
	"net"
	"os"

	"time"
)

// runHealthcheck backs the -healthcheck flag. When enabled it dials address
// once and terminates the process with a status that reflects reachability of
// the listening TCP port; when disabled it returns so normal serving continues.
func runHealthcheck(enabled bool, address string) {
	if !enabled {
		return
	}
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		os.Exit(1)
	}
	_ = conn.Close()
	os.Exit(0)
}
