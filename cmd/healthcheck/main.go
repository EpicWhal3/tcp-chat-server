// Command healthcheck dials a TCP address once and exits non-zero when the
// port is unreachable. It backs the docker-compose healthcheck, which verifies
// that the chat server is accepting connections.
package main

import (
	"flag"
	"fmt"
	"net"

	"os"
	"time"
)

func main() {
	address := flag.String("addr", "127.0.0.1:9000", "TCP address to probe")
	timeout := flag.Duration("timeout", 3*time.Second, "dial timeout")
	flag.Parse()

	conn, err := net.DialTimeout("tcp", *address, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		os.Exit(1)
	}
	_ = conn.Close()
}
