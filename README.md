# tcp-chat-server

A small, production-minded **TCP chat server** written in Go. It speaks a **custom length-prefixed binary protocol**
directly over TCP — no HTTP, no
WebSockets — and ships with an interactive console client, a demo bot,
container images and a `docker compose` stack.

The project is deliberately dependency-light: the server and client use only
the Go standard library (`net`, `io`, `encoding/binary`, `sync`, `context`);
[`stretchr/testify`](https://github.com/stretchr/testify) is used exclusively in
tests.

---

## Highlights

- **Custom binary framing** — `[Type][PayloadLength][Payload]`, big-endian,
  decoded with `io.ReadFull` so partially received packets are handled safely.
- **Goroutine per connection** — each connection owns an independent read loop
  and write loop, both guarded by read/write deadlines.
- **Thread-safe registry** — active sessions live in a `map[string]*Client`
  protected by a `sync.RWMutex`.
- **Channel-based broadcast** — messages fan out through a buffered channel; the
  sender is excluded from its own broadcast.
- **Ping/Pong keepalive** — `0x05` frames let peers probe liveness.
- **Graceful shutdown** — `SIGINT`/`SIGTERM` trigger a bounded drain before the
  process exits.
- **Multi-stage Docker build** — a static `CGO_ENABLED=0` binary on a minimal
  `alpine` runtime running as a **non-root** user.
- **Unit + integration tests** — table-driven protocol tests and real-TCP
  integration tests, race-detector clean.

---

## Requirements

- **Go 1.23+** for local development.
- **Docker** + **Docker Compose** (v2) for the container workflow.
- A C compiler (`gcc`/`clang`) only if you want to run `go test -race` locally.

---

## Repository layout

```
tcp-chat-server/
├── cmd/
│   ├── server/        # TCP chat server entrypoint (+ flags, healthcheck)
│   ├── client/        # interactive console client
│   ├── bot/           # demo bot (!ping / !echo, periodic keepalive)
│   └── healthcheck/   # standalone TCP-port probe used by Docker healthcheck
├── internal/
│   ├── protocol/      # Frame type, ReadFrame/WriteFrame (wire codec)
│   ├── server/        # accept loop, read/write loops, hub, sessions
│   └── client/        # reusable client used by cmd/client and cmd/bot
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── go.mod
└── README.md
```

---

## Binary protocol

Every message is a **Frame** consisting of a fixed 3-byte header followed by an
optional payload. All multibyte integers are **big-endian**.

| Field           | Size    | Type           | Notes                        |
|-----------------|---------|----------------|------------------------------|
| `Type`          | 1 byte  | `uint8`        | Frame kind (see below)       |
| `PayloadLength` | 2 bytes | `uint16` (BE)  | Length of `Payload` in bytes |
| `Payload`       | N bytes | UTF-8 / opaque | `N == PayloadLength`         |

- Header size: **3 bytes**.
- Maximum payload: **65535 bytes** (`2^16 − 1`).
- An empty payload is legal (`PayloadLength == 0`).

Byte layout:

```
 bit   0        8        16       24                                
      +--------+--------+--------+--------------------------------+
      |  Type  |    PayloadLength        |         Payload         |
      +--------+--------+--------+--------------------------------+
         1 byte      2 bytes (big-endian)      N bytes
```

### Frame types

| Value | Name         | Direction       | Payload                                    |
|-------|--------------|-----------------|--------------------------------------------|
| 0x01  | `Auth`       | client → server | Nickname (UTF-8)                           |
| 0x02  | `Message`    | both            | JSON `{"from":"…","text":"…"}`             |
| 0x03  | `Disconnect` | both            | Optional human-readable reason             |
| 0x04  | `Error`      | server → client | Human-readable error text                  |
| 0x05  | `Ping`       | both            | Opaque token; the peer echoes it as a Pong |

A connection must send an `Auth` frame first; any other frame before
authentication is rejected with a `0x04 Error`. Message payloads are JSON so the
wire format stays language-agnostic while remaining trivial to extend.

---

## Architecture

```
        +----------------------------------------+
        |            cmd/client (CLI)            |
        +-------------------+--------------------+
                            |  TCP  (binary frames)
                            v
        +----------------------------------------+
        |             internal/server            |
        |                                        |
        |   Accept loop ──► per-connection:      |
        |                   ┌───────────────┐    |
        |                   │   readLoop    │    |  decode → authenticate
        |                   │  (decoder)    │    |
        |                   └──────┬────────┘    |
        |                          │ enqueue     |
        |                   ┌──────v────────┐    |
        |                   │  Hub / Router │    |  fan-out via channels
        |                   │ map+sync.RWMutex│  |
        |                   └──────┬────────┘    |
        |                          │             |
        |                   ┌──────v────────┐    |
        |                   │  writeLoop    │    |  encode → TCP
        |                   │  (encoder)    │    |
        |                   └───────────────┘    |
        +----------------------------------------+
```

**Data path:** `Client → TCP → Decoder → Hub/Router → Encoder → TCP → Client`.

- The **accept loop** hands each new `net.Conn` to its own goroutine.
- The **read loop** decodes frames, enforces the authentication handshake, and
  publishes valid messages to the hub.
- The **hub** looks up every registered session (under `RWMutex`) and pushes the
  encoded broadcast onto each session's outbound channel, excluding the sender.
- The **write loop** drains that channel and writes frames to the socket,
  applying a write deadline so a stalled peer can never block the hub.
- Shutdown closes the listener, signals readers, drains pending writes within the
  configured timeout, then returns.

---

## Quick start with Docker

Build and run the server plus the demo bot in the background:

```bash
docker compose up -d --build
docker compose logs -f server
```

Attach an interactive client on the same network:

```bash
docker compose --profile client run --rm client
```

Tear everything down (SIGTERM is delivered first, then the 15s grace period
lets queued broadcasts drain):

```bash
docker compose down
```

The `server` service exposes **`0.0.0.0:9000`** and declares a TCP-port
healthcheck that runs the bundled `/app/healthcheck` probe against
`127.0.0.1:9000`.

---

## Local development

The [`Makefile`](Makefile) wraps the common commands:

| Target              | Description                                                       |
|---------------------|-------------------------------------------------------------------|
| `make build`        | Compile `server`, `client`, `bot` and `healthcheck` into `./bin`. |
| `make run-server`   | Run the server on `$(ADDR)` (default `127.0.0.1:9000`).           |
| `make run-client`   | Run the console client as `$(NAME)`.                              |
| `make run-bot`      | Run the demo bot as `$(BOTNAME)`.                                 |
| `make test`         | Run the unit and integration suites.                              |
| `make race`         | Run the suites under the data-race detector.                      |
| `make lint`         | `go fmt` + `go vet`.                                              |
| `make docker-build` | Build the multi-stage image.                                      |
| `make docker-up`    | `docker compose up -d --build`.                                   |
| `make docker-down`  | `docker compose down`.                                            |

Positional variables can be overridden, e.g.:

```bash
make run-server ADDR=0.0.0.0:9000
make run-client ADDR=0.0.0.0:9000 NAME=bob
```

Without `make`, use the equivalent `go run` commands:

```bash
go run ./cmd/server -addr 127.0.0.1:9000
go run ./cmd/client -addr 127.0.0.1:9000 -name alice
go run ./cmd/bot    -addr 127.0.0.1:9000 -name demo-bot
```

### Server flags

| Flag                | Default          | Purpose                                         |
|---------------------|------------------|-------------------------------------------------|
| `-addr`             | `127.0.0.1:9000` | TCP listen address                              |
| `-healthcheck`      | `false`          | Dial `-addr` once, exit `0` if the port is open |
| `-shutdown-timeout` | `15s`            | Maximum time to drain pending writes            |
| `-max-connections`  | `1024`           | Maximum concurrent TCP connections              |
| `-send-queue`       | `128`            | Outbound frames buffered per connection         |
| `-broadcast-queue`  | `256`            | Pending broadcasts buffered on the hub          |
| `-auth-timeout`     | `10s`            | Time allowed for the initial `Auth` frame       |
| `-idle-timeout`     | `5m`             | Time allowed to read each subsequent frame      |
| `-write-timeout`    | `5s`             | Time allowed to write each frame                |

---

## Tests

The suite covers the wire codec (round-trips, coalesced/fragmented TCP reads,
oversized and truncated frames) and the server (broadcast semantics, concurrent
clients, shutdown draining, bounded queues).

```bash
go test ./...
```

Verbose, with the data-race detector:

```bash
go test -v -race ./...
```

> **Note:** `-race` requires cgo. On a machine with a C compiler, enable it with
> `CGO_ENABLED=1` (e.g. `CGO_ENABLED=1 go test -race ./...`). If no C compiler
> is available the race target is skipped; the tests remain deterministic and
> race-free by construction.
