# tcp-chat-server — a lightweight, concurrent TCP chat server that speaks a
# custom length-prefixed binary protocol. These targets wrap the everyday local
# and container workflows described in README.md.

GO      ?= go
IMAGE   ?= tcp-chat-server:latest
ADDR    ?= 127.0.0.1:9000
NAME    ?= alice
BOTNAME ?= demo-bot
BINDIR  ?= bin

# Windows executables need an explicit .exe suffix; Unix-like systems do not.
ifeq ($(OS),Windows_NT)
BIN_EXT := .exe
else
BIN_EXT :=
endif

.DEFAULT_GOAL := build

.PHONY: build run-server run-client run-bot test race vet fmt lint \
        docker-build docker-up docker-down clean

# build: compile the server, client, bot and healthcheck binaries into ./bin.
build:
	$(GO) build -trimpath -o $(BINDIR)/server$(BIN_EXT)      ./cmd/server
	$(GO) build -trimpath -o $(BINDIR)/client$(BIN_EXT)      ./cmd/client
	$(GO) build -trimpath -o $(BINDIR)/bot$(BIN_EXT)         ./cmd/bot
	$(GO) build -trimpath -o $(BINDIR)/healthcheck$(BIN_EXT) ./cmd/healthcheck

# run-server: start the TCP chat server on $(ADDR).
run-server:
	$(GO) run ./cmd/server -addr $(ADDR)

# run-client: attach the interactive console client as $(NAME).
run-client:
	$(GO) run ./cmd/client -addr $(ADDR) -name $(NAME)

# run-bot: start the demo bot as $(BOTNAME).
run-bot:
	$(GO) run ./cmd/bot -addr $(ADDR) -name $(BOTNAME)

# test: run the unit and integration suites.
test:
	$(GO) test -timeout 120s ./...

# race: run the suite under the data-race detector (needs cgo + a C compiler).
race:
	$(GO) test -race -timeout 180s ./...

# vet: run the standard static analysis pass.
vet:
	$(GO) vet ./...

# fmt: format every package.
fmt:
	$(GO) fmt ./...

# lint: formatting plus vet — the same checks a CI pipeline runs.
lint: fmt vet

# docker-build: build the multi-stage image.
docker-build:
	docker build -t $(IMAGE) .

# docker-up: build and start the server and demo bot in the background.
docker-up:
	docker compose up -d --build

# docker-down: stop and remove the compose stack.
docker-down:
	docker compose down

# clean: remove locally built binaries.
clean:
	$(GO) clean
	$(RM) -r $(BINDIR)
