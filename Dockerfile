# syntax=docker/dockerfile:1

# ---- builder: compile static binaries ----
FROM golang:1.23-alpine AS builder

WORKDIR /src

# Cache the module graph before copying source for faster incremental builds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 yields statically linked binaries that run on scratch/alpine.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server      ./cmd/server \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/client      ./cmd/client \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bot         ./cmd/bot \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

# ---- runtime: minimal image, non-root user ----
FROM alpine:latest

# Unprivileged account; the containers never need root.
RUN addgroup -S app && adduser -S -G app -h /app app

WORKDIR /app
COPY --from=builder /out/server /out/client /out/bot /out/healthcheck /app/

USER app

EXPOSE 9000
ENTRYPOINT ["/app/server"]
CMD ["-addr", "0.0.0.0:9000"]
