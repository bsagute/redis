# Dockerfile
# ── Build Stage ────────────────────────────────────────────────────────────────
FROM golang:1.21-alpine AS builder

WORKDIR /app

# cache deps
COPY go.mod go.sum ./
RUN go mod download

# copy sources and build
COPY . .
RUN CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    go build -ldflags="-s -w" -o redis-demo .

# ── Runtime Stage ──────────────────────────────────────────────────────────────
FROM alpine:latest

# for TLS/certs if needed
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# copy built binary
COPY --from=builder /app/redis-demo .

# expose port if your app listens on one (e.g. 8080)
# EXPOSE 8080

# default: just run the demo
CMD ["./redis-demo"]
