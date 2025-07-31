# Makefile
# ────────────────────────────────────────────────────────────────────────────────

IMAGE_NAME := redis-go-app
BINARY     := redis-demo

.PHONY: all build docker-build up down logs clean

all: build

# 1) build the Go binary locally
build:
	go mod tidy
	go build -o $(BINARY) .

# 2) build the Docker image
docker-build:
	docker build -t $(IMAGE_NAME):latest .

# 3) bring up Redis + your app (rebuild image first)
up: docker-build
	docker-compose up -d

# 4) tear down
down:
	docker-compose down

# 5) follow logs
logs:
	docker-compose logs -f

# 6) cleanup local binary
clean:
	go clean
	rm -f $(BINARY)
