.PHONY: build test test-unit test-integration coverage lint docker-server docker-worker docker-dispatcher run-server run-worker run-dispatcher tidy

build:
	go build ./...

test: test-unit

test-unit:
	go test ./internal/... ./cmd/...

# Requires Docker: real Postgres and RabbitMQ.
test-integration:
	go test -tags=integration ./tests/integration/...

coverage:
	go test ./... -coverprofile=coverage.out -coverpkg=./...
	go tool cover -func=coverage.out | tail -1

lint:
	go vet ./...

tidy:
	go mod tidy

docker-server:
	docker build --build-arg TARGET=server -t fiapx-saga-orchestrator:server .

docker-worker:
	docker build --build-arg TARGET=worker -t fiapx-saga-orchestrator:worker .

docker-dispatcher:
	docker build --build-arg TARGET=outbox-dispatcher -t fiapx-saga-orchestrator:dispatcher .

run-server:
	go run ./cmd/server

run-worker:
	go run ./cmd/worker

run-dispatcher:
	go run ./cmd/outbox-dispatcher
