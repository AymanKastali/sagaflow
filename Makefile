.PHONY: up down generate lint breaking schemas-register test test-integration

up:
	docker compose up -d --wait

down:
	docker compose down -v

generate:
	go tool buf generate

lint:
	go tool buf lint
	go vet ./...

breaking:
	go tool buf breaking --against '.git#branch=main'

schemas-register:
	go run ./cmd/schemactl -registry http://localhost:8080/apis/ccompat/v7

test:
	go test -race -short ./...

test-integration:
	go test -race -timeout 15m ./...
