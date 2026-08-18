.PHONY: up down generate lint breaking schemas-register test test-integration

up:
	docker compose up -d --wait

down:
	docker compose down -v

generate:
	go tool buf generate

# go vet runs first and always. buf lint is skipped, loudly, until contracts
# exist (Phase 3a) -- buf fails outright on a module with no .proto files, and a
# lint target that cannot be run is a lint target nobody runs.
lint:
	go vet ./...
	@if [ -f buf.yaml ]; then go tool buf lint; \
	else echo "lint: skipping buf lint -- no buf.yaml yet (arrives in Phase 3a)"; fi

breaking:
	@if [ -f buf.yaml ]; then go tool buf breaking --against '.git#branch=main'; \
	else echo "breaking: skipping -- no buf.yaml yet (arrives in Phase 3a)"; fi

schemas-register:
	go run ./cmd/schemactl -registry http://localhost:8080/apis/ccompat/v7

test:
	go test -race -short ./...

test-integration:
	go test -race -timeout 15m ./...
