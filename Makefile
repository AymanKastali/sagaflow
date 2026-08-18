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
	@# gofmt reports, it does not fail, so turn any output into an error.
	@out=$$(gofmt -l . ; cd contracts && gofmt -l . | sed 's|^|contracts/|'); \
	if [ -n "$$out" ]; then echo "gofmt: not formatted:"; echo "$$out"; exit 1; fi
	go vet ./...
	cd contracts && go vet ./...
	@if [ -f buf.yaml ]; then go tool buf lint; \
	else echo "lint: skipping buf lint -- no buf.yaml yet (arrives in Phase 3a)"; fi

breaking:
	@if [ -f buf.yaml ]; then go tool buf breaking --against '.git#branch=main'; \
	else echo "breaking: skipping -- no buf.yaml yet (arrives in Phase 3a)"; fi

schemas-register:
	go run ./cmd/schemactl -registry http://localhost:8080/apis/ccompat/v7

# The contracts module has no container tests, so it needs no -short variant and
# no timeout bump -- but it is a separate module, so the root ./... never reaches it.
test:
	go test -race -short ./...
	cd contracts && go test -race ./...

test-integration:
	go test -race -timeout 15m ./...
	cd contracts && go test -race ./...
