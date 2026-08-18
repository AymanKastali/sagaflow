.PHONY: up down generate lint breaking schemas-register run-inventory test test-integration

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
	@# gofmt -l . already recurses into contracts/ from the root, and reports
	@# formatting issues without failing, so turn its output into an error; a
	@# file that fails to parse makes gofmt exit non-zero with empty stdout, so
	@# that exit code has to fail the check too, not just a non-empty listing.
	@out=$$(gofmt -l . 2>&1); status=$$?; \
	if [ $$status -ne 0 ] || [ -n "$$out" ]; then echo "gofmt: not formatted:"; echo "$$out"; exit 1; fi
	go vet ./...
	cd contracts && go vet ./...
	@if [ -f buf.yaml ]; then go tool buf lint; \
	else echo "lint: skipping buf lint -- no buf.yaml yet (arrives in Phase 3a)"; fi

breaking:
	@if [ -f buf.yaml ]; then go tool buf breaking --against '.git#branch=main'; \
	else echo "breaking: skipping -- no buf.yaml yet (arrives in Phase 3a)"; fi

schemas-register:
	go run ./cmd/schemactl -registry http://localhost:8080/apis/ccompat/v7

# Needs `make up` and `make schemas-register` first: the service resolves every
# schema id it will use at startup and refuses to run without them.
run-inventory:
	go run ./cmd/inventory

# The contracts module has no container tests, so it needs no -short variant and
# no timeout bump -- but it is a separate module, so the root ./... never reaches it.
test:
	go test -race -short ./...
	cd contracts && go test -race ./...

test-integration:
	go test -race -timeout 15m ./...
	cd contracts && go test -race ./...
