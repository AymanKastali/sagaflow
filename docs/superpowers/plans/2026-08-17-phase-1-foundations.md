# SagaFlow Phase 1 — Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the module, the pinned infrastructure and the Postgres and Kafka test harnesses, so every later phase has a real database and a real broker to test against.

**Architecture:** One Go module at the repository root with tools pinned through `go.mod`. Compose runs the four service databases plus Kafka, Apicurio and Jaeger at exact versions. Two helper packages — `pgtest` and `kafkatest` — start real containers for integration tests, because every guarantee in this system is a property of Postgres or Kafka and cannot be proven against a fake.

**Tech Stack:** Go 1.26.6, Postgres 18.6 (pgx v5.10.0, tern v2.4.2), Kafka 4.3.1 (franz-go v1.21.6), testcontainers-go v0.44.0.

**Spec:** [docs/superpowers/specs/2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) — §5 (infrastructure), §13 phase 1

**Plan sequence:** this is plan 1 of 6. See [README.md](README.md). Nothing depends on an earlier plan; every later plan depends on this one.

**Deliverable that ends this phase:** `make up` brings the stack to healthy, and `go test ./internal/platform/...` starts a real Postgres 18.6 and a real Kafka 4.3.1 and round-trips against both.

## Global Constraints

Copied verbatim from spec §5 and §3. Every task's requirements implicitly include this section.

- **Go 1.26.6.** `go.mod` declares `go 1.26.6`; with `GOTOOLCHAIN=auto` the go command fetches that toolchain itself, so the installed go may be older and no machine-level upgrade is required.
- **Module path:** `github.com/kptac/sagaflow`. One module at the repository root.
- **Pinned images, never `latest`:** `apache/kafka:4.3.1`, `postgres:18.6`, `apicurio/apicurio-registry:3.3.1`, `cr.jaegertracing.io/jaegertracing/jaeger:2.20.0`.
- **Pinned Go dependencies** (spec §5): franz-go v1.21.6, `franz-go/pkg/sr` v1.8.0, `franz-go/pkg/kadm` v1.18.0, pgx/v5 v5.10.0, tern/v2 v2.4.2, google/uuid v1.6.0, protobuf v1.36.12, otel + otel/sdk v1.45.0, testcontainers-go v0.44.0. **Add no dependency not listed in §5.**
- **The OTel modules move as a set.** `otel`, `otel/sdk` and the trace exporter are released together; mixing versions fails at runtime, not build time.
- **Never `testcontainers-go/modules/kafka`** (spec D16). It only runs Confluent images. Kafka test containers come from `platform/kafkatest`.
- **One transaction writes exactly one stream** (spec §7.2), plus its outbox rows and its inbox row. Never two streams.
- **`global_seq` is diagnostic only** (spec §6.4). No component may read events by a monotonic local cursor.
- **`expires_at` and `fire_at` are application-supplied, never `DEFAULT now()`** (spec §10.5). Only diagnostic columns such as `recorded_at` use the database clock.
- **The outbox is at-least-once, never exactly-once** (spec §10.1). Exactly-once application comes from the inbox.
- **Services never auto-register schemas** (spec D14). Registration is an explicit `make` target.
- **Postgres error code `23505`** is a unique violation; it is a control-flow signal in this codebase, not a crash.

### Two deliberate refinements to the spec

Both are noted here so a reviewer does not read them as drift:

1. **`Append` takes a `context.Context` first parameter.** Spec §6.2 writes `Append(tx pgx.Tx, streamID string, expectedVersion int, evts []Event) error`. Every pgx call requires a context, so the real signature is `Append(ctx, tx, streamID, expectedVersion, evts)`. The spec's point — that the caller supplies the transaction and the expected version — is unchanged.
2. **Proto files live at `proto/sagaflow/<service>/v1/`, not `proto/<service>/v1/`.** Spec §7's tree shows the shorter path, but §8.1 requires `ce_type` values like `sagaflow.inventory.v1.SeatHeld`, which means the proto package is `sagaflow.inventory.v1`, and buf's `STANDARD` lint rule `PACKAGE_DIRECTORY_MATCH` requires the directory to match the package. §8.1's names are load-bearing at runtime — they are the `ce_type` header and the `protoregistry` lookup key — so the directory yields.

---


## File Structure

| File | Responsibility |
|---|---|
| `go.mod`, `go.sum` | Module, pinned dependencies, pinned tools via `go tool` |
| `Makefile` | `up`, `down`, `generate`, `lint`, `test`, `test-integration` |
| `docker-compose.yml` | Kafka, 4× Postgres, Apicurio, Jaeger at pinned versions |
| `.gitignore` | Build output, coverage, local env |
| `internal/platform/pg/pg.go` | Pool construction and `WithTx` — the transaction helper every handler runs inside |
| `internal/platform/pg/migrate.go` | tern migration runner over an `fs.FS` |
| `internal/platform/pgtest/pgtest.go` | `postgres:18.6` container, one per test package, N independent databases |
| `internal/platform/kafkatest/kafkatest.go` | `apache/kafka:4.3.1` container via the generic container API |

`pgtest` and `kafkatest` are ordinary packages, not `_test.go` files, because other packages' tests import them.

Both expose the same two-function shape — `Start()` for `TestMain` and `Shared(t)` for tests — because spec §12.4 requires one container per package rather than per test. Every integration test package in this repository therefore begins with a `TestMain` that starts what it needs; a package needing both a database and a broker calls both `Start` functions.

---

## Phase 1 Tasks

### Task 1: Module, toolchain and Makefile

**Files:**
- Create: `go.mod`, `.gitignore`, `Makefile`, `internal/platform/version/version_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: a module at `github.com/kptac/sagaflow` on Go 1.26.6; `make test` runs unit tests; `go tool buf` and `go tool protoc-gen-go` resolve at pinned versions.

- [ ] **Step 1: Confirm the toolchain can reach 1.26.6 — do not install anything**

```bash
go env GOTOOLCHAIN   # expect: auto
```

With `GOTOOLCHAIN=auto` (the default since Go 1.21), the `go 1.26.6` directive this task writes into `go.mod` is self-fulfilling: the go command downloads the `go1.26.6` toolchain into the module cache and re-execs into it. The installed go can be older — here it is `go1.26.5` — and every build, test and `go tool` invocation still runs under 1.26.6. Nothing outside the repository changes, which is the point: a plan should not require a developer to re-provision their machine to build the project.

If `GOTOOLCHAIN` is anything other than `auto` or `local+auto`, either unset it for this repository or install 1.26.6 through your usual route; do not lower the `go` directive to match an older toolchain, because that silently changes the language version the whole module compiles against.

- [ ] **Step 2: Initialise the module and pin every dependency**

```bash
cd /home/qas/Documents/me/sagaflow
go mod init github.com/kptac/sagaflow

go get github.com/twmb/franz-go@v1.21.6
go get github.com/twmb/franz-go/pkg/sr@v1.8.0
go get github.com/twmb/franz-go/pkg/kadm@v1.18.0
go get github.com/jackc/pgx/v5@v5.10.0
go get github.com/jackc/tern/v2@v2.4.2
go get github.com/google/uuid@v1.6.0
go get google.golang.org/protobuf@v1.36.12
go get go.opentelemetry.io/otel@v1.45.0
go get go.opentelemetry.io/otel/sdk@v1.45.0
go get github.com/testcontainers/testcontainers-go@v0.44.0
go get github.com/testcontainers/testcontainers-go/modules/postgres@v0.44.0

go get -tool github.com/bufbuild/buf/cmd/buf@v1.72.0
go get -tool google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
```

`go get -tool` records the tools in `go.mod` under a `tool` directive, so `go tool buf` always runs 1.72.0 with no separate install and no PATH ambiguity. This is why the plan never asks you to `brew install buf`.

- [ ] **Step 3: Verify the go directive and tool resolution**

```bash
grep -E '^go |^toolchain' go.mod
go tool buf --version          # expect 1.72.0
go tool protoc-gen-go --version # expect v1.36.12
```

If the `go` directive reads anything other than `go 1.26.6`, fix it with `go mod edit -go=1.26.6`. Do this *before* the `go get` calls in Step 2, so they too run under the pinned toolchain.

**Do not run `go mod tidy` until Phase 4b is complete.** Nothing imports the eleven libraries yet, so `go get` records them all as `// indirect` and `tidy` would delete every one of them — undoing this task. The versions are still reproducible in the meantime: they are recorded in `go.mod` and hashed in `go.sum`, so module resolution selects them regardless of the `indirect` marker, and each becomes a direct requirement as the phase that needs it lands. The two `tool` entries are unaffected — the `tool` directive is itself the thing that keeps them.

The same cause produces one recurring symptom, so expect it rather than debugging it each time: the first task to import a *sub-package* may fail with `missing go.sum entry for module providing package …`. `go get` at module granularity only records sums for what the build graph needed at the time, and a sub-package can pull a transitive module the root did not — `pgx/v5/pgxpool` needs `jackc/puddle/v2`, for instance. Fix it by naming the sub-package at its pinned version, never by tidying:

```bash
go get github.com/jackc/pgx/v5/pgxpool@v5.10.0
```

That adds the missing sums and leaves the selected versions untouched, which `go list -m` will confirm.

- [ ] **Step 4: Write `.gitignore`**

```gitignore
/bin/
*.test
coverage.out
.env
```

- [ ] **Step 5: Write the Makefile**

```makefile
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
```

`test` uses `-short` so the fast suite never starts a container; `test-integration` runs everything. Integration tests added later in this plan call `testing.Short()` and skip themselves.

- [ ] **Step 6: Write a failing test that proves the module compiles and `-short` works**

Create `internal/platform/version/version_test.go`:

```go
package version

import (
	"go/version"
	"runtime"
	"testing"
)

// Minimum is the toolchain floor spec §5 pins. It matches the `go` directive in
// go.mod, which is what actually causes the toolchain to be fetched.
const Minimum = "go1.26.6"

// meetsFloor reports whether a Go release version is at least Minimum.
//
// go/version.Compare understands Go's release ordering, including that go1.26.10
// is newer than go1.26.9 — which string comparison gets wrong.
func meetsFloor(v string) bool {
	return version.Compare(v, Minimum) >= 0
}

// TestToolchainMeetsTheFloor guards the pin for the build actually running.
func TestToolchainMeetsTheFloor(t *testing.T) {
	got := runtime.Version()
	if !version.IsValid(got) {
		// Devel and gccgo builds report versions Compare cannot order; say so
		// rather than failing on something this test cannot judge.
		t.Skipf("runtime.Version() = %q is not a comparable release version", got)
	}
	if !meetsFloor(got) {
		t.Fatalf("built with %s, want %s or newer — see Global Constraints; "+
			"GOTOOLCHAIN=auto should fetch it from the go.mod directive", got, Minimum)
	}
}

// TestFloorRejectsOlderReleases proves the guard has teeth.
//
// Without this, TestToolchainMeetsTheFloor is a test whose only observed outcome
// is "pass" — it would look identical if the comparison were inverted or the
// floor were unreachable. This pins the rejection behaviour without needing an
// older toolchain installed to demonstrate it.
func TestFloorRejectsOlderReleases(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want bool
	}{
		{"go1.25.13", false}, // previous minor, however high its patch
		{"go1.26.4", false},
		{"go1.26.5", false}, // what was installed when this was written
		{"go1.26.6", true},  // the floor itself
		{"go1.26.7", true},
		{"go1.26.10", true}, // string comparison would call this older than 1.26.6
		{"go1.27.0", true},
	} {
		if got := meetsFloor(tc.v); got != tc.want {
			t.Errorf("meetsFloor(%q) = %v, want %v", tc.v, got, tc.want)
		}
	}
}
```

A one-line "reject exactly go1.26.5" check would have been worse than nothing: it reads like a floor, passes on go1.26.4, and its only observable outcome is success. The table is what makes the guard falsifiable.

- [ ] **Step 7: Run it**

Run: `make test`
Expected: PASS. If it fails with `go1.26.5`, Step 1 did not take effect.

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum .gitignore Makefile internal/platform/version/version_test.go
git commit -m "chore: initialise module on Go 1.26.6 with pinned dependencies and tools"
```

---

### Task 2: Docker Compose at pinned versions

**Files:**
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing.
- Produces: `make up` yields Kafka on `localhost:9092`, four Postgres instances on `5433`–`5436`, Apicurio on `8080`, Jaeger UI on `16686` and OTLP gRPC on `4317`.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
name: sagaflow

x-postgres: &postgres
  image: postgres:18.6
  environment:
    POSTGRES_PASSWORD: sagaflow
    POSTGRES_USER: sagaflow
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U sagaflow"]
    interval: 2s
    timeout: 3s
    retries: 15

services:
  kafka:
    image: apache/kafka:4.3.1
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20

  booking-db:
    <<: *postgres
    environment:
      POSTGRES_PASSWORD: sagaflow
      POSTGRES_USER: sagaflow
      POSTGRES_DB: booking
    ports: ["5433:5432"]

  inventory-db:
    <<: *postgres
    environment:
      POSTGRES_PASSWORD: sagaflow
      POSTGRES_USER: sagaflow
      POSTGRES_DB: inventory
    ports: ["5434:5432"]

  hotel-db:
    <<: *postgres
    environment:
      POSTGRES_PASSWORD: sagaflow
      POSTGRES_USER: sagaflow
      POSTGRES_DB: hotel
    ports: ["5435:5432"]

  payment-db:
    <<: *postgres
    environment:
      POSTGRES_PASSWORD: sagaflow
      POSTGRES_USER: sagaflow
      POSTGRES_DB: payment
    ports: ["5436:5432"]

  apicurio:
    image: apicurio/apicurio-registry:3.3.1
    ports: ["8080:8080"]
    environment:
      # Apicurio 3.x has no "mem" storage kind — that was a separate 2.x image
      # (apicurio-registry-mem), and RegistryStorageProducer in 3.3.1 accepts
      # only sql, kafkasql, gitops and kubernetesops. The ephemeral dev store is
      # sql over in-memory H2, which is 3.x's default; it is set explicitly here
      # so the storage a local registry uses is visible rather than inherited.
      APICURIO_STORAGE_KIND: sql
      APICURIO_STORAGE_SQL_KIND: h2
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8080/apis/registry/v3/system/info || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20

  jaeger:
    image: cr.jaegertracing.io/jaegertracing/jaeger:2.20.0
    ports: ["16686:16686", "4317:4317", "4318:4318"]
```

Four Postgres services rather than one with four databases, per spec §5. Ports start at 5433 so a Postgres already running on 5432 does not collide.

- [ ] **Step 2: Bring it up and wait for health**

Run: `make up`
Expected: every service reports healthy. `--wait` blocks until they do, so a green exit is the assertion.

- [ ] **Step 3: Verify each component answers**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
curl -s http://localhost:8080/apis/ccompat/v7/subjects
psql "postgres://sagaflow:sagaflow@localhost:5434/inventory" -c 'select version()'
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:16686/
```

Expected: empty topic list, `[]` from Apicurio, `PostgreSQL 18.6` from psql, `200` from Jaeger. The Apicurio call is the one that proves the `ccompat` path prefix from spec §8.5 — if it 404s you have the wrong URL, not a broken registry.

- [ ] **Step 4: Tear down and commit**

```bash
make down
git add docker-compose.yml
git commit -m "chore: compose stack at pinned versions"
```

---

### Task 3: `platform/pg` — pool, `WithTx`, migrations

**Files:**
- Create: `internal/platform/pg/pg.go`, `internal/platform/pg/migrate.go`, `internal/platform/pgtest/pgtest.go`, `internal/platform/pg/pg_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error)`
  - `func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error`
  - `func Migrate(ctx context.Context, dsn string, fsys fs.FS) error`
  - `func pgtest.Start() (stop func(), err error)` — for `TestMain`
  - `func pgtest.Shared(t *testing.T) *pgtest.PG` and `func (p *PG) DSN(t *testing.T, dbName string) string`

- [ ] **Step 1: Write the test-container helper**

Create `internal/platform/pgtest/pgtest.go`. Not a `_test.go` file — other packages' tests import it.

Spec §12.4 requires **one container per package, started in `TestMain` and shared**, with isolation coming from database names rather than from fresh containers. That is why this package exposes `Start` (for `TestMain`) and `Shared` (for tests) and deliberately offers no function a test can call to start a container of its own.

```go
// Package pgtest starts one Postgres container per test package and hands out
// independent databases inside it.
//
// One container per package, started in TestMain, is spec §12.4: container
// startup dominates test time, and isolation comes from separate databases named
// after the test rather than from fresh containers. Separate databases also
// preserve the property that no transaction can span two services.
//
// There is intentionally no exported function that starts a container for a
// single test. A container per test is the mistake this API exists to prevent.
package pgtest

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const Image = "postgres:18.6"

// PG is the package's shared Postgres.
type PG struct {
	baseDSN string
	mu      sync.Mutex
	created map[string]bool
}

var shared *PG

// Start brings up the package's Postgres container and returns a stop function.
// Call it from TestMain:
//
//	func TestMain(m *testing.M) {
//		stop, err := pgtest.Start()
//		if err != nil {
//			fmt.Fprintln(os.Stderr, err)
//			os.Exit(1)
//		}
//		code := m.Run()
//		stop()
//		os.Exit(code)
//	}
//
// In -short mode it starts nothing and returns a no-op stop, so the fast suite
// never touches Docker.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, Image,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("sagaflow"),
		postgres.WithPassword("sagaflow"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("pgtest: start %s: %w", Image, err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("pgtest: connection string: %w", err)
	}
	shared = &PG{baseDSN: dsn, created: map[string]bool{}}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: terminate: %v\n", err)
		}
	}, nil
}

// Shared returns the container Start brought up, skipping the test in -short mode.
func Shared(t *testing.T) *PG {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	if shared == nil {
		t.Fatal("pgtest: no container running — this package needs a TestMain that calls pgtest.Start; see the pgtest package doc")
	}
	return shared
}

// DSN creates dbName for the calling test and returns a DSN pointing at it. Name
// the database after the test so tests in one package cannot collide. Calling it
// more than once within one test returns the same database.
func (p *PG) DSN(t *testing.T, dbName string) string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.created[dbName] {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, p.baseDSN)
		if err != nil {
			t.Fatalf("connect admin: %v", err)
		}
		defer conn.Close(ctx)
		// CREATE DATABASE takes no parameters, so the name has to be
		// interpolated. pgx.Identifier does the quoting and escaping; fmt's %q
		// only looks right because Go and SQL happen to share the double quote.
		name := pgx.Identifier{dbName}.Sanitize()
		// Drop first. Database names come from test names, so `go test -count=2`
		// asks for the same name twice in one process; without this the second run
		// inherits the first run's rows and fails on counts and version conflicts
		// that have nothing to do with the code under test. FORCE so a leaked
		// connection cannot wedge the whole suite.
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Fatalf("drop stale database %s: %v", dbName, err)
		}
		if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
			t.Fatalf("create database %s: %v", dbName, err)
		}
		p.created[dbName] = true
		// Forget the name when this test ends, so a repeat run recreates it rather
		// than reusing it. Cleanups run last-in-first-out, so the test's own pool
		// has already been closed by the time this runs.
		t.Cleanup(func() {
			p.mu.Lock()
			delete(p.created, dbName)
			p.mu.Unlock()
		})
	}
	dsn, err := replaceDBName(p.baseDSN, dbName)
	if err != nil {
		t.Fatalf("build dsn for %s: %v", dbName, err)
	}
	return dsn
}

// replaceDBName points a DSN at a different database, preserving everything else
// including the query string.
//
// Parsed rather than sliced at the last '/': a password or host containing a
// slash would send an index-based rewrite to the wrong place, and this helper's
// output is what every integration test connects through.
func replaceDBName(dsn, dbName string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	u.Path = "/" + dbName
	return u.String(), nil
}
```

- [ ] **Step 2: Write the failing test for `Open`, `WithTx` and `Migrate`**

Create `internal/platform/pg/pg_test.go`:

```go
package pg_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/platform/pgtest"
)

// One container for the whole package (spec §12.4). Every integration test
// package in this repository has a TestMain of exactly this shape.
func TestMain(m *testing.M) {
	stop, err := pgtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

var schema = fstest.MapFS{
	"001_widgets.sql": &fstest.MapFile{Data: []byte(
		"CREATE TABLE widgets (id INT PRIMARY KEY);\n" +
			"---- create above / drop below ----\n" +
			"DROP TABLE widgets;\n")},
}

func TestMigrateCreatesSchemaAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.Shared(t).DSN(t, "migrate_test")

	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("second migrate must be a no-op, got: %v", err)
	}

	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("widgets table missing after migrate: %v", err)
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.Shared(t).DSN(t, "withtx_test")
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	sentinel := errors.New("handler failed")
	err = pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO widgets (id) VALUES (1)"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel error returned to caller, got %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 rows after rollback, got %d", n)
	}
}

func TestWithTxCommitsOnSuccess(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.Shared(t).DSN(t, "withtx_commit_test")
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO widgets (id) VALUES (2)")
		return err
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM widgets WHERE id = 2").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 committed row, got %d", n)
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./internal/platform/pg/ -run TestMigrate -v`
Expected: FAIL to build — `undefined: pg.Migrate`, `undefined: pg.Open`, `undefined: pg.WithTx`.

- [ ] **Step 4: Implement `pg.go`**

```go
// Package pg holds Postgres plumbing shared by every service: pool
// construction, the transaction helper every handler runs inside, and the
// migration runner.
package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open builds a pool and verifies the database answers before returning, so a
// misconfigured DSN fails at startup rather than on first request.
func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// WithTx runs fn inside a transaction, committing when fn returns nil and
// rolling back otherwise. fn's error is returned unwrapped so callers can
// errors.Is against sentinels such as eventstore.ErrVersionConflict.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	// Deferred rather than called on the error path, so a panic inside fn also
	// releases the connection instead of leaking it until the pool closes. After
	// a successful Commit this is a no-op returning ErrTxClosed; the error is
	// discarded because fn's error is the interesting one, and a genuine rollback
	// failure means the connection is already gone, which pgx handles by
	// discarding it from the pool.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Implement `migrate.go`**

```go
package pg

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
)

// Migrate applies every migration in fsys that has not yet run.
//
// tern takes a *pgx.Conn rather than a pool because it holds a session-level
// advisory lock for the duration, which makes concurrent migrations from two
// starting replicas safe: the second waits, then finds nothing to do.
//
// Migration files must be named NNN_description.sql and use the separator
// "---- create above / drop below ----" between the up and down halves.
func Migrate(ctx context.Context, dsn string, fsys fs.FS) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_version")
	if err != nil {
		return fmt.Errorf("new migrator: %w", err)
	}
	if err := m.LoadMigrations(fsys); err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	if err := m.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/platform/pg/ -v`
Expected: all three PASS. First run pulls `postgres:18.6`, so allow a minute.

- [ ] **Step 7: Verify `-short` skips cleanly**

Run: `make test`
Expected: PASS with `internal/platform/pg` reporting no failures and the container tests skipped. This proves the fast suite stays fast.

- [ ] **Step 8: Commit**

```bash
git add internal/platform/pg internal/platform/pgtest
git commit -m "feat(pg): pool, WithTx and tern migration runner"
```

---

### Task 4: `platform/kafkatest` — a real 4.3.1 broker for tests

**Files:**
- Create: `internal/platform/kafkatest/kafkatest.go`, `internal/platform/kafkatest/kafkatest_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func Start() (stop func(), err error)`, `func Shared(t *testing.T) *Kafka` and `func (k *Kafka) Brokers() []string`.

This exists because `testcontainers-go/modules/kafka` cannot run `apache/kafka` (spec D16): its startup script sources `/etc/confluent/docker/bash-config`, which only Confluent images have. The generic container API has no such assumption.

The `Start`/`Shared` split mirrors `pgtest` for the same reason: spec §12.4 wants one broker per package, started in `TestMain`.

- [ ] **Step 1: Write the failing test**

Create `internal/platform/kafkatest/kafkatest_test.go`:

```go
package kafkatest_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kptac/sagaflow/internal/platform/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) {
	stop, err := kafkatest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

// The broker being 4.3.1 is guaranteed by the pinned Image constant at container
// creation (spec D16), not asserted here — a runtime version check would either
// be tautological or rest on inferring a release from its supported API
// versions. What this test proves is that the gated boot produced a broker the
// host can actually reach and round-trip against.
func TestSharedBrokerRoundTrips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "kafkatest.smoke"

	// The topic is created explicitly because the container disables
	// auto-creation: a typo'd topic name must fail, not silently work with the
	// broker's default partition count.
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// ConsumeTopics must be given at construction. AddConsumeTopics is a no-op on
	// a client that was not built as a consumer (it returns early on
	// !c.consuming()), and the poll below would then block until the context
	// expired rather than failing.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	rec := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// franz-go's default ConsumeResetOffset is NewOffset().AtStart(), so a record
	// produced before this poll is still delivered.
	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if string(recs[0].Value) != "v" {
		t.Fatalf("want value v, got %q", recs[0].Value)
	}
}
```

The bounded context matters: with `context.Background()` a broker that never becomes reachable makes this test hang until the Go test timeout instead of failing with a diagnosable error.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/platform/kafkatest/ -v`
Expected: FAIL to build — `undefined: kafkatest.Start`, `undefined: kafkatest.Shared`.

- [ ] **Step 3: Implement the container helper**

```go
// Package kafkatest starts a real apache/kafka broker for integration tests.
//
// It does not use testcontainers-go/modules/kafka: that module's startup script
// sources /etc/confluent/docker/bash-config and execs /etc/confluent/docker/launch,
// paths that exist only in confluentinc/confluent-local — whose default tag is a
// Kafka 3.5-era broker under the Confluent Community License. Testing against a
// different broker major than production runs is exactly the gap the offset and
// rebalance behaviour in this system cannot afford.
package kafkatest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Image = "apache/kafka:4.3.1"

type Kafka struct {
	brokers []string
}

func (k *Kafka) Brokers() []string { return k.brokers }

var shared *Kafka

// Shared returns the broker Start brought up, skipping the test in -short mode.
func Shared(t *testing.T) *Kafka {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	if shared == nil {
		t.Fatal("kafkatest: no broker running — this package needs a TestMain that calls kafkatest.Start")
	}
	return shared
}

// startGate is the file the container waits for before booting Kafka.
const startGate = "/tmp/sagaflow-advertised-listeners"

// Start brings up a single-node KRaft broker for the package and returns a stop
// function. Call it from TestMain, exactly as with pgtest.Start.
//
// The advertised listener has to name the *host-mapped* port, and that port does
// not exist until the container is running. advertised.listeners also cannot be
// changed after the broker boots: in Kafka 4.3.1,
// DynamicBrokerConfig.DynamicListenerConfig.RECONFIGURABLE_CONFIGS covers
// listeners and listener.security.protocol.map but not advertised.listeners, and
// ALL_DYNAMIC_CONFIGS is built from those same sets — so kafka-configs.sh rejects
// it as a non-dynamic config.
//
// So the boot is gated instead: the container starts with its command wrapped in
// a shell that blocks on startGate, we read the mapped port, write the real
// advertised listener into that file, and the broker then boots with a correct
// value it never has to change. The image makes this cheap — it declares
// CMD ["/etc/kafka/docker/run"] and no ENTRYPOINT, so wrapping the command is
// enough, and its Alpine base provides /bin/sh.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NUM_PARTITIONS":                           "6",
			// Off deliberately: every topic in this system is created explicitly
			// with a known partition count, and an auto-created topic would let a
			// typo'd name silently succeed.
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
			// KAFKA_ADVERTISED_LISTENERS is deliberately absent: the wrapper
			// command below exports it from startGate once the port is known.
		}),
		testcontainers.WithCmd("/bin/sh", "-c",
			"echo 'sagaflow: awaiting advertised listeners'; "+
				"while [ ! -f "+startGate+" ]; do sleep 0.1; done; "+
				"export KAFKA_ADVERTISED_LISTENERS=\"$(cat "+startGate+")\"; "+
				"exec /etc/kafka/docker/run"),
		// Wait only for the gate message here — Kafka has not booted yet.
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForLog("sagaflow: awaiting advertised listeners")),
	)
	if err != nil {
		return nil, fmt.Errorf("kafkatest: start %s: %w", Image, err)
	}

	fail := func(format string, args ...any) (func(), error) {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("kafkatest: "+format, args...)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return fail("host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return fail("mapped port: %w", err)
	}
	broker := fmt.Sprintf("%s:%s", host, port.Port())

	// Open the gate. The broker now boots advertising the host-mapped address —
	// a value it never has to change afterwards.
	//
	// Written to a temporary path and renamed into place, because `> file`
	// creates and truncates before printf writes anything. The waiting shell
	// tests only for existence, so an in-place write leaves a window — narrow,
	// but real — where it sees the file, cats it empty, and boots the broker with
	// no advertised listener at all. rename(2) is atomic within a filesystem, so
	// the gate either does not exist or holds the whole value.
	code, out, err := ctr.Exec(ctx, []string{"/bin/sh", "-c", fmt.Sprintf(
		"printf '%%s' 'PLAINTEXT://%s,BROKER://localhost:19092' > %s.tmp && mv %s.tmp %s",
		broker, startGate, startGate, startGate)}
	if err != nil || code != 0 {
		buf := make([]byte, 4096)
		n, _ := out.Read(buf)
		return fail("open start gate (code %d): %v: %s", code, err, buf[:n])
	}

	// Only now is Kafka actually starting, so this is where we wait for it —
	// behaviourally, by asking it for metadata, rather than by matching a log
	// line whose wording is not part of Kafka's public contract.
	if err := waitForBroker(ctx, broker, 3*time.Minute); err != nil {
		return fail("%w", err)
	}

	shared = &Kafka{brokers: []string{broker}}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "kafkatest: terminate: %v\n", err)
		}
	}, nil
}

// waitForBroker polls until the broker answers a metadata request.
//
// kgo.Ping "returns whether any broker is reachable and that the client can
// communicate with it" — which is the property tests actually need, and unlike a
// log-line match it cannot silently start failing because Kafka reworded a
// startup message.
func waitForBroker(ctx context.Context, broker string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// One client for the whole wait. franz-go retries metadata internally and a
	// failed Ping leaves it usable, so rebuilding it each attempt would spawn and
	// tear down goroutines hundreds of times to learn nothing extra.
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return fmt.Errorf("kafkatest: client for %s: %w", broker, err)
	}
	defer cl.Close()

	for {
		last := cl.Ping(ctx)
		if last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafkatest: broker %s never became ready: %w", broker, last)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
```

Add `"github.com/twmb/franz-go/pkg/kgo"` to this file's imports. `wait` is still needed for the gate strategy.

- [ ] **Step 4: Run the test**

Run: `go test ./internal/platform/kafkatest/ -v -timeout 5m`
Expected: PASS. First run pulls `apache/kafka:4.3.1`.

If it fails with a metadata or connection error rather than a container failure, the broker is advertising an address the host cannot reach. Diagnose with:

```bash
docker ps --filter ancestor=apache/kafka:4.3.1
docker logs <container-id> 2>&1 | grep -i 'advertised\|awaiting'
docker exec <container-id> cat /tmp/sagaflow-advertised-listeners
```

Expected: the gate file holds `PLAINTEXT://<host>:<mapped-port>,BROKER://localhost:19092`, and the log shows the awaiting line followed by a normal startup. If the log stops at the awaiting line, `ctr.Exec` never wrote the file. If the file holds `localhost:9092`, the mapped port was not substituted.

Note that the only log string this helper depends on is `sagaflow: awaiting advertised listeners`, which the wrapper command prints itself. Readiness is then established by `kgo.Ping`, so no part of this waits on wording Kafka could change between versions.

**Do not "simplify" this by setting `KAFKA_ADVERTISED_LISTENERS` up front and correcting it afterwards with `kafka-configs.sh`.** `advertised.listeners` is not dynamically reconfigurable: Kafka 4.3.1's `DynamicListenerConfig.RECONFIGURABLE_CONFIGS` lists `listeners` and `listener.security.protocol.map` but not `advertised.listeners`, and `ALL_DYNAMIC_CONFIGS` is composed from those same sets, so the alter is rejected. Gating the boot is what makes the value correct the first time.

- [ ] **Step 5: Verify the module we are avoiding really is unusable**

This step exists so the D16 decision is verified rather than trusted. Run:

```bash
grep -r 'etc/confluent' $(go env GOMODCACHE)/github.com/testcontainers/testcontainers-go/modules/kafka@v0.44.0/ 2>/dev/null || echo "module not downloaded — fine, we do not depend on it"
```

Expected: either the Confluent paths appear in `kafka.go`, confirming the incompatibility, or the module is absent from the cache because nothing imports it. Both outcomes are correct; a third outcome — the module having `apache/kafka` support — would mean this task can be deleted, so report it rather than silently keeping the helper.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/kafkatest
git commit -m "feat(kafkatest): apache/kafka 4.3.1 test broker via generic container API"
```

---

