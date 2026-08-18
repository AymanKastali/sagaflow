# SagaFlow — Design Spec

**Date:** 2026-08-17
**Status:** Approved design, ready for implementation planning

## 1. Purpose

Build a distributed, event-sourced trip-booking system in Go whose architecture forces first-hand
implementation of four things:

1. **Distributed transactions** — no transaction spans two services, so the booking flow must be a saga
2. **Compensating actions** — every reversible step needs an explicit business inverse
3. **Idempotency** — Kafka delivers at least once, so every consumer must tolerate redelivery
4. **Eventual consistency** — read models lag writes, and the design must be correct anyway

This is a learning project built to production standards. Where a shortcut would hide one of the four
topics, the shortcut is rejected even when it is cheaper.

## 2. Scope

**In scope**

- Four independently deployable Go services, each owning its own database
- Postgres event store per service, implemented from scratch
- Transactional outbox, consumer inbox, business-level idempotency keys
- Orchestrated saga, itself event-sourced, with a full compensation matrix
- Protobuf contracts in a CloudEvents envelope, validated by a schema registry
- OpenTelemetry tracing spanning the whole saga including compensations
- Integration tests over real Kafka and Postgres via testcontainers

**Out of scope for v1**

- Kubernetes manifests and CI/CD pipelines — real work, but teaches deployment, not consistency
- Load and soak testing — the first thing to add afterwards; seat contention is the interesting target
- Authentication, authorization, multi-tenancy
- A user interface. HTTP + JSON is the only entry point
- Snapshots. The stream-boundary decision in §6.3 makes them permanently unnecessary
- Runtime failure-injection middleware. Failures are driven by test fixtures instead (§12.3)

## 3. Decisions

Each row is settled. Rationale and sources are in the referenced section.

| # | Decision | Rationale |
|---|---|---|
| D1 | Go 1.26.6 | Latest stable (§5). No event-sourcing framework will hide the mechanics |
| D2 | Four separate services, four separate databases | Distributed transactions only exist where no shared transaction does (§4) |
| D3 | Postgres event store, hand-written | The append/load/replay machinery is the subject matter, not an obstacle |
| D4 | Kafka 4.3.1 in KRaft mode, `franz-go` client | Industry default; 4.x is KRaft-only, ZooKeeper is gone. franz-go is pure Go, no cgo (§5) |
| D5 | Trip booking: seat + hotel room + payment | Yields contention, TTL-driven compensation, and three compensatable steps |
| D6 | One stream per **seat**, not per seat map | The oversell invariant does not exist for assigned seating (§6.3) |
| D7 | Claim-based outbox, never a monotonic cursor | `BIGSERIAL` commits out of order and would silently lose events (§6.4) |
| D8 | Orchestrated saga, itself event-sourced | Crash recovery is replay; the decision function is pure and exhaustively testable (§9) |
| D9 | Protobuf payloads, CloudEvents envelope, Apicurio registry | Production and standards-compliance posture (§8) |
| D10 | One `.proto` source, two encodings: protobuf on Kafka, `protojson` in Postgres | Keeps the event store independent of the registry (§8.4) |
| D11 | Package by service, flat inside; no `domain/`/`application/` layers | Go consensus is package-by-feature (§7) |
| D12 | Failures come from test fixtures, not runtime configuration | Same coverage as an injection harness, zero production surface (§12.3) |
| D13 | Marked offsets, committed only after the handler's transaction, and a record settled before the next in its partition | franz-go's default autocommit would lose messages, and marks never rewind — so an unmarked failure with a marked record behind it is silent loss (§10.2) |
| D14 | Schemas registered by CI only; services never auto-register | Registration is the reviewed step that actually prevents an incompatible change (§8.3) |
| D15 | Every component pinned to its latest stable, none to `latest` | Verified compatible as a set on 2026-08-17. A floating tag can change the broker under a green suite (§5) |
| D16 | `apache/kafka` in tests via the generic container API, not `testcontainers-go/modules/kafka` | The module only runs Confluent images and would pin tests to a Kafka 3.5 broker (§5) |

### 3.1 Rejected alternatives

| Rejected | Reason |
|---|---|
| Modular monolith with an in-process bus | Removes the real transaction boundary that makes sagas necessary |
| KurrentDB / EventStoreDB | Provides the append, subscription and projection machinery that is the point of building this |
| Kafka as the event store | No per-stream optimistic concurrency, no cheap single-stream load, retention fights replay |
| Watermill | Mature and the right choice under deadline, but ships the outbox and DLQ we want to write, and is built on sarama rather than franz-go |
| Choreographed saga | Compensation logic smears across four codebases and no single place answers "what state is booking X in?" |
| Avro | Its strength is reader/writer resolution for data lakes; Protobuf wins on tooling (`buf breaking` has no Avro equivalent) |
| JSON Schema | Least formalized evolution model, no built-in schema resolution |
| Dynamic Consistency Boundaries | Genuinely interesting, but needs event-store support we would have to build, and D6 makes it unnecessary |
| Confluent Schema Registry | Confluent Community License is not OSI-approved. Apicurio is Apache 2.0 and API-compatible |
| Separate Go modules per service | Maximum boundary fidelity, but replace directives and version bumps per event change dominate the dev loop |
| Snapshots | Streams are 3–5 events long under D6 |
| Dedicated saga reply channel keyed by saga id | Per-stream ordering already suffices; the saga touches at most one stream per service |
| Debezium / CDC-based outbox | The usual production answer, and the one most references recommend. Rejected because Debezium *is* the machinery this project exists to build — its outbox event router hides the claim/publish/mark cycle behind configuration. Polling costs one goroutine and keeps that cycle visible and testable |
| Broker-side schema validation | Not available: it requires a Confluent Enterprise license and does not exist in Apache Kafka (§8.3) |

## 4. Architecture

Four processes, four databases, communicating only over Kafka. There is no code path by which one
service can read or write another's data, and no transaction that spans two of them.

| Service | Owns | Streams | Compensating actions it provides |
|---|---|---|---|
| `booking` | Public HTTP API, the booking lifecycle, and the saga | `booking-{id}`, `saga-{id}` | — (it *issues* compensations) |
| `inventory` | Flight seats, holds with TTL | `seat-{flight}-{date}-{seat}` | `ReleaseSeatHold` |
| `hotel` | Room reservations | `room-{hotel}-{date}-{roomId}` | `CancelRoomReservation` |
| `payment` | Payments against an external provider stub | `payment-{id}` | `RefundPayment` |

`booking` is the only service with a public API. The others expose health and read-only debug
endpoints only.

### 4.1 What is and is not distributed

Distributed: process boundaries, failure domains, databases, schema migrations, and all
communication. No shared transaction is available anywhere.

Not distributed: one git repository, one Go module, one `docker compose up`. `internal/platform/`
compiles into all four binaries.

A monorepo is not a monolith, but sharing generated contract code across services does let a producer
and consumer change in one commit — which independent deploys cannot do. §8.3 addresses this with
two layers of compatibility enforcement rather than relying on discipline.

## 5. Infrastructure

Every version below is the latest stable release as of **2026-08-17**, checked against the authoritative
source for each — the Go download index, the module proxy, Docker Hub, and each project's own release
feed. Nothing is `latest`: a floating tag means a `docker compose pull` can change the broker underneath
a passing test suite, which is the least debuggable class of failure this project can have.

| Component | Version | Image / source | Notes |
|---|---|---|---|
| Go | **1.26.6** | go.dev/dl | Latest stable. The target environment currently has 1.26.5 — upgrade before phase 1 |
| Kafka | **4.3.1** | `apache/kafka:4.3.1` | Latest stable, KRaft only. Apache-2.0 image, env-var configurable, self-formatting |
| Postgres | **18.6** | `postgres:18.6` | Latest stable major. Four containers, one per service |
| Apicurio Registry | **3.3.1** | `apicurio/apicurio-registry:3.3.1` | Apache 2.0. Single image in 3.x; storage selected by `APICURIO_STORAGE_KIND`, whose 3.x values are `sql`, `kafkasql`, `gitops`, `kubernetesops` — **not** 2.x's `mem`. Use `sql` with `APICURIO_STORAGE_SQL_KIND=h2` for the ephemeral in-memory store |
| Jaeger | **2.20.0** | `cr.jaegertracing.io/jaegertracing/jaeger:2.20.0` | Jaeger **v2**, not the v1 `jaegertracing/all-in-one` image. OTLP-native: gRPC 4317, HTTP 4318, UI 16686 |
| `buf` CLI | **1.72.0** | bufbuild/buf release | Pinned in the Makefile, not installed from `latest` |
| Docker / Compose | 29.7.2 / v5.3.1 | — | Verified in the target environment |

Four Postgres containers rather than one container with four databases. One container would still
make cross-service transactions impossible — Postgres cannot transact across databases — but four
containers also separate failure domains and resource contention, which matches the fidelity chosen
elsewhere. Collapsing to a single container with four databases and four roles is a valid
resource-saving fallback that preserves the transactional guarantee.

Go dependencies, deliberately few, each at its latest stable:

| Module | Version | Purpose |
|---|---|---|
| `github.com/twmb/franz-go` | v1.21.6 | Kafka client |
| `github.com/twmb/franz-go/pkg/sr` | v1.8.0 | Schema registry client and Confluent wire framing |
| `github.com/twmb/franz-go/pkg/kadm` | v1.18.0 | Topic creation with explicit partition counts, in Compose bootstrap and tests |
| `github.com/jackc/pgx/v5` | v5.10.0 | Postgres driver and pool |
| `github.com/jackc/tern/v2` | v2.4.2 | Migrations; pgx-native, so it needs no second driver |
| `github.com/google/uuid` | v1.6.0 | UUIDv7 for `ce_id` — v1.6.0 is the release that added v7 |
| `google.golang.org/protobuf` | v1.36.12 | Generated types, `proto` and `protojson` |
| `go.opentelemetry.io/otel` + `/sdk` | v1.45.0 | Tracing and metrics API/SDK |
| `.../exporters/otlp/otlptrace/otlptracegrpc` | v1.45.0 | OTLP export to Jaeger v2 |
| `.../exporters/prometheus` | v0.67.0 | Metrics scrape endpoint (pre-1.0, as all OTel metric exporters still are) |
| `go.opentelemetry.io/contrib/.../otelhttp` | v0.70.0 | Instruments `booking`'s HTTP entry point |
| `github.com/testcontainers/testcontainers-go` | v0.44.0 | Integration test infrastructure |
| `.../testcontainers-go/modules/postgres` | v0.44.0 | Postgres test container |
| `log/slog` | stdlib | Logging |

**Compatibility is verified, not assumed.** Every module above declares `go 1.25.0` as its minimum, so all
of them build on 1.26.6 with room to spare, and none of them constrain each other: franz-go, pgx,
protobuf and testcontainers-go share no dependencies that could conflict. The OTel modules must move as a
set — `otel`, `otel/sdk` and the trace exporter are released together at the same version, and mixing
1.45.0 with an older SDK is the one upgrade mistake in this list that produces a confusing runtime error
rather than a build failure.

**One deliberate omission: `testcontainers-go/modules/kafka`.** The module cannot run
`apache/kafka:4.3.1`. Its container startup writes a script that sources `/etc/confluent/docker/bash-config`
and execs `/etc/confluent/docker/launch` — paths that exist only in `confluentinc/confluent-local`, whose
`Run` default is `7.5.0`, a Kafka 3.5-era broker under the Confluent Community License. Its
`validateKRaftVersion` check silently skips non-Confluent images, so passing `apache/kafka:4.3.1` fails at
container start rather than at compile time.

Using it would be wrong on three counts, and the third is the one that matters: it contradicts the
licensing rationale in §8.5, it pins tests three Kafka majors behind production, and Kafka 3.5's group
coordinator is not the one §10.2's offset design is written against — so the rebalance test in §12.3 would
be proving a property of the wrong broker. Instead, `platform/kafkatest` starts `apache/kafka:4.3.1`
through testcontainers-go's generic container API. The image is fully driven by `KAFKA_`-prefixed
environment variables, formats its own KRaft storage on first boot, and treats `CLUSTER_ID` as optional,
so this is roughly twenty-five lines and no ongoing maintenance. See the
[Apache Kafka Docker examples](https://github.com/apache/kafka/blob/4.3.1/docker/examples/README.md).

## 6. Event store

### 6.1 Schema

Identical in all four services, created by each service's own migrations in its own database.

```sql
CREATE TABLE events (
    global_seq  BIGSERIAL   PRIMARY KEY,   -- diagnostic and replay tooling ONLY (see §6.4)
    stream_id   TEXT        NOT NULL,
    version     INT         NOT NULL,      -- 1,2,3… per stream, gapless
    type        TEXT        NOT NULL,      -- fully qualified protobuf message name
    data        JSONB       NOT NULL,      -- protojson encoding of the event
    meta        JSONB       NOT NULL,      -- trace_id, correlation_id, causation_id
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, version)
);
```

No separate index on `(stream_id, version)`. The `UNIQUE` constraint already builds a btree on exactly
those columns in that order, which is the index `Load` needs — a second one would never be chosen by the
planner and would double index maintenance on the hottest table in the system.

### 6.2 Append and optimistic concurrency

```go
// expectedVersion is the version the caller's in-memory state was folded from.
func (s *Store) Append(tx pgx.Tx, streamID string, expectedVersion int, evts []Event) error
```

A single multi-row `INSERT` at versions `expectedVersion+1 … +n`. A `23505` unique violation on
`(stream_id, version)` is returned as `ErrVersionConflict`. No `SELECT … FOR UPDATE`, no table locks.

Two writers who both fold from version 7 and both attempt version 8: one commits, the other conflicts,
reloads, and retries against version 8. Command handlers retry a bounded number of times
(§10.3) and expose conflict rate as a metric — a rising conflict rate means stream boundaries are too
coarse.

### 6.3 Stream boundaries

The rule is that a stream is the smallest unit that can still enforce its invariant in one
transaction: one stream, one transaction, one consistency boundary. Streams spanning a whole flight or
venue are a known contention anti-pattern.

**Seats are one stream per seat**, e.g. `seat-BA117-2026-09-01-14A`.

The reason this is safe is that the invariant which appears to span seats does not exist in this
domain. "Do not oversell" is only a real invariant when decrementing a capacity counter — general
admission, "150 tickets left". For **assigned seating** the seat map is fixed, and the only rule is
*this specific seat is held at most once*, which is entirely local to one seat.

```
stream: seat-BA117-2026-09-01-14A

SeatHeld{hold_id, booking_id, expires_at}  ──▶  SeatConfirmed
                                            │   SeatHoldReleased   (compensation)
                                            └─  SeatHoldExpired    (TTL)
```

Consequences:

- **The atomic hold is the unique constraint.** Two users race for 14A: both fold from version 0, both
  attempt version 1, Postgres rejects one. It reloads, sees `SeatHeld`, and its decision function
  returns `ErrSeatUnavailable` → HTTP 409. No locks, no counter, no lost update.
- **Zero contention between different seats** — a flight scales with seats rather than serialising.
- **Streams stay 3–5 events long**, so snapshots are unnecessary rather than deferred.
- **Availability browsing is a projection and deliberately stale.** The worst case is a user selecting a
  gone seat and receiving a 409 from the atomic hold, so no amount of staleness can cause an oversell.
  Cheap stale reads with strict writes is the standard split.
- Version-conflict retries now fire only when two users want the *same* seat. The `booking` and `saga`
  streams carry most of that test coverage instead — see §12.3.
- `SeatHoldExpired` needs a timer **inside `inventory`**, not only in the saga, so a hold expires even
  when the booking service is down (§10.5).

Other conventions: `booking-{uuid}`, `saga-{uuid}` (same uuid as its booking, different prefix),
`payment-{uuid}`, `room-{hotel}-{date}-{roomId}`.

### 6.4 Why `global_seq` is diagnostic only

`BIGSERIAL` values are assigned at insert but become visible at commit, so they **commit out of order**:
transaction A can take `global_seq` 41 and commit after B takes 42 and commits. Any consumer tracking
`WHERE global_seq > cursor` reads 42, advances, and never sees 41. It loses events silently, only under
concurrency, so it passes every test written on a quiet machine.

The design removes the need for a fix:

- **The outbox poller claims rows by flag, not by cursor** — `WHERE published_at IS NULL … FOR UPDATE
  SKIP LOCKED`. A late-committing row is claimed on the next pass. Gaps are irrelevant because nothing
  advances past them.
- **Projections consume from Kafka** using committed consumer-group offsets, never by scanning `events`.

No component reads events by a monotonic local cursor, so no component can be bitten by this.

This also yields per-stream publication ordering for free, which is the only ordering property the
system relies on: two appends to the *same* stream can never be concurrent, because
`UNIQUE(stream_id, version)` serialises them. Their outbox rows are therefore created in commit order,
a single poller publishes them in `id` order, and Kafka keyed by `stream_id` preserves that within a
partition.

**Cross-stream ordering is not guaranteed, and the saga must not assume it.** Every path where a reply
arrives late or out of order is an explicit row in the compensation matrix (§9.3).

## 7. Repository structure

Per [go.dev/doc/modules/layout](https://go.dev/doc/modules/layout): one module at the root, one
directory per binary under `cmd/`, server logic under `internal/`. No `pkg/` — that comes from
`golang-standards/project-layout`, which its own README states is not an official standard.

Packages are organised by service, not by technical layer. The Go consensus is package-by-feature, so
there are no `domain/`, `application/` or `infrastructure/` directories; files are split only when one
becomes genuinely unwieldy.

```
sagaflow/
├── go.mod                          # module github.com/kptac/sagaflow  (adjust to your remote)
├── docker-compose.yml
├── Makefile                        # buf generate, migrate, test, up
├── buf.yaml
├── buf.gen.yaml
├── proto/                          # contract source of truth
│   ├── booking/v1/*.proto
│   ├── inventory/v1/*.proto
│   ├── hotel/v1/*.proto
│   └── payment/v1/*.proto
├── cmd/
│   ├── booking/main.go             # ~20 lines: config, wire.New, Run
│   ├── inventory/main.go
│   ├── hotel/main.go
│   └── payment/main.go
└── internal/
    ├── platform/                   # shared mechanics — where the four topics live
    │   ├── eventstore/             # Append with expected version, Load, Replay
    │   ├── outbox/                 # tx-local Enqueue + claim-based poller
    │   ├── inbox/                  # consume-once deduplication
    │   ├── kafka/                  # franz-go producer/consumer, CloudEvents headers, SR framing
    │   ├── saga/                   # state-machine runtime, timer scheduler
    │   ├── contracts/              # generated protobuf + CloudEvents envelope mapping
    │   ├── pg/                     # pool, migrations, WithTx
    │   └── obs/                    # OTel setup, slog
    ├── booking/
    ├── inventory/
    ├── hotel/
    └── payment/
```

`internal/platform/` will feel like writing a framework. That is deliberate: it is where the four
topics live, and all four services need it.

### 7.1 Inside a service

`internal/booking/` is the richest, owning both the booking stream and the saga:

```
internal/booking/
├── migrations/
│   ├── 001_events.sql          # events + UNIQUE(stream_id, version)
│   ├── 002_outbox.sql
│   ├── 003_inbox.sql
│   ├── 004_timers.sql
│   └── 005_projections.sql     # bookings_view
├── booking.go       # booking stream: state, Apply, pure decision functions. No I/O, no ctx
├── events.go        # event types and registration
├── commands.go      # command types and their handlers
├── errors.go
├── saga.go          # SagaState.Decide(event) → []Command   ← the compensation logic
├── saga_state.go    # saga state and Apply; the saga is itself a stream
├── consumers.go     # Kafka handlers
├── http.go          # POST /bookings, GET /bookings/{id}
├── projection.go    # folds events into bookings_view
├── store.go         # typed wrapper over platform/eventstore
├── wire.go          # New(cfg, deps) (*Service, error); Run(ctx)
└── *_test.go
```

`internal/payment/` is the same shape without `saga*.go`, plus a provider adapter behind an interface.
`inventory` and `hotel` likewise.

**`wire.go` returning a runnable service is a hard requirement, not a style preference.** It is what
makes "crash a service mid-saga" a `cancel()` call in a test rather than a container restart (§12.3).

### 7.2 The one invariant governing every handler

A single transaction writes **exactly one stream**, plus its outbox rows, plus its inbox row. Never two
streams.

```go
tx.Begin()
  fresh := inbox.MarkConsumed(tx, consumer, source, ceID)  // INSERT … ON CONFLICT DO NOTHING
  if !fresh { tx.Rollback(); return ack }                  // already handled ⇒ nothing to do
  state := store.Load(tx, streamID)                        // fold events
  cmds  := state.Decide(event)                             // pure
  store.Append(tx, streamID, expectedVersion, newEvents)
  outbox.Enqueue(tx, cmds, newEvents)                      // same commit ⇒ no lost messages
tx.Commit()
```

`MarkConsumed` uses `ON CONFLICT DO NOTHING` and reports rows-affected rather than letting the unique
violation raise. This is not a style choice: in Postgres *any* error aborts the whole transaction, so a
raised `23505` leaves the connection in `25P02` where every subsequent statement fails and `COMMIT`
silently degrades to a rollback. Detecting the duplicate by rows-affected keeps the transaction healthy
and makes the duplicate path an ordinary branch instead of an exception.

The `booking` and `saga` streams share one database, so both *could* be written in one transaction.
They must not be. Holding the one-stream-per-transaction line is what keeps the code correct if a
stream later moves to a separate database, and what forces the saga to be event-driven rather than a
function call.

## 8. Contracts

### 8.1 Envelope: CloudEvents v1.0.2

CloudEvents graduated in the CNCF in January 2024 and defines a formal
[Kafka protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md).
Binary content mode: attributes travel as Kafka headers, the payload is the message body.

| Header | Value |
|---|---|
| `ce_specversion` | `1.0` |
| `ce_id` | UUIDv7 generated at outbox enqueue time |
| `ce_source` | `/sagaflow/inventory` |
| `ce_type` | `sagaflow.inventory.v1.SeatHeld` |
| `ce_subject` | the stream id |
| `ce_correlationid` | the saga id (extension) |
| `ce_causationid` | the `ce_id` that caused this (extension) |
| `traceparent` | W3C trace context |
| Kafka record key | the target stream id |

`ce_source` + `ce_id` is specified to be unique, which is exactly the property idempotent consumption
needs — so it becomes the inbox deduplication key rather than something we define ourselves.

Postgres 18 ships a native `uuidv7()`, and it is deliberately not used. `ce_id` must be known to the code
that enqueues the outbox row — it is written into the headers and becomes the causation id of whatever the
message triggers — so a database-generated value would have to be read back before it could be used.
Generating it in Go also keeps the rule in §10.5 intact: identifiers and timing that tests need to control
come from the application, and only diagnostic columns use the database.

### 8.2 Payload: Protobuf

`.proto` files under `proto/` are the single source of truth. `buf generate` produces Go into
`internal/platform/contracts/`.

Discipline for events that are persisted forever: never reuse a field number, never repurpose a field,
always `reserved` on removal. Adding a field is backward compatible; anything else requires a new
message version (`SeatHeldV2`) plus an upcaster on the read path.

### 8.3 Three layers of compatibility enforcement, all client-side

- **`buf lint` and `buf breaking` in CI** — fast feedback on a pull request, before anything is registered
- **Apicurio `BACKWARD` compatibility at schema registration** — rejects an incompatible schema for a subject
- **Produce fails closed** — a service serialises against a schema id it looked up; no id, no publish

All three are client-side, and it is worth being precise about what that means, because the tempting
claim — "the registry makes an incompatible change impossible" — is false. Registry compatibility is
checked when a *schema is registered*, not on every message. The only mechanism that enforces schemas at
the broker is Confluent's [broker-side schema ID validation](https://docs.confluent.io/platform/current/schema-registry/schema-validation.html),
which requires a Confluent Enterprise license and does not exist in Apache Kafka. Nothing in an
open-source stack can stop a determined producer from publishing arbitrary bytes.

So the posture is defence in depth against *mistakes*, not enforcement against *bypass*:

- **Services never auto-register.** `auto.register.schemas` off is the standard production setting;
  a service that meets an unregistered schema fails to start rather than quietly defining the contract
- **CI is the only writer to the registry**, after `buf breaking` has passed. Registration is therefore a
  reviewed step, which is what actually prevents the incompatible change
- **Consumers validate on decode** — unknown type or unresolvable schema id is a permanent technical
  failure and goes straight to the DLQ (§10.2)

**Subject naming strategy: `TopicRecordNameStrategy`.** Our topics carry multiple event types, and the
default `TopicNameStrategy` permits only one schema per topic — it would break on the second event
type. With `TopicRecordNameStrategy` the subject is `<topic>-<fully.qualified.MessageName>` and each
event type versions independently.

Wire framing is handled by [`franz-go/pkg/sr`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/sr),
whose `ConfluentHeader` produces the Confluent format — magic byte `0x00`, big-endian schema id,
protobuf message index, payload. No `srclient`, no `confluent-kafka-go`, no cgo.

### 8.4 One schema, two encodings

Wire format and storage format are separate concerns. If the event store held protobuf bytes, a
registry outage would block *replay*, and `psql` would show unreadable blobs during an incident.

- **Kafka:** `proto.Marshal` + Confluent framing — compact, registry-validated
- **Postgres:** `protojson.Marshal` into the `data JSONB` column — queryable and human-readable

One generated type serves both, so there are no hand-written duplicate event structs and no mapping
layer. `protojson` output is not byte-stable across library versions, so it is never hashed or compared
byte-wise.

### 8.5 Registry choice

Apicurio Registry, Apache 2.0, implementing the Confluent SR API. Confluent Schema Registry ships under
the Confluent Community License, which is not OSI-approved. Swapping to Confluent SR is a one-line
Compose change if the license is acceptable.

**The compatibility API is path-scoped**, which is the one thing that will waste an afternoon otherwise.
Apicurio serves the Confluent-shaped REST API under `/apis/ccompat/v7` (v8 also available), not at the
root, so `sr.URLs` must be `http://apicurio:8080/apis/ccompat/v7` — pointed at the base URL, every call
404s. See the
[Apicurio ccompat documentation](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-confluent-schema-registry-compatibility.html).

## 9. Saga

### 9.1 Kafka topology

Commands and events are separate topics, one pair per service.

| Topic | Produced by | Key | Consumed by |
|---|---|---|---|
| `inventory.commands` | `booking` (saga) | seat stream id | `inventory` |
| `inventory.events` | `inventory` | seat stream id | saga, projections |
| `hotel.commands` / `.events` | saga / `hotel` | room stream id | `hotel` / saga, projections |
| `payment.commands` / `.events` | saga / `payment` | payment stream id | `payment` / saga, projections |
| `booking.events` | `booking` | booking stream id | projections |
| `*.dlq` | any | original key | replay tooling |

Keys are always the target stream id, giving per-stream ordering — the only ordering guarantee relied
upon. Replies route back to their saga via `ce_correlationid`.

Consumer groups are per purpose, not per service — `booking.saga`, `booking.projection`,
`inventory.commands` — which is why `consumer` is part of the inbox primary key.

### 9.2 Steps

```
HoldSeat ──▶ SeatHeld ──▶ ReserveRoom ──▶ RoomReserved ──▶ CapturePayment
                                                                 │
                                                         PaymentCaptured
                                                                 │
                                        ConfirmSeatHold + ConfirmRoom  ◀── pivot
                                                                 │
                                                         BookingConfirmed
```

`HoldSeat`, `ReserveRoom` and `CapturePayment` are **compensatable** — each has a business inverse.
`ConfirmSeatHold` is the **pivot**: once a hold converts to a confirmed seat the saga goes forward only.
Everything after the pivot is **retriable** — it must eventually succeed and has no inverse.

The saga is itself event-sourced on `saga-{id}`: `SagaStarted`, `StepDispatched`, `StepSucceeded`,
`StepFailed`, `CompensationStarted`, `CompensationSucceeded`, `SagaCompleted`. Crash recovery is
replay, and `Decide` is a pure function with no database, no Kafka and no context:

```go
func (s SagaState) Decide(e Event) []Command
```

### 9.3 Compensation matrix

| Failure | Completed | Compensations, reverse order |
|---|---|---|
| `SeatUnavailable` | — | none → `BookingRejected` |
| `RoomUnavailable` | seat held | `ReleaseSeatHold` |
| `PaymentDeclined` | seat, room | `CancelRoom`, `ReleaseSeatHold` |
| `SeatHoldExpired` before capture | seat expired, room maybe | `CancelRoom` only — the seat released itself |
| `SeatHoldExpired` after capture | seat expired, room, payment | `RefundPayment`, `CancelRoom` |
| `ConfirmSeatHold` fails at pivot | all three | `RefundPayment`, `CancelRoom` |
| Step timeout, no reply | unknown | re-dispatch the step (idempotent); compensate after N attempts |
| Late `SeatHeld` after compensation | step abandoned | `ReleaseSeatHold` |

Three properties follow, and they are the substance of the saga design:

**Compensations never dead-letter.** A failed `RefundPayment` means real money is stranded and real
inventory is held. It retries with backoff indefinitely and raises an alert. The DLQ policy in §10.2
applies to forward steps only.

**Compensation is not rollback.** Cancelling a *confirmed* booking looks identical — refund, cancel
room, release seat — but is a separate business flow with its own saga, its own policy (cancellation
fees, non-refundable windows) and its own events. Modelling it as saga compensation would conflate
"this transaction failed" with "the customer changed their mind", which need different audit trails.

**Late replies are absorbed, not errors.** `SeatHeld` arriving after the saga already compensated hits a
state where the step is abandoned, so `Decide` returns `ReleaseSeatHold` rather than proceeding.

## 10. Reliability machinery

### 10.1 Outbox

```sql
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,      -- stream id ⇒ partition key ⇒ per-stream order
    payload      BYTEA NOT NULL,     -- protobuf, Confluent-framed
    headers      JSONB NOT NULL,     -- CloudEvents attributes + traceparent
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
```

The partial index keeps the poller's query cheap regardless of table size.

```sql
SELECT … FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED;
-- produce to Kafka with acks=all
UPDATE outbox SET published_at = now() WHERE id = ANY($1);
```

A crash between produce and `UPDATE` republishes on the next pass. **The outbox guarantees at-least-once,
never exactly-once** — which is precisely why the inbox exists.

One active poller per service, elected with `pg_try_advisory_lock`, so two instances cannot reorder a
stream. `SKIP LOCKED` is for safety during failover, not parallelism. Woken by `LISTEN`/`NOTIFY` on
commit, with a 1s ticker as a floor.

**Published rows are deleted, not kept.** The partial index keeps the poller's *query* cheap no matter how
large the table grows, which makes it easy to miss that the table itself grows without bound — the outbox
is a queue, and a queue that is never drained is a leak. A `DELETE FROM outbox WHERE published_at <
now() - interval '7 days'` on the same poller loop is enough; the window exists only so a publish can be
audited after the fact, since the events themselves are already durable in `events`.

### 10.2 Inbox and retry policy

```sql
CREATE TABLE inbox (
    consumer   TEXT NOT NULL,
    source     TEXT NOT NULL,        -- ce_source
    event_id   TEXT NOT NULL,        -- ce_id
    handled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, source, event_id)
);
```

The insert is the **first** statement in the handler's transaction and in the **same** transaction as the
state change. A unique violation means already handled: roll back, ack, move on. Splitting them across
two transactions produces either double-apply or silently dropped work, depending on commit order.

`consumer` is in the key because several consumers in one service read the same message — the saga and
the projection both see `SeatHeld` and must deduplicate independently. Rows are pruned past Kafka's
retention window.

The distinction that matters most, and the most common mistake in these systems:

| Class | Example | Policy |
|---|---|---|
| **Business failure** | `PaymentDeclined`, `SeatUnavailable` | A valid event. Appended, drives compensation. Never retried, never dead-lettered |
| **Version conflict** | `ErrVersionConflict` | Retry *immediately* after reloading state. No backoff — someone else won a race |
| **Transient technical** | network, provider 5xx | Bounded exponential backoff with jitter |
| **Permanent technical** | undecodable payload, unknown type | Straight to `<topic>.dlq`, no retries |

Retries happen before the consumer moves on; then the offset is committed and the message goes to the DLQ.
Blocking a partition on one poison message would stall every other stream sharing it, so the offset is
never held hostage.

**A record must be settled — succeeded, or dead-lettered — before the consumer touches the next record in
the same partition.** `MarkCommitRecords` keeps the highest marked offset per partition and, per its own
documentation, "does not allow rewinds", so marking offset 6 while offset 5 sits unmarked behind it commits
the group to 7 and destroys offset 5's work: no error, no redelivery, healthy-looking offsets. A consumer
therefore may not return a bare error and carry on. It retries in place within the bounded budget above,
dead-letters what outlives it — a failure that survives its whole retry budget is by definition no longer
transient — and only stops marking a partition when it genuinely cannot settle a record, in which case that
partition stalls while the others keep flowing. The retry budget has to fit inside `RebalanceTimeout`,
because `BlockRebalanceOnPoll` means no rebalance can proceed while a batch is being retried.

DLQ records preserve the original headers and `traceparent`, and add `sagaflow_dlq_topic`,
`sagaflow_dlq_partition`, `sagaflow_dlq_offset` and `sagaflow_dlq_error`. Headers alone are not enough to
replay: a replay tool needs to know which topic to put the record back on, and an operator needs to know
what went wrong without re-running it. Provenance also makes the DLQ readable as an incident record rather
than a pile of bytes.

#### Offset commits

The offset commit strategy is part of the delivery guarantee, not client configuration trivia, so it is
specified here. **franz-go's default autocommit would silently lose messages in this design.** It commits
every *polled* offset on a timer, including records the handler has not processed yet, so a rebalance or a
crash mid-batch advances the group past work that never happened. Nothing redelivers it. The whole design
absorbs duplicates and cannot absorb loss, which makes this the one default that must be overridden:

```go
kgo.AutoCommitMarks()        // commit only offsets explicitly marked
kgo.BlockRebalanceOnPoll()   // no rebalance between polling and marking
kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
    cl.CommitMarkedOffsets(ctx)   // flush marks before losing the partitions
})
```

`MarkCommitRecords` is called **only after the handler's transaction commits**. The ordering is
deliberate and gives at-least-once in the safe direction: crash after commit but before mark ⇒
redelivery ⇒ the inbox absorbs it. Crash after mark but before commit is the loss case, and this ordering
makes it unreachable. `RebalanceTimeout` must exceed the slowest handler, or a long transaction gets its
partitions pulled mid-flight. See
[franz-go's offset-management notes](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md)
and [examples/group_committing](https://github.com/twmb/franz-go/blob/master/examples/group_committing/main.go).

### 10.3 Concrete limits

| Setting | Value |
|---|---|
| Seat hold TTL | 15 min (200 ms–2 s in tests) |
| Saga step timeout | 30 s |
| Outbox poll | `NOTIFY`-driven, 1 s floor |
| Timer poll | 1 s |
| Forward step retries | 5 attempts, 100 ms base, ×2, jittered, 30 s cap |
| Compensation retries | unbounded, 5 min backoff cap, alert after 10 attempts |
| Version conflict retries | 3 immediate reload-retries, then HTTP 409 |
| Kafka | 6 partitions, `acks=all`, RF=1 locally |
| `RebalanceTimeout` | 60 s — must exceed the slowest handler transaction (§10.2) |
| Outbox retention | published rows deleted after 7 days |

### 10.4 Business idempotency keys

The inbox deduplicates *messages*. It does nothing about a duplicate *charge*, which is a different
failure: the payment service calls the provider, the provider succeeds, the process dies before
committing. On restart the message is redelivered, the inbox has no record, and the card is charged
twice.

```sql
CREATE TABLE payment_attempts (
    idempotency_key TEXT PRIMARY KEY,   -- deterministic: payment_id + step
    payment_id      TEXT NOT NULL,
    status          TEXT NOT NULL,      -- pending | succeeded | failed
    provider_ref    TEXT
);
```

The key is **derived deterministically from the saga's command**, never generated randomly — a random key
would defeat the entire mechanism on retry. Written `pending` before the provider call, sent as
`Idempotency-Key` (the Stripe convention), reconciled after. A `pending` row found on restart is
resolved by *asking the provider about that key*, not by retrying blind.

This is the one place where the honest answer is reconciliation rather than a database constraint.

### 10.5 Two timers, deliberately separate

**Seat-hold TTL — `inventory`.** It owns the resource, so it must expire holds even when `booking` is
dead. Without this, a crashed saga strands seat 14A forever.

**Saga step timeout — `booking`.** "I dispatched `HoldSeat` 30 s ago and heard nothing." Protects the
*flow*, not the resource.

```sql
CREATE TABLE timers (
    id       BIGSERIAL PRIMARY KEY,
    fire_at  TIMESTAMPTZ NOT NULL,
    subject  TEXT NOT NULL,      -- seat stream id, or saga id
    token    TEXT NOT NULL,      -- fences a stale timer against a newer attempt
    fired_at TIMESTAMPTZ
);
CREATE INDEX timers_due ON timers (fire_at) WHERE fired_at IS NULL;
```

Same claim-based pattern as the outbox, and both emit through the normal append-plus-outbox path — so a
timeout is just another event with no special delivery path to test.

Late and duplicate fires are absorbed for free: `Decide` is state-driven, so a `StepTimedOut` arriving
after `SeatHeld` landed hits a state where the step is complete and returns no commands. `token` covers
the sharper case where a retried step has superseded the one the timer was set for.

`expires_at` and `fire_at` are **application-supplied**, never `DEFAULT now()`, so tests can control them.
Only diagnostic columns such as `recorded_at` use the database clock.

## 11. Observability

`traceparent` travels as a Kafka header, injected on produce and extracted on consume via the OTel
propagator, so one booking plus all its compensations reads as a single Jaeger waterfall. Export is OTLP
gRPC to the Jaeger v2 container on 4317 — v2 receives OTLP natively, so there is no collector and no
Jaeger-specific exporter in the dependency list.

**The outbox breaks the trace if not handled explicitly.** The transaction that enqueues a message and the
poller goroutine that publishes it are different execution contexts, so naive instrumentation produces
two disconnected traces. `traceparent` is serialised into `outbox.headers` at enqueue time and restored
at publish time, with a span link from the poller's own span. Without this, every trace stops at the
service boundary — which would defeat the main reason for adopting tracing.

`trace_id` also goes into each event's `meta` column, so any stored event can be traced back to the
request that caused it.

**Span naming follows OTel messaging semantic conventions at the transport layer**, which specify
`{messaging.operation.name} {destination}` — so `send inventory.commands` and `process
inventory.commands`, carrying `messaging.system=kafka`, `messaging.destination.name`,
`messaging.kafka.offset` and friends. The conventions are still Development status, but backends group,
filter and aggregate on that shape, and a custom naming scheme opts out of every messaging dashboard
Jaeger and its peers already know how to draw. See
[messaging-spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/).

Domain meaning lives one level down, as child spans of the transport span: `inventory.handle HoldSeat`,
`saga.booking.dispatch HoldSeat`, `eventstore.append seat-BA117-2026-09-01-14A`, `outbox.publish`. That is
the better home for it anyway — a waterfall then reads transport, then intent, and the step name is an
attribute of work rather than of a Kafka operation.

Metrics worth exporting from the start: version-conflict rate per stream prefix, outbox lag, inbox
duplicate rate, compensation count by type, DLQ depth, saga duration by outcome.

## 12. Testing

### 12.1 Level 1 — pure, no infrastructure (the bulk of the suite)

Table tests over `Decide(state, event) → []Command`, one row per line of the compensation matrix,
including every awkward path: hold expiring after capture, late `SeatHeld` after compensation, timeout
followed by a real reply. These run in microseconds, which is the only reason covering all of them is
realistic.

Two properties asserted rather than enumerated:

- **Feeding any event twice produces no commands the second time** — idempotency at the logic level,
  independent of the inbox.
- **The saga never compensates a step it never started, and never compensates the same step twice** —
  checked over generated event sequences.

### 12.2 Level 2 — one service, real Postgres

- Append with a stale expected version returns `ErrVersionConflict`
- Two goroutines appending to one stream from the same version: exactly one commits
- Roll back the handler transaction: nothing reaches the outbox, so nothing publishes
- Handle the same `ce_id` twice: state advances once
- Fold a projection, drop it, rebuild from scratch: identical result

### 12.3 Level 3 — all four services plus Kafka

**Failures live in the test data, not in configuration.** This is how real payment sandboxes work — Stripe's
`4000000000000002` always declines — so there are no env flags and no injection middleware in production
code.

| Fixture value | Behaviour |
|---|---|
| seat `13F` | always unavailable |
| card `4000…0002` | always declined |
| room type `UNAVAILABLE` | always fails |
| card `4000…0119` | succeeds, then refund fails — exercises retry-forever compensation |

Forcing the remaining conditions:

- **Duplicate delivery** — produce the same record twice with the same `ce_id`
- **Out-of-order** — produce replies in reverse
- **Mid-saga crash** — cancel `booking`'s context after `SeatHeld`, construct it again, assert the saga
  resumes from its stream and reaches a terminal state. This is the test that proves event-sourcing the
  saga was worthwhile
- **Hold expiry mid-saga** — 200 ms TTL plus a slow payment stub
- **Version conflict** — two concurrent HTTP cancels of one booking
- **Rebalance mid-batch** — start a second consumer in the same group while the first is inside a handler
  transaction, and assert every event is still applied exactly once. This is the test that would have
  caught the autocommit default in §10.2, and loss is invisible without it: the suite passes, the offsets
  look healthy, and the events are simply gone

### 12.4 Determinism

No `time.Sleep` in assertions. Tests subscribe to `booking.events` and block until the terminal event for
their booking id arrives, so completion is a signal rather than a guess, with a context deadline as the
failure mode.

One Kafka and one Postgres container per package, started in `TestMain` and shared — `apache/kafka:4.3.1`
via `platform/kafkatest` and `postgres:18.6` via the testcontainers Postgres module, the same versions
Compose runs, so a test never proves a property of a broker that production does not use. Isolation comes from
stream ids and consumer groups derived from the test name, not from fresh containers, so tests run in
parallel and the suite stays in the tens of seconds.

### 12.5 Not tested

Kafka's delivery guarantees, Postgres' unique constraints, and the provider stub itself.

### 12.6 Coverage map

| Topic | Where it is proven |
|---|---|
| Distributed transactions | outbox rollback test; no cross-service transaction exists to cheat with |
| Compensating actions | matrix table tests; `4000…0119` refund-failure path |
| Idempotency | duplicate-delivery test; double-event property test; payment idempotency key |
| Eventual consistency | projection rebuild; stale-availability → 409; mid-saga crash recovery |

## 13. Build order

Each phase ends somewhere demonstrable.

1. **Foundations** — upgrade Go to 1.26.6, then module, Compose at the pinned versions in §5 (Kafka,
   4× Postgres, Apicurio, Jaeger v2), Makefile, pinned `buf` toolchain, `platform/pg` with `tern`
   migrations, and `platform/kafkatest` so every later phase has a real broker to test against
2. **Event store** — `Append`/`Load`/`Replay` with optimistic concurrency, plus §12.2 tests. Ends with a
   proven concurrency-conflict test
3. **Contracts** — `proto/`, `buf generate`, CloudEvents header mapping, SR framing via `franz-go/pkg/sr`
   against the `ccompat` endpoint, schema registration as a `make` target rather than auto-registration
4. **Outbox and inbox** — claim-based poller, advisory-lock election, published-row pruning, dedup via
   `ON CONFLICT`, and the marked-offset consumer configuration. Ends with an event travelling from one
   service's transaction to another's handler, exactly once applied
5. **Inventory** — seat streams, holds, TTL timer, availability projection. Ends with two concurrent
   holds on one seat producing one 409
6. **Hotel and payment** — same shape; payment adds the provider stub and idempotency keys
7. **Saga** — `Decide`, the saga stream, step timeouts, the full compensation matrix with §12.1 tests
8. **Booking API and projection** — HTTP entry point, `bookings_view`
9. **Observability** — OTel wiring including the outbox trace continuation, metrics
10. **Level 3 tests** — happy path, every compensation path, mid-saga crash recovery

This is too much for a single implementation plan. **Phases 1–4 are the first plan** — they end with an
event travelling from one service's transaction to another service's handler, applied exactly once, which
is the smallest increment that proves the reliability machinery works and is the foundation every later
phase builds on. Phases 5–8 are a second plan, 9–10 a third. Each gets its own plan and its own
implementation cycle against this spec.

## 14. Deferred, with triggers

| Deferred | Add when |
|---|---|
| Load testing | Before trusting the seat-contention design under real concurrency |
| Kubernetes and CI/CD | The system is stable and deployment becomes the learning goal |
| Payment authorize/capture split | You want a cheaper compensation (void) than a refund |
| Snapshots | Never, under §6.3. Only if a stream design changes to accumulate events |
| Choreographed variant | As a deliberate contrast, on the notification path only |
| KurrentDB port of one service | To feel what a purpose-built event store buys |
| Cancellation saga | Modelling post-confirmation cancellation with fee policy (§9.3) |
