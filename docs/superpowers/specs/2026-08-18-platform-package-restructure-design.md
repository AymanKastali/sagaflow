# Platform Package Restructure — Design Spec

**Date:** 2026-08-18
**Status:** Approved design, ready for implementation planning
**Amends:** [2026-08-17-sagaflow-design.md](2026-08-17-sagaflow-design.md) §7, §7.1

## 1. Purpose

Correct four structural defects in `internal/platform/` before plan set 2 writes four services against
them. Nothing here changes behaviour: no new capability, no altered wire format, no changed SQL. Every
existing test must pass with only its import lines edited.

The window matters more than the defects do. There is no service code today, so the churn is a
mechanical rename. The moment `internal/inventory/` gains a `wire.go`, each of these boundaries has
four consumers and the same change costs an order of magnitude more.

## 2. Scope

**In scope**

- The transport layer's dependency on the persistence layer
- Two packages that hold only test files
- Test scaffolding sitting as a sibling of shipped code
- Generated contracts buried four levels deep inside `internal/`

**Explicitly out of scope**

- Observability, config loading, health endpoints, graceful shutdown, CI. These are missing, and their
  absence is what makes the repo not production-ready — but they are *unbuilt*, not *misbuilt*.
  Rearranging directories produces none of them. Spec §7 already lists `obs/`; §13 phases 5–9 cover the
  rest.
- Proto package names. `sagaflow.inventory.v1` stays. Changing it would change `TypeName()`, which is
  simultaneously the `events.type` column, the `ce_type` header, and the registry subject — a
  wire-contract change wearing a layout change's clothes.
- Renaming packages after roles. `pg` and `kafka` keep their names; see §4.

## 3. What is not wrong

Recorded so a later reader does not "fix" it.

**The dependency graph is flat and acyclic.** Three production edges exist in the entire tree:
`codec→eventstore`, `pgtest→pg`, `kafka→outbox`. Only the third is wrong. No package exceeds 300 lines.

**The three-way split of contracts is a deliberate improvement on §7.** Spec §7 names one
`platform/contracts/` for "generated protobuf + CloudEvents envelope mapping". The implementation split
it three ways, and should stay split, because the three serve different boundaries. Named below as they
are after §5.2 — `schema` is today's `kafka/serde.go`:

| Package | Boundary | Format |
|---|---|---|
| `codec` | message ⇄ Postgres | protojson — readable in `psql`, resolves types without the registry |
| `envelope` | identity ⇄ Kafka headers | `ce_*` string headers, CloudEvents binary content mode |
| `schema` | message ⇄ Kafka body | Confluent framing over registry-resolved schema ids |

Merging them would couple replay to the registry, which §8.4 forbids.

## 4. What is wrong, and what is not the reason

### 4.1 A persistence type is the transport DTO

`kafka` imports `outbox` for two reasons. One is legitimate: `outbox.Publisher` is an interface defined
where it is consumed, which is correct Go. The other is not — `Publisher`'s signature is
`Publish(ctx, []outbox.Claimed)`, so the payload struct crosses the boundary with it.

The consequence is visible at `internal/platform/kafka/consumer.go:291`, where the dead-letter path
constructs an `outbox.Claimed{Message: outbox.Message{...}}`. A dead letter never came from the outbox.
The consumer fabricates a claim, with an ID it does not have, for a row that does not exist, because
that is the only shape the publisher accepts.

### 4.2 Two packages contain no production code

`internal/platform` is itself a package holding one file, `delivery_test.go` (347 lines), and no
production code. `internal/platform/version` holds one file, `version_test.go`, and no production code.

Both are tests that never found a home, parked in the namespace. The `platform` case is worse: the
directory is simultaneously a namespace root and a package, so `go list ./...` reports a package whose
name promises shared mechanics and delivers an integration test.

### 4.3 Test scaffolding is indistinguishable from shipped code

`pgtest`, `kafkatest`, and `srtest` are siblings of `pg`, `kafka`, and `eventstore`. Only a naming
convention separates scaffolding from product.

This is a clarity cost, not a binary-size cost. Go's linker includes only what non-test code reaches,
and nothing reaches them — a service binary will never contain testcontainers. What it does cost is
that `go list ./...`, coverage totals, and lint treat 509 lines of container plumbing as production
packages.

### 4.4 Contracts are buried and sealed

`internal/platform/contracts/sagaflow/inventory/v1` is four levels below `internal/`, under a directory
named for platform mechanics. Contracts are not mechanics; they are the shared language between
services. Being under `internal/` also means nothing outside this module can ever import the event
types — for a system whose subject is inter-service contracts, that should be a decision rather than an
accident.

### 4.5 Not a defect: the name `kafka`

`kafka` is the least cohesive package — five files, 569 lines, four unrelated jobs. That is a real
problem and §5.2 fixes it.

The name is not the problem. Renaming it `broker` or `bus` would advertise a portability that does not
exist: the code is franz-go-specific and there is no second broker coming. `pg` is likewise honest that
the SQL depends on advisory locks, `ON CONFLICT`, and `NOTIFY`. A role-based rename would make both
packages less truthful, not more.

## 5. Target structure

```
sagaflow/
├── proto/sagaflow/inventory/v1/*.proto
├── contracts/                        # separate Go module, public
│   ├── go.mod                        # github.com/kptac/sagaflow/contracts
│   └── … generated *.pb.go
├── cmd/schemactl/
└── internal/
    ├── platform/
    │   ├── eventstore/               # unchanged
    │   ├── outbox/                   # Claimed embeds envelope.Message
    │   ├── inbox/                    # unchanged
    │   ├── codec/                    # unchanged
    │   ├── pg/                       # unchanged
    │   ├── envelope/                 # + Message
    │   ├── schema/                   # serde.go + compat.go, moved out of kafka
    │   └── kafka/                    # admin, producer, consumer
    ├── testsupport/
    │   ├── pgtest/  kafkatest/  srtest/
    ├── integration/                  # delivery_test.go
    └── toolchain/                    # version_test.go
```

### 5.1 The dependency fix

`envelope` gains the neutral message. It is the right home: `envelope` already owns the header
vocabulary and has no internal dependencies, and a message *is* an envelope plus a body plus a routing
key.

```go
// Message is one message to publish: an envelope's headers, a body, and the
// routing key that keeps a stream's events in order.
type Message struct {
    Topic   string
    Key     string
    Payload []byte
    Headers map[string]string
}
```

`outbox.Message` is deleted. `outbox.Claimed` becomes `{ID int64; envelope.Message}` and turns into a
purely internal record — the publisher never sees it.

`Publisher` stays in `outbox`, because the poller is what consumes it, but its signature narrows:

```go
type Publisher interface {
    Publish(ctx context.Context, msgs []envelope.Message) error
}
```

The row ids were never the publisher's business. `Drain` already publishes all-or-nothing and marks the
whole batch with `ids(claimed)` on success (`poller.go:190-194`), so it maps its claims to messages
before the call and keeps the ids to itself.

`kafka` then imports `envelope` — a leaf with no internal dependencies — and nothing else. The
dead-letter path builds an `envelope.Message`, which is exactly what a dead letter is.

One consequence: `producer.go`'s compile-time assertion `var _ outbox.Publisher = (*Producer)(nil)`
would reintroduce the very import being removed. It moves to `producer_test.go`. Test imports do not
appear in the production dependency graph, and the assertion is a test's job.

`Message.validate()` moves with the struct and becomes exported, since `outbox.Enqueue` is now an
outside caller.

### 5.2 The schema extraction

`serde.go` and `compat.go` move to `internal/platform/schema/` unchanged.

The extraction is mechanically free, and this is the evidence rather than a judgement: `producer.go`,
`consumer.go`, and `admin.go` reference no symbol declared in either file. They are already an isolated
island inside the package. `Serde` has no production caller at all yet — only its own test — and
`Subject` and `EnsureBackwardCompatibility` have exactly one, `cmd/schemactl`.

`ErrSubjectNotRegistered`'s message text changes from `kafka: …` to `schema: …`, matching the package
that returns it.

### 5.3 Test-only packages

`pgtest`, `kafkatest`, and `srtest` move under `internal/testsupport/` keeping their package names. A
directory move, nothing more. Renaming them to `postgres`, `kafka`, and `registry` would collide with
`platform/kafka` at every import site and buy nothing.

`delivery_test.go` moves to `internal/integration/`; `version_test.go` moves to `internal/toolchain/`.
Both remain packages holding only tests, which §4.2 called a defect — the distinction is that the
defect was a namespace root and a library-sounding name holding tests. A directory named for what its
tests cover states plainly what it is.

### 5.4 Contracts as a separate module

A public `contracts/` inside the service module still forces every consumer to inherit pgx, franz-go,
and testcontainers. A separate module is what makes "public" mean anything.

```
require github.com/kptac/sagaflow/contracts v0.0.0
replace github.com/kptac/sagaflow/contracts => ./contracts
```

No `go.work`. The cost is honest and small: `go test ./...` at the root no longer reaches the nested
module, so `test`, `test-integration`, and `lint` each gain a second invocation.

It also gives `buf breaking` teeth. Today it guards a package no one outside the module can import;
afterwards it guards a published API.

**Open mechanical detail.** Proto package `sagaflow.inventory.v1` plus buf's `PACKAGE_DIRECTORY_MATCH`
pins the source path, and `paths=source_relative` mirrors it into the output, giving
`contracts/sagaflow/inventory/v1` and the stuttering import
`github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1`. The intent is to remove the stutter with an
explicit `go_package` override in buf managed mode. If that fights managed mode, the stutter is
accepted and recorded — proto packages are not renamed to avoid it, per §2.

## 6. Order and verification

Five commits, each independently green.

1. `envelope.Message`; `outbox` adopts it and narrows `Publisher`; `kafka` drops the `outbox` import
   and the dead-letter path builds an `envelope.Message`. The only commit that touches logic, and it
   cannot be split — narrowing `Publisher` breaks `kafka.Producer` in the same instant, so a commit
   containing only half of it does not build.
2. Extract `platform/schema`; update `cmd/schemactl`.
3. Move `testsupport/*`.
4. Move `integration/` and `toolchain/`.
5. `contracts` module, buf config, regenerate, Makefile.

The existing suite is the verification. No test assertion changes; only import lines. **A test whose
logic needs editing means step 1 changed behaviour, which is a stop-and-report, not a fix-the-test.**

Additionally:

- `go build ./...` proves no production package reaches `internal/testsupport/`.
- `TestEventCrossesServicesExactlyOnce` passes from `internal/integration/`.
- `make test` still starts no container.
- `go vet ./...` clean in both modules.

## 7. Amendment to §7 of the design spec

Spec §7's tree is updated to match §5 above. Three changes, each recorded there with its reason:

1. `platform/contracts/` becomes the root-level `contracts/` module, and the one entry becomes three —
   `codec`, `envelope`, `schema` — per §3.
2. `platform/kafka/` narrows to broker plumbing, with registry framing in `platform/schema/`.
3. `internal/testsupport/`, `internal/integration/`, and `internal/toolchain/` are added. §7 did not
   name them, and they exist regardless; the tree should say so.

§7.1, the per-service layout, is unaffected. §7.2, the one invariant, is unaffected.
