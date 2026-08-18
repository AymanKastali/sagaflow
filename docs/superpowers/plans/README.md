# SagaFlow Implementation Plans

Each plan is a separate execution cycle: implement it, review it, then start the next. Plans are deliberately small — a phase that grows past about four tasks gets split and suffixed (`3a`, `3b`).

**Spec:** [2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md), as amended by [2026-08-18-platform-package-restructure-design.md](../specs/2026-08-18-platform-package-restructure-design.md). The spec is the authority; where a plan and the spec disagree, the spec wins and the plan is wrong.

## Plan set 1 — the reliability machinery (spec §13 phases 1–4)

| # | Plan | Ends when | Depends on |
|---|---|---|---|
| 1 | [Phase 1 — Foundations](2026-08-17-phase-1-foundations.md) | `make up` is healthy and tests start a real Postgres 18.6 and Kafka 4.3.1 | — |
| 2 | [Phase 2 — Event store](2026-08-17-phase-2-event-store.md) | Eight racing writers produce one commit and seven `ErrVersionConflict` | 1 |
| 3 | [Phase 3a — Proto toolchain and codec](2026-08-17-phase-3a-proto-toolchain-and-codec.md) | A `SeatHeld` round-trips through JSONB with every field intact | 1, 2 |
| 4 | [Phase 3b — CloudEvents and registry](2026-08-17-phase-3b-cloudevents-and-registry.md) | A serde built against an unregistered subject refuses to construct | 3a |
| 5 | [Phase 4a — Transactional outbox](2026-08-17-phase-4a-transactional-outbox.md) | A rolled-back handler publishes nothing; a failed publish keeps its rows | 1, 2 |
| 6 | [Phase 4b — Inbox and exactly-once delivery](2026-08-17-phase-4b-inbox-and-exactly-once-delivery.md) | An event crosses two databases exactly once, and survives a rebalance | all of the above |

Plans 3a/3b and 4a/4b are independent of each other after plan 2, so 3a→3b and 4a can proceed in either order. Plan 6 needs all five.

## Interlude — platform restructure

| # | Plan | Ends when | Depends on |
|---|---|---|---|
| 7 | [Platform package restructure](2026-08-18-platform-package-restructure.md) | `platform/kafka` imports only `envelope`, and the contracts are their own public module | all of plan set 1 |

Ran between the two plan sets deliberately: the boundaries it corrects gain four consumers each the
moment a service is written, and there are none yet.

## Plan set 2 — the domain (spec §13 phases 5–8)

Not yet written. Inventory seat streams and TTL timers; hotel and payment with the provider stub and idempotency keys; the saga's `Decide` and compensation matrix; the booking API and its projection. Write these only after plan set 1 is complete, so the plans argue from machinery that exists.

## Plan set 3 — observability and end-to-end (spec §13 phases 9–10)

Not yet written. OTel wiring including the outbox trace continuation, metrics, then the full compensation-path suite.

## Conventions across all plans

- **TDD.** Every task writes the failing test, runs it to see it fail, implements the minimum, and runs it again.
- **Task steps end in a commit.** Every task's last step is its commit.
- **`make test` never starts a container.** Integration tests call `testing.Short()` and skip. `make test-integration` runs everything.
- **One container per package, started in `TestMain`** (spec §12.4). `pgtest`, `kafkatest` and `srtest` each expose `Start()` for `TestMain` and `Shared(t)` for tests; none of them can start a container for a single test. Isolation comes from database names, topic names and consumer groups derived from the test.
- **No `time.Sleep` in assertions** (spec §12.4). Tests wait on a signal — a channel, a terminal event — with a context deadline as the failure mode.
