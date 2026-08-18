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

| # | Plan | Ends when | Depends on |
|---|---|---|---|
| 8 | [Phase 5a — Seat streams and holds](2026-08-18-phase-5a-seat-streams-and-holds.md) | Two concurrent holds on one seat produce one `SeatHeld` and one `SeatUnavailable` | 7 |

Still to write: phase 5b (the seat-hold TTL timer and the availability projection), phase 6 (hotel and payment, with the provider stub and idempotency keys), phase 7 (the saga's `Decide` and the compensation matrix), phase 8 (the booking API and its projection).

Three decisions taken while writing 5a, recorded here because later plans inherit them:

- **The timer scheduler is `platform/timers/`, not `platform/saga/`.** Spec §7's tree puts it inside `saga/`, but `inventory` needs the seat-hold TTL and must not import `platform/saga` — that is the `kafka→outbox` edge the restructure removed. `platform/saga` will use `platform/timers` for step timeouts. Arrives in 5b, with the matching §7 amendment.
- **Phase 5's "one 409" is the domain refusal, not HTTP.** HTTP arrives in phase 8; what it renders as a 409 is a `SeatUnavailable` reply, and that is what 5a asserts.
- **The §7.2 handler transaction stays written out in each service for now.** Extracting a shared helper from one consumer is guessing; phase 6 gives it the second and third, which is when the shape is known rather than predicted.

## Plan set 3 — observability and end-to-end (spec §13 phases 9–10)

Not yet written. OTel wiring including the outbox trace continuation, metrics, then the full compensation-path suite.

## Conventions across all plans

- **TDD.** Every task writes the failing test, runs it to see it fail, implements the minimum, and runs it again.
- **Task steps end in a commit.** Every task's last step is its commit.
- **`make test` never starts a container.** Integration tests call `testing.Short()` and skip. `make test-integration` runs everything.
- **One container per package, started in `TestMain`** (spec §12.4). `pgtest`, `kafkatest` and `srtest` each expose `Start()` for `TestMain` and `Shared(t)` for tests; none of them can start a container for a single test. Isolation comes from database names, topic names and consumer groups derived from the test.
- **No `time.Sleep` in assertions** (spec §12.4). Tests wait on a signal — a channel, a terminal event — with a context deadline as the failure mode.
