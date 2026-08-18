# Architecture

Four services take part in one booking: a flight seat, a hotel room, a payment.
They have **four separate databases** and no way to share a transaction. That is
the entire problem, and it is deliberate — one shared database would make the
problem disappear along with the lesson.

Everything here follows from a single constraint:

> A service can commit to its own database atomically. It cannot commit to its
> database and to Kafka atomically, and it cannot commit to another service's
> database at all.

Consistency is therefore not enforced. It is *arranged*, by making every
crossing point recoverable: a change and the message announcing it commit
together, a message that arrives twice is applied once, and a business step that
cannot be undone is never taken until everything that can be undone has
succeeded.

New here? Read the [README](../README.md) first, and keep the
[glossary](glossary.md) open.

---

## Topology

```mermaid
flowchart LR
    HTTP([POST /bookings]) --> B

    subgraph B["booking — saga orchestrator"]
        BDB[("Postgres<br/>events · outbox · inbox")]
    end
    subgraph I["inventory — seats"]
        IDB[("Postgres<br/>events · outbox · inbox")]
    end
    subgraph H["hotel — rooms"]
        HDB[("Postgres<br/>events · outbox · inbox")]
    end
    subgraph P["payment"]
        PDB[("Postgres<br/>events · outbox · inbox")]
    end

    B -- inventory.commands --> I
    I -- inventory.events --> B
    B -- hotel.commands --> H
    H -- hotel.events --> B
    B -- payment.commands --> P
    P -- payment.events --> B
```

*Today only `inventory` is a working service. `booking` has its database
migrations and nothing else; `hotel` and `payment` do not exist. See the
[README's status table](../README.md#status).*

Each service owns a pair of topics — commands in, events out — and nothing else
reads its database.

| Topic | Produced by | Key | Consumed by |
|---|---|---|---|
| `inventory.commands` | `booking` | seat stream id | `inventory` |
| `inventory.events` | `inventory` | seat stream id | saga, projections |
| `hotel.commands` / `.events` | saga / `hotel` | room stream id | `hotel` / saga, projections |
| `payment.commands` / `.events` | saga / `payment` | payment stream id | `payment` / saga, projections |
| `booking.events` | `booking` | booking stream id | projections |
| `*.dlq` | any | original key | replay tooling |

**The key is always the target stream id.** That gives per-stream ordering, and
per-stream ordering is the only ordering guarantee anything in this system
relies on. Two events about the same seat arrive in order; two events about
different seats have no defined order and nothing may assume one.

Consumer groups are named per *purpose*, not per service — `booking.saga`,
`booking.projection`, `inventory.commands` — because one service consumes the
same topic more than once for different reasons. That is why the consumer name
is part of the inbox primary key.

Every service's database holds the same three tables plus its own domain
projections:

| Table | Holds |
|---|---|
| `events` | the append-only log. `UNIQUE (stream_id, version)` |
| `outbox` | messages waiting to be published, written in the transaction that produced them |
| `inbox` | which messages this service has already applied |

---

## The delivery path

This is the sequence that makes "the state changed" and "the world was told"
survive a crash at any point. It is the heart of the system.

```mermaid
sequenceDiagram
    autonumber
    participant H as handler (inventory)
    participant DB as Postgres (inventory)
    participant P as poller (inventory)
    participant K as Kafka
    participant C as consumer (booking)
    participant DB2 as Postgres (booking)

    H->>DB: BEGIN
    H->>DB: INSERT inbox (consumer, source, event_id)
    H->>DB: INSERT events (stream_id, version, …)
    H->>DB: INSERT outbox (topic, key, payload, headers)
    H->>DB: SELECT pg_notify('outbox', '')
    H->>DB: COMMIT
    Note over DB: all four writes commit together,<br/>or none of them do
    DB-->>P: NOTIFY delivered on commit
    P->>DB: SELECT … WHERE published_at IS NULL<br/>FOR UPDATE SKIP LOCKED
    P->>K: produce (acks=all)
    K-->>P: ack
    P->>DB: UPDATE outbox SET published_at = now()
    Note over P,K: a crash here republishes:<br/>at-least-once, by design
    K->>C: deliver (possibly twice)
    C->>DB2: BEGIN
    C->>DB2: INSERT inbox … ON CONFLICT DO NOTHING
    Note over C,DB2: 0 rows affected ⇒ already applied ⇒<br/>commit nothing, acknowledge
    C->>DB2: INSERT events …
    C->>DB2: COMMIT
```

Three things about this diagram are worth stating plainly.

**The `NOTIFY` is transactional.** Postgres delivers it on commit and discards it
on rollback, so a woken poller can never chase a row that was never written.
This is why the wake-up needs no separate reliability story of its own.

**The gap between produce and mark is not closable.** The poller can publish a
row and die before recording that it did, and will publish it again on restart.
Every design that removes this window puts a worse one somewhere else —
mark-then-publish loses messages outright. So the duplicate is accepted, and
corrected at the consumer.

**The correction is the inbox insert, and it is inside the consumer's
transaction.** If the insert reports zero rows affected, this message has been
applied before: the handler commits nothing and acknowledges. Because the mark
and the effects share one transaction, they cannot come apart — which is what
turns at-least-once delivery into applied-exactly-once.

Where the code lives: `internal/platform/outbox` (enqueue and poller),
`internal/platform/inbox` (deduplication), `internal/platform/kafka` (produce
and consume), `internal/platform/eventstore` (the log). Each has its own chapter
— run `go doc ./internal/platform/outbox` and so on.

---

## The saga

*Not built. This section describes the design from the spec; phase 7 implements
it. Nothing in `internal/` corresponds to it yet.*

A booking is a business transaction spanning services that cannot share a
database transaction. SagaFlow **orchestrates** it: one service, `booking`,
holds the sequence and dispatches each step, rather than each service reacting
to the previous one's events. The orchestrator is itself event-sourced on a
`saga-{id}` stream, so crash recovery is replay and nothing else.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> HoldSeat
    HoldSeat --> ReserveRoom: SeatHeld
    ReserveRoom --> CapturePayment: RoomReserved
    CapturePayment --> ConfirmSeatHold: PaymentCaptured
    ConfirmSeatHold --> BookingConfirmed: SeatConfirmed
    BookingConfirmed --> [*]

    note left of CapturePayment
        compensatable
        every step so far has
        a business inverse
    end note
    note right of ConfirmSeatHold
        THE PIVOT
        after this the saga
        goes forward only
    end note
```

`HoldSeat`, `ReserveRoom` and `CapturePayment` are **compensatable** — each has
a business inverse. `ConfirmSeatHold` is the **pivot**: once a hold becomes a
confirmed seat there is no inverse, so everything after it is **retriable** and
must eventually succeed.

The saga's own decision function is pure — no database, no Kafka, no context:

```go
func (s SagaState) Decide(e Event) []Command
```

That is what makes every path through the compensation matrix testable without
infrastructure.

---

## Compensation

*Not built. Phase 7 implements the matrix; phase 10 tests every path through
it.*

When a step fails, the completed steps are undone in reverse order.

```mermaid
flowchart TD
    F{"which step failed?"}
    F -->|SeatUnavailable| R1["nothing to undo<br/>→ BookingRejected"]
    F -->|RoomUnavailable| R2["ReleaseSeatHold"]
    F -->|PaymentDeclined| R3["CancelRoom<br/>then ReleaseSeatHold"]
    F -->|"SeatHoldExpired<br/>before capture"| R4["CancelRoom only<br/>the seat released itself"]
    F -->|"SeatHoldExpired<br/>after capture"| R5["RefundPayment<br/>then CancelRoom"]
    F -->|"ConfirmSeatHold fails<br/>at the pivot"| R6["RefundPayment<br/>then CancelRoom"]
    F -->|"timeout, no reply"| R7["re-dispatch the step<br/>compensate after N attempts"]
    F -->|"late SeatHeld after<br/>compensation"| R8["ReleaseSeatHold"]
```

Three properties follow, and they are the substance of the design.

**Compensation is not rollback.** A refund is a new fact, recorded forever, not
an erasure of the payment. Cancelling a *confirmed* booking looks identical from
the outside — refund, cancel room, release seat — but is a separate business
flow with its own saga and its own policy, because "this transaction failed" and
"the customer changed their mind" need different audit trails.

**Compensations never dead-letter.** A failed `RefundPayment` means real money is
stranded and real inventory is held. It retries with backoff indefinitely and
raises an alert. The dead-letter policy applies to forward steps only.

**Late replies are absorbed, not errors.** A `SeatHeld` arriving after the saga
has already compensated finds a state where that step was abandoned, so the
decision function returns `ReleaseSeatHold` instead of proceeding.

---

## Where to go next

- [The message lifecycle](message-lifecycle.md) — this delivery path traced
  concretely, with the actual rows, headers and bytes.
- `go doc ./internal/platform/eventstore` — how state is stored.
- The [design spec](superpowers/specs/2026-08-17-sagaflow-design.md) — every
  decision, including the ones that were rejected and why.
