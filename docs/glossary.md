# Glossary

Every term this system uses that you would not already know. Defined once;
everything else links here.

A term used by only one package is defined in that package's `doc.go` instead —
see [conventions](conventions.md).

Entries marked **(not built yet)** describe parts of the design that exist in
the [design spec](superpowers/specs/2026-08-17-sagaflow-design.md) but not yet
in code. See the status table in the [README](../README.md).

---

## At-least-once delivery

A guarantee that a message will arrive, possibly more than once. It is what you
get from any system that retries on failure, because a sender that does not know
whether its message arrived can only choose between sending again (duplicates)
and giving up (loss).

SagaFlow chooses duplicates, everywhere. The correction happens at the receiver
— see [inbox](#inbox).

## Causation id

The `ce_id` of the message that directly caused this one. Distinct from a
[correlation id](#correlation-id): causation is the parent link, correlation is
the whole tree. Follow causation ids backwards and you get the exact chain that
produced a message; group by correlation id and you get everything belonging to
one business operation.

Carried as the `ce_causationid` header. See `internal/platform/envelope`.

## CloudEvents

A CNCF specification for describing an event's metadata in a transport-neutral
way — who emitted it, what type it is, what it is about — so that the same event
can cross Kafka, HTTP and anything else without its metadata being reinvented.

SagaFlow uses CloudEvents v1.0.2 in *binary content mode*, meaning the
attributes travel as Kafka headers (`ce_id`, `ce_source`, `ce_type`,
`ce_subject`, `ce_correlationid`, `ce_causationid`) and the message body is the
payload alone. See `internal/platform/envelope`.

## Command

A message asking for something to happen: `HoldSeat`, `ReleaseSeatHold`. It may
be refused. Commands are addressed to exactly one service, and travel on that
service's `*.commands` topic.

Contrast with an [event](#event), which reports something that already happened
and cannot be refused.

## Compensation

The business inverse of a completed step, run when a later step fails.
`ReleaseSeatHold` compensates `HoldSeat`; `RefundPayment` compensates
`CapturePayment`.

Compensation is **not rollback**. A refund is a new fact, recorded forever, not
an erasure of the payment. Compensations run in reverse order of completion, and
they never dead-letter: a failed refund means real money is stranded, so it
retries indefinitely and raises an alert.

## Compensatable step

A saga step that has a business inverse, and so can be undone if a later step
fails. `HoldSeat`, `ReserveRoom` and `CapturePayment` are compensatable.

Contrast with [retriable](#retriable-step), and see [pivot](#pivot).

## Confluent wire format

The framing Confluent's clients put around a message body so a consumer can
discover which schema to decode it with: one magic byte `0x00`, a four-byte
big-endian schema id, a protobuf message-index path, then the payload.

The message-index path names which message inside the `.proto` file this is.
Every `.proto` file in this repository holds exactly one message, so the path is
always `[0]`, which the format shortens to a single zero byte. That constraint
is load-bearing — a second message in a file would be framed under the wrong
index and be unreadable by other Confluent clients, while still round-tripping
through our own code. See `internal/platform/schema`.

## Consumer group

Kafka's unit of work-sharing: every partition of a topic is assigned to exactly
one member of a group, and each group tracks its own position independently.

SagaFlow's groups are **per purpose, not per service** — `booking.saga`,
`booking.projection`, `inventory.commands` — because one service may consume the
same topic twice for different reasons. That is why the consumer name is part of
the [inbox](#inbox) primary key.

## Correlation id

An identifier shared by every message belonging to one business operation. All
messages produced while booking one trip carry the same `ce_correlationid`,
whichever service produced them.

It is how a reply finds its way back to the saga that is waiting for it, and how
one booking's whole story is retrieved from logs or traces. Contrast with
[causation id](#causation-id).

## Dead-letter queue (DLQ)

A topic where messages that cannot be processed are parked, so that one bad
message does not block the partition behind it.

In SagaFlow the DLQ policy applies to **forward steps only**. Compensations
never dead-letter — see [compensation](#compensation).

## Envelope

The metadata around a message body: its id, type, source, subject, correlation
and causation ids. In SagaFlow the envelope is [CloudEvents](#cloudevents)
attributes carried as Kafka headers, and `internal/platform/envelope` is the
package that maps between them.

The body is protobuf and knows nothing about the envelope; the envelope is
transport metadata and knows nothing about the body. Keeping them separate is
what lets the same message be stored in Postgres and published to Kafka with two
different encodings of one schema.

## Event

A message reporting something that has already happened: `SeatHeld`,
`SeatHoldReleased`. Events are facts, are named in the past tense, and cannot be
refused or retracted — only followed by a further event.

Not every reply is an event. `SeatUnavailable` is a refusal, and nothing
happened to the seat, so it is a [reply](#reply) and is never appended to a
stream.

## Event sourcing

Storing state as the complete ordered sequence of events that produced it,
rather than as a row that is overwritten. Current state is computed by replaying
the events — see [fold](#fold).

The reason SagaFlow does it: the history *is* the audit trail, concurrency is
enforced by an append constraint rather than by locking, and a projection that
turns out to be wrong can be dropped and rebuilt from the events. See
`internal/platform/eventstore`.

## Exactly-once

Applied exactly once — never *delivered* exactly once. Kafka delivers
[at least once](#at-least-once-delivery); the receiver makes duplicate delivery
harmless by recording what it has already applied, inside the same transaction
as the effect. See [inbox](#inbox).

The distinction matters because "exactly-once delivery" is not achievable
across a network, and designs that assume it are wrong in ways that only show up
under failure.

## Fold

Replaying a stream's events in order to compute current state, each event
applied to the state the previous ones produced.

In SagaFlow folds are pure functions with no database, no clock and no context,
which is what makes them testable without infrastructure. See
`internal/inventory/seat.go`.

## Idempotency key

A caller-supplied identifier that lets a downstream system recognise a retry of
a request it has already performed, and return the original result instead of
performing it twice.

Used for the payment provider, where a duplicated capture is a duplicated
charge. **(not built yet — phase 6)**

## Inbox

A table recording every message a consumer has already applied, written inside
the same transaction as that message's effects, so that a redelivered message is
recognised and ignored.

This is what turns Kafka's [at-least-once delivery](#at-least-once-delivery)
into apply-[exactly-once](#exactly-once). It is the mirror image of the
[outbox](#outbox): the outbox makes sure the message is sent, the inbox makes
sure it is applied once. See `internal/platform/inbox`.

## Optimistic concurrency

Allowing concurrent writers to proceed without locking, and detecting the
collision at write time instead of preventing it.

In SagaFlow every event carries a version, and `UNIQUE(stream_id, version)`
means the second writer to reach version *n* simply fails. There is no lock, no
timeout and no deadlock — the loser reloads and decides again. See
[version conflict](#version-conflict).

## Outbox

A table into which outgoing messages are written **in the same transaction as
the state change they announce**, so that the change and the intent to publish
commit together or not at all. A separate poller publishes committed rows
afterwards.

It exists because a service cannot atomically update its database and a broker.
The cost is that a crash between publishing and marking republishes, which is
why the [inbox](#inbox) exists. See `internal/platform/outbox`.

## Pivot

The saga step after which there is no going back: everything before it is
[compensatable](#compensatable-step), everything after it is
[retriable](#retriable-step) and must eventually succeed.

In SagaFlow the pivot is `ConfirmSeatHold`. Once a hold becomes a confirmed
seat, the saga rolls forward only. **(not built yet — phase 7)**

## Poller

The loop that reads committed [outbox](#outbox) rows and publishes them,
separately from the transaction that wrote them. It wakes on a Postgres
`NOTIFY` sent by that transaction — transactional, so it is delivered on commit
and discarded on rollback, and a woken poller never chases a row that was never
written. See `internal/platform/outbox/poller.go`.

## Projection

A queryable view built by consuming events, kept separate from the events
themselves. Because it derives entirely from the stream it can be dropped and
rebuilt, which is what makes it safe to change its shape.

`seat_availability` is a projection: a seat map derived from the seat streams,
rebuilt by folding them again. `bookings_view` will be another.
**(`bookings_view` not built yet — phase 8)**

## Reply

A message sent back to whoever issued a [command](#command), reporting the
outcome. Every command gets a reply; only a change gets an [event](#event).

The distinction is load-bearing. `SeatUnavailable` is a reply and not an event:
nothing happened to the seat, so appending it would grow the seat's history by
one row for every losing racer. Nothing may produce silence, because a saga step
that hears nothing re-dispatches forever.

## Retriable step

A saga step with no business inverse, which must therefore eventually succeed.
Everything after the [pivot](#pivot) is retriable.

## Saga

A long-running business transaction spread across services that cannot share a
database transaction, made consistent by compensating completed steps when a
later step fails.

SagaFlow uses an **orchestrated** saga: one service (`booking`) holds the
sequence and dispatches each step, rather than each service reacting to the
previous one's events. The orchestrator is itself event-sourced, so crash
recovery is replay. **(not built yet — phase 7)**

## Schema registry

A service holding the schema for each message type under a name called a
*subject*, so producers and consumers agree on the format and incompatible
changes are rejected before they reach the wire.

SagaFlow registers schemas explicitly through `make schemas-register` and never
auto-registers, so an unregistered subject is a startup failure rather than a
surprise at publish time. See `internal/platform/schema`.

## Stream

The unit of ordering and concurrency in the event store: an ordered sequence of
events sharing a `stream_id`, each with a version, unique together.

**Choosing what a stream is, is the central design decision here.** A seat is a
stream — `seat-BA117-2026-09-01-14A` — so "this seat is held at most once" is
enforced by `UNIQUE(stream_id, version)` and by nothing else: no lock, no
check-then-act, no race. A stream per flight would have serialised every seat in
the aircraft; a stream per booking would have made the constraint unable to see
the conflict at all.

## Subject

The name a schema is registered under. SagaFlow uses
*TopicRecordNameStrategy* — `<topic>-<fully.qualified.MessageName>`, for example
`inventory.events-sagaflow.inventory.v1.SeatHeld`.

The default strategy allows only one schema per topic; SagaFlow's topics carry
several message types, so it would break on the second one.

## Version conflict

The error returned when two writers try to append the same version to the same
[stream](#stream) and the unique constraint rejects the loser.

It is expected, not exceptional. The loser reloads the stream and decides again
against the state that actually exists — which is the point: replaying the old
decision would append a second hold, whereas re-deciding turns it into a
refusal. Three immediate retries, then the command fails.
