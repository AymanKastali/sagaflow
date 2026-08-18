# The life of one message

The [architecture](architecture.md) describes the delivery path in the
abstract. This traces one message through it concretely: a `HoldSeat` command
arriving at `inventory`, and the `SeatHeld` event that leaves.

Every value below is real — a column that exists in a migration, a header the
code actually emits, a constant that is actually declared. Each step names the
file it comes from, so you can check any line of this against the code.

---

## Before it arrives

The command was produced by another service's transaction, so it reached Kafka
by exactly the path the second half of this document describes. Skip ahead and
come back if you want that end first.

It arrives as a Kafka record on topic `inventory.commands`, keyed by the seat
stream id.

**The key matters more than it looks.** Kafka guarantees ordering only within a
partition, and the key is what chooses the partition. Keying by stream id means
every message about `seat-BA117-2026-09-01-14A` lands in one partition and is
therefore ordered. Messages about different seats have no defined order, and
nothing in the system may assume one.

**Headers** — CloudEvents v1.0.2, binary content mode, so the attributes are
headers and the body is the payload alone (`internal/platform/envelope`):

| Header | Value | Notes |
|---|---|---|
| `ce_specversion` | `1.0` | the only version this system speaks |
| `ce_id` | a UUIDv7 | `envelope.NewID()`. Unique with `ce_source` by the CloudEvents spec — which is exactly what deduplication needs, so the pair is the inbox key rather than something we invent |
| `ce_source` | `/sagaflow/booking` | who emitted it |
| `ce_type` | `sagaflow.inventory.v1.HoldSeat` | the fully qualified protobuf message name |
| `ce_subject` | `seat-BA117-2026-09-01-14A` | the stream id |
| `ce_correlationid` | the saga id | everything belonging to this one booking shares it |
| `ce_causationid` | the `ce_id` of the message that caused this one | the parent link |
| `content-type` | `application/protobuf` | |

Optional attributes are **omitted entirely rather than written empty**: an absent
`ce_subject` is a different statement from an empty one.

**The body** is Confluent-framed protobuf (`internal/platform/schema`):

```
byte 0      0x00                     magic byte
bytes 1-4   big-endian int32         the registered schema id
byte 5      0x00                     message-index path [0], shortened
bytes 6+    protobuf                 the HoldSeat message itself
```

Byte 5 is the message-index path — which message inside the `.proto` file this
is. Every `.proto` file in this repository holds exactly one message, so the path
is always `[0]`, which the format shortens to a single zero byte. That is not a
style preference: a second message in a file would be framed under the wrong
index and be unreadable by any other Confluent client, while still round-tripping
perfectly through our own code.

---

## Applied, in one transaction

The handler is `internal/inventory/commands.go`. Everything below happens inside
a single database transaction that either commits all of it or none of it.

### 1. Is this the first time we have seen it?

```sql
INSERT INTO inbox (consumer, source, event_id)
VALUES ('inventory.commands', '/sagaflow/booking', '<ce_id>')
ON CONFLICT (consumer, source, event_id) DO NOTHING
```

Rows affected is the answer. **One** means this is the first delivery, so carry
on. **Zero** means this exact message has been applied before: the handler
commits nothing and acknowledges, and the duplicate is absorbed.

The primary key is `(consumer, source, event_id)`
(`internal/inventory/migrations/003_inbox.sql`). `consumer` is in the key
because several consumers inside one service read the same message for different
purposes and must each deduplicate independently.

**This insert is inside the transaction, not before it.** That placement is
load-bearing twice over: it is what makes the mark and the effects inseparable,
and it is what lets a failed attempt be retried — a rolled-back transaction
takes the mark with it, so the retry sees an unconsumed message rather than its
own footprint.

### 2. Load the seat's history and fold it

```sql
SELECT version, type, data FROM events WHERE stream_id = $1 ORDER BY version
```

The rows are decoded to protobuf messages and replayed in order to compute the
seat's current state (`internal/inventory/seat.go`). The state is small: a
version, a status, the live hold's id, its booking id and its expiry.

**A seat is a stream.** That single decision is why "this seat is held at most
once" needs no lock: the constraint is `UNIQUE (stream_id, version)`
(`internal/inventory/migrations/001_events.sql`) and nothing else. A stream per
flight would have serialised every seat in the aircraft. A stream per booking
would have left the constraint unable to see the conflict at all.

### 3. Decide

```go
func Decide(s SeatState, cmd proto.Message) (Outcome, error)
```

Pure — no context, no database, and deliberately no clock. It returns an
`Outcome` with two lists:

| | Goes to | Because |
|---|---|---|
| `Events` | the seat's stream **and** the outbox | something happened to the seat |
| `Replies` | the outbox only | the caller must hear an answer, but nothing happened |

For a free seat, `HoldSeat` produces one event, `SeatHeld`. For a seat already
held by someone else it produces one reply, `SeatUnavailable` — which is
deliberately *not* appended, because nothing happened to the seat and appending
it would grow the seat's history by one row for every losing racer.

**Every command gets a reply; only a change gets an event.** Nothing may produce
silence, because a saga step that hears nothing re-dispatches forever.

There is no clock in this function because expiry is an event, appended by
inventory's own timer, not a comparison against `now()`. That is what stops a new
hold racing an expiry: the stream is the only truth about whether a hold is live.

### 4. Append the events

```sql
INSERT INTO events (stream_id, version, type, data, meta) VALUES …
```

`version` is the folded version plus one. If another transaction got there
first, `UNIQUE (stream_id, version)` rejects this insert and the handler gets a
version conflict.

**A conflict is expected, not exceptional.** The loser reloads the stream and
decides again — three times, then it fails
(`ConflictRetries = 3` in `internal/inventory/commands.go`). Re-deciding is the
whole point: replaying the old decision would append a second hold, whereas
deciding again against the state that now exists turns it into a refusal.

`data` is JSONB, not the protobuf bytes. **One schema, two encodings**: protojson
in Postgres so the log is greppable and inspectable by hand, binary protobuf on
the wire so it is compact. Both come from the same `.proto` file, so they cannot
drift (`internal/platform/codec`).

### 5. Enqueue the outgoing messages

```sql
INSERT INTO outbox (topic, key, payload, headers) VALUES …
SELECT pg_notify('outbox', '')
```

Each outgoing message gets a fresh `ce_id`, the incoming `ce_correlationid`
unchanged, and the incoming `ce_id` as its `ce_causationid`. The key is the seat
stream id again, so the reply is ordered with everything else about that seat.

The `NOTIFY` is transactional: Postgres delivers it on commit and discards it on
rollback, so the poller can never be woken for a row that was never written.

### 6. Commit

Four writes — the inbox mark, the events, the outbox rows, the notification —
commit together or not at all. That is the one invariant every handler in this
system obeys, and it is the reason nothing has to reconcile anything afterwards.

---

## Published, afterwards

The poller (`internal/platform/outbox/poller.go`) runs separately, woken by the
`NOTIFY` or by its own timer.

```sql
SELECT id, topic, key, payload, headers
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED
```

**Rows are claimed by flag, never by a cursor over `id`.** `BIGSERIAL` values are
handed out at insert but become visible at commit, so a row that commits late can
carry a lower id than one already published. A cursor would step over it and lose
it silently — and only ever under concurrency, which is the worst kind of bug to
own. A flag has no such window: whatever is still `NULL` is claimed on the next
pass.

`SKIP LOCKED` is for failover, not throughput: a second poller taking over
mid-batch makes progress instead of blocking behind the first one's rows.

The rows are produced to Kafka with `acks=all`, and only then:

```sql
UPDATE outbox SET published_at = now() WHERE id = ANY($1)
```

**A crash between the produce and this update republishes the message.** That
window cannot be closed — marking first would lose messages outright when the
produce fails. So it is accepted, and corrected at the far end by step 1 of this
document, running in the consumer.

That is the whole loop: at-least-once out, applied-exactly-once in.

---

## Checking any of this

Every claim above is in code you can read:

| Claim | File |
|---|---|
| CloudEvents header names and the omit-empty rule | `internal/platform/envelope/envelope.go` |
| Confluent framing and the `[0]` message index | `internal/platform/schema/serde.go` |
| protojson in Postgres, protobuf on the wire | `internal/platform/codec/codec.go` |
| `UNIQUE (stream_id, version)` | `internal/inventory/migrations/001_events.sql` |
| The inbox primary key and its comment | `internal/inventory/migrations/003_inbox.sql` |
| The one-transaction handler and `ConflictRetries` | `internal/inventory/commands.go` |
| Claim-by-flag, `SKIP LOCKED`, and why not a cursor | `internal/platform/outbox/poller.go` |
| The transactional `NOTIFY` | `internal/platform/outbox/outbox.go` |

And the tests that prove it end to end are in `internal/integration/`:
`TestEventCrossesServicesExactlyOnce` and `TestRebalanceMidHandlerLosesNothing`.
