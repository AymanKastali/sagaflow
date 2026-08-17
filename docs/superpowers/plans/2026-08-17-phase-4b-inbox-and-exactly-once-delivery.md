# SagaFlow Phase 4b — Inbox and Exactly-Once Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the loop. An event committed in one service's transaction reaches another service's handler over real Kafka and is applied exactly once — including when it is delivered twice and when the consumer group rebalances mid-handler.

**Architecture:** The inbox turns at-least-once delivery into exactly-once application: its insert is the first statement in the handler's transaction and commits with the state change, so a redelivery finds the row and does nothing. The consumer commits offsets only after that transaction commits, using marked offsets rather than franz-go's default autocommit — which would advance past unprocessed records and lose them. Permanent failures route to a DLQ carrying enough provenance to be replayed.

**Tech Stack:** Go 1.26.6, Postgres 18.6, Kafka 4.3.1, franz-go v1.21.6 + `pkg/kadm` v1.18.0.

**Spec:** [docs/superpowers/specs/2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) — §10.2 (inbox, retry policy, offset commits), §9.1 (topology), §13 phase 4

**Plan sequence:** this is plan 6 of 6. See [README.md](README.md). **Depends on Phases 1, 2, 3a, 3b and 4a.** Completing it completes spec §13 phase 4; phases 5–8 are the next plan set.

**Deliverable that ends this phase:** `TestEventCrossesServicesExactlyOnce` — an event appended in database A arrives at a handler backed by database B and is applied once; producing the identical record a second time changes nothing; and a rebalance triggered mid-handler loses nothing.

## Global Constraints

Copied verbatim from spec §5 and §3. Every task's requirements implicitly include this section.

- **Go 1.26.6.** Module path `github.com/kptac/sagaflow`. **Add no dependency not listed in spec §5.**
- **Marked offsets, committed only after the handler's transaction** (spec D13/§10.2). franz-go's default autocommit commits every *polled* offset on a timer, including records not yet processed, so a rebalance or crash mid-batch advances the group past work that never happened. This design absorbs duplicates and cannot absorb loss.
- **`AutoCommitMarks()` + `BlockRebalanceOnPoll()`**, `MarkCommitRecords` after commit, `CommitMarkedOffsets` in `OnPartitionsRevoked` (spec §10.2). With `BlockRebalanceOnPoll` the loop **must** call `AllowRebalance()` after each poll is processed, or the group never rebalances.
- **A record is settled — succeeded or dead-lettered — before the consumer touches the next record in its partition** (spec D13/§10.2). `MarkCommitRecords` keeps the highest marked offset per partition and its documentation says it "does not allow rewinds", so marking a later offset while an earlier one sits unmarked commits the group past the earlier record and destroys its work silently.
- **`RebalanceTimeout` 60 s** — must exceed the slowest handler transaction (spec §10.3), and the whole retry budget must fit inside it, because `BlockRebalanceOnPoll` blocks rebalancing while a batch is being retried.
- **The inbox insert is the first statement in the handler's transaction and in the same transaction as the state change** (spec §10.2). Splitting them produces either double-apply or dropped work, depending on commit order.
- **`consumer` is part of the inbox primary key** (spec §10.2) because several consumers in one service read the same message — the saga and the projection both see `SeatHeld` and must deduplicate independently.
- **`ce_source` + `ce_id` is the deduplication key** (spec §8.1), not a locally invented identity.
- **Duplicate detection uses `ON CONFLICT DO NOTHING` and rows-affected, never a raised `23505`** (spec §7.2). In Postgres any error aborts the transaction; a raised unique violation leaves the connection in `25P02` where every later statement fails and `COMMIT` silently degrades to a rollback.
- **Retry policy by class** (spec §10.2): business failure — never retried, never dead-lettered; version conflict — retried *immediately* after reloading, no backoff; transient technical — bounded exponential backoff with jitter; permanent technical — straight to `<topic>.dlq`, no retries.
- **The offset is never held hostage** (spec §10.2). Blocking a partition on one poison message stalls every other stream sharing it.
- **DLQ records preserve the original headers and `traceparent`**, plus `sagaflow_dlq_topic`, `sagaflow_dlq_partition`, `sagaflow_dlq_offset` and `sagaflow_dlq_error` (spec §10.2).
- **Kafka: 6 partitions, `acks=all`, RF=1 locally** (spec §10.3). Both are franz-go defaults for `acks` and idempotency; do not disable them.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/inventory/migrations/003_inbox.sql` | Inventory's inbox table |
| `internal/booking/migrations/003_inbox.sql` | Booking's, identical, separate database |
| `internal/platform/inbox/inbox.go` | `MarkConsumed`, `Prune` |
| `internal/platform/inbox/inbox_test.go` | Dedup semantics, per-consumer independence, transactionality |
| `internal/platform/kafka/admin.go` | Topic creation with explicit partition counts |
| `internal/platform/kafka/producer.go` | `acks=all` producer implementing `outbox.Publisher` |
| `internal/platform/kafka/consumer.go` | Marked-offset consumer group, error classification, DLQ routing |
| `internal/platform/kafka/consumer_test.go` | Offset-commit behaviour, DLQ provenance |
| `internal/platform/delivery_test.go` | The phase deliverable: exactly-once across two databases |

`delivery_test.go` sits in `internal/platform/` rather than in a service package because at this stage there are no service packages to put it in — the services are phases 5–8. It stands up two databases and wires the platform pieces the way a real service will.

---

## Phase 4b Tasks

### Task 1: The inbox

**Files:**
- Create: `internal/inventory/migrations/003_inbox.sql`, `internal/booking/migrations/003_inbox.sql`, `internal/platform/inbox/inbox.go`, `internal/platform/inbox/inbox_test.go`

**Interfaces:**
- Consumes: `pg.WithTx`, `pgtest` from Phase 1.
- Produces:
  - `func MarkConsumed(ctx context.Context, tx pgx.Tx, consumer, source, eventID string) (fresh bool, err error)`
  - `func Prune(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int64, error)`

- [ ] **Step 1: Write `internal/inventory/migrations/003_inbox.sql`**

```sql
CREATE TABLE inbox (
    consumer   TEXT NOT NULL,
    source     TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    handled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, source, event_id)
);

COMMENT ON TABLE inbox IS
    'Consume-once deduplication. (source, event_id) is CloudEvents ce_source + ce_id, which the spec guarantees unique. consumer is in the key because several consumers in one service read the same message and must deduplicate independently.';

---- create above / drop below ----

DROP TABLE inbox;
```

- [ ] **Step 2: Copy to booking**

```bash
cp internal/inventory/migrations/003_inbox.sql internal/booking/migrations/003_inbox.sql
ls internal/booking/migrations
```

Expected: `001_events.sql`, `002_outbox.sql`, `003_inbox.sql`, `migrations.go`.

- [ ] **Step 3: Write the failing test**

Create `internal/platform/inbox/inbox_test.go`:

```go
package inbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	migrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/inbox"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/platform/pgtest"
)

// One container for the package (spec §12.4).
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

func newDB(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.Shared(t).DSN(t, name)
	if err := pg.Migrate(ctx, dsn, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func mark(t *testing.T, pool *pgxpool.Pool, consumer, source, id string) bool {
	t.Helper()
	ctx := context.Background()
	var fresh bool
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		fresh, err = inbox.MarkConsumed(ctx, tx, consumer, source, id)
		return err
	}); err != nil {
		t.Fatalf("mark: %v", err)
	}
	return fresh
}

func TestFirstDeliveryIsFreshAndSecondIsNot(t *testing.T) {
	pool := newDB(t, "inbox_dedup")

	if !mark(t, pool, "booking.saga", "/sagaflow/inventory", "ce-1") {
		t.Fatal("first delivery must report fresh")
	}
	if mark(t, pool, "booking.saga", "/sagaflow/inventory", "ce-1") {
		t.Fatal("second delivery of the same ce_id must report not fresh")
	}
}

// The transaction must stay usable after a duplicate is detected. This is the
// whole reason for ON CONFLICT DO NOTHING over catching 23505: a raised unique
// violation aborts the transaction, and every statement after it fails with
// 25P02 while COMMIT quietly becomes a rollback.
func TestDuplicateLeavesTheTransactionUsable(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "inbox_tx_usable")

	if !mark(t, pool, "booking.saga", "/sagaflow/inventory", "ce-1") {
		t.Fatal("first delivery must be fresh")
	}

	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		fresh, err := inbox.MarkConsumed(ctx, tx, "booking.saga", "/sagaflow/inventory", "ce-1")
		if err != nil {
			return err
		}
		if fresh {
			return errors.New("expected duplicate")
		}
		// If the duplicate had raised, this would fail with 25P02.
		var one int
		return tx.QueryRow(ctx, "SELECT 1").Scan(&one)
	})
	if err != nil {
		t.Fatalf("transaction was not usable after a duplicate: %v", err)
	}
}

// Spec §10.2: the saga and the projection both see SeatHeld and must dedupe
// independently, which is why consumer is part of the primary key.
func TestDifferentConsumersDeduplicateIndependently(t *testing.T) {
	pool := newDB(t, "inbox_per_consumer")

	if !mark(t, pool, "booking.saga", "/sagaflow/inventory", "ce-1") {
		t.Fatal("saga first delivery must be fresh")
	}
	if !mark(t, pool, "booking.projection", "/sagaflow/inventory", "ce-1") {
		t.Fatal("projection must see the same ce_id as fresh — it is a different consumer")
	}
	if mark(t, pool, "booking.projection", "/sagaflow/inventory", "ce-1") {
		t.Fatal("projection's second delivery must not be fresh")
	}
}

func TestSameEventIDFromADifferentSourceIsFresh(t *testing.T) {
	pool := newDB(t, "inbox_per_source")

	if !mark(t, pool, "booking.saga", "/sagaflow/inventory", "ce-1") {
		t.Fatal("first must be fresh")
	}
	if !mark(t, pool, "booking.saga", "/sagaflow/hotel", "ce-1") {
		t.Fatal("ce_id is only unique within a source, so this must be fresh")
	}
}

// A handler that fails after marking must not leave the message marked, or the
// redelivery would be swallowed and the work lost forever.
func TestRollbackUnmarksTheMessage(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "inbox_rollback")

	sentinel := errors.New("handler failed after marking")
	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := inbox.MarkConsumed(ctx, tx, "booking.saga", "/sagaflow/inventory", "ce-1"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}

	if !mark(t, pool, "booking.saga", "/sagaflow/inventory", "ce-1") {
		t.Fatal("after rollback the message must be deliverable again")
	}
}

func TestPruneRemovesOldRowsOnly(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "inbox_prune")

	mark(t, pool, "booking.saga", "/sagaflow/inventory", "old")
	mark(t, pool, "booking.saga", "/sagaflow/inventory", "recent")
	if _, err := pool.Exec(ctx,
		`UPDATE inbox SET handled_at = now() - interval '30 days' WHERE event_id = 'old'`); err != nil {
		t.Fatalf("age: %v", err)
	}

	deleted, err := inbox.Prune(ctx, pool, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("want 1 pruned, got %d", deleted)
	}
	// Pruned rows become deliverable again, which is safe only because the
	// retention window is longer than Kafka's — nothing can still be in flight.
	if !mark(t, pool, "booking.saga", "/sagaflow/inventory", "old") {
		t.Fatal("a pruned row should no longer be remembered")
	}
	if mark(t, pool, "booking.saga", "/sagaflow/inventory", "recent") {
		t.Fatal("a recent row must still be remembered")
	}
}
```

- [ ] **Step 4: Run to verify failure**

Run: `go test ./internal/platform/inbox/ -v`
Expected: FAIL to build — `undefined: inbox.MarkConsumed`.

- [ ] **Step 5: Implement `inbox.go`**

```go
// Package inbox turns Kafka's at-least-once delivery into exactly-once
// application.
//
// The contract is narrow and load-bearing: MarkConsumed must be the first
// statement in the handler's transaction, and that transaction must be the same
// one that writes the state change. Split across two transactions, the outcome is
// either double-apply or silently dropped work depending on commit order — and
// which one you get depends on timing, so testing will not reliably reveal it.
package inbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const markSQL = `
INSERT INTO inbox (consumer, source, event_id)
VALUES ($1, $2, $3)
ON CONFLICT (consumer, source, event_id) DO NOTHING`

// MarkConsumed records that consumer has handled (source, eventID).
//
// It returns fresh=false when the message was already handled, in which case the
// caller rolls back and acknowledges without applying anything.
//
// Duplicates are detected by rows-affected rather than by catching a unique
// violation. A raised 23505 would abort the transaction: every subsequent
// statement fails with 25P02 and COMMIT degrades to a rollback, so the "harmless"
// version of that bug is a handler that appears to work and commits nothing.
//
// source and eventID are CloudEvents ce_source and ce_id, which the specification
// guarantees unique as a pair — so this reuses an identity the producer already
// had to establish rather than inventing one.
func MarkConsumed(ctx context.Context, tx pgx.Tx, consumer, source, eventID string) (bool, error) {
	if consumer == "" || source == "" || eventID == "" {
		return false, fmt.Errorf("inbox: consumer, source and event_id are all required (got %q, %q, %q)",
			consumer, source, eventID)
	}
	tag, err := tx.Exec(ctx, markSQL, consumer, source, eventID)
	if err != nil {
		return false, fmt.Errorf("inbox: mark %s/%s for %s: %w", source, eventID, consumer, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Prune deletes rows older than olderThan.
//
// olderThan must exceed Kafka's retention, because a pruned row becomes
// deliverable again: if a message could still be replayed from the log after its
// inbox row was deleted, it would be applied twice.
func Prune(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx,
		`DELETE FROM inbox WHERE handled_at < now() - $1::interval`, olderThan)
	if err != nil {
		return 0, fmt.Errorf("inbox: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/platform/inbox/ -race -v`
Expected: all seven PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/inventory/migrations internal/booking/migrations internal/platform/inbox
git commit -m "feat(inbox): consume-once dedup via ON CONFLICT with rows-affected"
```

---

### Task 2: Kafka admin and producer

**Files:**
- Create: `internal/platform/kafka/admin.go`, `internal/platform/kafka/producer.go`, `internal/platform/kafka/producer_test.go`
- Modify: `internal/platform/kafka/serde_test.go` — its `TestMain` (written in Phase 3b) must now start a broker as well as a registry

**Interfaces:**
- Consumes: `outbox.Claimed`, `outbox.Publisher` from Phase 4a; `kafkatest` from Phase 1.
- Produces:
  - `func EnsureTopics(ctx context.Context, brokers []string, partitions int32, replication int16, topics ...string) error`
  - `func NewProducer(brokers []string) (*Producer, error)`
  - `func (p *Producer) Publish(ctx context.Context, msgs []outbox.Claimed) error` — satisfies `outbox.Publisher`
  - `func (p *Producer) Close()`
  - `const Partitions int32 = 6`

- [ ] **Step 1: Extend the package's `TestMain` to start a broker**

`internal/platform/kafka/serde_test.go` already starts a registry (Phase 3b). This package now needs a broker too, and spec §12.4 wants one of each for the whole package — so both go in the same `TestMain` rather than being started per test. Replace it with:

```go
func TestMain(m *testing.M) {
	stopRegistry, err := srtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	stopKafka, err := kafkatest.Start()
	if err != nil {
		stopRegistry()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stopKafka()
	stopRegistry()
	os.Exit(code)
}
```

Add `"github.com/kptac/sagaflow/internal/platform/kafkatest"` to that file's imports.

- [ ] **Step 2: Write the failing test**

Create `internal/platform/kafka/producer_test.go`:

```go
package kafka_test

import (
	"context"
	"testing"

	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/kafkatest"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestEnsureTopicsCreatesWithSixPartitionsAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	brokers := kafkatest.Shared(t).Brokers()

	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, "inventory.events"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// Running it again must not fail — services call this at startup.
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, "inventory.events"); err != nil {
		t.Fatalf("second ensure: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	meta, err := kadm.NewClient(cl).ListTopics(ctx, "inventory.events")
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	topic, ok := meta["inventory.events"]
	if !ok {
		t.Fatal("inventory.events was not created")
	}
	if got := len(topic.Partitions); got != 6 {
		t.Fatalf("want 6 partitions, got %d", got)
	}
}

func TestPublishPreservesKeyHeadersAndPayload(t *testing.T) {
	ctx := context.Background()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "producer.roundtrip"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	p, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()

	err = p.Publish(ctx, []outbox.Claimed{{
		ID: 1,
		Message: outbox.Message{
			Topic:   topic,
			Key:     "seat-BA117-2026-09-01-14A",
			Payload: []byte{0x00, 0x0a, 0x0b},
			Headers: map[string]string{
				"ce_id":       "01920000-0000-7000-8000-000000000001",
				"ce_type":     "sagaflow.inventory.v1.SeatHeld",
				"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			},
		},
	}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	r := recs[0]

	if string(r.Key) != "seat-BA117-2026-09-01-14A" {
		t.Errorf("key: got %q", r.Key)
	}
	if len(r.Value) != 3 || r.Value[0] != 0x00 {
		t.Errorf("payload: got %v", r.Value)
	}
	got := map[string]string{}
	for _, h := range r.Headers {
		got[h.Key] = string(h.Value)
	}
	for k, want := range map[string]string{
		"ce_id":       "01920000-0000-7000-8000-000000000001",
		"ce_type":     "sagaflow.inventory.v1.SeatHeld",
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	} {
		if got[k] != want {
			t.Errorf("header %s: want %q, got %q", k, want, got[k])
		}
	}
}

func TestPublishSameKeyKeepsOrder(t *testing.T) {
	ctx := context.Background()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "producer.ordering"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	p, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()

	var batch []outbox.Claimed
	for i := range 20 {
		batch = append(batch, outbox.Claimed{
			ID: int64(i + 1),
			Message: outbox.Message{
				Topic:   topic,
				Key:     "seat-14A", // one key ⇒ one partition ⇒ ordered
				Payload: []byte{byte(i)},
				Headers: map[string]string{"ce_id": "id"},
			},
		})
	}
	if err := p.Publish(ctx, batch); err != nil {
		t.Fatalf("publish: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	var seen []byte
	for len(seen) < 20 {
		fetches := cl.PollRecords(ctx, 20)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("poll: %v", err)
		}
		for _, r := range fetches.Records() {
			seen = append(seen, r.Value[0])
		}
	}
	for i := range 20 {
		if seen[i] != byte(i) {
			t.Fatalf("order broken at %d: %v", i, seen)
		}
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/platform/kafka/ -run 'TestEnsureTopics|TestPublish' -v`
Expected: FAIL to build — `undefined: kafka.EnsureTopics`, `undefined: kafka.NewProducer`.

- [ ] **Step 4: Implement `admin.go`**

```go
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Partitions is the partition count for every topic in the system (spec §10.3).
// It bounds consumer parallelism; per-stream ordering is preserved regardless
// because the record key is always the stream id.
const Partitions int32 = 6

// EnsureTopics creates topics if they are absent and succeeds if they exist.
//
// Explicit creation rather than auto-creation: an auto-created topic gets the
// broker's default partition count, which would silently cap parallelism and, in
// a cluster, silently reduce the replication factor.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, replication int16, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka: admin client: %w", err)
	}
	defer cl.Close()

	resp, err := kadm.NewClient(cl).CreateTopics(ctx, partitions, replication, nil, topics...)
	if err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	for _, ct := range resp {
		if ct.Err != nil && !errors.Is(ct.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: create topic %s: %w", ct.Topic, ct.Err)
		}
	}
	return nil
}
```

- [ ] **Step 5: Implement `producer.go`**

```go
package kafka

import (
	"context"
	"fmt"

	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes outbox rows to Kafka. It satisfies outbox.Publisher.
type Producer struct {
	cl *kgo.Client
}

// NewProducer builds a durable producer.
//
// acks=all and idempotent production are franz-go defaults (RequireAllISRAcks,
// idempotency on unless explicitly disabled), so the durability the spec asks for
// costs no configuration here — but it is worth knowing it is a default rather
// than an omission, because DisableIdempotentWrite anywhere would quietly remove
// it.
func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0), // latency matters more than batching for a saga
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

func (p *Producer) Close() { p.cl.Close() }

// Publish sends every message and returns the first error.
//
// ProduceSync waits for all records, so a partial failure leaves the whole batch
// unmarked in the outbox and the successful records are republished on the next
// pass. That is a duplicate, which the inbox absorbs — the alternative, marking a
// partially published batch, would be a loss, which nothing absorbs.
func (p *Producer) Publish(ctx context.Context, msgs []outbox.Claimed) error {
	if len(msgs) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		headers := make([]kgo.RecordHeader, 0, len(m.Headers))
		for k, v := range m.Headers {
			headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs = append(recs, &kgo.Record{
			Topic:   m.Topic,
			Key:     []byte(m.Key),
			Value:   m.Payload,
			Headers: headers,
		})
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish %d records: %w", len(recs), err)
	}
	return nil
}

var _ outbox.Publisher = (*Producer)(nil)
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/platform/kafka/ -race -v -timeout 15m`
Expected: all PASS, including the Phase 3b serde tests.

- [ ] **Step 7: Commit**

```bash
git add internal/platform/kafka
git commit -m "feat(kafka): topic admin and acks=all producer implementing outbox.Publisher"
```

---

### Task 3: Marked-offset consumer with DLQ routing

**Files:**
- Create: `internal/platform/kafka/consumer.go`, `internal/platform/kafka/consumer_test.go`

**Interfaces:**
- Consumes: `EnsureTopics`, `NewProducer` from Task 2.
- Produces:
  - `type Record struct { Topic, Key string; Value []byte; Headers map[string]string; Partition int32; Offset int64 }`
  - `type Handler func(ctx context.Context, r Record) error`
  - `var ErrPermanent = errors.New(...)` — wrap a handler error with this to route to the DLQ
  - `type ConsumerConfig struct { Brokers []string; Group string; Topics []string; Handler Handler; DLQ *Producer; MaxAttempts int; BaseBackoff time.Duration }`
  - `func NewConsumer(cfg ConsumerConfig) (*Consumer, error)`
  - `func (c *Consumer) Run(ctx context.Context) error`
  - `func (c *Consumer) Close()`
  - `const DefaultMaxAttempts = 5`, `const DefaultBaseBackoff = 100 * time.Millisecond`

**Why this consumer retries in place instead of returning and moving on.** A record whose handler fails must be settled before the loop touches the next record *in that partition*, because `MarkCommitRecords` keeps the highest offset per partition and its own documentation says it "does not allow rewinds". So marking offset 6 after skipping a failed offset 5 advances the group to 7 and offset 5 is gone — no error, no redelivery, healthy-looking offsets. The only ways to avoid that are to settle each record before moving on, or to abandon the rest of the partition. Settling is what spec §10.2 already asks for: transient technical failures get bounded exponential backoff with jitter, and a failure that outlives the budget is by definition no longer transient, so it dead-letters. `ErrPermanent` still skips the retries entirely.

The retry budget must stay well under `RebalanceTimeout`, because `BlockRebalanceOnPoll` means a rebalance cannot proceed while a batch is being retried. Five attempts at a 100 ms base is about 1.5 s of backoff, against a 60 s timeout.

- [ ] **Step 1: Write the failing test**

Create `internal/platform/kafka/consumer_test.go`:

```go
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/kafkatest"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

func produce(t *testing.T, brokers []string, topic, key string, headers map[string]string, payload []byte) {
	t.Helper()
	p, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), []outbox.Claimed{{
		ID:      1,
		Message: outbox.Message{Topic: topic, Key: key, Payload: payload, Headers: headers},
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestConsumerDeliversRecordWithHeadersAndProvenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.deliver"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A",
		map[string]string{"ce_id": "ce-1", "ce_source": "/sagaflow/inventory"}, []byte{7})

	got := make(chan kafka.Record, 1)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   "test.deliver",
		Topics:  []string{topic},
		Handler: func(_ context.Context, r kafka.Record) error {
			got <- r
			return nil
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer c.Close()
	go func() { _ = c.Run(ctx) }()

	select {
	case r := <-got:
		if r.Key != "seat-14A" {
			t.Errorf("key: got %q", r.Key)
		}
		if r.Headers["ce_id"] != "ce-1" {
			t.Errorf("headers: got %v", r.Headers)
		}
		if r.Topic != topic {
			t.Errorf("topic: got %q", r.Topic)
		}
		if r.Offset != 0 {
			t.Errorf("offset: got %d", r.Offset)
		}
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
}

// A handler whose failure is never settled must not advance the group past the
// record. MaxAttempts 1 with no DLQ is the "cannot settle it" configuration: the
// record is neither retried into success nor dead-lettered, so its offset must
// stay uncommitted and a fresh member must see it again.
func TestFailingHandlerDoesNotCommitTheOffset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.nocommit"
	const group = "test.nocommit"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})

	attempted := make(chan struct{}, 16)
	failing, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       group,
		Topics:      []string{topic},
		MaxAttempts: 1, // no retries, and no DLQ below ⇒ nothing can settle it
		Handler: func(_ context.Context, _ kafka.Record) error {
			select {
			case attempted <- struct{}{}:
			default:
			}
			return errors.New("transient failure")
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	go func() { _ = failing.Run(runCtx) }()

	// Wait on a signal rather than spinning on a counter (spec §12.4).
	select {
	case <-attempted:
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
	stop()
	failing.Close()

	// A fresh consumer in the same group must see the record again.
	redelivered := make(chan struct{}, 1)
	ok, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   group,
		Topics:  []string{topic},
		Handler: func(_ context.Context, _ kafka.Record) error {
			select {
			case redelivered <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("second consumer: %v", err)
	}
	defer ok.Close()
	go func() { _ = ok.Run(ctx) }()

	select {
	case <-redelivered:
	case <-ctx.Done():
		t.Fatal("record was not redelivered — the offset was committed despite the handler failing")
	}
}

// The regression test for the loss this task's design note describes: a failed
// record followed by a successful one in the same partition. Marking the second
// would advance the group past the first, and MarkCommitRecords cannot rewind.
func TestFailedRecordIsSettledBeforeTheNextInItsPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.settle_in_order"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic, topic+".dlq"); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	// One key ⇒ one partition ⇒ offsets 0 and 1, delivered in that order.
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-2"}, []byte{2})

	dlq, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("dlq producer: %v", err)
	}
	defer dlq.Close()

	seen := make(chan byte, 32)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       "test.settle_in_order",
		Topics:      []string{topic},
		DLQ:         dlq,
		MaxAttempts: 2,
		BaseBackoff: time.Millisecond,
		Handler: func(_ context.Context, r kafka.Record) error {
			seen <- r.Value[0]
			if r.Value[0] == 1 {
				return errors.New("transient failure on the first record")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer c.Close()
	go func() { _ = c.Run(ctx) }()

	// The first record is attempted twice — the retry budget — before the second
	// record is touched at all. If the second is attempted before the first has
	// exhausted its attempts, the loop moved on with an unmarked failure behind it.
	want := []byte{1, 1, 2}
	for i, w := range want {
		select {
		case got := <-seen:
			if got != w {
				t.Fatalf("attempt %d: want record %d, got %d — the loop advanced past an unsettled failure", i, w, got)
			}
		case <-ctx.Done():
			t.Fatalf("only %d of %d attempts happened", i, len(want))
		}
	}

	// The first record exhausted its budget, so it is in the DLQ rather than lost.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic+".dlq"))
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	defer cl.Close()

	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("poll dlq: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want the exhausted record in the dlq, got %d records", len(recs))
	}
	if recs[0].Value[0] != 1 {
		t.Fatalf("wrong record dead-lettered: %v", recs[0].Value)
	}
}

func TestTransientFailureIsRetriedThenSucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.retry"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})

	attempts := make(chan int, 16)
	var n int
	done := make(chan struct{}, 1)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       "test.retry",
		Topics:      []string{topic},
		MaxAttempts: 5,
		BaseBackoff: time.Millisecond,
		Handler: func(_ context.Context, _ kafka.Record) error {
			n++
			attempts <- n
			if n < 3 {
				return errors.New("provider 503")
			}
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer c.Close()
	go func() { _ = c.Run(ctx) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("the handler never succeeded — retries did not happen")
	}
	for want := 1; want <= 3; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("want attempt %d, got %d", want, got)
			}
		default:
			t.Fatalf("only %d attempts recorded, want 3", want-1)
		}
	}
}

func TestPermanentErrorRoutesToDLQWithProvenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.poison"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic, topic+".dlq"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A",
		map[string]string{"ce_id": "ce-1", "traceparent": "00-aaaa-bbbb-01"}, []byte{9})

	dlqProducer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("dlq producer: %v", err)
	}
	defer dlqProducer.Close()

	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   "test.poison",
		Topics:  []string{topic},
		DLQ:     dlqProducer,
		Handler: func(_ context.Context, _ kafka.Record) error {
			return fmt.Errorf("%w: unknown event type", kafka.ErrPermanent)
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer c.Close()
	go func() { _ = c.Run(ctx) }()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic+".dlq"))
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	defer cl.Close()

	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("poll dlq: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 dlq record, got %d", len(recs))
	}

	h := map[string]string{}
	for _, hdr := range recs[0].Headers {
		h[hdr.Key] = string(hdr.Value)
	}
	if h["ce_id"] != "ce-1" {
		t.Errorf("original headers must survive: %v", h)
	}
	if h["traceparent"] != "00-aaaa-bbbb-01" {
		t.Errorf("traceparent must survive so the failure stays traceable: %v", h)
	}
	if h["sagaflow_dlq_topic"] != topic {
		t.Errorf("want provenance topic %q, got %q", topic, h["sagaflow_dlq_topic"])
	}
	if h["sagaflow_dlq_offset"] != "0" {
		t.Errorf("want provenance offset 0, got %q", h["sagaflow_dlq_offset"])
	}
	if h["sagaflow_dlq_error"] == "" {
		t.Error("want the error recorded so an operator need not re-run it")
	}
	if string(recs[0].Key) != "seat-14A" {
		t.Errorf("dlq must preserve the original key for replay: %q", recs[0].Key)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/platform/kafka/ -run TestConsumer -v`
Expected: FAIL to build — `undefined: kafka.NewConsumer`.

- [ ] **Step 3: Implement `consumer.go`**

```go
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrPermanent marks a handler error the message can never recover from — an
// undecodable payload, an unknown event type. Wrap with it to route straight to
// the DLQ with no retries (spec §10.2).
var ErrPermanent = errors.New("permanent failure")

// RebalanceTimeout must exceed the slowest handler transaction (spec §10.3),
// otherwise a long-running handler has its partitions pulled mid-flight.
const RebalanceTimeout = 60 * time.Second

const (
	// DefaultMaxAttempts bounds retries of one record, including the first try.
	//
	// The whole budget has to fit inside RebalanceTimeout: BlockRebalanceOnPoll
	// means a rebalance cannot proceed while a batch is being retried. Five
	// attempts at DefaultBaseBackoff is roughly 1.5 s of backoff against 60 s.
	DefaultMaxAttempts = 5
	// DefaultBaseBackoff is the first retry delay; it doubles each attempt and
	// carries jitter (spec §10.2).
	DefaultBaseBackoff = 100 * time.Millisecond
)

// Record is one consumed message.
type Record struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
}

// Handler processes one record. Returning nil means "committed, safe to advance".
//
// A returned error is retried with backoff up to MaxAttempts and then
// dead-lettered; an error wrapping ErrPermanent skips the retries and
// dead-letters immediately. Business outcomes are not errors — a handler that
// decides "nothing to do" returns nil, per spec §10.2's retry policy.
type Handler func(ctx context.Context, r Record) error

type ConsumerConfig struct {
	Brokers []string
	Group   string
	Topics  []string
	Handler Handler
	// DLQ publishes settled failures to <topic>.dlq. Optional, but a consumer
	// without one cannot settle a failing record: it stops advancing that
	// partition instead, which is safe but stalls. Only leave it nil in tests.
	DLQ *Producer
	// MaxAttempts and BaseBackoff override DefaultMaxAttempts and
	// DefaultBaseBackoff. Tests set them small; services leave them zero.
	MaxAttempts int
	BaseBackoff time.Duration
}

type Consumer struct {
	cl  *kgo.Client
	cfg ConsumerConfig
	log *slog.Logger
}

// NewConsumer builds a consumer group whose offsets advance only for records the
// handler finished.
//
// The three options below are the difference between at-least-once and silent
// loss, and none of them is franz-go's default:
//
//   - AutoCommitMarks: commit only explicitly marked offsets. The default commits
//     every polled offset on a timer, including records still in flight.
//   - BlockRebalanceOnPoll: no rebalance between polling and marking, so a
//     partition cannot be reassigned while its records are mid-handler.
//   - OnPartitionsRevoked → CommitMarkedOffsets: flush what was finished before
//     losing the partitions, so completed work is not reprocessed.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Handler == nil {
		return nil, errors.New("kafka: consumer needs a handler")
	}
	if cfg.Group == "" {
		return nil, errors.New("kafka: consumer needs a group")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = DefaultBaseBackoff
	}
	c := &Consumer{cfg: cfg, log: slog.Default().With("group", cfg.Group)}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(RebalanceTimeout),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				c.log.Error("commit on revoke failed", "error", err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	c.cl = cl
	return c, nil
}

func (c *Consumer) Close() { c.cl.Close() }

// topicPartition identifies one partition within one batch.
type topicPartition struct {
	topic     string
	partition int32
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollRecords(ctx, 100)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			// Fetch errors are retried internally; a surfaced one is worth seeing
			// but is not fatal to the loop.
			c.log.Error("fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
		}

		// stalled holds the partitions whose current record could not be settled.
		// Once a partition is stalled, the rest of its records in this batch are
		// left untouched: marking a later offset would advance the group past the
		// unsettled one, and MarkCommitRecords keeps the highest offset per
		// partition and cannot rewind. Other partitions keep flowing, so one
		// stuck stream does not stop the others.
		stalled := make(map[topicPartition]bool)
		fetches.EachRecord(func(kr *kgo.Record) {
			tp := topicPartition{kr.Topic, kr.Partition}
			if stalled[tp] {
				return
			}
			if !c.handle(ctx, kr) {
				stalled[tp] = true
			}
		})

		// Required with BlockRebalanceOnPoll: without this the group never
		// rebalances and a new member waits forever.
		c.cl.AllowRebalance()
	}
}

// handle settles one record and reports whether its offset was marked.
//
// false means the record is unsettled: nothing later in its partition may be
// marked, and the record will be redelivered after the next rebalance or restart.
func (c *Consumer) handle(ctx context.Context, kr *kgo.Record) (marked bool) {
	headers := make(map[string]string, len(kr.Headers))
	for _, h := range kr.Headers {
		headers[h.Key] = string(h.Value)
	}
	r := Record{
		Topic:     kr.Topic,
		Key:       string(kr.Key),
		Value:     kr.Value,
		Headers:   headers,
		Partition: kr.Partition,
		Offset:    kr.Offset,
	}

	var err error
	for attempt := 1; ; attempt++ {
		err = c.cfg.Handler(ctx, r)
		if err == nil {
			// Mark only now. Ordering matters: crash after the handler's commit but
			// before the mark means redelivery, which the inbox absorbs. The
			// reverse would be loss.
			c.cl.MarkCommitRecords(kr)
			return true
		}
		if errors.Is(err, ErrPermanent) || attempt >= c.cfg.MaxAttempts {
			break
		}
		// Bounded exponential backoff with jitter (spec §10.2): each wait is
		// uniform in [backoff, 2·backoff), so a fleet of consumers retrying the
		// same downstream outage spreads out instead of retrying in lockstep.
		backoff := c.cfg.BaseBackoff << (attempt - 1)
		wait := time.Duration(rand.Int64N(int64(backoff)) + int64(backoff))
		c.log.Warn("handler failed; retrying",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
			"attempt", attempt, "backoff", wait, "error", err)
		select {
		case <-ctx.Done():
			return false // shutting down: leave it unsettled and redeliverable
		case <-time.After(wait):
		}
	}

	// The failure is settled by dead-lettering it: either it was permanent from
	// the start, or it outlived a bounded retry budget and is therefore no longer
	// a transient failure. Either way the partition must not be held hostage —
	// blocking it on one poison message stalls every other stream sharing it
	// (spec §10.2).
	if derr := c.deadLetter(ctx, r, err); derr != nil {
		c.log.Error("dead-letter failed; partition will not advance",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
			"error", derr, "cause", err)
		return false
	}
	c.cl.MarkCommitRecords(kr)
	return true
}

func (c *Consumer) deadLetter(ctx context.Context, r Record, cause error) error {
	if c.cfg.DLQ == nil {
		// Deliberately an error, not a silent drop. Without a DLQ there is nowhere
		// to put the record, and marking it anyway would discard it — so the
		// partition stalls and an operator has to notice.
		return fmt.Errorf("kafka: no DLQ configured, cannot settle: %w", cause)
	}
	headers := make(map[string]string, len(r.Headers)+4)
	for k, v := range r.Headers {
		headers[k] = v // original headers and traceparent survive
	}
	headers["sagaflow_dlq_topic"] = r.Topic
	headers["sagaflow_dlq_partition"] = strconv.Itoa(int(r.Partition))
	headers["sagaflow_dlq_offset"] = strconv.FormatInt(r.Offset, 10)
	headers["sagaflow_dlq_error"] = cause.Error()

	return c.cfg.DLQ.Publish(ctx, []outbox.Claimed{{
		Message: outbox.Message{
			Topic:   r.Topic + ".dlq",
			Key:     r.Key, // same key so a replay lands on the same partition
			Payload: r.Value,
			Headers: headers,
		},
	}})
}
```

- [ ] **Step 4: Run the consumer tests**

Run: `go test ./internal/platform/kafka/ -race -v -timeout 20m`
Expected: all PASS.

If `TestFailingHandlerDoesNotCommitTheOffset` fails because the record is not redelivered, `AutoCommitMarks()` is missing and the default autocommit advanced the group — which is exactly the bug this phase exists to prevent.

If `TestFailedRecordIsSettledBeforeTheNextInItsPartition` reports that record 2 was attempted before record 1 exhausted its attempts, `handle` is returning to the poll loop with an unsettled record — either the retry loop is missing or `Run` is ignoring `handle`'s return value. Either way the group would advance past record 1 the moment record 2 succeeded.

If any consumer test hangs, `AllowRebalance()` is missing from the poll loop.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/kafka
git commit -m "feat(kafka): marked-offset consumer with DLQ routing and provenance"
```

---

### Task 4: The deliverable — exactly-once across two services

**Files:**
- Create: `internal/platform/delivery_test.go`

**Interfaces:**
- Consumes: everything from Phases 1–4b.
- Produces: no new API. This task proves the phase.

This is the test spec §13 names as the end of phase 4: *an event travelling from one service's transaction to another service's handler, applied exactly once.* Two databases in one container — separate databases, so no transaction can span them, which is the property being demonstrated.

- [ ] **Step 1: Write the test**

Create `internal/platform/delivery_test.go`:

```go
package platform_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingmigrations "github.com/kptac/sagaflow/internal/booking/migrations"
	inventorymigrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	inventoryv1 "github.com/kptac/sagaflow/internal/platform/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/platform/codec"
	"github.com/kptac/sagaflow/internal/platform/envelope"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/inbox"
	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/kafkatest"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/platform/pgtest"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// One Postgres and one Kafka for the whole package (spec §12.4). Both tests
// below stand up their own databases and topics inside them.
func TestMain(m *testing.M) {
	stopPG, err := pgtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	stopKafka, err := kafkatest.Start()
	if err != nil {
		stopPG()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stopKafka()
	stopPG()
	os.Exit(code)
}

const (
	eventsTopic  = "delivery.inventory.events"
	sagaConsumer = "booking.saga"
	source       = "/sagaflow/inventory"
)

func db(t *testing.T, p *pgtest.PG, name string, schema fs.FS) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := p.DSN(t, name)
	if err := pg.Migrate(ctx, dsn, schema); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// bookingHandler is the shape every real handler will take (spec §7.2): dedupe,
// load, decide, append, enqueue — one transaction, one stream.
func bookingHandler(pool *pgxpool.Pool, applied chan<- string) kafka.Handler {
	return func(ctx context.Context, r kafka.Record) error {
		env, err := envelope.Parse(r.Headers)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		var wrote bool
		if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			wrote = false
			fresh, err := inbox.MarkConsumed(ctx, tx, sagaConsumer, env.Source, env.ID)
			if err != nil {
				return err
			}
			if !fresh {
				return nil // already applied; commit nothing, ack
			}

			streamID := "saga-" + env.Subject
			existing, err := eventstore.Load(ctx, tx, streamID)
			if err != nil {
				return err
			}
			ev, err := codec.Encode(&inventoryv1.SeatHeld{
				HoldId: env.ID, SeatId: env.Subject,
			}, eventstore.Meta{CorrelationID: env.CorrelationID, CausationID: env.ID})
			if err != nil {
				return err
			}
			if err := eventstore.Append(ctx, tx, streamID, len(existing), []eventstore.Event{ev}); err != nil {
				return err
			}
			wrote = true
			return nil
		}); err != nil {
			return err
		}
		// Signalled after the commit, not inside it. Signalling from inside the
		// transaction would tell the test "applied" for work a failed commit then
		// threw away.
		if wrote {
			select {
			case applied <- env.ID:
			default:
			}
		}
		return nil
	}
}

func TestEventCrossesServicesExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container := pgtest.Shared(t)
	inventoryDB := db(t, container, "delivery_inventory", inventorymigrations.FS)
	bookingDB := db(t, container, "delivery_booking", bookingmigrations.FS)

	brokers := kafkatest.Shared(t).Brokers()
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, eventsTopic, eventsTopic+".dlq"); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// --- inventory side: one transaction writes the stream and the outbox row ---
	const seat = "seat-BA117-2026-09-01-14A"
	ceID := envelope.NewID()

	held := &inventoryv1.SeatHeld{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    seat,
		ExpiresAt: timestamppb.New(time.Now().Add(15 * time.Minute)),
	}
	storedEvent, err := codec.Encode(held, eventstore.Meta{TraceID: "trace-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	env := envelope.Envelope{
		ID: ceID, Source: source, Type: storedEvent.Type, Subject: seat,
		CorrelationID: "saga-booking-1",
	}
	// The wire payload here is the stored protojson rather than registry-framed
	// protobuf: framing is exercised in Phase 3b's serde tests, and keeping this
	// test registry-free proves delivery does not depend on the registry.
	wire := storedEvent.Data

	if err := pg.WithTx(ctx, inventoryDB, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, seat, 0, []eventstore.Event{storedEvent}); err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, []outbox.Message{{
			Topic: eventsTopic, Key: seat, Payload: wire, Headers: env.Headers(),
		}})
	}); err != nil {
		t.Fatalf("inventory handler: %v", err)
	}

	// --- booking side: consume and apply ---
	applied := make(chan string, 8)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   sagaConsumer,
		Topics:  []string{eventsTopic},
		Handler: bookingHandler(bookingDB, applied),
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()
	go func() { _ = consumer.Run(ctx) }()

	// --- publish what inventory committed ---
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	n, err := outbox.NewPoller(inventoryDB, producer).Drain(ctx)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 message published, got %d", n)
	}

	select {
	case got := <-applied:
		if got != ceID {
			t.Fatalf("applied the wrong event: want %s, got %s", ceID, got)
		}
	case <-ctx.Done():
		t.Fatal("booking never applied the event")
	}

	assertBookingStreamLength(t, bookingDB, "saga-"+seat, 1)

	// --- duplicate delivery: same ce_id produced again changes nothing ---
	if err := producer.Publish(ctx, []outbox.Claimed{{
		Message: outbox.Message{Topic: eventsTopic, Key: seat, Payload: wire, Headers: env.Headers()},
	}}); err != nil {
		t.Fatalf("republish: %v", err)
	}

	// No sleeping: send a second, distinct event and wait for it. Once that has
	// been applied, the duplicate ahead of it in the same partition has certainly
	// been processed, because one partition is handled in order.
	secondID := envelope.NewID()
	second := env
	second.ID = secondID
	if err := producer.Publish(ctx, []outbox.Claimed{{
		Message: outbox.Message{Topic: eventsTopic, Key: seat, Payload: wire, Headers: second.Headers()},
	}}); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	for {
		select {
		case got := <-applied:
			if got == secondID {
				goto drained
			}
			if got == ceID {
				t.Fatal("the duplicate was applied a second time — the inbox did not deduplicate")
			}
		case <-ctx.Done():
			t.Fatal("second event never applied")
		}
	}
drained:

	// One append from the first event, one from the second, none from the duplicate.
	assertBookingStreamLength(t, bookingDB, "saga-"+seat, 2)
	assertInboxRows(t, bookingDB, 2)
}

func assertBookingStreamLength(t *testing.T, pool *pgxpool.Pool, streamID string, want int) {
	t.Helper()
	ctx := context.Background()
	var got int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE stream_id = $1`, streamID).Scan(&got); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if got != want {
		t.Fatalf("stream %s: want %d events, got %d", streamID, want, got)
	}
}

func assertInboxRows(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	ctx := context.Background()
	var got int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox`).Scan(&got); err != nil {
		t.Fatalf("count inbox: %v", err)
	}
	if got != want {
		t.Fatalf("want %d inbox rows, got %d", want, got)
	}
}

// TestRebalanceMidHandlerLosesNothing is the test that would have caught
// franz-go's default autocommit. A second consumer joins the group while the
// first is inside a handler transaction; every event must still be applied
// exactly once. Loss here is invisible without the assertion: offsets look
// healthy and the events are simply gone.
func TestRebalanceMidHandlerLosesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	container := pgtest.Shared(t)
	bookingDB := db(t, container, "rebalance_booking", bookingmigrations.FS)

	brokers := kafkatest.Shared(t).Brokers()
	const topic = "delivery.rebalance"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// One key per event so records spread across partitions and a rebalance
	// actually moves work between members.
	const total = 60
	ids := make([]string, total)
	var batch []outbox.Claimed
	for i := range total {
		id := envelope.NewID()
		ids[i] = id
		e := envelope.Envelope{
			ID: id, Source: source, Type: "sagaflow.inventory.v1.SeatHeld",
			Subject: fmt.Sprintf("seat-%03d", i),
		}
		batch = append(batch, outbox.Claimed{Message: outbox.Message{
			Topic: topic, Key: e.Subject, Payload: []byte(`{}`), Headers: e.Headers(),
		}})
	}
	if err := producer.Publish(ctx, batch); err != nil {
		t.Fatalf("publish: %v", err)
	}

	applied := make(chan string, total*2)
	slowFirst := true
	newMember := func(slow bool) *kafka.Consumer {
		c, err := kafka.NewConsumer(kafka.ConsumerConfig{
			Brokers: brokers,
			Group:   "test.rebalance",
			Topics:  []string{topic},
			Handler: func(ctx context.Context, r kafka.Record) error {
				if slow {
					// Hold the handler open so the rebalance lands mid-transaction.
					time.Sleep(150 * time.Millisecond)
				}
				env, err := envelope.Parse(r.Headers)
				if err != nil {
					return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
				}
				var wrote bool
				if err := pg.WithTx(ctx, bookingDB, func(tx pgx.Tx) error {
					wrote = false
					fresh, err := inbox.MarkConsumed(ctx, tx, sagaConsumer, env.Source, env.ID)
					if err != nil || !fresh {
						return err
					}
					ev, err := codec.Encode(&inventoryv1.SeatHeld{SeatId: env.Subject}, eventstore.Meta{})
					if err != nil {
						return err
					}
					existing, err := eventstore.Load(ctx, tx, env.Subject)
					if err != nil {
						return err
					}
					if err := eventstore.Append(ctx, tx, env.Subject, len(existing),
						[]eventstore.Event{ev}); err != nil {
						return err
					}
					wrote = true
					return nil
				}); err != nil {
					return err
				}
				// After the commit: the count below must only include work that
				// actually landed in the database.
				if wrote {
					applied <- env.ID
				}
				return nil
			},
		})
		if err != nil {
			t.Fatalf("consumer: %v", err)
		}
		return c
	}

	first := newMember(slowFirst)
	defer first.Close()
	go func() { _ = first.Run(ctx) }()

	// Let the first member get into its handlers, then force a rebalance by
	// adding a second member to the group.
	seen := map[string]bool{}
	seen[<-applied] = true // count it: the inbox will never let it be re-signalled

	second := newMember(false)
	defer second.Close()
	go func() { _ = second.Run(ctx) }()

	for len(seen) < total {
		select {
		case id := <-applied:
			seen[id] = true
		case <-ctx.Done():
			t.Fatalf("only %d/%d events applied — the rebalance lost work", len(seen), total)
		}
	}

	var events, inboxRows int
	if err := bookingDB.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if err := bookingDB.QueryRow(ctx, `SELECT count(*) FROM inbox`).Scan(&inboxRows); err != nil {
		t.Fatalf("count inbox: %v", err)
	}
	if events != total {
		t.Fatalf("want exactly %d events, got %d — duplicates were applied", total, events)
	}
	if inboxRows != total {
		t.Fatalf("want exactly %d inbox rows, got %d", total, inboxRows)
	}
}
```

- [ ] **Step 2: Run the deliverable**

Run: `go test ./internal/platform/ -run 'TestEventCrossesServicesExactlyOnce' -race -v -timeout 10m`
Expected: PASS.

Failure diagnosis:
- *"booking never applied the event"* — the poller published but the consumer did not receive. Check `EnsureTopics` ran for the same topic name the producer used.
- *"the duplicate was applied a second time"* — `MarkConsumed` is not the first statement in the transaction, or its result is being ignored.
- *stream length 3 instead of 2* — the duplicate produced an append, meaning the handler proceeded past a `fresh == false`.

- [ ] **Step 3: Run the rebalance test**

Run: `go test ./internal/platform/ -run TestRebalanceMidHandlerLosesNothing -race -v -timeout 10m`
Expected: PASS.

If it reports fewer than 60 applied, the offset was committed for records that had not been processed — check `AutoCommitMarks()` and that `MarkCommitRecords` is called only after `pg.WithTx` returns nil.

- [ ] **Step 4: Run the entire suite, fast and full**

```bash
make test              # must be quick: no containers
make test-integration  # everything
```
Expected: both PASS. `make test` finishing in seconds is the signal that the unit and integration split held.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/delivery_test.go
git commit -m "test(platform): event crosses two services exactly once, survives rebalance"
```

- [ ] **Step 6: Verify the phase-4 deliverable against the spec**

Re-read spec §13 phase 4 and confirm each clause has a passing test:

| Spec clause | Test |
|---|---|
| claim-based poller | `TestDrainPublishesInIDOrderAndMarks` (Phase 4a) |
| advisory-lock election | `TestTryElectAllowsOnlyOnePoller` (Phase 4a) |
| published-row pruning | `TestPruneDeletesOldPublishedRowsOnly` (Phase 4a) |
| dedup via `ON CONFLICT` | `TestDuplicateLeavesTheTransactionUsable` |
| marked-offset consumer | `TestFailingHandlerDoesNotCommitTheOffset`, `TestFailedRecordIsSettledBeforeTheNextInItsPartition` |
| retry policy by class (§10.2) | `TestTransientFailureIsRetriedThenSucceeds`, `TestPermanentErrorRoutesToDLQWithProvenance` |
| event crosses services, exactly once | `TestEventCrossesServicesExactlyOnce` |

Report any clause without a test rather than adding one silently — a gap here means the spec and the plan disagree, which is worth a conversation.

---

## Phase Complete

Spec §13 phases 1–4 are done. The next plan set covers phases 5–8: inventory's seat streams and TTL timers, hotel and payment with the provider stub and idempotency keys, the saga's `Decide` and compensation matrix, and the booking API with its projection.
