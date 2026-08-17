# SagaFlow Phase 4a — Transactional Outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make "the state changed" and "the message was sent" the same commit, then publish those messages from a claim-based poller that cannot lose a row and cannot reorder a stream.

**Architecture:** `Enqueue` writes rows in the caller's transaction, so a rolled-back handler publishes nothing and a committed handler cannot fail to publish. A single elected poller claims unpublished rows with `FOR UPDATE SKIP LOCKED`, hands them to a `Publisher`, and marks them — claiming by flag rather than by cursor, because `BIGSERIAL` commits out of order and a cursor would silently skip late-committing rows. The `Publisher` is an interface, so every guarantee in this phase is tested without Kafka; the real Kafka producer arrives in Phase 4b.

**Tech Stack:** Go 1.26.6, Postgres 18.6, pgx v5.10.0.

**Spec:** [docs/superpowers/specs/2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) — §6.4 (why no cursor), §10.1 (outbox), §13 phase 4

**Plan sequence:** this is plan 5 of 6. See [README.md](README.md). **Depends on Phase 1** (`pg`, `pgtest`) and **Phase 2** (`eventstore.Append`, service migrations). Phase 4b depends on this one.

**Deliverable that ends this phase:** a handler that rolls back publishes nothing; a handler that commits publishes exactly its rows in `id` order; a publish failure leaves the rows claimable; and two pollers against one database result in one active poller.

## Global Constraints

Copied verbatim from spec §5 and §3. Every task's requirements implicitly include this section.

- **Go 1.26.6.** Module path `github.com/kptac/sagaflow`. **Add no dependency not listed in spec §5.**
- **One transaction writes exactly one stream** (spec §7.2), plus its outbox rows and its inbox row. Never two streams.
- **`global_seq` is diagnostic only** (spec §6.4). No component may read events by a monotonic local cursor — `BIGSERIAL` values are assigned at insert but become visible at commit, so transaction A can take 41 and commit after B takes 42. A consumer tracking `WHERE global_seq > cursor` reads 42, advances, and never sees 41. It loses events silently, only under concurrency, so it passes every test written on a quiet machine.
- **The outbox guarantees at-least-once, never exactly-once** (spec §10.1). A crash between produce and mark republishes. This is why the inbox exists, and it must not be "fixed" here.
- **The Kafka record key is the stream id** (spec §10.1), which is what gives per-stream ordering.
- **One active poller per service**, elected with `pg_try_advisory_lock`, so two instances cannot reorder a stream. `SKIP LOCKED` is for safety during failover, not parallelism (spec §10.1).
- **Published rows are deleted after 7 days** (spec §10.1/§10.3). The partial index keeps the *query* cheap at any table size, which is exactly what makes an unbounded table easy to miss.
- **Outbox poll is `NOTIFY`-driven with a 1 s floor** (spec §10.3).
- **`ce_id` and timing are application-supplied, never `DEFAULT now()`** (spec §10.5), so tests control them. Only diagnostic columns such as `published_at` use the database clock.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/inventory/migrations/002_outbox.sql` | Inventory's outbox table and partial index |
| `internal/booking/migrations/002_outbox.sql` | Booking's, identical, separate database |
| `internal/platform/outbox/outbox.go` | `Message`, `Enqueue`, the `Publisher` interface |
| `internal/platform/outbox/poller.go` | `Poller`, `Drain`, advisory-lock election, `Prune`, `Run` |
| `internal/platform/outbox/outbox_test.go` | Transactionality, ordering, failure retention, election, pruning |

`Drain` is exported so tests drive one claim-publish-mark cycle synchronously. `Run` is the loop that calls it, and tests never rely on `Run`'s timing — spec §12.4 forbids `time.Sleep` in assertions.

---

## Phase 4a Tasks

### Task 1: Outbox table and transactional `Enqueue`

**Files:**
- Create: `internal/inventory/migrations/002_outbox.sql`, `internal/booking/migrations/002_outbox.sql`, `internal/platform/outbox/outbox.go`, `internal/platform/outbox/outbox_test.go`

**Interfaces:**
- Consumes: `pg.WithTx`, `pg.Migrate`, `pgtest` from Phase 1; `eventstore.Append` from Phase 2.
- Produces:
  - `type Message struct { Topic, Key string; Payload []byte; Headers map[string]string }`
  - `type Claimed struct { ID int64; Message }`
  - `type Publisher interface { Publish(ctx context.Context, msgs []Claimed) error }`
  - `func Enqueue(ctx context.Context, tx pgx.Tx, msgs []Message) error`
  - `const NotifyChannel = "outbox"`

- [ ] **Step 1: Write `internal/inventory/migrations/002_outbox.sql`**

```sql
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    headers      JSONB NOT NULL,
    published_at TIMESTAMPTZ
);

-- Partial index: the poller only ever asks for unpublished rows, so the index
-- stays the size of the backlog rather than the size of history.
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

COMMENT ON COLUMN outbox.key IS
    'Kafka partition key — always the stream id, which is what preserves per-stream ordering.';

---- create above / drop below ----

DROP TABLE outbox;
```

- [ ] **Step 2: Copy it to booking and verify both apply**

```bash
cp internal/inventory/migrations/002_outbox.sql internal/booking/migrations/002_outbox.sql
ls internal/inventory/migrations internal/booking/migrations
```

Expected: each directory holds `001_events.sql`, `002_outbox.sql` and `migrations.go`.

- [ ] **Step 3: Write the failing test**

Create `internal/platform/outbox/outbox_test.go`:

```go
package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	migrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/platform/pgtest"
)

// One container for the package (spec §12.4). Note this makes the advisory-lock
// election test meaningful for the right reason: advisory locks are scoped to a
// database, and every test here gets its own database inside the one cluster.
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

func msg(topic, key string) outbox.Message {
	return outbox.Message{
		Topic:   topic,
		Key:     key,
		Payload: []byte{0x00, 0x01, 0x02},
		Headers: map[string]string{"ce_id": "id-" + key, "ce_type": "T"},
	}
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	q := "SELECT count(*) FROM outbox"
	if where != "" {
		q += " WHERE " + where
	}
	if err := pool.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestEnqueueCommitsWithTheHandler(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "outbox_commit")

	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{
			{Type: "sagaflow.inventory.v1.SeatHeld", Data: []byte(`{}`)},
		}); err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, []outbox.Message{msg("inventory.events", "seat-14A")})
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	if got := countOutbox(t, pool, ""); got != 1 {
		t.Fatalf("want 1 outbox row, got %d", got)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 1 {
		t.Fatalf("want the row unpublished, got %d unpublished", got)
	}
}

// This is the test that proves the outbox pattern rather than the outbox table:
// a handler that fails must not leave a message behind to be published.
func TestEnqueueRollsBackWithTheHandler(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "outbox_rollback")

	sentinel := errors.New("handler failed after enqueue")
	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{
			{Type: "sagaflow.inventory.v1.SeatHeld", Data: []byte(`{}`)},
		}); err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, []outbox.Message{msg("inventory.events", "seat-14A")}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}

	if got := countOutbox(t, pool, ""); got != 0 {
		t.Fatalf("want 0 outbox rows after rollback, got %d", got)
	}
	var events int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 0 {
		t.Fatalf("want 0 events after rollback, got %d", events)
	}
}

func TestEnqueuePreservesOrderAndHeaders(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "outbox_order")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, []outbox.Message{
			msg("inventory.events", "seat-14A"),
			msg("inventory.events", "seat-14B"),
			msg("inventory.commands", "seat-14C"),
		})
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rows, err := pool.Query(ctx, `SELECT topic, key, payload, headers FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		topic, key string
		payload    []byte
		headers    map[string]string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.topic, &r.key, &r.payload, &r.headers); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	wantKeys := []string{"seat-14A", "seat-14B", "seat-14C"}
	for i, k := range wantKeys {
		if got[i].key != k {
			t.Fatalf("row %d: want key %s, got %s", i, k, got[i].key)
		}
	}
	if got[2].topic != "inventory.commands" {
		t.Fatalf("want third row on inventory.commands, got %s", got[2].topic)
	}
	if got[0].headers["ce_id"] != "id-seat-14A" {
		t.Fatalf("headers did not round-trip: %v", got[0].headers)
	}
	if len(got[0].payload) != 3 || got[0].payload[0] != 0x00 {
		t.Fatalf("payload did not round-trip: %v", got[0].payload)
	}
}

func TestEnqueueEmptyIsANoOp(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "outbox_empty")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, nil)
	}); err != nil {
		t.Fatalf("enqueue nil: %v", err)
	}
	if got := countOutbox(t, pool, ""); got != 0 {
		t.Fatalf("want 0 rows, got %d", got)
	}
}

func TestEnqueueRejectsAMessageWithNoKey(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "outbox_nokey")

	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, []outbox.Message{{Topic: "t", Payload: []byte{1}}})
	})
	if err == nil {
		t.Fatal("want an error for a keyless message, got nil")
	}
}
```

A keyless message must be rejected loudly: Kafka would round-robin it across partitions, which silently destroys the per-stream ordering everything downstream assumes. Failing at enqueue makes that a bug in one handler rather than an intermittent ordering anomaly.

- [ ] **Step 4: Run to verify failure**

Run: `go test ./internal/platform/outbox/ -v`
Expected: FAIL to build — `undefined: outbox.Enqueue`, `undefined: outbox.Message`.

- [ ] **Step 5: Implement `outbox.go`**

```go
// Package outbox makes "the state changed" and "the message was sent" the same
// commit.
//
// Enqueue writes into the caller's transaction; a separate poller publishes what
// was committed. The guarantee is at-least-once and deliberately not
// exactly-once — a crash between publishing and marking republishes, which is
// precisely why platform/inbox exists (spec §10.1).
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// NotifyChannel is the LISTEN/NOTIFY channel the poller wakes on.
const NotifyChannel = "outbox"

// Message is one message to publish.
//
// Key is always the target stream id: it becomes the Kafka partition key, which
// is what keeps two events for one stream in order.
type Message struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// Claimed is a Message plus the row id the poller must mark once it is published.
type Claimed struct {
	ID int64
	Message
}

// Publisher sends claimed messages. It is an interface so that every property of
// the poller — ordering, retention on failure, election — is testable without a
// broker. The Kafka implementation lives in platform/kafka.
type Publisher interface {
	Publish(ctx context.Context, msgs []Claimed) error
}

const enqueueSQL = `
INSERT INTO outbox (topic, key, payload, headers)
SELECT t.topic, t.key, t.payload, t.headers
FROM unnest($1::text[], $2::text[], $3::bytea[], $4::jsonb[])
     WITH ORDINALITY AS t(topic, key, payload, headers, ord)
ORDER BY t.ord`

// Enqueue writes msgs into tx. It must be called in the same transaction as the
// state change it accompanies; that co-commit is the entire point.
//
// The NOTIFY is transactional — Postgres delivers it on commit and discards it on
// rollback — so a woken poller never chases a message that was never written.
func Enqueue(ctx context.Context, tx pgx.Tx, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	topics := make([]string, len(msgs))
	keys := make([]string, len(msgs))
	payloads := make([][]byte, len(msgs))
	headers := make([]string, len(msgs))

	for i, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("outbox: message %d has no topic", i)
		}
		if m.Key == "" {
			// Without a key Kafka round-robins the record, which destroys the
			// per-stream ordering every consumer downstream relies on.
			return fmt.Errorf("outbox: message %d for %s has no key", i, m.Topic)
		}
		if len(m.Payload) == 0 {
			return fmt.Errorf("outbox: message %d for %s has no payload", i, m.Topic)
		}
		h, err := json.Marshal(m.Headers)
		if err != nil {
			return fmt.Errorf("outbox: marshal headers for %s: %w", m.Topic, err)
		}
		topics[i], keys[i], payloads[i], headers[i] = m.Topic, m.Key, m.Payload, string(h)
	}

	if _, err := tx.Exec(ctx, enqueueSQL, topics, keys, payloads, headers); err != nil {
		return fmt.Errorf("outbox: enqueue %d messages: %w", len(msgs), err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, '')", NotifyChannel); err != nil {
		return fmt.Errorf("outbox: notify: %w", err)
	}
	return nil
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/platform/outbox/ -race -v`
Expected: all six PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/inventory/migrations internal/booking/migrations internal/platform/outbox
git commit -m "feat(outbox): transactional Enqueue with NOTIFY on commit"
```

---

### Task 2: The claim-based poller

**Files:**
- Create: `internal/platform/outbox/poller.go`
- Modify: `internal/platform/outbox/outbox_test.go`

**Interfaces:**
- Consumes: `Message`, `Claimed`, `Publisher`, `NotifyChannel` from Task 1.
- Produces:
  - `type Poller struct { … }`
  - `func NewPoller(pool *pgxpool.Pool, pub Publisher) *Poller`
  - `func (p *Poller) Drain(ctx context.Context) (int, error)` — one claim-publish-mark cycle, returns rows published
  - `func (p *Poller) Prune(ctx context.Context, olderThan time.Duration) (int64, error)`
  - `func (p *Poller) TryElect(ctx context.Context) (held bool, release func(), err error)`
  - `func (p *Poller) Run(ctx context.Context) error` — elect, then loop on NOTIFY with a 1 s floor
  - `const BatchSize = 100`, `const AdvisoryLockKey = 0x5A6A_0001`

- [ ] **Step 1: Write the failing tests**

Append to `internal/platform/outbox/outbox_test.go`:

```go
type fakePublisher struct {
	mu   sync.Mutex
	got  [][]outbox.Claimed
	err  error
	call int
}

func (f *fakePublisher) Publish(_ context.Context, msgs []outbox.Claimed) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.call++
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, msgs)
	return nil
}

func (f *fakePublisher) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, batch := range f.got {
		for _, m := range batch {
			out = append(out, m.Key)
		}
	}
	return out
}

func enqueue(t *testing.T, pool *pgxpool.Pool, keys ...string) {
	t.Helper()
	ctx := context.Background()
	msgs := make([]outbox.Message, len(keys))
	for i, k := range keys {
		msgs[i] = msg("inventory.events", k)
	}
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, msgs)
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

func TestDrainPublishesInIDOrderAndMarks(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "poller_order")
	enqueue(t, pool, "seat-14A", "seat-14B", "seat-14C")

	pub := &fakePublisher{}
	n, err := outbox.NewPoller(pool, pub).Drain(ctx)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 3 {
		t.Fatalf("want 3 published, got %d", n)
	}

	want := []string{"seat-14A", "seat-14B", "seat-14C"}
	got := pub.keys()
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}

	if left := countOutbox(t, pool, "published_at IS NULL"); left != 0 {
		t.Fatalf("want 0 unpublished after drain, got %d", left)
	}
}

func TestDrainIsANoOpWhenNothingIsPending(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "poller_empty")

	pub := &fakePublisher{}
	n, err := outbox.NewPoller(pool, pub).Drain(ctx)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 published, got %d", n)
	}
	if pub.call != 0 {
		t.Fatalf("publisher must not be called with an empty batch, got %d calls", pub.call)
	}
}

// A publish failure must leave the rows claimable. Marking them anyway would
// lose the message permanently — the one failure this design must never have.
func TestDrainLeavesRowsUnpublishedWhenPublishFails(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "poller_failure")
	enqueue(t, pool, "seat-14A", "seat-14B")

	boom := errors.New("broker unreachable")
	pub := &fakePublisher{err: boom}
	p := outbox.NewPoller(pool, pub)

	if _, err := p.Drain(ctx); !errors.Is(err, boom) {
		t.Fatalf("want the publisher's error, got %v", err)
	}
	if left := countOutbox(t, pool, "published_at IS NULL"); left != 2 {
		t.Fatalf("want both rows still unpublished, got %d", left)
	}

	// The next pass, with a working broker, publishes them.
	pub.err = nil
	n, err := p.Drain(ctx)
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 published on retry, got %d", n)
	}
}

// Spec §10.1: the outbox is at-least-once. This test asserts the duplicate
// rather than treating it as a defect, so nobody later "fixes" it by marking
// rows before the publish succeeds.
func TestRepublishAfterAMarkFailureIsADuplicateNotALoss(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "poller_at_least_once")
	enqueue(t, pool, "seat-14A")

	pub := &fakePublisher{}
	p := outbox.NewPoller(pool, pub)
	if _, err := p.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Simulate a crash between "produced to Kafka" and "marked published".
	if _, err := pool.Exec(ctx, "UPDATE outbox SET published_at = NULL"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := p.Drain(ctx); err != nil {
		t.Fatalf("second drain: %v", err)
	}

	if got := pub.keys(); len(got) != 2 {
		t.Fatalf("want the message published twice (at-least-once), got %d", len(got))
	}
}

func TestPruneDeletesOldPublishedRowsOnly(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "poller_prune")
	enqueue(t, pool, "seat-old", "seat-recent", "seat-pending")

	p := outbox.NewPoller(pool, &fakePublisher{})

	// Publish the first two, then age one of them past the retention window.
	if _, err := pool.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE key IN ('seat-old', 'seat-recent')`); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE outbox SET published_at = now() - interval '30 days' WHERE key = 'seat-old'`); err != nil {
		t.Fatalf("age: %v", err)
	}

	deleted, err := p.Prune(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("want 1 row pruned, got %d", deleted)
	}
	if got := countOutbox(t, pool, "key = 'seat-pending'"); got != 1 {
		t.Fatal("prune must never delete an unpublished row")
	}
	if got := countOutbox(t, pool, "key = 'seat-recent'"); got != 1 {
		t.Fatal("prune must not delete a recently published row")
	}
}

// Two instances of a service must not both publish: interleaved publishing from
// two pollers can reorder two events for the same stream, which is the one
// ordering guarantee the system relies on.
func TestTryElectAllowsOnlyOnePoller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := newDB(t, "poller_election")

	first := outbox.NewPoller(pool, &fakePublisher{})
	held, release, err := first.TryElect(ctx)
	if err != nil {
		t.Fatalf("first elect: %v", err)
	}
	if !held {
		t.Fatal("first poller should win the election")
	}
	defer release()

	second := outbox.NewPoller(pool, &fakePublisher{})
	held2, release2, err := second.TryElect(ctx)
	if err != nil {
		t.Fatalf("second elect: %v", err)
	}
	if held2 {
		release2()
		t.Fatal("second poller must not win the election while the first holds it")
	}

	// Once the first releases, the second can take over — this is failover.
	release()
	held3, release3, err := second.TryElect(ctx)
	if err != nil {
		t.Fatalf("third elect: %v", err)
	}
	if !held3 {
		t.Fatal("second poller should win after the first releases")
	}
	release3()
}
```

Add these imports to the test file: `sync`, `time`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/platform/outbox/ -run 'TestDrain|TestPrune|TestTryElect|TestRepublish' -v`
Expected: FAIL to build — `undefined: outbox.NewPoller`.

- [ ] **Step 3: Implement `poller.go`**

```go
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// BatchSize bounds one claim. Spec §10.3.
	BatchSize = 100
	// AdvisoryLockKey elects the single active poller per database.
	AdvisoryLockKey = 0x5A6A_0001
	// PollFloor is the ticker interval that backs up LISTEN/NOTIFY, so a missed
	// notification costs a second of latency rather than a stuck queue.
	PollFloor = time.Second
	// Retention is how long published rows are kept for after-the-fact auditing.
	Retention = 7 * 24 * time.Hour
)

// Poller publishes committed outbox rows.
type Poller struct {
	pool *pgxpool.Pool
	pub  Publisher
	log  *slog.Logger
}

func NewPoller(pool *pgxpool.Pool, pub Publisher) *Poller {
	return &Poller{pool: pool, pub: pub, log: slog.Default()}
}

const claimSQL = `
SELECT id, topic, key, payload, headers
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

const markSQL = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`

// Drain runs one claim-publish-mark cycle and returns how many rows it published.
//
// Rows are claimed by flag, never by a cursor over id. BIGSERIAL values become
// visible at commit, not at insert, so a late-committing row can carry a lower id
// than a row already published — a cursor would step over it and lose it
// silently, and only under concurrency (spec §6.4). A flag has no such window:
// whatever is still NULL gets claimed on the next pass.
//
// Publishing happens inside the claiming transaction. That holds a database
// transaction open for the duration of a Kafka round trip, which is the accepted
// cost of the alternative being worse: mark-then-publish loses messages, and
// publish-outside-the-lock lets a second poller claim the same rows.
func (p *Poller) Drain(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, claimSQL, BatchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox: claim: %w", err)
	}

	var (
		claimed []Claimed
		ids     []int64
	)
	for rows.Next() {
		var (
			c           Claimed
			headersJSON []byte
		)
		if err := rows.Scan(&c.ID, &c.Topic, &c.Key, &c.Payload, &headersJSON); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox: scan claimed row: %w", err)
		}
		if err := json.Unmarshal(headersJSON, &c.Headers); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox: unmarshal headers for row %d: %w", c.ID, err)
		}
		claimed = append(claimed, c)
		ids = append(ids, c.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("outbox: iterate claimed rows: %w", err)
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	if err := p.pub.Publish(ctx, claimed); err != nil {
		// Rolling back releases the claim, so the rows are picked up next pass.
		return 0, fmt.Errorf("outbox: publish %d messages: %w", len(claimed), err)
	}
	if _, err := tx.Exec(ctx, markSQL, ids); err != nil {
		return 0, fmt.Errorf("outbox: mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		// The messages are already on the broker; the next pass republishes them.
		// At-least-once, absorbed by the inbox.
		return 0, fmt.Errorf("outbox: commit mark: %w", err)
	}
	return len(claimed), nil
}

// Prune deletes rows published longer ago than olderThan. The events themselves
// are already durable in the events table, so this window exists only so a
// publish can be audited after the fact.
func (p *Poller) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - $1::interval`,
		olderThan)
	if err != nil {
		return 0, fmt.Errorf("outbox: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}

// TryElect attempts to become the single active poller for this database.
//
// The lock is session-scoped, held on one dedicated connection for as long as
// this poller runs, and released when that connection goes away — so a crashed
// instance loses the lock without anything having to notice it died.
func (p *Poller) TryElect(ctx context.Context) (held bool, release func(), err error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("outbox: acquire election conn: %w", err)
	}
	if err := conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1)", int64(AdvisoryLockKey)).Scan(&held); err != nil {
		conn.Release()
		return false, nil, fmt.Errorf("outbox: try advisory lock: %w", err)
	}
	if !held {
		conn.Release()
		return false, func() {}, nil
	}
	return true, func() {
		// Unlock explicitly so a graceful shutdown hands over immediately rather
		// than waiting for the connection to be reaped.
		_, _ = conn.Exec(context.WithoutCancel(ctx),
			"SELECT pg_advisory_unlock($1)", int64(AdvisoryLockKey))
		conn.Release()
	}, nil
}

// Run elects this poller and then publishes until ctx is cancelled.
//
// It wakes on NOTIFY and on a PollFloor ticker. The ticker is not redundant: a
// notification delivered while the poller was mid-batch is coalesced by Postgres,
// and a dropped listener connection would otherwise leave the queue stalled.
func (p *Poller) Run(ctx context.Context) error {
	held, release, err := p.TryElect(ctx)
	if err != nil {
		return err
	}
	if !held {
		p.log.Info("outbox poller standing by; another instance holds the lock")
		<-ctx.Done()
		return nil
	}
	defer release()

	listenConn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("outbox: acquire listen conn: %w", err)
	}
	if _, err := listenConn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		listenConn.Release()
		return fmt.Errorf("outbox: listen: %w", err)
	}

	notified := make(chan struct{}, 1)
	var listener sync.WaitGroup
	listener.Add(1)
	go func() {
		defer listener.Done()
		for {
			if _, err := listenConn.Conn().WaitForNotification(ctx); err != nil {
				return // ctx cancelled, or the connection went away
			}
			select {
			case notified <- struct{}{}:
			default: // a wake-up is already pending; coalesce
			}
		}
	}()
	// Wait for the listener to stop before releasing its connection: a released
	// connection goes back to the pool and may be handed to another caller, so a
	// goroutine still inside WaitForNotification on it is a use-after-free.
	defer func() {
		listener.Wait()
		listenConn.Release()
	}()

	ticker := time.NewTicker(PollFloor)
	defer ticker.Stop()
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()

	for {
		// Drain fully: a batch that filled BatchSize means more is waiting.
		for {
			n, err := p.Drain(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				p.log.Error("outbox drain failed; retrying", "error", err)
				break
			}
			if n < BatchSize {
				break
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-notified:
		case <-ticker.C:
		case <-pruneTicker.C:
			if deleted, err := p.Prune(ctx, Retention); err != nil {
				p.log.Error("outbox prune failed", "error", err)
			} else if deleted > 0 {
				p.log.Info("pruned published outbox rows", "deleted", deleted)
			}
		}
	}
}
```

- [ ] **Step 4: Run the poller tests**

Run: `go test ./internal/platform/outbox/ -race -v -timeout 10m`
Expected: every test PASSes.

If `TestTryElectAllowsOnlyOnePoller` fails because the second poller also wins, the pool handed both `TryElect` calls the same connection — advisory locks are per-session, so two calls on one connection both succeed. Confirm `pool.Acquire` is returning distinct connections by checking `MinConns`/`MaxConns` in `pg.Open` are at least 2.

- [ ] **Step 5: Confirm the partial index is actually used**

```bash
psql "$DSN" -c "EXPLAIN SELECT id FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100"
```

Run this against a database with a few thousand published rows and a handful of pending ones. Expected: an `Index Scan using outbox_unpublished`. A `Seq Scan` means the partial index predicate does not match the query's `IS NULL` form, which would make the poller slower as history grows.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/outbox
git commit -m "feat(outbox): claim-based poller with advisory-lock election and pruning"
```

---
