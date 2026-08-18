package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	migrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/envelope"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/testsupport/pgtest"
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
	return pgtest.Shared(t).Migrated(t, name, migrations.FS)
}

func msg(topic, key string) envelope.Message {
	return envelope.Message{
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
	ctx := t.Context()
	pool := newDB(t, "outbox_commit")

	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{
			{Type: "sagaflow.inventory.v1.SeatHeld", Data: []byte(`{}`)},
		}); err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, []envelope.Message{msg("inventory.events", "seat-14A")})
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
	ctx := t.Context()
	pool := newDB(t, "outbox_rollback")

	sentinel := errors.New("handler failed after enqueue")
	err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{
			{Type: "sagaflow.inventory.v1.SeatHeld", Data: []byte(`{}`)},
		}); err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, []envelope.Message{msg("inventory.events", "seat-14A")}); err != nil {
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
	ctx := t.Context()
	pool := newDB(t, "outbox_order")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, []envelope.Message{
			msg("inventory.events", "seat-14A"),
			msg("inventory.events", "seat-14B"),
			msg("inventory.commands", "seat-14C"),
		})
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	type row struct {
		Topic   string
		Key     string
		Payload []byte
		Headers map[string]string
	}
	rows, err := pool.Query(ctx, `SELECT topic, key, payload, headers FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}

	// id order must match the order the caller passed, because the poller claims
	// and publishes ORDER BY id and that is the only thing preserving per-stream
	// ordering for two messages enqueued in one transaction.
	gotKeys := make([]string, 0, len(got))
	for _, r := range got {
		gotKeys = append(gotKeys, r.Key)
	}
	if want := []string{"seat-14A", "seat-14B", "seat-14C"}; !slices.Equal(gotKeys, want) {
		t.Fatalf("want keys %v in id order, got %v", want, gotKeys)
	}
	if got[2].Topic != "inventory.commands" {
		t.Fatalf("want third row on inventory.commands, got %s", got[2].Topic)
	}
	if got[0].Headers["ce_id"] != "id-seat-14A" {
		t.Fatalf("headers did not round-trip: %v", got[0].Headers)
	}
	// payload is BYTEA, not JSONB, so unlike events.data it is byte-preserving
	// and can be compared exactly.
	if !slices.Equal(got[0].Payload, []byte{0x00, 0x01, 0x02}) {
		t.Fatalf("payload did not round-trip: %v", got[0].Payload)
	}
}

func TestEnqueueEmptyIsANoOp(t *testing.T) {
	ctx := t.Context()
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

// Every field Enqueue validates gets a case. A keyless message is the one that
// matters most — Kafka would round-robin it across partitions, silently
// destroying the per-stream ordering everything downstream assumes — but a
// validation that is written and not exercised is indistinguishable from one
// that was never written.
func TestEnqueueRejectsIncompleteMessages(t *testing.T) {
	ctx := t.Context()
	pool := newDB(t, "outbox_invalid")

	for _, tc := range []struct {
		name string
		msg  envelope.Message
	}{
		{"no topic", envelope.Message{Key: "seat-14A", Payload: []byte{1}}},
		{"no key", envelope.Message{Topic: "inventory.events", Payload: []byte{1}}},
		{"no payload", envelope.Message{Topic: "inventory.events", Key: "seat-14A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
				return outbox.Enqueue(ctx, tx, []envelope.Message{tc.msg})
			})
			if err == nil {
				t.Fatalf("want an error for a message with %s, got nil", tc.name)
			}
			if got := countOutbox(t, pool, ""); got != 0 {
				t.Fatalf("a rejected message left %d rows behind", got)
			}
		})
	}
}

type fakePublisher struct {
	mu   sync.Mutex
	got  [][]envelope.Message
	err  error
	call int
	// published, when set, receives each batch. Tests that drive Run block on it
	// instead of sleeping: spec §12.4 wants completion to be a signal with a
	// context deadline as the failure mode, not a guess at a duration.
	published chan []envelope.Message
}

func (f *fakePublisher) Publish(_ context.Context, msgs []envelope.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.call++
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, msgs)
	if f.published != nil {
		select {
		case f.published <- msgs:
		default: // buffered and full; a test that cares reads every batch
		}
	}
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
	ctx := t.Context()
	msgs := make([]envelope.Message, len(keys))
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
	ctx := t.Context()
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
	if got := pub.keys(); !slices.Equal(got, want) {
		t.Fatalf("want %v published in id order, got %v", want, got)
	}

	if left := countOutbox(t, pool, "published_at IS NULL"); left != 0 {
		t.Fatalf("want 0 unpublished after drain, got %d", left)
	}
}

func TestDrainIsANoOpWhenNothingIsPending(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
	pool := newDB(t, "poller_election")

	first := outbox.NewPoller(pool, &fakePublisher{})
	held, release, err := first.TryElect(ctx)
	if err != nil {
		t.Fatalf("first elect: %v", err)
	}
	if !held {
		t.Fatal("first poller should win the election")
	}
	// Released explicitly below as well: release must be idempotent, because the
	// natural calling pattern is a defer plus an explicit hand-over on shutdown.
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

// The trap spec §6.4 exists to warn about, made executable.
//
// BIGSERIAL values are handed out at insert and become visible at commit, so id
// order and visibility order are not the same order. A poller tracking
// `WHERE id > cursor` reads the higher id, advances past it, and never sees the
// lower one — losing a message silently, only under concurrency, which is why it
// survives every test written on a quiet machine.
//
// Claiming by flag has no such window. This test passes for the implementation we
// have and fails for a cursor, which is the only reason it is worth writing.
func TestClaimByFlagSurvivesAnOutOfOrderCommit(t *testing.T) {
	ctx := t.Context()
	pool := newDB(t, "poller_out_of_order")

	// A inserts first, so it takes the lower id. It commits last.
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()
	if err := outbox.Enqueue(ctx, txA, []envelope.Message{msg("inventory.events", "lower-id")}); err != nil {
		t.Fatalf("enqueue A: %v", err)
	}

	// B inserts second, so it takes the higher id, and commits first.
	txB, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	if err := outbox.Enqueue(ctx, txB, []envelope.Message{msg("inventory.events", "higher-id")}); err != nil {
		t.Fatalf("enqueue B: %v", err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("commit B: %v", err)
	}

	pub := &fakePublisher{}
	p := outbox.NewPoller(pool, pub)

	// Only B is visible, so only B is published — and the poller has now published
	// a row whose id is *higher* than one still to come.
	n, err := p.Drain(ctx)
	if err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 published while A is uncommitted, got %d", n)
	}

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	// This is the assertion a cursor design fails: the late-committing lower id
	// must still be published.
	n, err = p.Drain(ctx)
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("want the late-committing row published, got %d — a lower id that "+
			"commits after a higher one was published must not be skipped", n)
	}

	want := []string{"higher-id", "lower-id"}
	if got := pub.keys(); !slices.Equal(got, want) {
		t.Fatalf("want %v (published in commit order, not id order), got %v", want, got)
	}

	// Confirm the premise rather than assuming it: the row published second really
	// does carry the lower id. Without this the test could pass while proving
	// nothing about out-of-order ids.
	var lower, higher int64
	if err := pool.QueryRow(ctx,
		`SELECT (SELECT id FROM outbox WHERE key = 'lower-id'),
		        (SELECT id FROM outbox WHERE key = 'higher-id')`).Scan(&lower, &higher); err != nil {
		t.Fatalf("read ids: %v", err)
	}
	if lower >= higher {
		t.Fatalf("the test did not reproduce out-of-order ids: lower-id got %d, higher-id got %d",
			lower, higher)
	}
}

// The partial index is what keeps the claim cheap as history grows. A Seq Scan
// here would not fail anything — it would just get slower for years — so the plan
// shape is asserted rather than eyeballed once.
func TestClaimUsesThePartialIndex(t *testing.T) {
	ctx := t.Context()
	pool := newDB(t, "poller_index")

	// A realistic shape: a long tail of published history, a short pending queue.
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox (topic, key, payload, headers, published_at)
		SELECT 'inventory.events', 'old-' || g, '\x00'::bytea, '{}'::jsonb, now()
		FROM generate_series(1, 5000) g`); err != nil {
		t.Fatalf("seed published: %v", err)
	}
	enqueue(t, pool, "pending-1", "pending-2", "pending-3")
	if _, err := pool.Exec(ctx, "ANALYZE outbox"); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	rows, err := pool.Query(ctx, "EXPLAIN "+outbox.ClaimSQL, outbox.BatchSize)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	plan := strings.Join(lines, "\n")

	if !strings.Contains(plan, "outbox_unpublished") {
		t.Fatalf("the claim does not use the partial index:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan") {
		t.Fatalf("the claim falls back to a sequential scan:\n%s", plan)
	}
}

// Run is the only part of this package that production actually calls, and it was
// otherwise untested. It covers three things one test can reasonably cover: the
// election, the drain loop, and — under -race — the listener goroutine's
// lifecycle, since releasing the listen connection while that goroutine is still
// inside WaitForNotification would be a use-after-free.
//
// The wake-up may come from NOTIFY or from the PollFloor ticker, and the test
// deliberately does not care which: both paths exist precisely so that either one
// alone is sufficient.
func TestRunPublishesAndShutsDownCleanly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := newDB(t, "poller_run")

	pub := &fakePublisher{published: make(chan []envelope.Message, 4)}
	runDone := make(chan error, 1)
	go func() { runDone <- outbox.NewPoller(pool, pub).Run(ctx) }()

	enqueue(t, pool, "seat-14A")

	select {
	case batch := <-pub.published:
		if len(batch) != 1 || batch[0].Key != "seat-14A" {
			t.Fatalf("want one batch holding seat-14A, got %+v", batch)
		}
	case <-ctx.Done():
		t.Fatal("Run never published the enqueued row")
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned an error on shutdown: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// Enqueue's doc comment claims the NOTIFY is transactional. That claim is about
// our code, not about Postgres: swapping tx.Exec for pool.Exec would send the
// notification immediately and wake a poller to hunt for a message that was never
// written. So it is pinned here rather than trusted.
func TestNotifyIsWithheldUntilCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := newDB(t, "outbox_notify")

	listen := func(t *testing.T) *pgxpool.Conn {
		t.Helper()
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire listener: %v", err)
		}
		t.Cleanup(conn.Release)
		if _, err := conn.Exec(ctx, "LISTEN "+outbox.NotifyChannel); err != nil {
			t.Fatalf("listen: %v", err)
		}
		return conn
	}

	// A separate connection per phase: a WaitForNotification that ends in a
	// deadline may leave its connection unusable, and that must not be mistaken
	// for the second half of this test failing.
	rolledBack := listen(t)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := outbox.Enqueue(ctx, tx, []envelope.Message{msg("inventory.events", "rolled-back")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	brief, cancelBrief := context.WithTimeout(ctx, time.Second)
	defer cancelBrief()
	if n, err := rolledBack.Conn().WaitForNotification(brief); err == nil {
		t.Fatalf("a rolled-back enqueue delivered a notification on %q", n.Channel)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting for the absence of a notification: %v", err)
	}

	committed := listen(t)
	enqueue(t, pool, "committed")

	n, err := committed.Conn().WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("want a notification after commit: %v", err)
	}
	if n.Channel != outbox.NotifyChannel {
		t.Fatalf("want channel %q, got %q", outbox.NotifyChannel, n.Channel)
	}
}
