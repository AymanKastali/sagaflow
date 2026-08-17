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
	// id order must match the order the caller passed, because the poller claims
	// and publishes ORDER BY id and that is the only thing preserving per-stream
	// ordering for two messages enqueued in one transaction.
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
	// payload is BYTEA, not JSONB, so unlike events.data it is byte-preserving
	// and can be compared exactly.
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

// Every field Enqueue validates gets a case. A keyless message is the one that
// matters most — Kafka would round-robin it across partitions, silently
// destroying the per-stream ordering everything downstream assumes — but a
// validation that is written and not exercised is indistinguishable from one
// that was never written.
func TestEnqueueRejectsIncompleteMessages(t *testing.T) {
	ctx := context.Background()
	pool := newDB(t, "outbox_invalid")

	for _, tc := range []struct {
		name string
		msg  outbox.Message
	}{
		{"no topic", outbox.Message{Key: "seat-14A", Payload: []byte{1}}},
		{"no key", outbox.Message{Topic: "inventory.events", Payload: []byte{1}}},
		{"no payload", outbox.Message{Topic: "inventory.events", Key: "seat-14A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
				return outbox.Enqueue(ctx, tx, []outbox.Message{tc.msg})
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
