package inbox_test

import (
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
	return pgtest.Shared(t).Migrated(t, name, migrations.FS)
}

func mark(t *testing.T, pool *pgxpool.Pool, consumer, source, id string) bool {
	t.Helper()
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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
