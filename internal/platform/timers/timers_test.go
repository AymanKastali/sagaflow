package timers_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	migrations "github.com/AymanKastali/sagaflow/internal/inventory/migrations"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// errBoom is a Firer failure with no meaning beyond "this did not work".
var errBoom = errors.New("boom")

func db(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	return pgtest.Shared(t).Migrated(t, name, migrations.FS)
}

// schedule is the two-line ceremony of putting one timer in the table, hoisted
// so the tests below read as what they assert rather than as transaction setup.
func schedule(t *testing.T, pool *pgxpool.Pool, fireAt time.Time, subject, token string) {
	t.Helper()
	err := pg.WithTx(t.Context(), pool, func(tx pgx.Tx) error {
		return timers.Schedule(t.Context(), tx, fireAt, subject, token)
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
}

// firerFunc adapts a function to the Firer interface.
type firerFunc func(context.Context, pgx.Tx, timers.Timer) error

func (f firerFunc) Fire(ctx context.Context, tx pgx.Tx, t timers.Timer) error {
	return f(ctx, tx, t)
}

func TestOnlyPastDeadlinesComeDue(t *testing.T) {
	// A timer is a promise about the future, so the whole table is not the
	// work list — only the part of it the clock has reached.
	ctx := t.Context()
	pool := db(t, "timers_due")

	schedule(t, pool, time.Now().Add(-time.Minute), "seat-1", "hold-1")
	schedule(t, pool, time.Now().Add(time.Hour), "seat-2", "hold-2")

	due, err := timers.Due(ctx, pool, timers.BatchSize)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("one deadline has passed, so one timer is due, got %d", len(due))
	}
	if due[0].Subject != "seat-1" || due[0].Token != "hold-1" {
		t.Fatalf("the due timer lost its subject or token: %+v", due[0])
	}
}

func TestExactlyOneClaimerWinsATimer(t *testing.T) {
	// This is the property that replaces leader election: two schedulers may
	// read the same due row, and the row itself decides which one gets it.
	ctx := t.Context()
	pool := db(t, "timers_claim")
	schedule(t, pool, time.Now().Add(-time.Minute), "seat-1", "hold-1")

	due, err := timers.Due(ctx, pool, timers.BatchSize)
	if err != nil || len(due) != 1 {
		t.Fatalf("setup: due returned %d timers, err %v", len(due), err)
	}

	var (
		mu   sync.Mutex
		wins int
		wg   sync.WaitGroup
	)
	for range 2 {
		wg.Go(func() {
			err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
				got, err := timers.MarkFired(ctx, tx, due[0].ID)
				if err != nil {
					return err
				}
				if got {
					mu.Lock()
					wins++
					mu.Unlock()
				}
				return nil
			})
			if err != nil {
				t.Errorf("claim: %v", err)
			}
		})
	}
	wg.Wait()

	if wins != 1 {
		t.Fatalf("a timer fires once: want exactly 1 winner, got %d", wins)
	}
}

func TestAClaimRolledBackIsClaimableAgain(t *testing.T) {
	// The claim and the effect share a transaction, so a failed effect must
	// leave the timer pending rather than consumed. This is what lets a
	// version conflict in a Firer resolve itself on the next pass.
	ctx := t.Context()
	pool := db(t, "timers_rollback")
	schedule(t, pool, time.Now().Add(-time.Minute), "seat-1", "hold-1")

	sched := timers.NewScheduler(pool, firerFunc(
		func(context.Context, pgx.Tx, timers.Timer) error {
			return errBoom
		}))

	if n, err := sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("a failing Firer fires nothing: got %d, err %v", n, err)
	}

	due, err := timers.Due(ctx, pool, timers.BatchSize)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("the rolled-back claim left the timer pending, so it is due again, got %d", len(due))
	}
}

func TestRunFiresATimerThatComesDue(t *testing.T) {
	// The only test of the loop itself. It waits on the effect rather than on a
	// duration, so it is bounded by t.Context's deadline and not by a sleep.
	ctx := t.Context()
	pool := db(t, "timers_run")
	schedule(t, pool, time.Now().Add(100*time.Millisecond), "seat-1", "hold-1")

	fired := make(chan timers.Timer, 1)
	sched := timers.NewScheduler(pool, firerFunc(
		func(_ context.Context, _ pgx.Tx, tm timers.Timer) error {
			fired <- tm
			return nil
		}))

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = sched.Run(runCtx) }()

	select {
	case tm := <-fired:
		if tm.Subject != "seat-1" || tm.Token != "hold-1" {
			t.Fatalf("fired the wrong timer: %+v", tm)
		}
	case <-ctx.Done():
		t.Fatal("the deadline passed and Run never fired the timer")
	}
}

func TestPruneKeepsPendingTimers(t *testing.T) {
	// Housekeeping must never delete work that has not happened yet.
	ctx := t.Context()
	pool := db(t, "timers_prune")
	schedule(t, pool, time.Now().Add(-time.Minute), "seat-1", "hold-1")
	schedule(t, pool, time.Now().Add(time.Hour), "seat-2", "hold-2")

	sched := timers.NewScheduler(pool, firerFunc(
		func(context.Context, pgx.Tx, timers.Timer) error { return nil }))
	if n, err := sched.Tick(ctx); err != nil || n != 1 {
		t.Fatalf("setup: fired %d timers, err %v", n, err)
	}

	deleted, err := timers.Prune(ctx, pool, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("only the fired timer is prunable: want 1 deleted, got %d", deleted)
	}
	due, err := timers.Due(ctx, pool, timers.BatchSize)
	if err != nil || len(due) != 0 {
		t.Fatalf("the pending timer survived but is not yet due: got %d, err %v", len(due), err)
	}
}
