# Phase 5b — Seat Hold Expiry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A seat held by a booking that never finishes frees itself, by appending `SeatHoldExpired` to its own stream — so that killing the booking service mid-hold costs the seat fifteen minutes, not forever.

**Architecture:** A durable `timers` table, scheduled inside the same transaction that appends `SeatHeld`. A scheduler reads due rows and, for each, opens one transaction that claims the row and applies its effect together. The claim is the whole once-only guarantee: no inbox row, because nothing was delivered, and no leader election, because the effect never leaves the transaction. Deciding whether a due timer still means anything is a pure function of the folded seat.

**Tech Stack:** Go 1.26.6, protobuf via buf v2, pgx v5, `internal/platform/{eventstore,codec,envelope,inbox,outbox,pg,schema}`.

**Spec:** [2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) §7 (tree), §7.2 (the one-transaction invariant), §10.3 (limits), §10.5 (two timers), §12.2, §12.4 — as amended by [2026-08-18-platform-package-restructure-design.md](../specs/2026-08-18-platform-package-restructure-design.md).

## Global Constraints

- Go 1.26.6; module `github.com/AymanKastali/sagaflow`; contracts are the second module `github.com/AymanKastali/sagaflow/contracts`.
- Proto package `sagaflow.inventory.v1` never changes — it is simultaneously `events.type`, `ce_type` and the registry subject.
- **One top-level message per `.proto` file.** `platform/schema.register` hardcodes `sr.Index(0)`, the Confluent message-index shortcut for "first message in its file". A second message in a file would be framed under the wrong index and no other Confluent client could read it.
- A single transaction writes **exactly one stream**, plus its outbox rows, its inbox row, and any timer it scheduled (spec §7.2).
- Seat hold TTL 15 min in production, 200 ms–2 s in tests (spec §10.3).
- `fire_at` and `expires_at` are **application-supplied, never `DEFAULT now()`** (spec §10.5). Only diagnostic columns use the database clock.
- Every package under `internal/` gets a `doc.go` with the six chapter headings ([docs/conventions.md](../../conventions.md)); `README.md` links every package under `internal/platform/` (D5). `internal/docs` fails the suite otherwise.
- `§` appears in `doc.go` and nowhere else in Go source (D4).
- Functions stay under about 40 lines; comments inside a function body are single lines (R4, R5).
- `make test` starts no container; integration tests skip under `-short`. One container per package, started in `TestMain` (spec §12.4).
- No `time.Sleep` in assertions (spec §12.4).
- Every task's last step is its commit.

## Scope

**In:** the `timers` table, `internal/platform/timers`, the `SeatHoldExpired` contract, the pure expiry decision, scheduling the timer inside the hold's own transaction, and inventory's expiry handler.

**Out, and where each lands:**

| Deferred | Where |
|---|---|
| Availability projection, `005_projections.sql`, `wire.go`, `cmd/inventory/main.go` | Phase 5c |
| Booking's saga step timeout — the *other* timer of §10.5 | Phase 7. It reuses this package unchanged; nothing here is inventory-specific except the `Firer`. |
| `ConfirmSeatHold` / `SeatConfirmed`, and what a confirmed seat does with a pending timer | Phase 7 — the pivot's semantics are a saga decision |
| Cancelling or rescheduling a timer | Never. See the invariant below. |

## The one invariant this phase introduces

**Silence is correct exactly when nobody asked.**

Phase 5a established that every command gets a reply, because a saga step that hears nothing re-dispatches forever. Expiry is the first thing in this system that is *not* a command. No one sent it, nothing is correlated to it, and there is no envelope to reply into. So when a due timer finds its hold already gone, the right answer is nothing at all — no event, no reply, no error.

That is what makes cancellation unnecessary, and the reasoning is worth following all the way through:

- A hold released at 14:59:59 with a timer due at 15:00:00 is a race no lock can remove.
- If the fire had to be cancelled, that race would be a bug, and cancellation would be a second operation that can fail.
- Because the fire is decided against the *stream* rather than the clock, the race resolves itself: whichever lands second finds a state where its work is already done.
- Once a fire is harmless, cancelling it is dead code.

**Corollary: the timer is a hint about when to look, never a source of truth about what is true.** The stream stays the only authority. This is the same reason phase 5a's decision functions have no clock.

---

### Task 1: The `timers` table and `internal/platform/timers`

**Files:**
- Create: `internal/inventory/migrations/004_timers.sql`
- Create: `internal/platform/timers/doc.go`
- Create: `internal/platform/timers/timers.go`
- Create: `internal/platform/timers/scheduler.go`
- Create: `internal/platform/timers/main_test.go`
- Test: `internal/platform/timers/timers_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: `internal/platform/pg.WithTx`, `internal/testsupport/pgtest`, `internal/inventory/migrations.FS` (tests only).
- Produces:
  - `type Timer struct { ID int64; FireAt time.Time; Subject, Token string }`
  - `func Schedule(ctx context.Context, tx pgx.Tx, fireAt time.Time, subject, token string) error`
  - `func Due(ctx context.Context, pool *pgxpool.Pool, limit int) ([]Timer, error)`
  - `func MarkFired(ctx context.Context, tx pgx.Tx, id int64) (bool, error)`
  - `func NextDue(ctx context.Context, pool *pgxpool.Pool) (time.Time, bool, error)`
  - `func Prune(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int64, error)`
  - `type Firer interface { Fire(ctx context.Context, tx pgx.Tx, t Timer) error }`
  - `type Scheduler struct{ … }`, `func NewScheduler(pool *pgxpool.Pool, f Firer) *Scheduler`
  - `func (s *Scheduler) Run(ctx context.Context) error`, `func (s *Scheduler) Tick(ctx context.Context) (int, error)`
  - `const BatchSize = 100`, `MaxSleep = time.Second`, `MinSleep = 5 * time.Millisecond`, `Retention = 7 * 24 * time.Hour`

- [ ] **Step 1: Write the migration**

Create `internal/inventory/migrations/004_timers.sql`:

```sql
CREATE TABLE timers (
    id       BIGSERIAL PRIMARY KEY,
    fire_at  TIMESTAMPTZ NOT NULL,
    subject  TEXT NOT NULL,
    token    TEXT NOT NULL,
    fired_at TIMESTAMPTZ
);

-- Partial index: the scheduler only ever asks for unfired rows, so the index
-- stays the size of the pending set rather than the size of history.
CREATE INDEX timers_due ON timers (fire_at) WHERE fired_at IS NULL;

COMMENT ON COLUMN timers.subject IS
    'The stream this timer is about — a seat id here, a saga id in booking.';
COMMENT ON COLUMN timers.token IS
    'What the subject looked like when the timer was set. A handler compares it
     against the stream it just loaded; a mismatch means the world moved on and
     this timer is stale.';
COMMENT ON COLUMN timers.fire_at IS
    'Application-supplied, never DEFAULT now(), so a test controls a deadline
     with a value instead of by waiting.';

---- create above / drop below ----

DROP TABLE timers;
```

- [ ] **Step 2: Write the failing tests**

Create `internal/platform/timers/main_test.go`:

```go
package timers_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
)

// One Postgres for the whole package. This package owns no migrations of its
// own — the timers table is per-service schema — so the tests borrow
// inventory's, exactly as the outbox and inbox tests do.
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
```

Create `internal/platform/timers/timers_test.go`:

```go
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/platform/timers/ -run Timer -v`
Expected: FAIL — the package does not exist.

- [ ] **Step 4: Write `timers.go`**

Create `internal/platform/timers/timers.go`:

```go
package timers

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Timer is one scheduled wake-up.
//
// Subject names the stream the timer is about; Token records what that stream
// looked like when the timer was set. The pair is deliberately just two strings:
// this package never interprets either one, so nothing here has to change when a
// new kind of timer appears.
type Timer struct {
	ID      int64
	FireAt  time.Time
	Subject string
	Token   string
}

const scheduleSQL = `INSERT INTO timers (fire_at, subject, token) VALUES ($1, $2, $3)`

// Schedule records a wake-up inside the caller's transaction, so the deadline
// and whatever made it necessary commit together or not at all.
//
// fireAt is passed in rather than computed from a clock here. A package with a
// clock of its own could not be tested without waiting in real time, and the
// caller already knows the deadline — it is in the event being appended.
func Schedule(ctx context.Context, tx pgx.Tx, fireAt time.Time, subject, token string) error {
	if _, err := tx.Exec(ctx, scheduleSQL, fireAt, subject, token); err != nil {
		return fmt.Errorf("timers: schedule %s at %s: %w", subject, fireAt, err)
	}
	return nil
}

// dueSQL reads what the clock has reached, earliest deadline first, and claims
// nothing. Claiming belongs in MarkFired, inside the transaction that also
// writes the timer's effect.
const dueSQL = `
SELECT id, fire_at, subject, token
FROM timers
WHERE fired_at IS NULL AND fire_at <= now()
ORDER BY fire_at
LIMIT $1`

// Due reads timers whose deadline has passed.
func Due(ctx context.Context, pool *pgxpool.Pool, limit int) ([]Timer, error) {
	rows, err := pool.Query(ctx, dueSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("timers: read due: %w", err)
	}
	defer rows.Close()

	var due []Timer
	for rows.Next() {
		var t Timer
		if err := rows.Scan(&t.ID, &t.FireAt, &t.Subject, &t.Token); err != nil {
			return nil, fmt.Errorf("timers: scan due row: %w", err)
		}
		due = append(due, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("timers: iterate due rows: %w", err)
	}
	return due, nil
}

const markFiredSQL = `UPDATE timers SET fired_at = now() WHERE id = $1 AND fired_at IS NULL`

// MarkFired claims one timer, reporting whether this caller is the one that got
// it. False means another scheduler already has it and this transaction should
// do nothing.
//
// The duplicate is detected by rows-affected rather than by a raised error, for
// the same reason inbox.MarkConsumed does it: in Postgres any error aborts the
// whole transaction, so a duplicate raised as an exception would poison the
// append and the enqueue that follow it in the same commit.
func MarkFired(ctx context.Context, tx pgx.Tx, id int64) (bool, error) {
	tag, err := tx.Exec(ctx, markFiredSQL, id)
	if err != nil {
		return false, fmt.Errorf("timers: claim %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

const nextDueSQL = `SELECT min(fire_at) FROM timers WHERE fired_at IS NULL`

// NextDue is when the earliest pending timer comes due, and whether there is one
// at all. It is how the scheduler decides how long to sleep.
func NextDue(ctx context.Context, pool *pgxpool.Pool) (time.Time, bool, error) {
	var at *time.Time
	if err := pool.QueryRow(ctx, nextDueSQL).Scan(&at); err != nil {
		return time.Time{}, false, fmt.Errorf("timers: next due: %w", err)
	}
	if at == nil {
		return time.Time{}, false, nil
	}
	return *at, true, nil
}

// Prune deletes timers fired longer ago than olderThan. Pending timers are never
// touched: a row that has not fired is work, not history.
//
// What the timer did is already durable in the events table, so this window
// exists only so that a fire can be audited after the fact.
func Prune(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx,
		`DELETE FROM timers WHERE fired_at IS NOT NULL AND fired_at <= now() - $1::interval`,
		olderThan)
	if err != nil {
		return 0, fmt.Errorf("timers: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}
```

- [ ] **Step 5: Write `scheduler.go`**

Create `internal/platform/timers/scheduler.go`:

```go
package timers

import (
	"context"
	"log/slog"
	"time"

	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// BatchSize bounds how many due timers one pass takes, so a backlog is
	// worked through in bounded steps rather than in one unbounded pass.
	BatchSize = 100
	// MaxSleep caps how long Run waits between passes. Without a cap, a
	// scheduler that found an empty table would sleep through a timer another
	// process scheduled a moment later.
	MaxSleep = time.Second
	// MinSleep keeps a timer that is already past due — or one whose Firer keeps
	// failing — from turning the loop into a spin.
	MinSleep = 5 * time.Millisecond
	// Retention is how long fired rows are kept for after-the-fact auditing.
	Retention = 7 * 24 * time.Hour
	// pruneInterval is how often rows older than Retention are deleted.
	pruneInterval = time.Hour
)

// Firer applies one due timer inside the transaction that claimed it.
//
// It takes the transaction rather than a pool because that is the whole design:
// claiming the row and acting on it must be the same commit, or a crash between
// them either loses the work or does it twice.
type Firer interface {
	Fire(ctx context.Context, tx pgx.Tx, t Timer) error
}

// Scheduler fires timers as their deadlines pass.
type Scheduler struct {
	pool *pgxpool.Pool
	fire Firer
	log  *slog.Logger
}

func NewScheduler(pool *pgxpool.Pool, f Firer) *Scheduler {
	return &Scheduler{pool: pool, fire: f, log: slog.Default()}
}

// Run fires due timers until ctx is cancelled.
//
// There is no leader election here, unlike the outbox poller, and the difference
// is the point. The outbox elects because its effect — the Kafka publish —
// happens outside the row's transaction, so two pollers on one row publish it
// twice. A timer's entire effect is inside the transaction that claims it, so two
// schedulers racing one row resolve themselves: the loser's claim reports no rows
// and it rolls back having done nothing.
func (s *Scheduler) Run(ctx context.Context) error {
	prune := time.NewTicker(pruneInterval)
	defer prune.Stop()

	for {
		if _, err := s.Tick(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("reading due timers failed; retrying", "error", err)
		}

		wait := time.NewTimer(s.sleep(ctx))
		select {
		case <-ctx.Done():
			wait.Stop()
			return nil
		case <-wait.C:
		case <-prune.C:
			wait.Stop()
			s.pruneFired(ctx)
		}
	}
}

// sleep is how long to wait before the next pass: until the next deadline, but
// never longer than MaxSleep and never shorter than MinSleep.
//
// A failed lookup sleeps the maximum rather than the minimum, so a database that
// is briefly unreachable is retried patiently instead of hammered.
func (s *Scheduler) sleep(ctx context.Context) time.Duration {
	next, pending, err := NextDue(ctx, s.pool)
	if err != nil || !pending {
		return MaxSleep
	}
	return min(max(time.Until(next), MinSleep), MaxSleep)
}

// Tick makes one pass over the due timers and returns how many fired.
//
// Exported because it is the whole scheduler minus the waiting: a test drives it
// directly with deadlines already in the past, and asserts on effects rather
// than on elapsed time.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	due, err := Due(ctx, s.pool, BatchSize)
	if err != nil {
		return 0, err
	}
	fired := 0
	for _, t := range due {
		claimed, err := s.fireOne(ctx, t)
		switch {
		case err != nil && ctx.Err() == nil:
			// One timer's failure must not hold up the rest of the batch: its
			// row stays pending and the next pass tries it again.
			s.log.Error("firing a timer failed; will retry",
				"timer", t.ID, "subject", t.Subject, "error", err)
		case claimed:
			fired++
		}
	}
	return fired, nil
}

// fireOne claims a single timer and applies its effect in a single transaction.
//
// A Firer that returns an error rolls the claim back with it, which is what makes
// a version conflict inside a Firer self-healing: the row is pending again and
// the next pass re-reads the stream it conflicted on.
func (s *Scheduler) fireOne(ctx context.Context, t Timer) (bool, error) {
	claimed := false
	err := pg.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		got, err := MarkFired(ctx, tx, t.ID)
		if err != nil || !got {
			return err // not got: another scheduler has it, commit nothing
		}
		claimed = true
		return s.fire.Fire(ctx, tx, t)
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// pruneFired deletes fired rows past Retention. Housekeeping must never stop the
// loop, so a failure is logged and the next pass carries on.
func (s *Scheduler) pruneFired(ctx context.Context) {
	deleted, err := Prune(ctx, s.pool, Retention)
	switch {
	case err != nil:
		s.log.Error("pruning fired timers failed", "error", err)
	case deleted > 0:
		s.log.Info("pruned fired timers", "deleted", deleted)
	}
}
```

- [ ] **Step 6: Write the chapter**

Create `internal/platform/timers/doc.go`. It must contain the package comment and nothing else — no imports, no code — and must carry all six headings in order, or `internal/docs` fails.

```go
// Package timers makes "wake me at this time" a row in the same transaction as
// the thing that needs waking.
//
// # The problem
//
// A seat is held for fifteen minutes. Something has to free it when the booking
// that took the hold never finishes — and that something cannot be the booking,
// because the case that matters is precisely the one where the booking process
// is dead. Nothing failed, no error was logged, no message arrived: seat 14A is
// simply unsellable forever.
//
// A time.AfterFunc solves this for exactly as long as the process lives. Restart
// the service and every pending deadline is gone, silently, with no way even to
// enumerate what was lost.
//
// # Why the obvious fixes do not work
//
// Give the hold an expiry column and treat an expired hold as free. Then the
// hold ends without an event, so nothing is published and the saga waiting on
// that seat waits forever. Worse, "is this hold live?" becomes a question about
// the current time rather than about the stream, so two readers a millisecond
// apart can disagree, and replaying that history next year produces different
// state than it did on the day.
//
// Schedule the wake-up right after the transaction commits. The gap between the
// commit and the schedule is small and it is fatal: a crash inside it leaves a
// hold that exists with no deadline attached, which is the original problem with
// extra steps.
//
// Cancel the timer when the hold is released. A release at 14:59:59 and a
// deadline at 15:00:00 race, and no lock removes that race — so a late fire has
// to be harmless regardless. Once it is harmless, cancelling is dead code that
// can fail.
//
// Elect a leader, as the outbox poller does. The outbox needs one because its
// effect, the Kafka publish, happens outside the row's transaction. A timer's
// whole effect is inside it, so two schedulers racing one row settle it between
// themselves: the loser's claim reports no rows and it rolls back.
//
// # What this package does
//
// One table. A row is scheduled inside the caller's transaction, so the deadline
// and the event that created it commit together or not at all. A scheduler reads
// rows the clock has reached and, for each, opens one transaction that claims the
// row and applies its effect together.
//
// The claim is an UPDATE reporting rows-affected rather than raising, so a timer
// another scheduler already took is an ordinary branch instead of an error that
// would poison the rest of the commit.
//
// Two columns carry all the meaning. Subject names the stream the timer is
// about; token records what that stream looked like when the timer was set. A
// handler compares the token against the stream it just loaded, and a mismatch
// means the world moved on.
//
// # What it deliberately does not do
//
// It does not know what a timer means. Firing hands the row to a Firer, which
// decides; this package never appends an event and never publishes anything.
//
// It has no clock of its own. Deadlines are passed in, so a test controls one
// with a value rather than by waiting.
//
// It does not cancel, reschedule, or deduplicate fires. A row fires once, and
// whether that fire does anything is the Firer's business.
//
// It is not a cron. There is no recurrence and no schedule expression.
//
// # Reading order
//
// Start in timers.go with Schedule, Due and MarkFired. Those three are the whole
// data model, and MarkFired is where the safety lives. Then scheduler.go for the
// loop that calls them, whose only subtlety is how long it waits between passes.
//
// # Where this comes from
//
// Design spec §10.5, two timers deliberately separate — the seat-hold TTL owned
// by inventory, the saga step timeout owned by booking — and §7.2, where a
// scheduled timer joins the outbox row and the inbox row in one commit.
package timers
```

- [ ] **Step 7: Add the package to `README.md`**

`internal/docs` check D5 requires `README.md` to link every directory under `internal/platform/`. Add a row to the topic table, immediately after the `internal/platform/inbox` row:

```markdown
| Durable timers, self-expiring holds | [internal/platform/timers](internal/platform/timers) | `go doc ./internal/platform/timers` |
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./internal/platform/timers/ -v` then `make lint && make test`
Expected: PASS, including `internal/docs`.

- [ ] **Step 9: Commit**

```bash
git add internal/platform/timers internal/inventory/migrations/004_timers.sql README.md
git commit -m "feat(timers): durable wake-ups scheduled in the caller's transaction"
```

---

### Task 2: `SeatHoldExpired` and the pure expiry decision

**Files:**
- Create: `proto/sagaflow/inventory/v1/seat_hold_expired.proto`
- Generated: `contracts/sagaflow/inventory/v1/seat_hold_expired.pb.go` (via `make generate`)
- Modify: `cmd/schemactl/main.go` — the `bindings` table
- Modify: `internal/inventory/seat.go` — `Apply`, `Outcome`, and the new `Expire`
- Test: `internal/inventory/seat_test.go`, `internal/inventory/example_test.go`

**Interfaces:**
- Consumes: `timers.Timer`'s `Subject`/`Token` convention from Task 1 — a seat id and a hold id, in that order.
- Produces:
  - `inventoryv1.SeatHoldExpired{HoldId, BookingId, SeatId string}`
  - `type Timer struct { FireAt time.Time; Token string }` in package `inventory`
  - `Outcome` gains a third field: `Timers []Timer`
  - `func (s SeatState) Expire(seatID, holdID string) Outcome`

- [ ] **Step 1: Write the contract**

Create `proto/sagaflow/inventory/v1/seat_hold_expired.proto`:

```proto
syntax = "proto3";

package sagaflow.inventory.v1;

// SeatHoldExpired records that a hold reached its deadline without being
// confirmed or released. It is inventory freeing its own resource, which is why
// it can happen when the booking that took the hold no longer exists.
message SeatHoldExpired {
  string hold_id = 1;
  string booking_id = 2;
  string seat_id = 3;
}
```

- [ ] **Step 2: Generate and register**

Run: `make generate`
Expected: `contracts/sagaflow/inventory/v1/seat_hold_expired.pb.go` appears.

Then add a line to the `bindings` table in `cmd/schemactl/main.go`, after the `seat_hold_released` row:

```go
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_hold_expired.proto", &inventoryv1.SeatHoldExpired{}},
```

- [ ] **Step 3: Write the failing tests**

Add to `internal/inventory/seat_test.go`:

```go
func TestAnExpiryOfTheLiveHoldFreesTheSeat(t *testing.T) {
	held := inventory.SeatState{
		Version: 1, Status: inventory.StatusHeld,
		HoldID: hold, BookingID: booking,
	}

	got := held.Expire(seat, hold)

	if len(got.Events) != 1 || len(got.Replies) != 0 {
		t.Fatalf("expiry changes the seat, so it is one event and no reply: %+v", got)
	}
	expired, ok := got.Events[0].(*inventoryv1.SeatHoldExpired)
	if !ok {
		t.Fatalf("wrong event type: %T", got.Events[0])
	}
	if expired.HoldId != hold || expired.BookingId != booking || expired.SeatId != seat {
		t.Fatalf("the expiry lost the identity of what it freed: %+v", expired)
	}

	after, err := held.Apply(expired)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if after.Status != inventory.StatusFree || after.HoldID != "" {
		t.Fatalf("an expired hold leaves the seat free: %+v", after)
	}
}

func TestAnExpiryOfAHoldThatIsGoneProducesNothingAtAll(t *testing.T) {
	// The only decision in this package that is allowed to be silent. Nothing
	// sent this and nothing is waiting on it, so there is nobody to answer.
	free := inventory.SeatState{Version: 2, Status: inventory.StatusFree}

	got := free.Expire(seat, hold)

	if len(got.Events) != 0 || len(got.Replies) != 0 {
		t.Fatalf("a timer for a hold that ended has nothing to say: %+v", got)
	}
}

func TestAnExpiryDoesNotTouchASupersededHold(t *testing.T) {
	// The token fence. hold-1's timer fires after hold-1 was released and the
	// seat was taken by hold-2; expiring hold-2 here would free a seat someone
	// is actively holding.
	current := inventory.SeatState{
		Version: 3, Status: inventory.StatusHeld,
		HoldID: "hold-2", BookingID: "booking-2",
	}

	got := current.Expire(seat, hold)

	if len(got.Events) != 0 || len(got.Replies) != 0 {
		t.Fatalf("a stale timer must not disturb the live hold: %+v", got)
	}
}

func TestHoldingASeatAsksForATimerAtTheDeadline(t *testing.T) {
	free := inventory.SeatState{Version: 0, Status: inventory.StatusFree}

	got := free.Hold(&inventoryv1.HoldSeat{
		HoldId: hold, BookingId: booking, SeatId: seat, ExpiresAt: expires,
	})

	if len(got.Timers) != 1 {
		t.Fatalf("a hold with a deadline asks for exactly one timer, got %d", len(got.Timers))
	}
	if !got.Timers[0].FireAt.Equal(expires.AsTime()) {
		t.Fatalf("the timer must fire at the command's deadline: want %v, got %v",
			expires.AsTime(), got.Timers[0].FireAt)
	}
	if got.Timers[0].Token != hold {
		t.Fatalf("the token is the hold it expires: want %q, got %q", hold, got.Timers[0].Token)
	}
}

func TestReAnnouncingAHoldAsksForNoSecondTimer(t *testing.T) {
	// Timers accompany events, never replies. A re-dispatched HoldSeat for the
	// hold that is already live changes nothing, and its timer already exists.
	held := inventory.SeatState{
		Version: 1, Status: inventory.StatusHeld,
		HoldID: hold, BookingID: booking,
	}

	got := held.Hold(&inventoryv1.HoldSeat{
		HoldId: hold, BookingId: booking, SeatId: seat, ExpiresAt: expires,
	})

	if len(got.Replies) != 1 || len(got.Events) != 0 {
		t.Fatalf("setup: expected a bare re-announcement, got %+v", got)
	}
	if len(got.Timers) != 0 {
		t.Fatalf("the timer was scheduled when the hold was taken, got %d more", len(got.Timers))
	}
}

func TestAHoldWithNoDeadlineExpiresImmediately(t *testing.T) {
	// A HoldSeat with no expires_at is malformed. Of the two ways to fail, the
	// timer still gets scheduled — at the zero time, which is already past — so
	// the seat is freed on the next pass and the saga is told. Refusing to
	// schedule would instead produce the one thing this phase exists to prevent:
	// a hold no clock will ever end.
	free := inventory.SeatState{Version: 0, Status: inventory.StatusFree}

	got := free.Hold(&inventoryv1.HoldSeat{HoldId: hold, BookingId: booking, SeatId: seat})

	if len(got.Timers) != 1 {
		t.Fatalf("a hold always gets a deadline, got %d timers", len(got.Timers))
	}
	if !got.Timers[0].FireAt.Before(time.Now()) {
		t.Fatalf("a missing deadline is already past, got %v", got.Timers[0].FireAt)
	}
}
```

Add `"time"` to that file's imports if it is not already there.

- [ ] **Step 4: Run the tests to verify they fail**

Run: `go test ./internal/inventory/ -short -run 'Expir|Timer|Deadline' -v`
Expected: FAIL — `Expire` undefined, `Outcome` has no field `Timers`.

- [ ] **Step 5: Extend `seat.go`**

Fold the new event. Replace the `SeatHoldReleased` case in `Apply` with:

```go
	// Released and expired differ in why the hold ended, not in what the seat
	// becomes. A fold that cared about the difference would be reading intent
	// out of history it is only supposed to be replaying.
	case *inventoryv1.SeatHoldReleased, *inventoryv1.SeatHoldExpired:
		s.Status, s.HoldID, s.BookingID, s.ExpiresAt = StatusFree, "", "", time.Time{}
```

Add the `Timer` type above `Outcome`:

```go
// Timer is a wake-up a decision asks for: come back at FireAt, carrying Token so
// whoever wakes up can tell whether the world moved on in the meantime.
//
// The decision names its own deadlines rather than a handler inferring them from
// the events, and it still needs no clock to do so — the deadline arrived in the
// command.
type Timer struct {
	FireAt time.Time
	Token  string
}
```

Add the field to `Outcome` and extend its comment:

```go
type Outcome struct {
	Events  []proto.Message
	Replies []proto.Message
	Timers  []Timer
}
```

```go
// Timers accompany events and never replies. A re-announced hold already has the
// timer that was scheduled when it was taken, so a second one would be a row in
// the table for a deadline that is already covered.
```

Populate it in `Hold`'s final return:

```go
	return Outcome{
		Events: []proto.Message{&inventoryv1.SeatHeld{
			HoldId:    cmd.HoldId,
			BookingId: cmd.BookingId,
			SeatId:    cmd.SeatId,
			ExpiresAt: cmd.ExpiresAt,
		}},
		// AsTime on a nil deadline is the zero time, which is already past: a
		// malformed hold expires on the next pass rather than never.
		Timers: []Timer{{FireAt: cmd.ExpiresAt.AsTime(), Token: cmd.HoldId}},
	}
```

Add `Expire` immediately after `Release`:

```go
// Expire decides that the hold named by holdID has reached its deadline.
//
// It is the one decision here that can produce nothing at all. Every other
// decision answers a command that someone sent and is waiting on, so silence
// would strand a saga. A timer is not a command: nothing sent it, nothing
// correlates to it, and there is no envelope to reply into. When the hold is
// already gone — released, or superseded by a later one — doing nothing is the
// whole correct answer.
//
// That is also why timers are never cancelled. A release and its hold's deadline
// can arrive in either order, and the outcome is the same either way, because the
// stream decides rather than the clock.
//
// seatID comes before holdID to match the timer's own Subject-then-Token order.
func (s SeatState) Expire(seatID, holdID string) Outcome {
	if s.Status != StatusHeld || s.HoldID != holdID {
		return Outcome{}
	}
	return Outcome{Events: []proto.Message{&inventoryv1.SeatHoldExpired{
		HoldId:    s.HoldID,
		BookingId: s.BookingID,
		SeatId:    seatID,
	}}}
}
```

- [ ] **Step 6: Add the worked example**

Append to `internal/inventory/example_test.go`:

```go
// ExampleSeatState_Expire shows the one decision in this package that answers
// with nothing.
//
// The hold this timer was set for is long gone — released, and the seat taken by
// a different booking since. Expiring it here would free a seat someone is
// actively holding, so the token fence stops it. Nothing comes back, and nothing
// is the right answer: no command was sent, so nobody is waiting for a reply.
func ExampleSeatState_Expire() {
	current := inventory.SeatState{
		Version:   3,
		Status:    inventory.StatusHeld,
		HoldID:    "hold-2",
		BookingID: "booking-2",
	}

	stale := current.Expire("seat-BA117-2026-09-01-14A", "hold-1")
	live := current.Expire("seat-BA117-2026-09-01-14A", "hold-2")

	fmt.Println("stale timer -> events:", len(stale.Events), "replies:", len(stale.Replies))
	fmt.Println("live timer  -> events:", len(live.Events), "replies:", len(live.Replies))
	fmt.Printf("live timer  -> event:   %s\n",
		live.Events[0].ProtoReflect().Descriptor().FullName())

	// Output:
	// stale timer -> events: 0 replies: 0
	// live timer  -> events: 1 replies: 0
	// live timer  -> event:   sagaflow.inventory.v1.SeatHoldExpired
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./internal/inventory/ -short -v && make lint`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add proto contracts cmd/schemactl/main.go internal/inventory
git commit -m "feat(inventory): SeatHoldExpired, and the decision that may say nothing"
```

---

### Task 3: Scheduling the timer in the hold's own transaction

**Files:**
- Modify: `internal/inventory/commands.go`
- Test: `internal/inventory/commands_test.go`

**Interfaces:**
- Consumes: `timers.Schedule` (Task 1), `Outcome.Timers` (Task 2).
- Produces: after `Handle` applies a `HoldSeat`, the `timers` table holds one pending row with `subject` = the seat id and `token` = the hold id, committed with the event.

- [ ] **Step 1: Write the failing test**

Add to `internal/inventory/commands_test.go`:

```go
func TestHoldingASeatCommitsItsTimerWithTheEvent(t *testing.T) {
	// The event and its deadline are one commit or neither. A hold that
	// committed without its timer is a seat no clock will ever free — the exact
	// failure the TTL exists to prevent.
	ctx := t.Context()
	pool := db(t, "inventory_hold_timer")
	h := inventory.NewHandler(pool, fakeEncoder{})

	if err := h.Handle(ctx, incoming("ce-1"), holdCmd(hold)); err != nil {
		t.Fatalf("handle: %v", err)
	}

	due, err := timers.Due(ctx, pool, timers.BatchSize)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	// expires is in the future, so nothing is due yet; read the row directly.
	if len(due) != 0 {
		t.Fatalf("the deadline has not passed, so nothing is due: %+v", due)
	}

	var subject, token string
	if err := pool.QueryRow(ctx,
		`SELECT subject, token FROM timers WHERE fired_at IS NULL`).
		Scan(&subject, &token); err != nil {
		t.Fatalf("the hold committed without its timer: %v", err)
	}
	if subject != seat || token != hold {
		t.Fatalf("the timer names the wrong seat or hold: subject %q, token %q", subject, token)
	}
}

func TestARefusedHoldSchedulesNoTimer(t *testing.T) {
	// Nothing changed, so there is nothing to expire. A timer here would be a
	// row waiting to fire on a hold this service never took.
	ctx := t.Context()
	pool := db(t, "inventory_refused_timer")
	h := inventory.NewHandler(pool, fakeEncoder{})

	if err := h.Handle(ctx, incoming("ce-1"), holdCmd("hold-1")); err != nil {
		t.Fatalf("first hold: %v", err)
	}
	if err := h.Handle(ctx, incoming("ce-2"), holdCmd("hold-2")); err != nil {
		t.Fatalf("second hold: %v", err)
	}

	var pending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM timers WHERE fired_at IS NULL`).Scan(&pending); err != nil {
		t.Fatalf("count timers: %v", err)
	}
	if pending != 1 {
		t.Fatalf("one hold was taken and one refused, so one timer: got %d", pending)
	}
}
```

Reuse whatever `incoming`, `holdCmd`, `fakeEncoder` and `db` helpers already exist in that file; if their names differ, use the existing ones rather than adding duplicates. Add the `timers` import.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/inventory/ -run Timer -v`
Expected: FAIL — no rows in `timers`.

- [ ] **Step 3: Schedule inside the transaction**

In `internal/inventory/commands.go`, add to `applyInOneTransaction`, immediately after the `AppendSeat` call and before `h.messages`:

```go
		if err := scheduleTimers(ctx, tx, seatID, decision.Timers); err != nil {
			return err
		}
```

and add the helper below `applyInOneTransaction`:

```go
// scheduleTimers records the decision's deadlines in the same transaction as the
// events that need them.
//
// It is a fourth thing in this commit, alongside the events, the outgoing
// messages and the consumed-record, and it is here for the same reason they are:
// a hold whose event committed without its deadline is a seat that stays held
// until someone notices by hand.
func scheduleTimers(ctx context.Context, tx pgx.Tx, seatID string, ts []Timer) error {
	for _, t := range ts {
		if err := timers.Schedule(ctx, tx, t.FireAt, seatID, t.Token); err != nil {
			return err
		}
	}
	return nil
}
```

Add `"github.com/AymanKastali/sagaflow/internal/platform/timers"` to the imports.

Also extend `applyInOneTransaction`'s doc comment: change "Three things are written together and must not come apart" to "Four things", and add a sentence after the existing two failure descriptions:

```go
// If the events committed but the timer did not, the hold would exist with no
// deadline attached — held forever, with nothing anywhere recording that
// something went wrong.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/inventory/ -v && make lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/inventory
git commit -m "feat(inventory): a hold's deadline commits with the hold"
```

---

### Task 4: Inventory's expiry handler

**Files:**
- Create: `internal/inventory/expiry.go`
- Modify: `internal/inventory/commands.go` — extract the message framing both handlers now need
- Test: `internal/inventory/expiry_test.go`

**Interfaces:**
- Consumes: `timers.Timer`, `timers.Firer` (Task 1); `SeatState.Expire` (Task 2).
- Produces:
  - `type Expirer struct{ … }`, `func NewExpirer(enc Encoder) *Expirer`
  - `func (e *Expirer) Fire(ctx context.Context, tx pgx.Tx, t timers.Timer) error` — satisfies `timers.Firer`
  - unexported `func outgoing(enc Encoder, msgs []proto.Message, tmpl envelope.Envelope) ([]envelope.Message, error)`, replacing `Handler.messages`

- [ ] **Step 1: Write the failing tests**

Create `internal/inventory/expiry_test.go`:

```go
package inventory_test

import (
	"testing"
	"time"

	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// tick runs one scheduler pass and returns how many timers fired.
func tick(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	n, err := timers.NewScheduler(pool, inventory.NewExpirer(fakeEncoder{})).Tick(t.Context())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	return n
}

// seatState folds the seat's stream as it stands now.
func seatState(t *testing.T, pool *pgxpool.Pool, seatID string) inventory.SeatState {
	t.Helper()
	var s inventory.SeatState
	if err := pg.WithTx(t.Context(), pool, func(tx pgx.Tx) error {
		var err error
		s, err = inventory.LoadSeat(t.Context(), tx, seatID)
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	return s
}

func TestADeadlineThatPassesFreesTheSeat(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_expiry")
	h := inventory.NewHandler(pool, fakeEncoder{})

	cmd := holdCmd(hold)
	cmd.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second)) // already past
	if err := h.Handle(ctx, incoming("ce-1"), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}

	if n := tick(t, pool); n != 1 {
		t.Fatalf("one deadline has passed, so one timer fires: got %d", n)
	}

	got := seatState(t, pool, seat)
	if got.Status != inventory.StatusFree {
		t.Fatalf("the expired hold left the seat held: %+v", got)
	}
	if got.Version != 2 {
		t.Fatalf("SeatHeld then SeatHoldExpired is two events: got version %d", got.Version)
	}

	var published int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox`).Scan(&published); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if published != 2 {
		t.Fatalf("the hold and the expiry are both announced: want 2 outbox rows, got %d", published)
	}
}

func TestAnExpiryAnnouncesItselfToTheBookingThatHeldTheSeat(t *testing.T) {
	// Nothing sent this expiry, so there is no incoming envelope to copy a
	// correlation id from. The seat's own stream knows which booking held it,
	// which is what lets the saga recognise the message as part of its flow.
	ctx := t.Context()
	pool := db(t, "inventory_expiry_correlation")
	h := inventory.NewHandler(pool, fakeEncoder{})

	cmd := holdCmd(hold)
	cmd.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
	if err := h.Handle(ctx, incoming("ce-1"), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}
	tick(t, pool)

	var correlation, causation *string
	if err := pool.QueryRow(ctx, `
		SELECT headers->>'ce_correlationid', headers->>'ce_causationid'
		FROM outbox
		WHERE headers->>'ce_type' = $1`,
		"sagaflow.inventory.v1.SeatHoldExpired").Scan(&correlation, &causation); err != nil {
		t.Fatalf("read the expiry's headers: %v", err)
	}
	if correlation == nil || *correlation != booking {
		t.Fatalf("the expiry must carry the booking as its correlation id, got %v", correlation)
	}
	if causation != nil {
		t.Fatalf("no message caused this, so the chain ends here, got %q", *causation)
	}
}

func TestAStaleTimerLeavesTheNewHoldAlone(t *testing.T) {
	// hold-1's deadline passes after hold-1 was released and the seat retaken.
	// Without the token fence this frees a seat someone is actively holding.
	ctx := t.Context()
	pool := db(t, "inventory_expiry_stale")
	h := inventory.NewHandler(pool, fakeEncoder{})

	first := holdCmd("hold-1")
	first.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
	if err := h.Handle(ctx, incoming("ce-1"), first); err != nil {
		t.Fatalf("first hold: %v", err)
	}
	if err := h.Handle(ctx, incoming("ce-2"), releaseCmd("hold-1")); err != nil {
		t.Fatalf("release: %v", err)
	}
	second := holdCmd("hold-2")
	second.ExpiresAt = timestamppb.New(time.Now().Add(time.Hour))
	if err := h.Handle(ctx, incoming("ce-3"), second); err != nil {
		t.Fatalf("second hold: %v", err)
	}

	if n := tick(t, pool); n != 1 {
		t.Fatalf("the stale timer is due and is claimed: got %d", n)
	}

	got := seatState(t, pool, seat)
	if got.Status != inventory.StatusHeld || got.HoldID != "hold-2" {
		t.Fatalf("a stale timer disturbed the live hold: %+v", got)
	}
	if got.Version != 3 {
		t.Fatalf("held, released, held again — and the stale fire appended nothing: got version %d",
			got.Version)
	}
}

func TestATimerThatFiredIsNotDueAgain(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_expiry_once")
	h := inventory.NewHandler(pool, fakeEncoder{})

	cmd := holdCmd(hold)
	cmd.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
	if err := h.Handle(ctx, incoming("ce-1"), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}

	if n := tick(t, pool); n != 1 {
		t.Fatalf("first pass fires the timer: got %d", n)
	}
	if n := tick(t, pool); n != 0 {
		t.Fatalf("a fired timer is done: second pass fired %d", n)
	}

	if got := seatState(t, pool, seat); got.Version != 2 {
		t.Fatalf("the second pass appended a second expiry: version %d", got.Version)
	}
}
```

Reuse the existing `db`, `incoming`, `holdCmd`, `releaseCmd`, `fakeEncoder`, `seat`, `hold` and `booking` helpers from the package's other test files; if a `releaseCmd` helper does not exist, add one alongside the existing `holdCmd`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/inventory/ -run Expir -v`
Expected: FAIL — `inventory.NewExpirer` undefined.

- [ ] **Step 3: Extract the shared framing**

Both handlers now wrap outgoing messages in envelopes, and only the template differs. In `internal/inventory/commands.go`, delete the `messages` method and add this package-level function in its place:

```go
// outgoing frames each message and wraps it in its own envelope, taking
// everything but the id and the type from tmpl.
//
// Each message gets a new ce_id because each is a distinct message. The rest —
// which flow it belongs to, what caused it, which seat it is about — is the
// caller's to decide, because a reply to a command and a timer's expiry answer
// those questions differently.
func outgoing(enc Encoder, msgs []proto.Message, tmpl envelope.Envelope) ([]envelope.Message, error) {
	out := make([]envelope.Message, 0, len(msgs))
	for _, m := range msgs {
		payload, err := enc.Encode(m)
		if err != nil {
			return nil, fmt.Errorf("inventory: frame %s: %w", codec.TypeName(m), err)
		}
		e := tmpl
		e.ID = envelope.NewID()
		e.Type = codec.TypeName(m)
		out = append(out, envelope.Message{
			Topic:   EventsTopic,
			Key:     e.Subject, // the stream id, which is what preserves per-seat ordering
			Payload: payload,
			Headers: e.Headers(),
		})
	}
	return out, nil
}
```

and change the call site in `applyInOneTransaction`:

```go
		// A reply keeps the incoming correlation id so the saga can route it, and
		// takes the incoming ce_id as its causation, so the chain can be walked
		// back message by message.
		msgs, err := outgoing(h.enc, decision.Messages(), envelope.Envelope{
			Source:        Source,
			Subject:       seatID,
			CorrelationID: incoming.CorrelationID,
			CausationID:   incoming.ID,
			TraceParent:   incoming.TraceParent,
		})
		if err != nil {
			return err
		}
```

- [ ] **Step 4: Write `expiry.go`**

Create `internal/inventory/expiry.go`:

```go
package inventory

import (
	"context"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/AymanKastali/sagaflow/internal/platform/outbox"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5"
)

// Expirer frees seat holds whose deadlines have passed. It is inventory's
// timers.Firer, and it is what makes a hold survivable when the booking that
// took it does not.
//
// It holds no pool. The scheduler hands it the transaction that claimed the
// timer row, and everything it does has to land in that same commit.
type Expirer struct {
	enc Encoder
}

func NewExpirer(enc Encoder) *Expirer { return &Expirer{enc: enc} }

// Fire expires the hold its timer was set for, if that hold is still the live
// one.
//
// There is no inbox row here, which is the difference worth noticing. Every other
// handler in this package deduplicates a delivery, because a broker can hand it
// the same message twice. Nothing was delivered here: the timer row's own claim,
// already taken by the caller in this very transaction, is the whole once-only
// guarantee.
//
// A version conflict needs no retry loop either. Returning the error rolls back
// the claim along with everything else, so the row is pending again and the next
// pass re-reads a seat that has moved on.
func (e *Expirer) Fire(ctx context.Context, tx pgx.Tx, t timers.Timer) error {
	state, err := LoadSeat(ctx, tx, t.Subject)
	if err != nil {
		return err
	}
	decision := state.Expire(t.Subject, t.Token)
	if len(decision.Events) == 0 {
		return nil // the hold ended already; claiming the row is all this pass does
	}
	// No incoming envelope exists, so the seat's own stream supplies the flow
	// this expiry belongs to: the booking that is holding it.
	meta := eventstore.Meta{CorrelationID: state.BookingID}
	if err := AppendSeat(ctx, tx, t.Subject, state.Version, decision.Events, meta); err != nil {
		return err
	}
	msgs, err := outgoing(e.enc, decision.Messages(), envelope.Envelope{
		Source:        Source,
		Subject:       t.Subject,
		CorrelationID: state.BookingID,
		// No causation id: nothing caused this but a deadline passing. A reader
		// walking the chain backwards should find that it ends here rather than
		// find a plausible-looking lie.
	})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, msgs)
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/inventory/ -v && make lint && make test`
Expected: PASS, including `internal/docs`.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory
git commit -m "feat(inventory): expire a hold from its own timer, with no inbox row"
```

---

### Task 5: The chapter, the architecture doc, and the phase's proof

**Files:**
- Modify: `internal/inventory/doc.go` — the seat lifecycle now has a third ending
- Modify: `docs/architecture.md`
- Modify: `docs/message-lifecycle.md`
- Modify: `README.md` — the build-order table
- Test: `internal/inventory/expiry_test.go` — the one test that proves the phase's claim

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces: nothing new in code.

- [ ] **Step 1: Write the failing test**

This is the test the phase exists for. Add to `internal/inventory/expiry_test.go`:

```go
func TestASeatFreesItselfWhileNothingElseIsRunning(t *testing.T) {
	// The claim of this phase, stated as a test. A hold is taken and then
	// nothing further happens — no release arrives, no consumer runs, no saga
	// exists. The scheduler is the only thing still moving, and the seat comes
	// back on its own.
	ctx := t.Context()
	pool := db(t, "inventory_self_healing")

	cmd := holdCmd(hold)
	cmd.ExpiresAt = timestamppb.New(time.Now().Add(150 * time.Millisecond))
	if err := inventory.NewHandler(pool, fakeEncoder{}).
		Handle(ctx, incoming("ce-1"), cmd); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if got := seatState(t, pool, seat); got.Status != inventory.StatusHeld {
		t.Fatalf("setup: the seat should be held, got %+v", got)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	sched := timers.NewScheduler(pool, inventory.NewExpirer(fakeEncoder{}))
	go func() { _ = sched.Run(runCtx) }()

	// Poll the effect rather than wait a duration: the assertion is bounded by
	// t.Context's deadline, so a slow machine is slow rather than flaky.
	for {
		if seatState(t, pool, seat).Status == inventory.StatusFree {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("the deadline passed and the seat is still held")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
```

Add `"context"` to that file's imports.

- [ ] **Step 2: Run the test to verify it passes**

Run: `go test ./internal/inventory/ -run FreesItself -v`
Expected: PASS on the work already done in Tasks 1–4. This test adds no mechanism; it only assembles what the earlier tasks built. A failure here is a real defect in one of them, not a missing piece of this task.

- [ ] **Step 3: Update inventory's chapter**

In `internal/inventory/doc.go`, the seat lifecycle is described as ending in a release. Find the passage that describes what ends a hold and extend it so expiry is a peer of release, not a footnote. Add to *What this package does*:

```go
// A hold ends in one of three ways, and all three are events on the seat's own
// stream: the saga confirms it, the saga releases it, or its deadline passes and
// inventory expires it. The third exists because the first two require the
// booking service to be alive, and the seat has to come back even when it is not.
```

Add to *What it deliberately does not do*:

```go
// It does not treat a deadline as making a hold false. An expired hold is held
// until SeatHoldExpired is appended, which is why a new hold can never race an
// expiry and why the decision functions still need no clock.
```

Add to *Reading order*: `expiry.go` — one clause, naming it as the handler with no inbox row.

- [ ] **Step 4: Update the prose docs**

In `docs/architecture.md`, add the timer to whatever diagram or list enumerates what a service runs alongside its consumers and its outbox poller. It is a third background loop and belongs beside them.

In `docs/message-lifecycle.md`, add a short section — a few sentences — on the message nobody sent: a `SeatHoldExpired` has a correlation id and no causation id, and that asymmetry is how a reader tells a reaction from an initiation.

In `README.md`, add phase 5 to the build-order table:

```markdown
| 5 | Inventory — seat streams, holds, self-expiring TTL | `internal/inventory`, `internal/platform/timers` | **in progress** |
```

- [ ] **Step 5: Run the full suite**

Run: `make lint && make test && make test-integration`
Expected: PASS everywhere, including `internal/docs`.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory docs README.md
git commit -m "docs(inventory): a hold ends three ways, and one of them needs nobody alive"
```

---

## Self-review

**Spec coverage.** §10.5's table is Task 1 verbatim, its `token` fence is Task 2, and its "same claim-based pattern" is Task 1's `MarkFired`. §7.2's amended invariant — one stream, outbox rows, inbox row, timers — is Task 3. §10.3's 15-minute production TTL is supplied by the caller in the `HoldSeat` command, so nothing in this phase hardcodes it; the 200 ms–2 s test range appears in Tasks 4 and 5. §12.4's determinism rule is why `Tick` is exported and why Task 5 polls an effect instead of sleeping. Not covered here, by design: the availability projection and `wire.go` (phase 5c), and booking's step timeout (phase 7), which reuses `platform/timers` without changing it.

**Type consistency.** `timers.Timer` (ID, FireAt, Subject, Token) and `inventory.Timer` (FireAt, Token) are different types with the same name in different packages; Task 3's `scheduleTimers` is the single place that converts one to the other, supplying `Subject` from the seat id. `Expire(seatID, holdID)` takes its arguments in the same order as the timer's `Subject`, `Token`, and Task 4 passes `t.Subject, t.Token` — the one call site where transposing them would compile and be wrong.

**Ordering.** Task 2 depends on Task 1 only for the `Subject`/`Token` convention, not for any symbol, so it could run first. Task 3 needs both. Task 4 needs Task 3's extracted `outgoing`. Task 5 needs all of them and adds no new mechanism.
