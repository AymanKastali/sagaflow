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
