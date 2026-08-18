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
