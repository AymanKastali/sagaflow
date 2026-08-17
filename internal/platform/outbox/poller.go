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
	// release is idempotent. The natural calling pattern is `defer release()` plus
	// an explicit release on the hand-over path, and a pooled connection that has
	// already gone back to the pool panics on use — pgxpool.Conn.Exec dereferences
	// a nil resource — so calling twice must be safe rather than fatal.
	var once sync.Once
	return true, func() {
		once.Do(func() {
			// Unlock explicitly so a graceful shutdown hands over immediately
			// rather than waiting for the connection to be reaped.
			_, _ = conn.Exec(context.WithoutCancel(ctx),
				"SELECT pg_advisory_unlock($1)", int64(AdvisoryLockKey))
			conn.Release()
		})
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
	listener.Go(func() {
		for {
			if _, err := listenConn.Conn().WaitForNotification(ctx); err != nil {
				return // ctx cancelled, or the connection went away
			}
			select {
			case notified <- struct{}{}:
			default: // a wake-up is already pending; coalesce
			}
		}
	})
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
