package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kptac/sagaflow/internal/platform/envelope"
)

const (
	// BatchSize bounds one claim (spec §10.3).
	BatchSize = 100
	// AdvisoryLockKey elects the single active poller per database.
	AdvisoryLockKey = 0x5A6A_0001
	// PollFloor is the ticker interval backing up LISTEN/NOTIFY, so a missed
	// notification costs a second of latency rather than a stuck queue.
	PollFloor = time.Second
	// Retention is how long published rows are kept for after-the-fact auditing.
	Retention = 7 * 24 * time.Hour
	// pruneInterval is how often rows older than Retention are deleted.
	pruneInterval = time.Hour
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

// Run elects this poller and publishes until ctx is cancelled. A poller that
// loses the election waits, ready to take over if the winner goes away.
func (p *Poller) Run(ctx context.Context) error {
	held, release, err := p.TryElect(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !held {
		p.log.Info("outbox poller standing by; another instance holds the lock")
		<-ctx.Done()
		return nil
	}

	woken, stopListening, err := p.listen(ctx)
	if err != nil {
		return err
	}
	defer stopListening()

	// The ticker is not redundant with NOTIFY: Postgres coalesces a notification
	// that arrives mid-batch, and a dropped listener connection would otherwise
	// leave the queue stalled.
	poll := time.NewTicker(PollFloor)
	defer poll.Stop()
	prune := time.NewTicker(pruneInterval)
	defer prune.Stop()

	for {
		p.drainAll(ctx)

		select {
		case <-ctx.Done():
			return nil
		case <-woken:
		case <-poll.C:
		case <-prune.C:
			p.pruneOldRows(ctx)
		}
	}
}

// drainAll publishes everything currently committed. A batch that filled
// BatchSize means more is waiting, so it keeps draining until one does not.
//
// Failures are logged rather than returned: the next wake-up retries, and the
// rows stay claimable in the meantime.
func (p *Poller) drainAll(ctx context.Context) {
	for {
		n, err := p.Drain(ctx)
		if err != nil {
			if ctx.Err() == nil {
				p.log.Error("outbox drain failed; retrying", "error", err)
			}
			return
		}
		if n < BatchSize {
			return
		}
	}
}

// pruneOldRows deletes published rows past Retention. Housekeeping must never
// stop publishing, so a failure is logged and the loop carries on.
func (p *Poller) pruneOldRows(ctx context.Context) {
	deleted, err := p.Prune(ctx, Retention)
	switch {
	case err != nil:
		p.log.Error("outbox prune failed", "error", err)
	case deleted > 0:
		p.log.Info("pruned published outbox rows", "deleted", deleted)
	}
}

// listen subscribes to NotifyChannel and returns a channel that fires when there
// may be work. Wake-ups are coalesced, so it is a hint to drain, not a count.
//
// Call stop once ctx is cancelled: it waits for the listening goroutine before
// returning the connection to the pool, because a pooled connection can be handed
// straight to another caller while that goroutine is still reading from it.
func (p *Poller) listen(ctx context.Context) (woken <-chan struct{}, stop func(), err error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("outbox: acquire listen conn: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		conn.Release()
		return nil, nil, fmt.Errorf("outbox: listen: %w", err)
	}

	wake := make(chan struct{}, 1)
	var listener sync.WaitGroup
	listener.Go(func() {
		for {
			if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
				return // ctx cancelled, or the connection went away
			}
			select {
			case wake <- struct{}{}:
			default: // a wake-up is already pending; coalesce
			}
		}
	})
	return wake, func() {
		listener.Wait()
		conn.Release()
	}, nil
}

// claimSQL takes unpublished rows and holds them for this transaction.
//
// Rows are claimed by flag, never by a cursor over id. BIGSERIAL values are
// handed out at insert but become visible at commit, so a late-committing row can
// carry a lower id than one already published — a cursor would step over it and
// lose it silently, only under concurrency (spec §6.4). A flag has no such
// window: whatever is still NULL gets claimed on the next pass.
//
// SKIP LOCKED is for failover, not throughput: a second poller taking over mid
// batch makes progress instead of blocking on the previous one's rows.
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
// Publishing happens inside the claiming transaction. That holds a database
// transaction open for the duration of a Kafka round trip, which is the accepted
// cost of the alternatives being worse: mark-then-publish loses messages, and
// publishing after the commit lets a second poller claim the same rows.
func (p *Poller) Drain(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	claimed, err := claim(ctx, tx)
	if err != nil {
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	if err := p.pub.Publish(ctx, messages(claimed)); err != nil {
		// Rolling back releases the claim, so the rows are picked up next pass.
		return 0, fmt.Errorf("outbox: publish %d messages: %w", len(claimed), err)
	}
	if _, err := tx.Exec(ctx, markSQL, ids(claimed)); err != nil {
		return 0, fmt.Errorf("outbox: mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		// The messages are already on the broker, so the next pass republishes
		// them: at-least-once, absorbed by the inbox.
		return 0, fmt.Errorf("outbox: commit mark: %w", err)
	}
	return len(claimed), nil
}

// claim reads and locks up to BatchSize unpublished rows, oldest first.
func claim(ctx context.Context, tx pgx.Tx) ([]Claimed, error) {
	rows, err := tx.Query(ctx, claimSQL, BatchSize)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	defer rows.Close()

	var claimed []Claimed
	for rows.Next() {
		var (
			c           Claimed
			headersJSON []byte
		)
		if err := rows.Scan(&c.ID, &c.Topic, &c.Key, &c.Payload, &headersJSON); err != nil {
			return nil, fmt.Errorf("outbox: scan claimed row: %w", err)
		}
		if err := json.Unmarshal(headersJSON, &c.Headers); err != nil {
			return nil, fmt.Errorf("outbox: unmarshal headers for row %d: %w", c.ID, err)
		}
		claimed = append(claimed, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: iterate claimed rows: %w", err)
	}
	return claimed, nil
}

func ids(claimed []Claimed) []int64 {
	out := make([]int64, len(claimed))
	for i, c := range claimed {
		out[i] = c.ID
	}
	return out
}

// messages strips the row ids the publisher has no use for. The correspondence
// with claimed is positional, but nothing needs it: Publish is all-or-nothing,
// so a success marks the whole batch.
func messages(claimed []Claimed) []envelope.Message {
	out := make([]envelope.Message, len(claimed))
	for i, c := range claimed {
		out[i] = c.Message
	}
	return out
}

// Prune deletes rows published longer ago than olderThan. The events themselves
// are already durable in the events table, so this window exists only so that a
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

// TryElect attempts to become the single active poller for this database. The
// returned release is idempotent, so `defer release()` alongside an explicit
// release on the hand-over path is safe.
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

	// Guarded by Once because a pooled connection panics when used after it has
	// gone back to the pool: pgxpool.Conn.Exec dereferences a nil resource.
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
