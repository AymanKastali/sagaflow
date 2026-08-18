package inbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const markSQL = `
INSERT INTO inbox (consumer, source, event_id)
VALUES ($1, $2, $3)
ON CONFLICT (consumer, source, event_id) DO NOTHING`

// MarkConsumed records that consumer has handled (source, eventID).
//
// It returns false when the message was already handled, in which case the
// caller rolls back and acknowledges without applying anything.
//
// Duplicates are detected by rows-affected rather than by catching a unique
// violation. A raised 23505 would abort the transaction: every subsequent
// statement fails with 25P02 and COMMIT degrades to a rollback, so the "harmless"
// version of that bug is a handler that appears to work and commits nothing.
//
// source and eventID are CloudEvents ce_source and ce_id, which the specification
// guarantees unique as a pair — so this reuses an identity the producer already
// had to establish rather than inventing one.
func MarkConsumed(ctx context.Context, tx pgx.Tx, consumer, source, eventID string) (bool, error) {
	if consumer == "" || source == "" || eventID == "" {
		return false, fmt.Errorf("inbox: consumer, source and event_id are all required (got %q, %q, %q)",
			consumer, source, eventID)
	}
	tag, err := tx.Exec(ctx, markSQL, consumer, source, eventID)
	if err != nil {
		return false, fmt.Errorf("inbox: mark %s/%s for %s: %w", source, eventID, consumer, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Prune deletes rows older than olderThan.
//
// olderThan must exceed Kafka's retention, because a pruned row becomes
// deliverable again: if a message could still be replayed from the log after its
// inbox row was deleted, it would be applied twice.
func Prune(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx,
		`DELETE FROM inbox WHERE handled_at < now() - $1::interval`, olderThan)
	if err != nil {
		return 0, fmt.Errorf("inbox: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}
