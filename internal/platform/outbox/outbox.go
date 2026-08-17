// Package outbox makes "the state changed" and "the message was sent" the same
// commit.
//
// Enqueue writes into the caller's transaction; a separate poller publishes what
// was committed. The guarantee is at-least-once and deliberately not
// exactly-once — a crash between publishing and marking republishes, which is
// precisely why platform/inbox exists (spec §10.1).
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// NotifyChannel is the LISTEN/NOTIFY channel the poller wakes on.
const NotifyChannel = "outbox"

// Message is one message to publish.
//
// Key is always the target stream id: it becomes the Kafka partition key, which
// is what keeps two events for one stream in order.
type Message struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// validate rejects a message the poller could not publish correctly.
func (m Message) validate() error {
	switch {
	case m.Topic == "":
		return errors.New("no topic")
	case m.Key == "":
		// Without a key Kafka round-robins the record, which destroys the
		// per-stream ordering every consumer downstream relies on.
		return errors.New("no key")
	case len(m.Payload) == 0:
		return errors.New("no payload")
	}
	return nil
}

// Claimed is a Message plus the row id the poller must mark once it is published.
type Claimed struct {
	ID int64
	Message
}

// Publisher sends claimed messages. It is an interface so that every property of
// the poller — ordering, retention on failure, election — is testable without a
// broker. The Kafka implementation lives in platform/kafka.
type Publisher interface {
	Publish(ctx context.Context, msgs []Claimed) error
}

// enqueueSQL inserts the whole batch in one statement. WITH ORDINALITY preserves
// the caller's order in the generated ids, which is what the poller publishes by.
const enqueueSQL = `
INSERT INTO outbox (topic, key, payload, headers)
SELECT t.topic, t.key, t.payload, t.headers
FROM unnest($1::text[], $2::text[], $3::bytea[], $4::jsonb[])
     WITH ORDINALITY AS t(topic, key, payload, headers, ord)
ORDER BY t.ord`

// Enqueue writes msgs into tx. It must be called in the same transaction as the
// state change it accompanies; that co-commit is the entire point.
//
// The NOTIFY is transactional — Postgres delivers it on commit and discards it on
// rollback — so a woken poller never chases a message that was never written.
func Enqueue(ctx context.Context, tx pgx.Tx, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	topics := make([]string, len(msgs))
	keys := make([]string, len(msgs))
	payloads := make([][]byte, len(msgs))
	headers := make([]string, len(msgs))

	for i, m := range msgs {
		if err := m.validate(); err != nil {
			return fmt.Errorf("outbox: message %d for %q: %w", i, m.Topic, err)
		}
		h, err := json.Marshal(m.Headers)
		if err != nil {
			return fmt.Errorf("outbox: marshal headers for %s: %w", m.Topic, err)
		}
		topics[i], keys[i], payloads[i], headers[i] = m.Topic, m.Key, m.Payload, string(h)
	}

	if _, err := tx.Exec(ctx, enqueueSQL, topics, keys, payloads, headers); err != nil {
		return fmt.Errorf("outbox: enqueue %d messages: %w", len(msgs), err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, '')", NotifyChannel); err != nil {
		return fmt.Errorf("outbox: notify: %w", err)
	}
	return nil
}
