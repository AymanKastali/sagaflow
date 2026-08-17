// Package eventstore is the append-only event log every service owns a copy of.
//
// It knows nothing about event payloads beyond "a type name and some JSON".
// Encoding lives in platform/codec; folding events into state lives in each
// service. This package's whole job is the version invariant.
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Meta travels with every event so a stored row can be traced back to the
// request that caused it (spec §11).
type Meta struct {
	TraceID       string `json:"trace_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
}

// Event is an event about to be appended. Data is protojson bytes: readable in
// psql and independent of the schema registry (spec §8.4).
type Event struct {
	Type string
	Data []byte
	Meta Meta
}

// Recorded is an event read back from the log.
type Recorded struct {
	Event
	StreamID   string
	Version    int
	GlobalSeq  int64
	RecordedAt time.Time
}

const appendSQL = `
INSERT INTO events (stream_id, version, type, data, meta)
SELECT $1, $2 + t.ord, t.type, t.data, t.meta
FROM unnest($3::text[], $4::jsonb[], $5::jsonb[])
     WITH ORDINALITY AS t(type, data, meta, ord)`

// Append writes evts to streamID at versions expectedVersion+1 … +n.
//
// expectedVersion is the version the caller's in-memory state was folded from.
// A unique violation on (stream_id, version) means someone else got there
// first, and is translated to ErrVersionConflict.
//
// One statement, one round trip, one error to interpret: WITH ORDINALITY
// numbers the unnested rows from 1, so version arithmetic happens in the
// database and there is no loop that could skip a number.
func Append(ctx context.Context, tx pgx.Tx, streamID string, expectedVersion int, evts []Event) error {
	if len(evts) == 0 {
		return nil
	}
	types := make([]string, len(evts))
	data := make([]string, len(evts))
	meta := make([]string, len(evts))
	for i, e := range evts {
		if e.Type == "" {
			return fmt.Errorf("eventstore: event %d has no type", i)
		}
		if len(e.Data) == 0 {
			return fmt.Errorf("eventstore: event %d (%s) has no data", i, e.Type)
		}
		m, err := json.Marshal(e.Meta)
		if err != nil {
			return fmt.Errorf("eventstore: marshal meta for %s: %w", e.Type, err)
		}
		types[i] = e.Type
		data[i] = string(e.Data)
		meta[i] = string(m)
	}

	_, err := tx.Exec(ctx, appendSQL, streamID, expectedVersion, types, data, meta)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrVersionConflict
		}
		return fmt.Errorf("eventstore: append to %s at %d: %w", streamID, expectedVersion, err)
	}
	return nil
}
