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
// request that caused it: the trace it was part of, the command it was
// correlated with, and the specific event that caused it.
type Meta struct {
	TraceID       string `json:"trace_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
}

// Event is an event about to be appended. Data is protojson bytes: readable
// directly in psql and decodable without consulting the schema registry.
type Event struct {
	Type string
	Data []byte
	Meta Meta
}

// validate rejects an event that could never be folded back into state.
func (e Event) validate() error {
	switch {
	case e.Type == "":
		return errors.New("no type")
	case len(e.Data) == 0:
		return errors.New("no data")
	}
	return nil
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
		if err := e.validate(); err != nil {
			return fmt.Errorf("eventstore: event %d (%q): %w", i, e.Type, err)
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
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return ErrVersionConflict
		}
		return fmt.Errorf("eventstore: append to %s at %d: %w", streamID, expectedVersion, err)
	}
	return nil
}

const loadSQL = `
SELECT global_seq, version, type, data, meta, recorded_at
FROM events
WHERE stream_id = $1
ORDER BY version`

// Load reads a stream in version order.
//
// It reads inside the caller's transaction, so the version of the last event
// returned is a safe expectedVersion for a subsequent Append in that same
// transaction — nothing can interleave.
func Load(ctx context.Context, tx pgx.Tx, streamID string) ([]Recorded, error) {
	rows, err := tx.Query(ctx, loadSQL, streamID)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load %s: %w", streamID, err)
	}
	defer rows.Close()

	var out []Recorded
	for rows.Next() {
		var (
			r        Recorded
			metaJSON []byte
		)
		if err := rows.Scan(&r.GlobalSeq, &r.Version, &r.Type, &r.Data, &metaJSON, &r.RecordedAt); err != nil {
			return nil, fmt.Errorf("eventstore: scan %s: %w", streamID, err)
		}
		if err := json.Unmarshal(metaJSON, &r.Meta); err != nil {
			return nil, fmt.Errorf("eventstore: unmarshal meta for %s v%d: %w", streamID, r.Version, err)
		}
		r.StreamID = streamID
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterate %s: %w", streamID, err)
	}
	return out, nil
}

const streamsSQL = `SELECT DISTINCT stream_id FROM events ORDER BY stream_id`

// Streams names every stream in the database, so a projection can be rebuilt by
// folding each one from scratch.
//
// Enumerating streams is what makes a rebuild safe, and the alternative is the
// trap this table is shaped to avoid. Row ids are handed out at insert but become
// visible at commit, so a rebuild that remembered "I have read up to id 42" would
// step straight over a row that took id 41 and committed later — losing an event
// silently, only under load. Folding a whole stream reads every version of it or
// fails; there is no cursor for anything to slip behind.
//
// It reads inside the caller's transaction, like Load, so a rebuild sees one
// consistent snapshot: the streams it enumerates and the events it then folds are
// the same log, not two reads of a moving one.
func Streams(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, streamsSQL)
	if err != nil {
		return nil, fmt.Errorf("eventstore: list streams: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("eventstore: scan stream id: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterate stream ids: %w", err)
	}
	return out, nil
}
