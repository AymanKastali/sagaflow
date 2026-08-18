package inventory

import (
	"context"
	"fmt"

	"github.com/AymanKastali/sagaflow/internal/platform/codec"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

// LoadSeat folds a seat stream into its state, inside the caller's transaction,
// and returns the metadata of the last event alongside it.
//
// Reading in the same transaction as the later append is what makes the returned
// Version a safe expected version: nothing can interleave between the two.
//
// The metadata is the flow that put the seat in the state returned with it — for
// a held seat, the saga that took the hold. A handler with no incoming envelope
// to copy from has nowhere else to learn that, and reading it back off the stream
// is better than storing a copy elsewhere, because the stream cannot go stale
// against itself. It is deliberately not part of SeatState: the fold sees only
// messages, and giving it envelope metadata would make replay depend on how the
// events happened to be delivered.
func LoadSeat(ctx context.Context, tx pgx.Tx, seatID string) (SeatState, eventstore.Meta, error) {
	recorded, err := eventstore.Load(ctx, tx, seatID)
	if err != nil {
		return SeatState{}, eventstore.Meta{}, err
	}
	msgs := make([]proto.Message, len(recorded))
	for i, r := range recorded {
		m, err := codec.Decode(r.Event)
		if err != nil {
			return SeatState{}, eventstore.Meta{}, fmt.Errorf(
				"inventory: decode %s v%d: %w", seatID, r.Version, err)
		}
		msgs[i] = m
	}
	var last eventstore.Meta
	if len(recorded) > 0 {
		last = recorded[len(recorded)-1].Meta
	}
	state, err := Fold(msgs)
	return state, last, err
}

// AppendSeat encodes evts and appends them at expectedVersion+1 …
//
// It returns eventstore.ErrVersionConflict untouched rather than wrapping it,
// because the handler's retry branches on it and a wrap here would be one more
// place for that to silently stop matching.
func AppendSeat(ctx context.Context, tx pgx.Tx, seatID string, expectedVersion int, evts []proto.Message, meta eventstore.Meta) error {
	if len(evts) == 0 {
		return nil
	}
	stored := make([]eventstore.Event, len(evts))
	for i, m := range evts {
		e, err := codec.Encode(m, meta)
		if err != nil {
			return fmt.Errorf("inventory: encode for %s: %w", seatID, err)
		}
		stored[i] = e
	}
	return eventstore.Append(ctx, tx, seatID, expectedVersion, stored)
}
