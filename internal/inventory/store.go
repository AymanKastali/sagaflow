package inventory

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/kptac/sagaflow/internal/platform/codec"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"google.golang.org/protobuf/proto"
)

// LoadSeat folds a seat stream into its state, inside the caller's transaction.
//
// Reading in the same transaction as the later append is what makes the returned
// Version a safe expected version: nothing can interleave between the two.
func LoadSeat(ctx context.Context, tx pgx.Tx, seatID string) (SeatState, error) {
	recorded, err := eventstore.Load(ctx, tx, seatID)
	if err != nil {
		return SeatState{}, err
	}
	msgs := make([]proto.Message, len(recorded))
	for i, r := range recorded {
		m, err := codec.Decode(r.Event)
		if err != nil {
			return SeatState{}, fmt.Errorf("inventory: decode %s v%d: %w", seatID, r.Version, err)
		}
		msgs[i] = m
	}
	return Fold(msgs)
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
