package inventory

import (
	"context"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/AymanKastali/sagaflow/internal/platform/outbox"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5"
)

// Expirer frees seat holds whose deadlines have passed. It is inventory's
// timers.Firer, and it is what makes a hold survivable when the booking that
// took it is not.
//
// It holds no pool. The scheduler hands it the transaction that claimed the
// timer row, and everything it does has to land in that same commit.
type Expirer struct {
	enc Encoder
}

func NewExpirer(enc Encoder) *Expirer { return &Expirer{enc: enc} }

// Fire expires the hold its timer was set for, if that hold is still the live
// one.
//
// There is no inbox row here, which is the difference worth noticing. Every other
// handler in this package deduplicates a delivery, because a broker can hand it
// the same message twice. Nothing was delivered here: the timer row's own claim,
// already taken by the caller in this very transaction, is the whole once-only
// guarantee.
//
// A version conflict needs no retry loop either. Returning the error rolls the
// claim back with everything else, so the row is pending again and the next pass
// re-reads a seat that has moved on.
func (e *Expirer) Fire(ctx context.Context, tx pgx.Tx, t timers.Timer) error {
	state, origin, err := LoadSeat(ctx, tx, t.Subject)
	if err != nil {
		return err
	}
	decision := state.Expire(t.Subject, t.Token)
	if len(decision.Events) == 0 {
		return nil // the hold ended already; claiming the row is all this pass does
	}
	// No message arrived to copy a correlation id from, so it comes off the
	// stream: the saga that took the hold is the flow this expiry belongs to,
	// and it is the saga that has to hear about it.
	meta := eventstore.Meta{CorrelationID: origin.CorrelationID}
	if err := AppendSeat(ctx, tx, t.Subject, state.Version, decision.Events, meta); err != nil {
		return err
	}
	msgs, err := outgoing(e.enc, decision.Messages(), envelope.Envelope{
		Source:        Source,
		Subject:       t.Subject,
		CorrelationID: origin.CorrelationID,
		// No causation id: nothing caused this but a deadline passing. A reader
		// walking the chain backwards should find that it ends here rather than
		// find a plausible-looking lie.
	})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, msgs)
}
