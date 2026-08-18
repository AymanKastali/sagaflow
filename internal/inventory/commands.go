package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/AymanKastali/sagaflow/internal/platform/codec"
	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/AymanKastali/sagaflow/internal/platform/inbox"
	"github.com/AymanKastali/sagaflow/internal/platform/outbox"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

const (
	// CommandsTopic and EventsTopic are separate topics, one pair per service,
	// so a consumer that only wants to react to inventory's own events never
	// has to filter out the commands other services send it.
	CommandsTopic = "inventory.commands"
	EventsTopic   = "inventory.events"

	// Source is this service's ce_source. With ce_id it forms the inbox key that
	// makes redelivery a no-op.
	Source = "/sagaflow/inventory"

	// Consumer is the inbox consumer name. A consumer group is scoped to one
	// purpose rather than one service — a service can run more than one — which
	// is why this name, not the service name, is part of the inbox primary key.
	Consumer = "inventory.commands"
)

// ConflictRetries is how many times Handle reloads and re-decides after a
// version conflict before giving up. Immediate rather than backed off, because
// a version conflict means the winning write is already committed — there is
// nothing to wait for.
const ConflictRetries = 3

// Encoder frames an event for the wire.
//
// An interface, not *schema.Serde, so the handler's tests need no registry
// container: framing is platform/schema's property and is proven in its tests.
type Encoder interface {
	Encode(m proto.Message) ([]byte, error)
}

// Handler applies inventory commands. One command, one seat, one transaction.
type Handler struct {
	pool *pgxpool.Pool
	enc  Encoder
}

func NewHandler(pool *pgxpool.Pool, enc Encoder) *Handler {
	return &Handler{pool: pool, enc: enc}
}

// Handle applies one command, retrying a version conflict by reloading and
// re-deciding.
//
// Re-deciding is the point: the loser of a race folded from state that no longer
// exists, so replaying its old decision would append a second hold. Reloading
// turns it into a refusal instead.
func (h *Handler) Handle(ctx context.Context, incoming envelope.Envelope, cmd proto.Message) error {
	var err error
	for range ConflictRetries + 1 {
		if err = h.applyInOneTransaction(ctx, incoming, cmd); !errors.Is(err, eventstore.ErrVersionConflict) {
			return err
		}
	}
	return fmt.Errorf("inventory: %s on %s after %d retries: %w",
		incoming.Type, incoming.Subject, ConflictRetries, err)
}

// applyInOneTransaction handles a single command in a single database
// transaction, which either commits all of its effects or none of them.
//
// Four things are written together and must not come apart: the events the
// command produced, the outgoing messages announcing them, any deadline those
// events need, and the record that this command was consumed. If the events
// committed but the messages did not, the world would never hear about a hold
// that exists. If the consumed-record committed but the events did not,
// redelivery would be ignored and the command would be lost. If the events
// committed but the timer did not, the hold would exist with no deadline
// attached — held forever, with nothing anywhere recording that anything went
// wrong.
//
// It writes exactly one stream, never two. A stream's invariant is only
// checkable within one transaction, so writing a second stream here would be
// betting on a guarantee this package does not have.
//
// The consumed-record is marked inside the transaction rather than before it for
// the same reason the rest is. On a version conflict the whole transaction rolls
// back, the mark with it, so the retry sees an unconsumed command. Marking
// outside would leave the retry looking at its own mark, concluding the command
// was already handled, and silently dropping it.
func (h *Handler) applyInOneTransaction(ctx context.Context, incoming envelope.Envelope, cmd proto.Message) error {
	seatID, err := SeatID(cmd)
	if err != nil {
		return err
	}
	return pg.WithTx(ctx, h.pool, func(tx pgx.Tx) error {
		firstDelivery, err := inbox.MarkConsumed(ctx, tx, Consumer, incoming.Source, incoming.ID)
		if err != nil || !firstDelivery {
			return err // not firstDelivery: already applied, commit nothing, ack
		}
		state, _, err := LoadSeat(ctx, tx, seatID)
		if err != nil {
			return err
		}
		decision, err := Decide(state, cmd)
		if err != nil {
			return err
		}
		// No TraceID: it is the trace id, not the whole W3C traceparent header,
		// and nothing extracts one until phase 9 wires OTel.
		meta := eventstore.Meta{
			CorrelationID: incoming.CorrelationID,
			CausationID:   incoming.ID,
		}
		if err := AppendSeat(ctx, tx, seatID, state.Version, decision.Events, meta); err != nil {
			return err
		}
		if err := scheduleTimers(ctx, tx, seatID, decision.Timers); err != nil {
			return err
		}
		// A reply keeps the incoming correlation id so the saga can route it, and
		// takes the incoming ce_id as its causation, so the chain can be walked
		// back message by message.
		msgs, err := outgoing(h.enc, decision.Messages(), envelope.Envelope{
			Source:        Source,
			Subject:       seatID,
			CorrelationID: incoming.CorrelationID,
			CausationID:   incoming.ID,
			TraceParent:   incoming.TraceParent,
		})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, msgs)
	})
}

// scheduleTimers records the decision's deadlines in the same transaction as the
// events that need them.
//
// The deadline is passed through untouched rather than computed here. It came
// from the command, which is what keeps the decision that produced it free of a
// clock and testable without one.
func scheduleTimers(ctx context.Context, tx pgx.Tx, seatID string, ts []Timer) error {
	for _, t := range ts {
		if err := timers.Schedule(ctx, tx, t.FireAt, seatID, t.Token); err != nil {
			return err
		}
	}
	return nil
}

// outgoing frames each message and wraps it in its own envelope, taking
// everything but the id and the type from tmpl.
//
// Each message gets a new ce_id because each is a distinct message. The rest —
// which flow it belongs to, what caused it, which seat it is about — is the
// caller's to fill in, because a reply to a command and a hold that ran out of
// time answer those questions differently.
func outgoing(enc Encoder, msgs []proto.Message, tmpl envelope.Envelope) ([]envelope.Message, error) {
	out := make([]envelope.Message, 0, len(msgs))
	for _, m := range msgs {
		payload, err := enc.Encode(m)
		if err != nil {
			return nil, fmt.Errorf("inventory: frame %s: %w", codec.TypeName(m), err)
		}
		e := tmpl
		e.ID = envelope.NewID()
		e.Type = codec.TypeName(m)
		out = append(out, envelope.Message{
			Topic:   EventsTopic,
			Key:     e.Subject, // the stream id, which is what preserves per-seat ordering
			Payload: payload,
			Headers: e.Headers(),
		})
	}
	return out, nil
}
