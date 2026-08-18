package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"google.golang.org/protobuf/proto"
)

// ProjectionConsumer is the consumer group that keeps seat_availability in step
// with the seat streams.
//
// A second group, on this service's own events, deliberately separate from the
// one applying commands. The two failures are not comparable: a view that falls
// behind shows a customer a seat that is already taken, and a command that falls
// behind leaves a saga waiting forever. Separate groups mean separate offsets, so
// one can stall without stopping the other.
const ProjectionConsumer = "inventory.projection"

// Decoder reads a framed message off the wire.
//
// Encoder's mirror, and an interface for the same reason: framing is
// platform/schema's property, proven in its own tests, so a test of this file
// needs no registry.
type Decoder interface {
	Decode(b []byte) (proto.Message, error)
}

// Commands is the handler for inventory.commands: parse the envelope, decode the
// command, apply it in one transaction.
//
// The three failures it can produce itself are all permanent, and saying so is
// the point. Headers that are not a CloudEvent, bytes that are not a message this
// binary knows, and a message that is not a command each fail identically on
// every redelivery — so each is dead-lettered on the first attempt instead of
// spending a five-attempt budget with the rest of its partition waiting behind
// it. Everything the handler itself returns stays transient, because a database
// that is down does come back.
func Commands(h *Handler, dec Decoder) kafka.Handler {
	return func(ctx context.Context, r kafka.Record) error {
		incoming, err := envelope.Parse(r.Headers)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		cmd, err := dec.Decode(r.Value)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		if err := h.Handle(ctx, incoming, cmd); err != nil {
			if errors.Is(err, ErrUnknownCommand) {
				return fmt.Errorf("%w: %w", kafka.ErrPermanent, err)
			}
			return err
		}
		return nil
	}
}

// Projections is the handler for inventory.events: bring the changed seat's row
// up to date.
//
// It never decodes the payload. All it takes from the record is ce_subject — the
// seat — because everything else it needs is in that seat's stream, in the same
// database. So it cannot fail on an event type this binary does not recognise, it
// needs no schema at all, and a notification about something that changed nothing
// costs one wasted read rather than a wrong row.
//
// It takes no inbox row either. An inbox stops a second delivery from applying a
// change twice; re-deriving a seat applies nothing, so there is no second
// application for one to prevent.
func Projections(p *Projector) kafka.Handler {
	return func(ctx context.Context, r kafka.Record) error {
		incoming, err := envelope.Parse(r.Headers)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		if incoming.Subject == "" {
			return fmt.Errorf("%w: inventory: event %s names no seat in ce_subject",
				kafka.ErrPermanent, incoming.ID)
		}
		return p.Project(ctx, incoming.Subject)
	}
}
