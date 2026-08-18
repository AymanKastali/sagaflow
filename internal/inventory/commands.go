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
func (h *Handler) Handle(ctx context.Context, env envelope.Envelope, cmd proto.Message) error {
	var err error
	for range ConflictRetries + 1 {
		if err = h.handleOnce(ctx, env, cmd); !errors.Is(err, eventstore.ErrVersionConflict) {
			return err
		}
	}
	return fmt.Errorf("inventory: %s on %s after %d retries: %w",
		env.Type, env.Subject, ConflictRetries, err)
}

// handleOnce enforces this package's one invariant: a single transaction
// writes exactly one stream, plus its outbox rows, plus its inbox row, and
// never two streams — a stream's own invariant is only checkable within one
// transaction, so writing a second stream in the same transaction would be
// betting on a guarantee this package does not actually have.
//
// The inbox mark is inside the transaction so a conflict rolls it back too —
// otherwise the retry would find its own mark and treat the command as already
// handled.
func (h *Handler) handleOnce(ctx context.Context, env envelope.Envelope, cmd proto.Message) error {
	seatID, err := SeatID(cmd)
	if err != nil {
		return err
	}
	return pg.WithTx(ctx, h.pool, func(tx pgx.Tx) error {
		fresh, err := inbox.MarkConsumed(ctx, tx, Consumer, env.Source, env.ID)
		if err != nil || !fresh {
			return err // not fresh: already applied, commit nothing, ack
		}
		state, err := LoadSeat(ctx, tx, seatID)
		if err != nil {
			return err
		}
		out, err := Decide(state, cmd)
		if err != nil {
			return err
		}
		// No TraceID: it is the trace id, not the whole W3C traceparent header,
		// and nothing extracts one until phase 9 wires OTel.
		meta := eventstore.Meta{
			CorrelationID: env.CorrelationID,
			CausationID:   env.ID,
		}
		if err := AppendSeat(ctx, tx, seatID, state.Version, out.Events, meta); err != nil {
			return err
		}
		msgs, err := h.messages(out.Messages(), env, seatID)
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, msgs)
	})
}

// messages frames each outgoing message and wraps it in its own envelope.
//
// Each gets a fresh ce_id because each is a distinct message, keeps the
// incoming correlation id so the saga can route the reply, and takes the
// incoming ce_id as its causation id, so each outgoing message still names
// the one that caused it and the chain can be walked back message by message.
func (h *Handler) messages(msgs []proto.Message, in envelope.Envelope, seatID string) ([]envelope.Message, error) {
	out := make([]envelope.Message, 0, len(msgs))
	for _, m := range msgs {
		payload, err := h.enc.Encode(m)
		if err != nil {
			return nil, fmt.Errorf("inventory: frame %s: %w", codec.TypeName(m), err)
		}
		env := envelope.Envelope{
			ID:            envelope.NewID(),
			Source:        Source,
			Type:          codec.TypeName(m),
			Subject:       seatID,
			CorrelationID: in.CorrelationID,
			CausationID:   in.ID,
			TraceParent:   in.TraceParent,
		}
		out = append(out, envelope.Message{
			Topic:   EventsTopic,
			Key:     seatID, // the stream id, which is what preserves per-seat ordering
			Payload: payload,
			Headers: env.Headers(),
		})
	}
	return out, nil
}
