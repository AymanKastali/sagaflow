package inventory_test

import (
	"errors"
	"testing"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// jsonDecoder is jsonEncoder's mirror: it reads back what that wrote. The type
// name travels in ce_type rather than in the bytes, standing in for the schema
// id a registry-framed payload carries in its header.
type jsonDecoder struct{ headers map[string]string }

func (d jsonDecoder) Decode(b []byte) (proto.Message, error) {
	var m proto.Message
	switch d.headers["ce_type"] {
	case "sagaflow.inventory.v1.HoldSeat":
		m = &inventoryv1.HoldSeat{}
	case "sagaflow.inventory.v1.ReleaseSeatHold":
		m = &inventoryv1.ReleaseSeatHold{}
	case "sagaflow.inventory.v1.SeatHeld":
		m = &inventoryv1.SeatHeld{}
	default:
		return nil, errors.New("no schema for " + d.headers["ce_type"])
	}
	return m, protojson.Unmarshal(b, m)
}

// record frames one message the way the wire would: protojson bytes plus the
// CloudEvents headers a producer sets.
func record(t *testing.T, topic string, incoming envelope.Envelope, m proto.Message) kafka.Record {
	t.Helper()
	payload, err := protojson.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	incoming.Type = string(m.ProtoReflect().Descriptor().FullName())
	return kafka.Record{Topic: topic, Key: incoming.Subject, Value: payload, Headers: incoming.Headers()}
}

// commandEnvelope is what booking puts on inventory.commands.
func commandEnvelope() envelope.Envelope {
	return envelope.Envelope{
		ID: envelope.NewID(), Source: "/sagaflow/booking",
		Subject: seat, CorrelationID: "saga-1",
	}
}

func commandsHandler(pool *pgxpool.Pool, headers map[string]string) kafka.Handler {
	return inventory.Commands(inventory.NewHandler(pool, jsonEncoder{}), jsonDecoder{headers: headers})
}

func TestAHoldSeatOffTheWireHoldsTheSeat(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_consume_hold")

	r := record(t, inventory.CommandsTopic, commandEnvelope(), holdSeat(hold))

	if err := commandsHandler(pool, r.Headers)(ctx, r); err != nil {
		t.Fatalf("handle: %v", err)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 1 || rows[0].ceTyp != "sagaflow.inventory.v1.SeatHeld" {
		t.Fatalf("want one SeatHeld queued for publication, got %+v", rows)
	}
}

func TestTheSameCommandDeliveredTwiceHoldsTheSeatOnce(t *testing.T) {
	// The inbox is what makes this true, and it is reached through the handler
	// rather than around it: a record arriving twice is Kafka's ordinary
	// behaviour, not an exceptional case.
	ctx := t.Context()
	pool := db(t, "inventory_consume_twice")

	r := record(t, inventory.CommandsTopic, commandEnvelope(), holdSeat(hold))
	handle := commandsHandler(pool, r.Headers)

	for range 2 {
		if err := handle(ctx, r); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	if rows := outboxRows(t, ctx, pool); len(rows) != 1 {
		t.Fatalf("a redelivered command must produce nothing the second time, got %+v", rows)
	}
}

func TestARecordWithNoCloudEventHeadersIsDeadLetteredImmediately(t *testing.T) {
	// Permanent, not transient: redelivery cannot add headers, so retrying five
	// times would spend the whole budget on a record that was never going to
	// parse — and everything behind it in that partition waits for all of it.
	ctx := t.Context()
	pool := db(t, "inventory_consume_unparseable")

	r := kafka.Record{Topic: inventory.CommandsTopic, Value: []byte(`{}`)}

	if err := commandsHandler(pool, nil)(ctx, r); !errors.Is(err, kafka.ErrPermanent) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
}

func TestAMessageThatIsNotACommandIsDeadLetteredImmediately(t *testing.T) {
	// A SeatHeld on inventory.commands is somebody's mistake. It decodes, so the
	// refusal comes from the decision layer — and it must still be settled rather
	// than retried.
	ctx := t.Context()
	pool := db(t, "inventory_consume_not_a_command")

	r := record(t, inventory.CommandsTopic, commandEnvelope(), &inventoryv1.SeatHeld{
		HoldId: hold, BookingId: booking, SeatId: seat, ExpiresAt: expires,
	})

	err := commandsHandler(pool, r.Headers)(ctx, r)
	if !errors.Is(err, kafka.ErrPermanent) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
	if !errors.Is(err, inventory.ErrUnknownCommand) {
		t.Fatalf("the cause must survive to the dead-letter header, got %v", err)
	}
}

func TestTheProjectionHandlerNeverLooksAtThePayload(t *testing.T) {
	// The claim the whole read side rests on: the view is re-derived from the
	// seat's stream, so the message body is not an input. Bytes that decode to
	// nothing at all still bring the view up to date.
	ctx := t.Context()
	pool := db(t, "inventory_project_from_wire")

	held := record(t, inventory.CommandsTopic, commandEnvelope(), holdSeat(hold))
	if err := commandsHandler(pool, held.Headers)(ctx, held); err != nil {
		t.Fatalf("hold: %v", err)
	}

	notification := kafka.Record{
		Topic: inventory.EventsTopic, Key: seat, Value: []byte("not a message at all"),
		Headers: envelope.Envelope{
			ID: envelope.NewID(), Source: inventory.Source,
			Type: "sagaflow.inventory.v1.SeatHeld", Subject: seat,
		}.Headers(),
	}
	if err := inventory.Projections(inventory.NewProjector(pool))(ctx, notification); err != nil {
		t.Fatalf("project: %v", err)
	}

	got, found, err := inventory.LoadAvailability(ctx, pool, seat)
	if err != nil || !found {
		t.Fatalf("no row for %s: found=%v err=%v", seat, found, err)
	}
	if got.Status != inventory.StatusHeld {
		t.Fatalf("the view did not follow the stream: %+v", got)
	}
}

func TestAnEventWithNoSubjectIsDeadLettered(t *testing.T) {
	// ce_subject is the only field this handler reads. Without it there is no seat
	// to re-derive, and no redelivery will supply one.
	ctx := t.Context()
	pool := db(t, "inventory_project_no_subject")

	r := kafka.Record{Topic: inventory.EventsTopic, Headers: envelope.Envelope{
		ID: envelope.NewID(), Source: inventory.Source, Type: "sagaflow.inventory.v1.SeatHeld",
	}.Headers()}

	err := inventory.Projections(inventory.NewProjector(pool))(ctx, r)
	if !errors.Is(err, kafka.ErrPermanent) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
}
