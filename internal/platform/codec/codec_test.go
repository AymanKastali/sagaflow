package codec_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	migrations "github.com/AymanKastali/sagaflow/internal/inventory/migrations"
	"github.com/AymanKastali/sagaflow/internal/platform/codec"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// One Postgres container for the whole package, shared by every test that
// needs one. The pure tests above do not, and pgtest.Start is a no-op under
// -short, so sharing one container here costs those tests nothing.
func TestMain(m *testing.M) {
	stop, err := pgtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

func seatHeld() *inventoryv1.SeatHeld {
	return &inventoryv1.SeatHeld{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    "seat-BA117-2026-09-01-14A",
		ExpiresAt: timestamppb.New(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)),
	}
}

func TestEncodeSetsTypeToFullProtoName(t *testing.T) {
	got, err := codec.Encode(seatHeld(), eventstore.Meta{TraceID: "t-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const want = "sagaflow.inventory.v1.SeatHeld"
	if got.Type != want {
		t.Fatalf("want type %q, got %q", want, got.Type)
	}
	if got.Meta.TraceID != "t-1" {
		t.Fatalf("meta not carried through: %+v", got.Meta)
	}
}

func TestEncodeProducesReadableJSONWithProtoFieldNames(t *testing.T) {
	got, err := codec.Encode(seatHeld(), eventstore.Meta{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Storage is protojson rather than protobuf bytes precisely so this is
	// readable in psql during an incident, so assert on the shape a human sees.
	s := string(got.Data)
	for _, want := range []string{`"hold_id"`, `"booking_id"`, `"seat_id"`, `"expires_at"`, "hold-1"} {
		if !strings.Contains(s, want) {
			t.Fatalf("stored JSON %s does not contain %s", s, want)
		}
	}
}

func TestRoundTripPreservesEveryField(t *testing.T) {
	want := seatHeld()

	e, err := codec.Encode(want, eventstore.Meta{CorrelationID: "saga-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := codec.Decode(e)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*inventoryv1.SeatHeld)
	if !ok {
		t.Fatalf("want *SeatHeld, got %T", msg)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("round trip lost data:\n want %v\n got  %v", want, got)
	}
}

func TestDecodeUnknownTypeIsErrUnknownType(t *testing.T) {
	_, err := codec.Decode(eventstore.Event{
		Type: "sagaflow.inventory.v1.NotAThing",
		Data: []byte(`{}`),
	})
	if !errors.Is(err, codec.ErrUnknownType) {
		t.Fatalf("want ErrUnknownType, got %v", err)
	}
}

func TestDecodeMalformedJSONIsAnError(t *testing.T) {
	_, err := codec.Decode(eventstore.Event{
		Type: "sagaflow.inventory.v1.SeatHeld",
		Data: []byte(`{"hold_id":`),
	})
	if err == nil {
		t.Fatal("want an error for truncated JSON, got nil")
	}
	if errors.Is(err, codec.ErrUnknownType) {
		t.Fatal("malformed data must not be reported as an unknown type")
	}
}

func TestRoundTripThroughPostgres(t *testing.T) {
	ctx := t.Context()
	pool := pgtest.Shared(t).Migrated(t, "codec_roundtrip", migrations.FS)

	want := seatHeld()
	e, err := codec.Encode(want, eventstore.Meta{TraceID: "t-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{e})
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	var loaded []eventstore.Recorded
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		loaded, err = eventstore.Load(ctx, tx, "seat-14A")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("want 1 event, got %d", len(loaded))
	}

	msg, err := codec.Decode(loaded[0].Event)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !proto.Equal(want, msg) {
		t.Fatalf("postgres round trip lost data:\n want %v\n got  %v", want, msg)
	}
	if loaded[0].Meta.TraceID != "t-1" {
		t.Fatalf("meta lost: %+v", loaded[0].Meta)
	}
}
