package codec_test

import (
	"encoding/json"
	"fmt"
	"sort"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/platform/codec"
	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
)

// ExampleEncode shows what an event actually looks like in Postgres.
//
// This is the concrete half of "one schema, two encodings": the same .proto file
// produces compact binary protobuf on the wire and readable JSON in the
// database. The field names are hold_id and booking_id — the .proto's own names,
// not lowerCamelCase — so a query written against the schema file matches what
// is stored.
//
// The type string is doing three jobs at once: it is this row's type column, it
// is the ce_type header on the wire, and it is the registry key Decode looks up.
//
// Note how this example reads the JSON rather than printing it. protojson
// deliberately varies its whitespace between runs precisely to stop anyone
// depending on the exact bytes, so an example that printed event.Data directly
// would pass and fail at random. That is the same reason stored JSON must never
// be hashed, signed, or compared byte-wise — the hazard this package's chapter
// warns about, met here in miniature.
func ExampleEncode() {
	event, err := codec.Encode(
		&inventoryv1.SeatHeld{
			HoldId:    "hold-1",
			BookingId: "booking-1",
			SeatId:    "seat-BA117-2026-09-01-14A",
		},
		eventstore.Meta{CorrelationID: "booking-1"},
	)
	if err != nil {
		panic(err)
	}

	var stored map[string]any
	if err := json.Unmarshal(event.Data, &stored); err != nil {
		panic(err)
	}
	keys := make([]string, 0, len(stored))
	for k := range stored {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println("type:", event.Type)
	for _, k := range keys {
		fmt.Printf("  %-11s %v\n", k, stored[k])
	}

	// Output:
	// type: sagaflow.inventory.v1.SeatHeld
	//   booking_id  booking-1
	//   hold_id     hold-1
	//   seat_id     seat-BA117-2026-09-01-14A
}

// ExampleDecode shows a stored row becoming a message again.
//
// The type name resolves through protobuf's global registry, which the generated
// code populates at package initialisation — a local map lookup, no network. That
// is what keeps replaying years of history independent of whether the schema
// registry happens to be up.
func ExampleDecode() {
	stored := eventstore.Event{
		Type: "sagaflow.inventory.v1.SeatHeld",
		Data: []byte(`{"hold_id":"hold-1","booking_id":"booking-1","seat_id":"seat-BA117-2026-09-01-14A"}`),
	}

	msg, err := codec.Decode(stored)
	if err != nil {
		panic(err)
	}

	held, ok := msg.(*inventoryv1.SeatHeld)
	fmt.Println("right type:", ok)
	fmt.Println("hold:      ", held.GetHoldId())
	fmt.Println("seat:      ", held.GetSeatId())

	// Output:
	// right type: true
	// hold:       hold-1
	// seat:       seat-BA117-2026-09-01-14A
}

// ExampleDecode_unknownType shows the failure that must never be retried.
//
// The registry is populated from compiled-in generated code, so a type absent
// from it is absent forever in this binary. Waiting cannot help, which is why
// this is a permanent failure that dead-letters at once instead of retrying and
// blocking every message behind it.
func ExampleDecode_unknownType() {
	_, err := codec.Decode(eventstore.Event{
		Type: "sagaflow.inventory.v1.SeatTeleported",
		Data: []byte(`{}`),
	})

	fmt.Println(err)

	// Output:
	// codec: unknown event type: sagaflow.inventory.v1.SeatTeleported
}
