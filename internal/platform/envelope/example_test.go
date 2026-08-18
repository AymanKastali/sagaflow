package envelope_test

import (
	"errors"
	"fmt"
	"sort"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
)

// ExampleEnvelope_Headers shows an envelope rendered as the Kafka headers a
// message actually travels with.
//
// The headers are printed in sorted order because Go randomises map iteration:
// an example that ranges over a map directly passes locally and then fails
// intermittently in CI. Anyone writing the next example here will meet that.
//
// Note what is absent. TraceParent was not set, so no traceparent header is
// emitted at all — an optional attribute is omitted rather than written empty,
// because an absent ce_subject is a different statement from an empty one.
func ExampleEnvelope_Headers() {
	e := envelope.Envelope{
		ID:            "0192f0c4-1b2a-7000-8000-000000000001",
		Source:        "/sagaflow/inventory",
		Type:          "sagaflow.inventory.v1.SeatHeld",
		Subject:       "seat-BA117-2026-09-01-14A",
		CorrelationID: "booking-1",
		CausationID:   "0192f0c4-1b2a-7000-8000-000000000000",
	}

	headers := e.Headers()
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%-18s %s\n", k, headers[k])
	}

	// Output:
	// ce_causationid     0192f0c4-1b2a-7000-8000-000000000000
	// ce_correlationid   booking-1
	// ce_id              0192f0c4-1b2a-7000-8000-000000000001
	// ce_source          /sagaflow/inventory
	// ce_specversion     1.0
	// ce_subject         seat-BA117-2026-09-01-14A
	// ce_type            sagaflow.inventory.v1.SeatHeld
	// content-type       application/protobuf
}

// ExampleParse shows the round trip: headers off the wire become an envelope
// again, with nothing lost.
func ExampleParse() {
	sent := envelope.Envelope{
		ID:            "0192f0c4-1b2a-7000-8000-000000000001",
		Source:        "/sagaflow/inventory",
		Type:          "sagaflow.inventory.v1.SeatHeld",
		Subject:       "seat-BA117-2026-09-01-14A",
		CorrelationID: "booking-1",
	}

	received, err := envelope.Parse(sent.Headers())
	if err != nil {
		panic(err)
	}

	fmt.Println("id:         ", received.ID)
	fmt.Println("type:       ", received.Type)
	fmt.Println("subject:    ", received.Subject)
	fmt.Println("correlation:", received.CorrelationID)
	fmt.Println("round trip: ", received == sent)

	// Output:
	// id:          0192f0c4-1b2a-7000-8000-000000000001
	// type:        sagaflow.inventory.v1.SeatHeld
	// subject:     seat-BA117-2026-09-01-14A
	// correlation: booking-1
	// round trip:  true
}

// ExampleParse_missingAttribute shows the failure that dead-letters immediately
// rather than retrying.
//
// A message with no ce_id can never become valid — no amount of waiting adds an
// attribute to bytes already on the wire — so retrying it would only delay the
// inevitable while blocking the messages behind it.
func ExampleParse_missingAttribute() {
	_, err := envelope.Parse(map[string]string{
		"ce_specversion": "1.0",
		"ce_source":      "/sagaflow/inventory",
		"ce_type":        "sagaflow.inventory.v1.SeatHeld",
	})

	// errors.Is against the sentinel is what a consumer actually branches on to
	// decide "dead-letter now" rather than "retry with backoff". Asserting only
	// that err != nil would pass even if the error stopped being classifiable,
	// which is the property that matters here.
	fmt.Println(err)
	fmt.Println("permanent:", errors.Is(err, envelope.ErrMissingAttribute))

	// Output:
	// envelope: missing required attribute: ce_id
	// permanent: true
}
