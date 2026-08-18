package envelope_test

import (
	"errors"
	"testing"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/google/uuid"
)

func full() envelope.Envelope {
	return envelope.Envelope{
		ID:            "01920000-0000-7000-8000-000000000001",
		Source:        "/sagaflow/inventory",
		Type:          "sagaflow.inventory.v1.SeatHeld",
		Subject:       "seat-BA117-2026-09-01-14A",
		CorrelationID: "saga-1",
		CausationID:   "01920000-0000-7000-8000-000000000000",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

func TestHeadersUseCloudEventsBinaryModeNames(t *testing.T) {
	h := full().Headers()

	want := map[string]string{
		"ce_specversion":   "1.0",
		"ce_id":            "01920000-0000-7000-8000-000000000001",
		"ce_source":        "/sagaflow/inventory",
		"ce_type":          "sagaflow.inventory.v1.SeatHeld",
		"ce_subject":       "seat-BA117-2026-09-01-14A",
		"ce_correlationid": "saga-1",
		"ce_causationid":   "01920000-0000-7000-8000-000000000000",
		"traceparent":      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"content-type":     "application/protobuf",
	}
	if len(h) != len(want) {
		t.Fatalf("want %d headers, got %d: %v", len(want), len(h), h)
	}
	for k, v := range want {
		if h[k] != v {
			t.Errorf("header %s: want %q, got %q", k, v, h[k])
		}
	}
	// traceparent is a W3C header and must NOT be ce_-prefixed.
	if _, bad := h["ce_traceparent"]; bad {
		t.Error("traceparent must not carry the ce_ prefix")
	}
}

func TestRoundTrip(t *testing.T) {
	want := full()
	got, err := envelope.Parse(want.Headers())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != want {
		t.Fatalf("round trip changed the envelope:\n want %+v\n got  %+v", want, got)
	}
}

func TestOptionalAttributesAreOmittedNotEmpty(t *testing.T) {
	e := envelope.Envelope{
		ID:     "id-1",
		Source: "/sagaflow/inventory",
		Type:   "sagaflow.inventory.v1.SeatHeld",
	}
	h := e.Headers()
	for _, k := range []string{"ce_subject", "ce_correlationid", "ce_causationid", "traceparent"} {
		if _, present := h[k]; present {
			t.Errorf("optional header %s should be absent when unset, got %q", k, h[k])
		}
	}
}

func TestParseRejectsMissingRequiredAttributes(t *testing.T) {
	for _, missing := range []string{"ce_id", "ce_source", "ce_type", "ce_specversion"} {
		h := full().Headers()
		delete(h, missing)
		_, err := envelope.Parse(h)
		if !errors.Is(err, envelope.ErrMissingAttribute) {
			t.Errorf("without %s: want ErrMissingAttribute, got %v", missing, err)
		}
	}
}

func TestParseRejectsUnknownSpecVersion(t *testing.T) {
	h := full().Headers()
	h["ce_specversion"] = "0.3"
	if _, err := envelope.Parse(h); err == nil {
		t.Fatal("want an error for specversion 0.3, got nil")
	}
}

func TestNewIDIsUUIDv7AndSortsByTime(t *testing.T) {
	a := envelope.NewID()
	b := envelope.NewID()

	ua, err := uuid.Parse(a)
	if err != nil {
		t.Fatalf("parse %q: %v", a, err)
	}
	if ua.Version() != 7 {
		t.Fatalf("want UUID version 7, got %d", ua.Version())
	}
	// v7 is time-ordered, which is why it is worth preferring over v4 for an id
	// that lands in a Postgres primary key and a Kafka header. google/uuid's
	// getV7Time holds a mutex and returns a strictly increasing milli<<12+seq,
	// so this is a guarantee rather than a timing accident.
	if !(a < b) {
		t.Fatalf("want time-ordered ids, got %q then %q", a, b)
	}
}
