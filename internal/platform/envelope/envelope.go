// Package envelope maps CloudEvents v1.0.2 attributes to and from Kafka
// headers in binary content mode, per the CNCF Kafka protocol binding.
//
// The binding puts attributes in headers prefixed "ce_" and leaves the payload
// as the message body. traceparent is a W3C header, not a CloudEvents
// attribute, so it is deliberately unprefixed.
package envelope

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const (
	// SpecVersion is the only CloudEvents version this system speaks.
	SpecVersion = "1.0"
	// ContentType is the CloudEvents datacontenttype, carried in the
	// content-type header as the binding requires.
	ContentType = "application/protobuf"
)

// ErrMissingAttribute means a required CloudEvents attribute was absent. It is a
// permanent technical failure: the message can never become valid, so it
// dead-letters without retrying (spec §10.2).
var ErrMissingAttribute = errors.New("envelope: missing required attribute")

// Envelope is the identity of one message.
//
// ID and Source together are specified by CloudEvents to be unique, which is
// exactly the property idempotent consumption needs — so that pair becomes the
// inbox deduplication key rather than something we define (spec §8.1).
type Envelope struct {
	ID            string // ce_id — UUIDv7, generated at outbox enqueue time
	Source        string // ce_source — e.g. /sagaflow/inventory
	Type          string // ce_type — the fully qualified protobuf message name
	Subject       string // ce_subject — the stream id
	CorrelationID string // ce_correlationid — the saga id (extension)
	CausationID   string // ce_causationid — the ce_id that caused this (extension)
	TraceParent   string // traceparent — W3C trace context, no ce_ prefix
}

// NewID returns a UUIDv7 for use as ce_id.
func NewID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// Headers renders the envelope as Kafka headers. Optional attributes are omitted
// entirely rather than written empty, because an empty ce_subject is a different
// statement from an absent one.
func (e Envelope) Headers() map[string]string {
	h := map[string]string{
		"ce_specversion": SpecVersion,
		"ce_id":          e.ID,
		"ce_source":      e.Source,
		"ce_type":        e.Type,
		"content-type":   ContentType,
	}
	set := func(k, v string) {
		if v != "" {
			h[k] = v
		}
	}
	set("ce_subject", e.Subject)
	set("ce_correlationid", e.CorrelationID)
	set("ce_causationid", e.CausationID)
	set("traceparent", e.TraceParent)
	return h
}

// Parse reads an envelope out of Kafka headers, rejecting anything that is not a
// well-formed CloudEvent this system can handle.
func Parse(h map[string]string) (Envelope, error) {
	need := func(k string) (string, error) {
		v, ok := h[k]
		if !ok || v == "" {
			return "", fmt.Errorf("%w: %s", ErrMissingAttribute, k)
		}
		return v, nil
	}

	sv, err := need("ce_specversion")
	if err != nil {
		return Envelope{}, err
	}
	if sv != SpecVersion {
		return Envelope{}, fmt.Errorf("envelope: unsupported ce_specversion %q, want %q", sv, SpecVersion)
	}

	var e Envelope
	for _, f := range []struct {
		key string
		dst *string
	}{
		{"ce_id", &e.ID},
		{"ce_source", &e.Source},
		{"ce_type", &e.Type},
	} {
		v, err := need(f.key)
		if err != nil {
			return Envelope{}, err
		}
		*f.dst = v
	}

	e.Subject = h["ce_subject"]
	e.CorrelationID = h["ce_correlationid"]
	e.CausationID = h["ce_causationid"]
	e.TraceParent = h["traceparent"]
	return e, nil
}
