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

// ErrMissingAttribute means a required CloudEvents attribute was absent. A
// message missing a required attribute can never become valid no matter how
// many times it is redelivered, so this is a permanent failure: it dead-letters
// immediately instead of being retried.
var ErrMissingAttribute = errors.New("envelope: missing required attribute")

// Envelope is the identity of one message.
//
// ID and Source together are specified by CloudEvents to be unique, which is
// exactly the property idempotent consumption needs — so that pair becomes the
// inbox deduplication key rather than something this system has to define and
// defend on its own.
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

// Headers renders the envelope as Kafka headers.
func (e Envelope) Headers() map[string]string {
	h := map[string]string{
		"ce_specversion": SpecVersion,
		"ce_id":          e.ID,
		"ce_source":      e.Source,
		"ce_type":        e.Type,
		"content-type":   ContentType,
	}
	// Optional attributes are omitted entirely rather than written empty: an empty
	// ce_subject is a different statement from an absent one.
	for key, value := range map[string]string{
		"ce_subject":       e.Subject,
		"ce_correlationid": e.CorrelationID,
		"ce_causationid":   e.CausationID,
		"traceparent":      e.TraceParent,
	} {
		if value != "" {
			h[key] = value
		}
	}
	return h
}

// Parse reads an envelope out of Kafka headers, rejecting anything that is not a
// well-formed CloudEvent this system can handle.
func Parse(h map[string]string) (Envelope, error) {
	switch sv := h["ce_specversion"]; sv {
	case SpecVersion:
	case "":
		return Envelope{}, fmt.Errorf("%w: ce_specversion", ErrMissingAttribute)
	default:
		return Envelope{}, fmt.Errorf("envelope: unsupported ce_specversion %q, want %q", sv, SpecVersion)
	}

	e := Envelope{
		ID:            h["ce_id"],
		Source:        h["ce_source"],
		Type:          h["ce_type"],
		Subject:       h["ce_subject"],
		CorrelationID: h["ce_correlationid"],
		CausationID:   h["ce_causationid"],
		TraceParent:   h["traceparent"],
	}
	// An attribute that is present but empty is as unusable as an absent one.
	for _, required := range []struct{ key, value string }{
		{"ce_id", e.ID},
		{"ce_source", e.Source},
		{"ce_type", e.Type},
	} {
		if required.value == "" {
			return Envelope{}, fmt.Errorf("%w: %s", ErrMissingAttribute, required.key)
		}
	}
	return e, nil
}

// Message is one message to publish: a body, the headers an Envelope renders,
// and the routing key.
//
// It lives here rather than in outbox because it is the vocabulary two packages
// share — outbox rows and dead letters are both Messages, and a dead letter was
// never in the outbox.
type Message struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// Validate rejects a message that could not be published correctly. It is
// exported because outbox.Enqueue is now an outside caller.
//
// The strings are unprefixed because every caller wraps them with its own
// context.
func (m Message) Validate() error {
	switch {
	case m.Topic == "":
		return errors.New("no topic")
	case m.Key == "":
		// Without a key Kafka round-robins the record, which destroys the
		// per-stream ordering every consumer downstream relies on.
		return errors.New("no key")
	case len(m.Payload) == 0:
		return errors.New("no payload")
	}
	return nil
}
