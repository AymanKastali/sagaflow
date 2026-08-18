package codec

import (
	"errors"
	"fmt"

	"github.com/AymanKastali/sagaflow/internal/platform/eventstore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ErrUnknownType means the event's type name is not in the generated
// registry. No amount of retrying adds a message type to a compiled binary's
// registry, so this is a permanent failure rather than a transient one: the
// caller dead-letters the message immediately instead of retrying it.
var ErrUnknownType = errors.New("codec: unknown event type")

// marshal uses proto field names so the stored JSON matches the .proto file
// rather than lowerCamelCase. Its output is not byte-stable across protojson
// library versions — a dependency bump can reorder fields or change
// whitespace without changing what the JSON means — so stored JSON must never
// be hashed, signed, or compared byte-for-byte, only decoded and compared as
// values.
var marshal = protojson.MarshalOptions{UseProtoNames: true}

// unmarshal rejects unknown fields. A field present in the data but absent
// from the compiled schema means the reader is older than the writer, which
// the backward-compatibility discipline this system enforces is meant to
// rule out, so it is better to fail loudly here than to silently drop data
// during a replay.
var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: false}

// TypeName is the fully qualified protobuf message name. It is simultaneously
// the events.type column, the ce_type header, and the protoregistry lookup key,
// so there is exactly one identifier for an event type in the whole system.
func TypeName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// Encode turns a message into an event ready for eventstore.Append.
func Encode(m proto.Message, meta eventstore.Meta) (eventstore.Event, error) {
	if m == nil {
		return eventstore.Event{}, errors.New("codec: nil message")
	}
	data, err := marshal.Marshal(m)
	if err != nil {
		return eventstore.Event{}, fmt.Errorf("codec: marshal %s: %w", TypeName(m), err)
	}
	return eventstore.Event{Type: TypeName(m), Data: data, Meta: meta}, nil
}

// Decode resolves the event's type name through the global registry and
// unmarshals into a fresh message of that type.
//
// The registry is populated by the generated code's package initialisers, so
// resolution is a local map lookup and needs no network — which is what keeps
// replay independent of the schema registry.
func Decode(e eventstore.Event) (proto.Message, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(e.Type))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownType, e.Type)
	}
	msg := mt.New().Interface()
	if err := unmarshal.Unmarshal(e.Data, msg); err != nil {
		return nil, fmt.Errorf("codec: unmarshal %s: %w", e.Type, err)
	}
	return msg, nil
}
