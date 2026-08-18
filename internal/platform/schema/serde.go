// Package schema frames protobuf messages for the wire against a Confluent-
// compatible registry, and pins the registry's compatibility level.
//
// It is separate from platform/kafka because it shares no symbol with the broker
// plumbing: framing is about schema ids and message bytes, not about brokers,
// partitions, or offsets.
package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
)

// ErrSubjectNotRegistered means a schema this service needs is absent from the
// registry. Services never auto-register (spec D14), so this is fatal at
// startup rather than something to paper over at produce time.
var ErrSubjectNotRegistered = errors.New("schema: subject not registered")

// Subject implements TopicRecordNameStrategy: <topic>-<fully.qualified.Name>.
//
// The default TopicNameStrategy allows only one schema per topic, and our topics
// carry several event types, so it would break on the second one (spec §8.3).
func Subject(topic, typeName string) string {
	return topic + "-" + typeName
}

// Serde encodes and decodes one topic's messages in the Confluent wire format:
// magic byte 0x00, big-endian schema id, protobuf message index, payload.
type Serde struct {
	inner *sr.Serde
	topic string
}

// latestVersion is the version selector meaning "whatever is current".
const latestVersion = -1

// NewTopicSerde resolves each prototype's schema id from the registry and builds
// a serde for that topic.
//
// Ids are resolved once, at construction, so the steady-state encode path makes
// no network call and a registry outage cannot stall publishing — it can only
// prevent a restart, which is the failure mode you want.
func NewTopicSerde(ctx context.Context, cl *sr.Client, topic string, prototypes ...proto.Message) (*Serde, error) {
	if len(prototypes) == 0 {
		return nil, errors.New("schema: NewTopicSerde needs at least one prototype")
	}
	inner := sr.NewSerde(sr.Header(&sr.ConfluentHeader{}))

	for _, prototype := range prototypes {
		subject := Subject(topic, string(prototype.ProtoReflect().Descriptor().FullName()))
		schema, err := cl.SchemaByVersion(ctx, subject, latestVersion)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrSubjectNotRegistered, subject, err)
		}
		register(inner, schema.ID, prototype)
	}
	return &Serde{inner: inner, topic: topic}, nil
}

// register teaches inner to frame one message type under its registered schema id.
//
// sr.Index(0) names the first top-level message in the .proto file, which is why
// every file in this project holds exactly one. Leaving it out still round-trips
// through this package while emitting payloads no other Confluent client can read.
func register(inner *sr.Serde, id int, prototype proto.Message) {
	inner.Register(id, prototype,
		sr.Index(0),
		sr.EncodeFn(func(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }),
		sr.DecodeFn(func(b []byte, v any) error { return proto.Unmarshal(b, v.(proto.Message)) }),
		sr.GenerateFn(func() any { return prototype.ProtoReflect().New().Interface() }),
	)
}

// Encode frames a message for the wire.
func (s *Serde) Encode(m proto.Message) ([]byte, error) {
	b, err := s.inner.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("schema: encode %s for %s: %w",
			m.ProtoReflect().Descriptor().FullName(), s.topic, err)
	}
	return b, nil
}

// Decode reads a framed payload back into a fresh message of the registered type.
func (s *Serde) Decode(b []byte) (proto.Message, error) {
	v, err := s.inner.DecodeNew(b)
	if err != nil {
		return nil, fmt.Errorf("schema: decode from %s: %w", s.topic, err)
	}
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("schema: decoded %T is not a proto.Message", v)
	}
	return m, nil
}
