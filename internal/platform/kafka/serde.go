// Package kafka holds the franz-go plumbing: wire framing against the schema
// registry, an acks=all producer, and a consumer whose offsets are committed
// only after the handler's transaction commits.
package kafka

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
var ErrSubjectNotRegistered = errors.New("kafka: subject not registered")

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

// NewTopicSerde resolves each prototype's schema id from the registry and builds
// a serde for that topic.
//
// Ids are resolved once, at construction, so the steady-state encode path makes
// no network call and a registry outage cannot stall publishing — it can only
// prevent a restart, which is the failure mode you want.
func NewTopicSerde(ctx context.Context, cl *sr.Client, topic string, prototypes ...proto.Message) (*Serde, error) {
	if len(prototypes) == 0 {
		return nil, errors.New("kafka: NewTopicSerde needs at least one prototype")
	}
	inner := sr.NewSerde(sr.Header(&sr.ConfluentHeader{}))

	for _, p := range prototypes {
		name := string(p.ProtoReflect().Descriptor().FullName())
		subject := Subject(topic, name)

		ss, err := cl.SchemaByVersion(ctx, subject, -1) // -1 is "latest"
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrSubjectNotRegistered, subject, err)
		}

		// Index [0] identifies the first top-level message in the .proto file,
		// which is why every file in this project holds exactly one.
		prototype := p
		inner.Register(ss.ID, prototype,
			sr.Index(0),
			sr.EncodeFn(func(v any) ([]byte, error) {
				return proto.Marshal(v.(proto.Message))
			}),
			sr.DecodeFn(func(b []byte, v any) error {
				return proto.Unmarshal(b, v.(proto.Message))
			}),
			sr.GenerateFn(func() any {
				return prototype.ProtoReflect().New().Interface()
			}),
		)
	}
	return &Serde{inner: inner, topic: topic}, nil
}

// Encode frames a message for the wire.
func (s *Serde) Encode(m proto.Message) ([]byte, error) {
	b, err := s.inner.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("kafka: encode %s for %s: %w",
			m.ProtoReflect().Descriptor().FullName(), s.topic, err)
	}
	return b, nil
}

// Decode reads a framed payload back into a fresh message of the registered type.
func (s *Serde) Decode(b []byte) (proto.Message, error) {
	v, err := s.inner.DecodeNew(b)
	if err != nil {
		return nil, fmt.Errorf("kafka: decode from %s: %w", s.topic, err)
	}
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("kafka: decoded %T is not a proto.Message", v)
	}
	return m, nil
}
