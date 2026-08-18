package kafka

import (
	"context"
	"fmt"

	"github.com/kptac/sagaflow/internal/platform/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes messages to Kafka. It satisfies outbox.Publisher — asserted
// in the test rather than here, because importing outbox for a compile-time
// assertion would reintroduce a dependency this package does not otherwise need.
type Producer struct {
	cl *kgo.Client
}

// NewProducer builds a durable producer.
//
// acks=all and idempotent production are franz-go defaults (RequireAllISRAcks,
// idempotency on unless explicitly disabled), so the durability the spec asks for
// costs no configuration here — but it is worth knowing it is a default rather
// than an omission, because DisableIdempotentWrite anywhere would quietly remove
// it.
func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0), // latency matters more than batching for a saga
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

func (p *Producer) Close() { p.cl.Close() }

// Publish sends every message and returns the first error.
//
// ProduceSync waits for all records, so a partial failure leaves the whole batch
// unmarked in the outbox and the successful records are republished on the next
// pass. That is a duplicate, which the inbox absorbs — the alternative, marking a
// partially published batch, would be a loss, which nothing absorbs.
func (p *Producer) Publish(ctx context.Context, msgs []envelope.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		recs = append(recs, record(m))
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish %d records: %w", len(recs), err)
	}
	return nil
}

// record converts one message to its wire form. The key is always the stream id,
// which is what keeps a stream's events in one partition and therefore in order.
func record(m envelope.Message) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(m.Headers))
	for k, v := range m.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return &kgo.Record{
		Topic:   m.Topic,
		Key:     []byte(m.Key),
		Value:   m.Payload,
		Headers: headers,
	}
}
