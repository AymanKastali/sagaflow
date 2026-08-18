package kafka_test

import (
	"slices"
	"testing"

	"github.com/kptac/sagaflow/internal/platform/envelope"
	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/kptac/sagaflow/internal/testsupport/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer must satisfy the interface the outbox poller consumes. The assertion
// lives here because kafka does not import outbox in production — a test import
// does not appear in the production dependency graph.
var _ outbox.Publisher = (*kafka.Producer)(nil)

// headersOf flattens a record's headers for comparison against a map literal.
func headersOf(r *kgo.Record) map[string]string {
	got := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		got[h.Key] = string(h.Value)
	}
	return got
}

func TestEnsureTopicsCreatesWithSixPartitionsAndIsIdempotent(t *testing.T) {
	ctx := t.Context()
	brokers := kafkatest.Shared(t).Brokers()

	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, "inventory.events"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// Running it again must not fail — services call this at startup.
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, "inventory.events"); err != nil {
		t.Fatalf("second ensure: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	meta, err := kadm.NewClient(cl).ListTopics(ctx, "inventory.events")
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	topic, ok := meta["inventory.events"]
	if !ok {
		t.Fatal("inventory.events was not created")
	}
	if got := len(topic.Partitions); got != 6 {
		t.Fatalf("want 6 partitions, got %d", got)
	}
}

func TestPublishPreservesKeyHeadersAndPayload(t *testing.T) {
	ctx := t.Context()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "producer.roundtrip"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	p, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()

	wantHeaders := map[string]string{
		"ce_id":       "01920000-0000-7000-8000-000000000001",
		"ce_type":     "sagaflow.inventory.v1.SeatHeld",
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	err = p.Publish(ctx, []envelope.Message{{
		Topic:   topic,
		Key:     "seat-BA117-2026-09-01-14A",
		Payload: []byte{0x00, 0x0a, 0x0b},
		Headers: wantHeaders,
	}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	r := recs[0]

	if string(r.Key) != "seat-BA117-2026-09-01-14A" {
		t.Errorf("key: got %q", r.Key)
	}
	if !slices.Equal(r.Value, []byte{0x00, 0x0a, 0x0b}) {
		t.Errorf("payload: got %v", r.Value)
	}
	got := headersOf(r)
	for k, want := range wantHeaders {
		if got[k] != want {
			t.Errorf("header %s: want %q, got %q", k, want, got[k])
		}
	}
}

func TestPublishSameKeyKeepsOrder(t *testing.T) {
	ctx := t.Context()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "producer.ordering"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	p, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()

	const total = 20
	var batch []envelope.Message
	for i := range total {
		batch = append(batch, envelope.Message{
			Topic:   topic,
			Key:     "seat-14A", // one key ⇒ one partition ⇒ ordered
			Payload: []byte{byte(i)},
			Headers: map[string]string{"ce_id": "id"},
		})
	}
	if err := p.Publish(ctx, batch); err != nil {
		t.Fatalf("publish: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	var seen []byte
	for len(seen) < total {
		fetches := cl.PollRecords(ctx, total)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("poll: %v", err)
		}
		for _, r := range fetches.Records() {
			seen = append(seen, r.Value[0])
		}
	}

	want := make([]byte, total)
	for i := range want {
		want[i] = byte(i)
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("order broken: got %v", seen)
	}
}
