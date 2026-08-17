package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/kafkatest"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

func produce(t *testing.T, brokers []string, topic, key string, headers map[string]string, payload []byte) {
	t.Helper()
	p, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()
	if err := p.Publish(t.Context(), []outbox.Claimed{{
		ID:      1,
		Message: outbox.Message{Topic: topic, Key: key, Payload: payload, Headers: headers},
	}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// run starts c in the background and stops it when the test ends.
func run(t *testing.T, ctx context.Context, c *kafka.Consumer) {
	t.Helper()
	t.Cleanup(c.Close)
	go func() { _ = c.Run(ctx) }()
}

// dlqRecords polls topic for want records. A DLQ assertion that polled once would
// pass a broker that had not yet appended the record, so this blocks on ctx.
func dlqRecords(t *testing.T, ctx context.Context, brokers []string, topic string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	defer cl.Close()

	var recs []*kgo.Record
	for len(recs) < want {
		fetches := cl.PollRecords(ctx, want-len(recs))
		if err := fetches.Err0(); err != nil {
			t.Fatalf("poll %s: %v", topic, err)
		}
		recs = append(recs, fetches.Records()...)
	}
	return recs
}

func headersOfRecord(r *kgo.Record) map[string]string {
	h := make(map[string]string, len(r.Headers))
	for _, hdr := range r.Headers {
		h[hdr.Key] = string(hdr.Value)
	}
	return h
}

func TestConsumerDeliversRecordWithHeadersAndProvenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.deliver"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A",
		map[string]string{"ce_id": "ce-1", "ce_source": "/sagaflow/inventory"}, []byte{7})

	got := make(chan kafka.Record, 1)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   "test.deliver",
		Topics:  []string{topic},
		Handler: func(_ context.Context, r kafka.Record) error {
			got <- r
			return nil
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	run(t, ctx, c)

	select {
	case r := <-got:
		if r.Key != "seat-14A" {
			t.Errorf("key: got %q", r.Key)
		}
		if r.Headers["ce_id"] != "ce-1" {
			t.Errorf("headers: got %v", r.Headers)
		}
		if r.Topic != topic {
			t.Errorf("topic: got %q", r.Topic)
		}
		if r.Offset != 0 {
			t.Errorf("offset: got %d", r.Offset)
		}
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
}

// An unsettleable record's offset must never be marked. MaxAttempts 1 with no DLQ
// is the "cannot settle it" configuration: the record is neither retried into
// success nor dead-lettered, so its offset must stay uncommitted and a fresh
// member must see it again.
//
// This guards the marking rule, not the client options: marking a record whose
// handler failed is what this fails on. What franz-go's default autocommit would
// do is covered by TestUnsettledOffsetSurvivesTheAutocommitTimer, which has to
// outlive the commit timer to see it.
func TestFailingHandlerDoesNotCommitTheOffset(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.nocommit"
	const group = "test.nocommit"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})

	attempted := make(chan struct{}, 16)
	failing, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       group,
		Topics:      []string{topic},
		MaxAttempts: 1, // no retries, and no DLQ below ⇒ nothing can settle it
		Handler: func(_ context.Context, _ kafka.Record) error {
			select {
			case attempted <- struct{}{}:
			default:
			}
			return errors.New("transient failure")
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	go func() { _ = failing.Run(runCtx) }()

	// Wait on a signal rather than spinning on a counter (spec §12.4).
	select {
	case <-attempted:
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
	stop()
	failing.Close()

	// A fresh consumer in the same group must see the record again.
	redelivered := make(chan struct{}, 1)
	ok, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   group,
		Topics:  []string{topic},
		Handler: func(_ context.Context, _ kafka.Record) error {
			select {
			case redelivered <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("second consumer: %v", err)
	}
	run(t, ctx, ok)

	select {
	case <-redelivered:
	case <-ctx.Done():
		t.Fatal("record was not redelivered — the offset was committed despite the handler failing")
	}
}

// A failing record's retries all happen before the next record in its partition
// is touched. The loop must not return to polling with an in-progress failure
// behind it, because the next record's success would mark a higher offset.
func TestFailedRecordIsSettledBeforeTheNextInItsPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.settle_in_order"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic, topic+".dlq"); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	// One key ⇒ one partition ⇒ offsets 0 and 1, delivered in that order.
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-2"}, []byte{2})

	dlq, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("dlq producer: %v", err)
	}
	defer dlq.Close()

	seen := make(chan byte, 32)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       "test.settle_in_order",
		Topics:      []string{topic},
		DLQ:         dlq,
		MaxAttempts: 2,
		BaseBackoff: time.Millisecond,
		Handler: func(_ context.Context, r kafka.Record) error {
			seen <- r.Value[0]
			if r.Value[0] == 1 {
				return errors.New("transient failure on the first record")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	run(t, ctx, c)

	// The first record is attempted twice — the retry budget — before the second
	// record is touched at all. If the second is attempted before the first has
	// exhausted its attempts, the loop moved on with an unmarked failure behind it.
	want := []byte{1, 1, 2}
	for i, w := range want {
		select {
		case got := <-seen:
			if got != w {
				t.Fatalf("attempt %d: want record %d, got %d — the loop advanced past an unsettled failure", i, w, got)
			}
		case <-ctx.Done():
			t.Fatalf("only %d of %d attempts happened", i, len(want))
		}
	}

	// The first record exhausted its budget, so it is in the DLQ rather than lost.
	recs := dlqRecords(t, ctx, brokers, topic+".dlq", 1)
	if recs[0].Value[0] != 1 {
		t.Fatalf("wrong record dead-lettered: %v", recs[0].Value)
	}
}

// The regression test for the loss this task's design note describes: an
// unsettleable record followed by a successful one in the same partition. Marking
// the second advances the group past the first, and MarkCommitRecords "does not
// allow rewinds" — so the first is gone, with healthy-looking offsets and no error.
//
// No DLQ here on purpose. With one, the first record is settled by dead-lettering
// and the second may safely proceed; it is the unsettleable case that has to gate
// the rest of the partition.
func TestUnsettleableRecordBlocksLaterOffsetsInItsPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.stall_gate"
	const group = "test.stall_gate"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	// One key ⇒ one partition ⇒ the failure sits at offset 0, the success at 1.
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-2"}, []byte{2})

	const attempts = 2
	spent := make(chan struct{}, attempts)
	stalling, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       group,
		Topics:      []string{topic},
		MaxAttempts: attempts,
		BaseBackoff: time.Millisecond,
		Handler: func(_ context.Context, r kafka.Record) error {
			if r.Value[0] != 1 {
				return nil
			}
			spent <- struct{}{}
			return errors.New("transient failure that outlives its budget")
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	go func() { _ = stalling.Run(runCtx) }()

	// Wait for the whole budget to be spent: at that point the record is settled as
	// far as it will ever be, and the gate either holds or the loop moves on.
	for range attempts {
		select {
		case <-spent:
		case <-ctx.Done():
			t.Fatal("the failing record never exhausted its attempts")
		}
	}
	stop()
	stalling.Close()

	// Both records must come back: the failure because it was never settled, and
	// the success because the gate never let it be handled.
	seen := make(chan byte, 8)
	fresh, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   group,
		Topics:  []string{topic},
		Handler: func(_ context.Context, r kafka.Record) error {
			seen <- r.Value[0]
			return nil
		},
	})
	if err != nil {
		t.Fatalf("fresh consumer: %v", err)
	}
	run(t, ctx, fresh)

	redelivered := map[byte]bool{}
	for len(redelivered) < 2 {
		select {
		case v := <-seen:
			redelivered[v] = true
		case <-ctx.Done():
			t.Fatalf("only %v came back — the group advanced past the unsettled record at offset 0", redelivered)
		}
	}
}

// An unsettled record must still be uncommitted after the autocommit timer has
// run. AutoCommitMarks is what makes that true, and nothing else in this package
// does: the loop leaves a record unsettled and then keeps polling, so any
// commit-what-was-polled behaviour eventually commits past it and loses it.
//
// Two properties of franz-go v1.21.6 make this the only shape of test that can see
// it. Default autocommit sets a poll's offsets *dirty* and promotes them to
// committable at the start of the *next* poll, an intentional one-poll lag that
// makes the default at-least-once for a batch still being processed — so a handler
// blocked inside a batch proves nothing. And the commit is on a 5 s timer, so this
// has to outlive it. The elapsed time here is that timer's, not a guess about our
// own work.
func TestUnsettledOffsetSurvivesTheAutocommitTimer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.unprocessed"
	const group = "test.unprocessed"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-2"}, []byte{2})

	// franz-go's default AutoCommitInterval.
	const autocommitInterval = 5 * time.Second

	failed := make(chan struct{}, 1)
	stalling, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       group,
		Topics:      []string{topic},
		MaxAttempts: 1, // no retries, no DLQ ⇒ the record stays unsettled
		Handler: func(_ context.Context, _ kafka.Record) error {
			select {
			case failed <- struct{}{}:
			default:
			}
			return errors.New("unsettleable failure")
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	go func() { _ = stalling.Run(runCtx) }()

	select {
	case <-failed:
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
	// The loop is polling again by now, which is what would promote the unsettled
	// offset to committable, so this wait is what gives the timer its chance.
	select {
	case <-time.After(2 * autocommitInterval):
	case <-ctx.Done():
		t.Fatal("timed out waiting out the autocommit interval")
	}
	stop()
	stalling.Close()

	seen := make(chan byte, 8)
	fresh, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   group,
		Topics:  []string{topic},
		Handler: func(_ context.Context, r kafka.Record) error {
			seen <- r.Value[0]
			return nil
		},
	})
	if err != nil {
		t.Fatalf("fresh consumer: %v", err)
	}
	run(t, ctx, fresh)

	redelivered := map[byte]bool{}
	for len(redelivered) < 2 {
		select {
		case v := <-seen:
			redelivered[v] = true
		case <-ctx.Done():
			t.Fatalf("only %v came back — an unsettled offset was committed on the autocommit timer", redelivered)
		}
	}
}

func TestTransientFailureIsRetriedThenSucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.retry"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})

	attempts := make(chan int, 16)
	var n int
	done := make(chan struct{}, 1)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:     brokers,
		Group:       "test.retry",
		Topics:      []string{topic},
		MaxAttempts: 5,
		BaseBackoff: time.Millisecond,
		Handler: func(_ context.Context, _ kafka.Record) error {
			n++
			attempts <- n
			if n < 3 {
				return errors.New("provider 503")
			}
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	run(t, ctx, c)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("the handler never succeeded — retries did not happen")
	}
	for want := 1; want <= 3; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("want attempt %d, got %d", want, got)
			}
		default:
			t.Fatalf("only %d attempts recorded, want 3", want-1)
		}
	}
}

func TestPermanentErrorRoutesToDLQWithProvenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "consumer.poison"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic, topic+".dlq"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	produce(t, brokers, topic, "seat-14A",
		map[string]string{"ce_id": "ce-1", "traceparent": "00-aaaa-bbbb-01"}, []byte{9})

	dlqProducer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("dlq producer: %v", err)
	}
	defer dlqProducer.Close()

	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   "test.poison",
		Topics:  []string{topic},
		DLQ:     dlqProducer,
		Handler: func(_ context.Context, _ kafka.Record) error {
			return fmt.Errorf("%w: unknown event type", kafka.ErrPermanent)
		},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	run(t, ctx, c)

	recs := dlqRecords(t, ctx, brokers, topic+".dlq", 1)
	h := headersOfRecord(recs[0])

	if h["ce_id"] != "ce-1" {
		t.Errorf("original headers must survive: %v", h)
	}
	if h["traceparent"] != "00-aaaa-bbbb-01" {
		t.Errorf("traceparent must survive so the failure stays traceable: %v", h)
	}
	if h["sagaflow_dlq_topic"] != topic {
		t.Errorf("want provenance topic %q, got %q", topic, h["sagaflow_dlq_topic"])
	}
	if h["sagaflow_dlq_offset"] != "0" {
		t.Errorf("want provenance offset 0, got %q", h["sagaflow_dlq_offset"])
	}
	if h["sagaflow_dlq_error"] == "" {
		t.Error("want the error recorded so an operator need not re-run it")
	}
	if string(recs[0].Key) != "seat-14A" {
		t.Errorf("dlq must preserve the original key for replay: %q", recs[0].Key)
	}
}
