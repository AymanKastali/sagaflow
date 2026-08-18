package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"github.com/AymanKastali/sagaflow/internal/testsupport/kafkatest"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produce writes one record and reports where it landed.
//
// It returns the partition and offset because the broker is shared by every test in
// the package and by repeated runs (-count=2), so no test may assume it is writing
// to an empty topic. Callers that only need the record ignore both.
//
// A kgo client rather than kafka.Producer: only ProduceSync's result carries the
// assigned offset, and Publish deliberately reports nothing but an error. The
// header mapping Publish does is covered by TestPublishPreservesKeyHeadersAndPayload.
func produce(t *testing.T, brokers []string, topic, key string, headers map[string]string, payload []byte) (partition int32, offset int64) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatalf("produce client: %v", err)
	}
	defer cl.Close()

	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	for k, v := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	res, err := cl.ProduceSync(t.Context(), rec).First()
	if err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
	return res.Partition, res.Offset
}

// run starts c in the background and stops it when the test ends.
func run(t *testing.T, ctx context.Context, c *kafka.Consumer) {
	t.Helper()
	t.Cleanup(c.Close)
	go func() { _ = c.Run(ctx) }()
}

// dlqRecord polls topic until the dead-lettered copy of sourceOffset appears.
//
// It selects by provenance rather than taking the topic's first record, because a
// DLQ topic outlives the test that filled it: the whole package shares one broker,
// and -count=2 replays every test against it, so "the first record" is somebody
// else's. Blocking on ctx rather than polling once also means a broker that has not
// appended the record yet fails as a timeout, not as a missing record.
func dlqRecord(t *testing.T, ctx context.Context, brokers []string, topic string, sourceOffset int64) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	defer cl.Close()

	want := strconv.FormatInt(sourceOffset, 10)
	for {
		fetches := cl.PollRecords(ctx, 16)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("poll %s: %v", topic, err)
		}
		for _, r := range fetches.Records() {
			if headersOfRecord(r)["sagaflow_dlq_offset"] == want {
				return r
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("no dead letter for offset %d on %s", sourceOffset, topic)
		}
	}
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
	wantPartition, wantOffset := produce(t, brokers, topic, "seat-14A",
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
		// Provenance has to match where the record actually landed: it is what the
		// DLQ carries, and a replay aimed at the wrong partition or offset is worse
		// than no replay at all.
		if r.Partition != wantPartition || r.Offset != wantOffset {
			t.Errorf("provenance: want %d/%d, got %d/%d",
				wantPartition, wantOffset, r.Partition, r.Offset)
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

	// Wait on a signal rather than spinning on a counter, so the test has no
	// flaky dependency on how fast the consumer happens to retry.
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

	// One key ⇒ one partition ⇒ consecutive offsets, delivered in that order.
	_, failingOffset := produce(t, brokers, topic, "seat-14A", map[string]string{"ce_id": "ce-1"}, []byte{1})
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
	dead := dlqRecord(t, ctx, brokers, topic+".dlq", failingOffset)
	if dead.Value[0] != 1 {
		t.Fatalf("wrong record dead-lettered: %v", dead.Value)
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

	// One key ⇒ one partition ⇒ the failure sits immediately before the success.
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
			t.Fatalf("only %v came back — the group advanced past the unsettled record", redelivered)
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
	_, wantOffset := produce(t, brokers, topic, "seat-14A",
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

	dead := dlqRecord(t, ctx, brokers, topic+".dlq", wantOffset)
	h := headersOfRecord(dead)

	if h["ce_id"] != "ce-1" {
		t.Errorf("original headers must survive: %v", h)
	}
	if h["traceparent"] != "00-aaaa-bbbb-01" {
		t.Errorf("traceparent must survive so the failure stays traceable: %v", h)
	}
	if h["sagaflow_dlq_topic"] != topic {
		t.Errorf("want provenance topic %q, got %q", topic, h["sagaflow_dlq_topic"])
	}
	if got := h["sagaflow_dlq_offset"]; got != strconv.FormatInt(wantOffset, 10) {
		t.Errorf("want provenance offset %d, got %q", wantOffset, got)
	}
	if h["sagaflow_dlq_error"] == "" {
		t.Error("want the error recorded so an operator need not re-run it")
	}
	if string(dead.Key) != "seat-14A" {
		t.Errorf("dlq must preserve the original key for replay: %q", dead.Key)
	}
}
