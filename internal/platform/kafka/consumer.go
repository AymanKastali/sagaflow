package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrPermanent marks a handler error the message can never recover from — an
// undecodable payload, an unknown event type. Wrap with it to route straight to
// the DLQ with no retries (spec §10.2).
var ErrPermanent = errors.New("permanent failure")

// RebalanceTimeout must exceed the slowest handler transaction (spec §10.3),
// otherwise a long-running handler has its partitions pulled mid-flight.
const RebalanceTimeout = 60 * time.Second

const (
	// DefaultMaxAttempts bounds retries of one record, including the first try.
	//
	// The whole budget has to fit inside RebalanceTimeout: BlockRebalanceOnPoll
	// means a rebalance cannot proceed while a batch is being retried. Five
	// attempts at DefaultBaseBackoff is roughly 1.5 s of backoff against 60 s.
	DefaultMaxAttempts = 5
	// DefaultBaseBackoff is the first retry delay; it doubles each attempt and
	// carries jitter (spec §10.2).
	DefaultBaseBackoff = 100 * time.Millisecond
	// pollBatch bounds one poll. Records are handled one at a time regardless, so
	// this only bounds how much work a single rebalance block covers.
	pollBatch = 100
)

// Record is one consumed message.
type Record struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
}

// Handler processes one record. Returning nil means "committed, safe to advance".
//
// A returned error is retried with backoff up to MaxAttempts and then
// dead-lettered; an error wrapping ErrPermanent skips the retries and
// dead-letters immediately. Business outcomes are not errors — a handler that
// decides "nothing to do" returns nil, per spec §10.2's retry policy.
type Handler func(ctx context.Context, r Record) error

type ConsumerConfig struct {
	Brokers []string
	Group   string
	Topics  []string
	Handler Handler
	// DLQ publishes settled failures to <topic>.dlq. Optional, but a consumer
	// without one cannot settle a failing record: it stops advancing that
	// partition instead, which is safe but stalls. Only leave it nil in tests.
	DLQ *Producer
	// MaxAttempts and BaseBackoff override DefaultMaxAttempts and
	// DefaultBaseBackoff. Tests set them small; services leave them zero.
	MaxAttempts int
	BaseBackoff time.Duration
}

type Consumer struct {
	cl  *kgo.Client
	cfg ConsumerConfig
	log *slog.Logger
}

// NewConsumer builds a consumer group whose offsets advance only for records the
// handler finished.
//
// The three options below are the difference between at-least-once and silent
// loss, and none of them is franz-go's default:
//
//   - AutoCommitMarks: commit only explicitly marked offsets. The default commits
//     every polled offset on a timer, including records still in flight.
//   - BlockRebalanceOnPoll: no rebalance between polling and marking, so a
//     partition cannot be reassigned while its records are mid-handler.
//   - OnPartitionsRevoked → CommitMarkedOffsets: flush what was finished before
//     losing the partitions, so completed work is not reprocessed.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Handler == nil {
		return nil, errors.New("kafka: consumer needs a handler")
	}
	if cfg.Group == "" {
		return nil, errors.New("kafka: consumer needs a group")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = DefaultBaseBackoff
	}
	c := &Consumer{cfg: cfg, log: slog.Default().With("group", cfg.Group)}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(RebalanceTimeout),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				c.log.Error("commit on revoke failed", "error", err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	c.cl = cl
	return c, nil
}

// Close leaves the group and waits for the final revoke.
//
// CloseAllowingRebalance rather than Close: with BlockRebalanceOnPoll, franz-go's
// Close "will hang if you polled, did not allow rebalances, and want to close",
// and leaving the group is itself a rebalance. Run releases every poll it takes,
// but Close races Run's last poll — so releasing here too removes the ordering
// hazard rather than documenting it. The final revoke only commits marked offsets,
// which is safe to run concurrently.
func (c *Consumer) Close() { c.cl.CloseAllowingRebalance() }

// topicPartition identifies one partition within one batch.
type topicPartition struct {
	topic     string
	partition int32
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for !c.poll(ctx) {
	}
	return nil
}

// poll handles one batch and reports whether the loop should stop.
//
// AllowRebalance is deferred so it pairs with the poll on every path, the exit
// path included. It is required with BlockRebalanceOnPoll — without it the group
// never rebalances and a new member waits forever — and a return that skips it
// deadlocks shutdown, because leaving the group is a rebalance too.
func (c *Consumer) poll(ctx context.Context) (done bool) {
	fetches := c.cl.PollRecords(ctx, pollBatch)
	defer c.cl.AllowRebalance()

	if fetches.IsClientClosed() || ctx.Err() != nil {
		return true
	}
	for _, fe := range fetches.Errors() {
		// Fetch errors are retried internally; a surfaced one is worth seeing but is
		// not fatal to the loop.
		c.log.Error("fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
	}
	c.handleBatch(ctx, fetches)
	return false
}

// handleBatch settles one poll's records, skipping the rest of any partition whose
// record could not be settled.
//
// Once a partition is stalled, the rest of its records in this batch are left
// untouched: marking a later offset would advance the group past the unsettled
// one, and MarkCommitRecords keeps the highest offset per partition and cannot
// rewind. Other partitions keep flowing, so one stuck stream does not stop the
// others (spec §10.2).
func (c *Consumer) handleBatch(ctx context.Context, fetches kgo.Fetches) {
	stalled := make(map[topicPartition]bool)
	fetches.EachRecord(func(kr *kgo.Record) {
		tp := topicPartition{kr.Topic, kr.Partition}
		if stalled[tp] {
			return
		}
		if !c.handle(ctx, kr) {
			stalled[tp] = true
		}
	})
}

// handle settles one record and reports whether its offset was marked.
//
// false means the record is unsettled: nothing later in its partition may be
// marked, and the record will be redelivered after the next rebalance or restart.
func (c *Consumer) handle(ctx context.Context, kr *kgo.Record) (marked bool) {
	r := toRecord(kr)

	err := c.attempt(ctx, r)
	if err == nil {
		// Mark only now. Ordering matters: crash after the handler's commit but
		// before the mark means redelivery, which the inbox absorbs. The reverse
		// would be loss.
		c.cl.MarkCommitRecords(kr)
		return true
	}
	if ctx.Err() != nil {
		return false // shutting down: leave it unsettled and redeliverable
	}

	// The failure is settled by dead-lettering it: either it was permanent from
	// the start, or it outlived a bounded retry budget and is therefore no longer
	// a transient failure.
	if derr := c.deadLetter(ctx, r, err); derr != nil {
		c.log.Error("dead-letter failed; partition will not advance",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
			"error", derr, "cause", err)
		return false
	}
	c.cl.MarkCommitRecords(kr)
	return true
}

// attempt runs the handler until it succeeds or its retry budget is spent, and
// returns the last error.
//
// Retries happen here rather than by returning to the poll loop because a failed
// record must be settled before the loop touches the next record in its partition.
func (c *Consumer) attempt(ctx context.Context, r Record) error {
	for n := 1; ; n++ {
		err := c.cfg.Handler(ctx, r)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrPermanent) || n >= c.cfg.MaxAttempts {
			return err
		}
		wait := backoff(c.cfg.BaseBackoff, n)
		c.log.Warn("handler failed; retrying",
			"topic", r.Topic, "partition", r.Partition, "offset", r.Offset,
			"attempt", n, "backoff", wait, "error", err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
}

// backoff is the wait before attempt n+1: bounded exponential with jitter (spec
// §10.2). Each wait is uniform in [d, 2·d), so a fleet of consumers retrying the
// same downstream outage spreads out instead of retrying in lockstep.
func backoff(base time.Duration, attempt int) time.Duration {
	d := int64(base << (attempt - 1))
	return time.Duration(rand.Int64N(d) + d)
}

func toRecord(kr *kgo.Record) Record {
	headers := make(map[string]string, len(kr.Headers))
	for _, h := range kr.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Record{
		Topic:     kr.Topic,
		Key:       string(kr.Key),
		Value:     kr.Value,
		Headers:   headers,
		Partition: kr.Partition,
		Offset:    kr.Offset,
	}
}

// deadLetter republishes r to <topic>.dlq with enough provenance to replay it.
func (c *Consumer) deadLetter(ctx context.Context, r Record, cause error) error {
	if c.cfg.DLQ == nil {
		// Deliberately an error, not a silent drop. Without a DLQ there is nowhere
		// to put the record, and marking it anyway would discard it — so the
		// partition stalls and an operator has to notice.
		return fmt.Errorf("kafka: no DLQ configured, cannot settle: %w", cause)
	}
	// The original headers, traceparent included, survive so the failure stays
	// traceable and the record stays replayable.
	headers := make(map[string]string, len(r.Headers)+4)
	maps.Copy(headers, r.Headers)
	headers["sagaflow_dlq_topic"] = r.Topic
	headers["sagaflow_dlq_partition"] = strconv.Itoa(int(r.Partition))
	headers["sagaflow_dlq_offset"] = strconv.FormatInt(r.Offset, 10)
	headers["sagaflow_dlq_error"] = cause.Error()

	return c.cfg.DLQ.Publish(ctx, []outbox.Claimed{{
		Message: outbox.Message{
			Topic:   r.Topic + ".dlq",
			Key:     r.Key, // same key so a replay lands on the same partition
			Payload: r.Value,
			Headers: headers,
		},
	}})
}
