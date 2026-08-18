package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Partitions is the partition count for every topic in the system (spec §10.3).
// It bounds consumer parallelism; per-stream ordering is preserved regardless
// because the record key is always the stream id.
const Partitions int32 = 6

// EnsureTopics creates topics if they are absent and succeeds if they exist.
//
// Explicit creation rather than auto-creation: an auto-created topic gets the
// broker's default partition count, which would silently cap parallelism and, in
// a cluster, silently reduce the replication factor.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, replication int16, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka: admin client: %w", err)
	}
	defer cl.Close()

	resp, err := kadm.NewClient(cl).CreateTopics(ctx, partitions, replication, nil, topics...)
	if err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	for _, ct := range resp {
		// Already existing is the expected outcome on every restart, not a failure.
		if ct.Err != nil && !errors.Is(ct.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafka: create topic %s: %w", ct.Topic, ct.Err)
		}
	}
	return nil
}
