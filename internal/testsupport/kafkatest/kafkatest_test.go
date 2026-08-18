package kafkatest_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AymanKastali/sagaflow/internal/testsupport/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) {
	stop, err := kafkatest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

// The broker being 4.3.1 is guaranteed by the pinned Image constant at container
// creation (spec D16), not asserted here — a runtime version check would either
// be tautological or rest on inferring a release from its supported API
// versions. What this test proves is that the gated boot produced a broker the
// host can actually reach and round-trip against.
func TestSharedBrokerRoundTrips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	brokers := kafkatest.Shared(t).Brokers()
	const topic = "kafkatest.smoke"

	// The topic is created explicitly because the container disables
	// auto-creation: a typo'd topic name must fail, not silently work with the
	// broker's default partition count.
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// ConsumeTopics must be given at construction. AddConsumeTopics is a no-op on
	// a client that was not built as a consumer (it returns early on
	// !c.consuming()), and the poll below would then block until the context
	// expired rather than failing.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	rec := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// franz-go's default ConsumeResetOffset is NewOffset().AtStart(), so a record
	// produced before this poll is still delivered.
	fetches := cl.PollRecords(ctx, 1)
	if err := fetches.Err0(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if string(recs[0].Value) != "v" {
		t.Fatalf("want value v, got %q", recs[0].Value)
	}
}
