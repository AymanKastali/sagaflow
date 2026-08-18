package integration_test

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/schema"
	"github.com/AymanKastali/sagaflow/internal/testsupport/kafkatest"
	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
	"github.com/AymanKastali/sagaflow/internal/testsupport/srtest"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const heldSeat = "seat-BA117-2026-09-01-14A"

// registerSchemas runs the operator's registration step against the test
// registry — the same command the Makefile documents, rather than a table copied
// out of it. A copy could drift; running the real thing means this test fails the
// day registration stops covering what the service needs.
func registerSchemas(t *testing.T, registry string) {
	t.Helper()
	cmd := exec.Command("go", "run", "./cmd/schemactl", "-registry", registry)
	cmd.Dir = "../.."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("schemactl: %v\n%s", err, out)
	}
}

// start builds the service and runs it in the background. The function it
// returns is the crash: it cancels the service's context and reports what Run
// gave back.
func start(t *testing.T, ctx context.Context, cfg inventory.Config) (kill func() error) {
	t.Helper()
	service, err := inventory.New(ctx, cfg)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan error, 1)
	go func() { stopped <- service.Run(runCtx) }()

	return func() error {
		cancel()
		err := <-stopped
		service.Close()
		return err
	}
}

// watch consumes inventory.events into a channel, so a test waits for the event
// it cares about instead of sleeping.
func watch(t *testing.T, ctx context.Context, brokers []string, group string) <-chan envelope.Envelope {
	t.Helper()
	seen := make(chan envelope.Envelope, 32)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers, Group: group, Topics: []string{inventory.EventsTopic},
		Handler: func(_ context.Context, r kafka.Record) error {
			incoming, err := envelope.Parse(r.Headers)
			if err != nil {
				return nil // not this watcher's business; the service has its own DLQ
			}
			seen <- incoming
			return nil
		},
	})
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	t.Cleanup(consumer.Close)
	go func() { _ = consumer.Run(ctx) }()
	return seen
}

// await blocks until an event of this type arrives for this seat.
func await(t *testing.T, ctx context.Context, seen <-chan envelope.Envelope, typ, subject string) {
	t.Helper()
	for {
		select {
		case incoming := <-seen:
			if incoming.Type == typ && incoming.Subject == subject {
				return
			}
		case <-ctx.Done():
			t.Fatalf("no %s for %s before the deadline", typ, subject)
		}
	}
}

// send frames a command and publishes it to inventory.commands, exactly as
// booking will once it exists.
func send(t *testing.T, ctx context.Context, producer *kafka.Producer, serde *schema.Serde, cmd proto.Message) {
	t.Helper()
	payload, err := serde.Encode(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	outgoing := envelope.Envelope{
		ID:      envelope.NewID(),
		Source:  "/sagaflow/booking",
		Type:    string(cmd.ProtoReflect().Descriptor().FullName()),
		Subject: heldSeat, CorrelationID: "saga-service-test",
	}
	if err := producer.Publish(ctx, []envelope.Message{{
		Topic: inventory.CommandsTopic, Key: heldSeat, Payload: payload, Headers: outgoing.Headers(),
	}}); err != nil {
		t.Fatalf("publish %s: %v", outgoing.Type, err)
	}
}

// awaitStatus polls the availability view until it agrees with the streams.
//
// Polling rather than waiting on a signal, because the view is the one thing here
// that is allowed to lag: nothing announces "the projection caught up", and
// inventing such a message would build the coupling this design exists to avoid.
func awaitStatus(t *testing.T, ctx context.Context, dsn, seatID string, want inventory.Status) {
	t.Helper()
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	for {
		got, found, err := inventory.LoadAvailability(ctx, pool, seatID)
		if err != nil {
			t.Fatalf("read the view: %v", err)
		}
		if found && got.Status == want {
			return
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("the view never reached %v for %s (found=%v)", want, seatID, found)
		}
	}
}

// TestInventoryRunsAsAServiceAndSurvivesBeingKilled is the phase deliverable: a
// seat is held by a running process, that process is killed by cancelling its
// context, and a second one picks the flow up from the database it left behind.
func TestInventoryRunsAsAServiceAndSurvivesBeingKilled(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	registry := srtest.Shared(t).URL()
	registerSchemas(t, registry)

	dsn := pgtest.Shared(t).DSN(t, "inventory_service")
	brokers := kafkatest.Shared(t).Brokers()
	cfg := inventory.Config{DSN: dsn, Brokers: brokers, Registry: registry}

	kill := start(t, ctx, cfg)

	client, err := sr.NewClient(sr.URLs(registry))
	if err != nil {
		t.Fatalf("registry client: %v", err)
	}
	commands, err := schema.NewTopicSerde(ctx, client, inventory.CommandsTopic,
		&inventoryv1.HoldSeat{}, &inventoryv1.ReleaseSeatHold{})
	if err != nil {
		t.Fatalf("commands serde: %v", err)
	}
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	seen := watch(t, ctx, brokers, "test.inventory.watch")

	send(t, ctx, producer, commands, &inventoryv1.HoldSeat{
		HoldId: "hold-service-1", BookingId: "booking-service-1", SeatId: heldSeat,
		ExpiresAt: timestamppb.New(time.Now().Add(15 * time.Minute)),
	})
	await(t, ctx, seen, "sagaflow.inventory.v1.SeatHeld", heldSeat)
	awaitStatus(t, ctx, dsn, heldSeat, inventory.StatusHeld)

	// The crash. Cancelling is the whole of it — no container to restart — and a
	// clean stop must not be reported as a failure.
	if err := kill(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled service must stop cleanly, got %v", err)
	}

	// A second process against the same database and the same topics. It knows
	// nothing except what the first one committed.
	kill = start(t, ctx, cfg)
	defer func() { _ = kill() }()

	send(t, ctx, producer, commands, &inventoryv1.ReleaseSeatHold{
		HoldId: "hold-service-1", BookingId: "booking-service-1", SeatId: heldSeat,
		Reason: "the customer changed their mind",
	})
	await(t, ctx, seen, "sagaflow.inventory.v1.SeatHoldReleased", heldSeat)
	awaitStatus(t, ctx, dsn, heldSeat, inventory.StatusFree)
}
