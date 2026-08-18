// Package integration holds cross-package deliverables — tests that exercise
// several platform packages together rather than any one of them.
//
// The first is spec §13 phase 4: an event committed in one service's transaction
// reaching another service's handler, applied exactly once. It lives here rather
// than in a service package because the services are phases 5–8. Two databases in
// one container, never one database with two schemas: no transaction can span
// them, which is the property being demonstrated.
package integration_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	bookingmigrations "github.com/kptac/sagaflow/internal/booking/migrations"
	inventorymigrations "github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/codec"
	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/platform/envelope"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/inbox"
	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/testsupport/kafkatest"
	"github.com/kptac/sagaflow/internal/testsupport/pgtest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// One Postgres and one Kafka for the whole package (spec §12.4). Both tests below
// stand up their own databases and topics inside them.
func TestMain(m *testing.M) {
	stopPG, err := pgtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	stopKafka, err := kafkatest.Start()
	if err != nil {
		stopPG()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stopKafka()
	stopPG()
	os.Exit(code)
}

const (
	eventsTopic  = "delivery.inventory.events"
	sagaConsumer = "booking.saga"
	source       = "/sagaflow/inventory"
)

func db(t *testing.T, name string, schema fs.FS) *pgxpool.Pool {
	t.Helper()
	return pgtest.Shared(t).Migrated(t, name, schema)
}

// applySeatHeld is the shape every real handler will take (spec §7.2): dedupe,
// load, decide, append — one transaction, one stream.
//
// It reports whether it wrote, so the caller signals only work that survived the
// commit. Signalling from inside the transaction would announce work a failed
// commit then threw away.
func applySeatHeld(ctx context.Context, pool *pgxpool.Pool, env envelope.Envelope, streamID string) (wrote bool, err error) {
	err = pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		wrote = false
		fresh, err := inbox.MarkConsumed(ctx, tx, sagaConsumer, env.Source, env.ID)
		if err != nil || !fresh {
			return err // not fresh: already applied, commit nothing, ack
		}
		existing, err := eventstore.Load(ctx, tx, streamID)
		if err != nil {
			return err
		}
		ev, err := codec.Encode(&inventoryv1.SeatHeld{HoldId: env.ID, SeatId: env.Subject},
			eventstore.Meta{CorrelationID: env.CorrelationID, CausationID: env.ID})
		if err != nil {
			return err
		}
		if err := eventstore.Append(ctx, tx, streamID, len(existing), []eventstore.Event{ev}); err != nil {
			return err
		}
		wrote = true
		return nil
	})
	return wrote, err
}

// bookingHandler applies one event to bookingDB and signals applied after the
// commit.
func bookingHandler(pool *pgxpool.Pool, applied chan<- string, streamID func(envelope.Envelope) string) kafka.Handler {
	return func(ctx context.Context, r kafka.Record) error {
		env, err := envelope.Parse(r.Headers)
		if err != nil {
			// An unparseable envelope will never parse on redelivery.
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		wrote, err := applySeatHeld(ctx, pool, env, streamID(env))
		if err != nil {
			return err
		}
		if wrote {
			applied <- env.ID
		}
		return nil
	}
}

func TestEventCrossesServicesExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	inventoryDB := db(t, "delivery_inventory", inventorymigrations.FS)
	bookingDB := db(t, "delivery_booking", bookingmigrations.FS)

	brokers := kafkatest.Shared(t).Brokers()
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, eventsTopic, eventsTopic+".dlq"); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// --- inventory side: one transaction writes the stream and the outbox row ---
	const seat = "seat-BA117-2026-09-01-14A"
	ceID := envelope.NewID()

	held := &inventoryv1.SeatHeld{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    seat,
		ExpiresAt: timestamppb.New(time.Now().Add(15 * time.Minute)),
	}
	storedEvent, err := codec.Encode(held, eventstore.Meta{TraceID: "trace-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	env := envelope.Envelope{
		ID: ceID, Source: source, Type: storedEvent.Type, Subject: seat,
		CorrelationID: "saga-booking-1",
	}
	// The wire payload is the stored protojson rather than registry-framed protobuf:
	// framing is exercised in Phase 3b's serde tests, and keeping this test
	// registry-free proves delivery does not depend on the registry.
	wire := storedEvent.Data

	if err := pg.WithTx(ctx, inventoryDB, func(tx pgx.Tx) error {
		if err := eventstore.Append(ctx, tx, seat, 0, []eventstore.Event{storedEvent}); err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, []envelope.Message{{
			Topic: eventsTopic, Key: seat, Payload: wire, Headers: env.Headers(),
		}})
	}); err != nil {
		t.Fatalf("inventory handler: %v", err)
	}

	// --- booking side: consume and apply ---
	applied := make(chan string, 8)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		Group:   sagaConsumer,
		Topics:  []string{eventsTopic},
		Handler: bookingHandler(bookingDB, applied, func(e envelope.Envelope) string {
			return "saga-" + e.Subject
		}),
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	go func() { _ = consumer.Run(ctx) }()

	// --- publish what inventory committed ---
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	n, err := outbox.NewPoller(inventoryDB, producer).Drain(ctx)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 message published, got %d", n)
	}

	select {
	case got := <-applied:
		if got != ceID {
			t.Fatalf("applied the wrong event: want %s, got %s", ceID, got)
		}
	case <-ctx.Done():
		t.Fatal("booking never applied the event")
	}
	assertCount(t, bookingDB, "events WHERE stream_id = 'saga-"+seat+"'", 1)

	// --- duplicate delivery: the same ce_id produced again changes nothing ---
	republish := func(e envelope.Envelope) {
		t.Helper()
		if err := producer.Publish(ctx, []envelope.Message{
			{Topic: eventsTopic, Key: seat, Payload: wire, Headers: e.Headers()},
		}); err != nil {
			t.Fatalf("publish %s: %v", e.ID, err)
		}
	}
	republish(env)

	// No sleeping: send a second, distinct event and wait for it. Once that has been
	// applied, the duplicate ahead of it in the same partition has certainly been
	// processed, because one partition is handled in order.
	second := env
	second.ID = envelope.NewID()
	republish(second)

	for {
		select {
		case got := <-applied:
			if got == ceID {
				t.Fatal("the duplicate was applied a second time — the inbox did not deduplicate")
			}
			if got == second.ID {
				// One append from the first event, one from the second, none from the
				// duplicate.
				assertCount(t, bookingDB, "events WHERE stream_id = 'saga-"+seat+"'", 2)
				assertCount(t, bookingDB, "inbox", 2)
				return
			}
		case <-ctx.Done():
			t.Fatal("second event never applied")
		}
	}
}

// TestRebalanceMidHandlerLosesNothing runs two members over 60 events with the
// first holding every handler open, so the rebalance lands mid-transaction. Every
// event must be applied exactly once: 60 signals, 60 events, 60 inbox rows. Loss
// here is invisible without the assertion — offsets look healthy and the events are
// simply gone.
//
// This is an end-to-end no-loss check, not the regression test for marked offsets.
// franz-go promotes a poll's offsets to committable only at the start of the next
// poll, so a batch that is fully settled before that — which every batch here is —
// commits correctly even under the default autocommit. The guard for that lives in
// kafka's TestUnsettledOffsetSurvivesTheAutocommitTimer, which has to leave a
// record unsettled to see the difference.
func TestRebalanceMidHandlerLosesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()

	bookingDB := db(t, "rebalance_booking", bookingmigrations.FS)

	brokers := kafkatest.Shared(t).Brokers()
	const topic = "delivery.rebalance"
	if err := kafka.EnsureTopics(ctx, brokers, kafka.Partitions, 1, topic); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// One key per event so records spread across partitions and a rebalance actually
	// moves work between members.
	const total = 60
	var batch []envelope.Message
	for i := range total {
		e := envelope.Envelope{
			ID: envelope.NewID(), Source: source, Type: "sagaflow.inventory.v1.SeatHeld",
			Subject: fmt.Sprintf("seat-%03d", i),
		}
		batch = append(batch, envelope.Message{
			Topic: topic, Key: e.Subject, Payload: []byte(`{}`), Headers: e.Headers(),
		})
	}
	if err := producer.Publish(ctx, batch); err != nil {
		t.Fatalf("publish: %v", err)
	}

	applied := make(chan string, total*2)
	// The first member holds each handler open so the rebalance lands mid-transaction.
	member := func(slow bool) *kafka.Consumer {
		t.Helper()
		handler := bookingHandler(bookingDB, applied, func(e envelope.Envelope) string {
			return e.Subject
		})
		c, err := kafka.NewConsumer(kafka.ConsumerConfig{
			Brokers: brokers,
			Group:   "test.rebalance",
			Topics:  []string{topic},
			Handler: func(ctx context.Context, r kafka.Record) error {
				if slow {
					time.Sleep(150 * time.Millisecond)
				}
				return handler(ctx, r)
			},
		})
		if err != nil {
			t.Fatalf("consumer: %v", err)
		}
		t.Cleanup(c.Close)
		go func() { _ = c.Run(ctx) }()
		return c
	}

	member(true)

	// Let the first member get into its handlers, then force a rebalance by adding a
	// second member to the group.
	seen := map[string]bool{}
	seen[<-applied] = true // count it: the inbox will never let it be re-signalled
	member(false)

	for len(seen) < total {
		select {
		case id := <-applied:
			seen[id] = true
		case <-ctx.Done():
			t.Fatalf("only %d/%d events applied — the rebalance lost work", len(seen), total)
		}
	}

	assertCount(t, bookingDB, "events", total)
	assertCount(t, bookingDB, "inbox", total)
}

// assertCount counts rows matching from — a table name, optionally with a WHERE
// clause — and fails unless it is exactly want.
func assertCount(t *testing.T, pool *pgxpool.Pool, from string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM "+from).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", from, err)
	}
	if got != want {
		t.Fatalf("%s: want %d rows, got %d", from, want, got)
	}
}
