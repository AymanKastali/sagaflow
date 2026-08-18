# Phase 5d — Inventory as a Service You Can Start

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `internal/inventory` from a library of decisions into a process:
two Kafka handlers, a `wire.go` that assembles the whole service from three
addresses, and a `cmd/inventory` that runs it — so "the service died mid-saga"
becomes `cancel()` in a test rather than a container restart.

**Architecture:** `consumers.go` adapts Kafka records to the two things this
service already knows how to do — apply a command, re-derive a seat — and
classifies which failures are permanent. `wire.go` builds the four loops that
make up a running service (commands consumer, events consumer, outbox poller,
timer scheduler) and runs them until the context is cancelled or one fails.
`cmd/inventory/main.go` is flags, signals, and those two calls.

**Tech Stack:** Go 1.26.6, franz-go (consumer/producer/admin, `pkg/sr`),
pgx/pgxpool, tern migrations, testcontainers (Postgres, Kafka, Apicurio).

**Spec:** `docs/superpowers/specs/2026-08-17-sagaflow-design.md`
(§7.1 `wire.go` as a hard requirement, §7.2 the one-transaction invariant,
§6.4 projections consume from Kafka, §10.2 inbox and retry policy,
§12.3 crash a service mid-saga by cancelling its context).

## Global Constraints

- Go 1.26.6. Module `github.com/AymanKastali/sagaflow`; `contracts/` is a second module.
- Every package under `internal/`, `cmd/` and `contracts/` carries a chapter:
  `doc.go`, package comment only, the six headings in order, **60–120 lines**
  (`docs/conventions.md` C1–C7, enforced by `internal/docs`).
- `§` may appear only under "Where this comes from" in a `doc.go` (rule D4).
  Nowhere else in Go source, tests included.
- Comment standard: what before why (R2), one line inside a function body (R4),
  functions under about 40 lines (R5), no citations outside `doc.go` (R1).
- Naming: domain terms never abbreviated (N1); short names, short lives (N2);
  `incoming` / `outgoing`, never `env` (N3).
- One transaction writes exactly one stream, plus its outbox rows, its inbox
  row, and any timer it scheduled. Never two streams.
- README must link every `internal/platform/*` package and list every
  `internal/` directory (D5, enforced).
- No `time.Sleep` in an assertion. Wait on a signal, or poll a condition against
  a context deadline.
- Failures come from test data, never from configuration. `Config` carries
  addresses only — no behaviour switches.

---

## Scope

| Concern | This phase | Where instead |
|---|---|---|
| Kafka handlers for the two topics | **yes**, `consumers.go` | — |
| `New`/`Run`/`Close` service assembly | **yes**, `wire.go` | — |
| `cmd/inventory` binary and its chapter | **yes** | — |
| A running service proven end to end | **yes**, `internal/integration` | — |
| HTTP (`POST /bookings`, the 409) | no | phase 8 |
| Saga orchestration, compensation matrix | no | phase 7 |
| OTel wiring, trace continuation | no | phase 9 |
| Schema registration | no | `cmd/schemactl`, already built |

**The phase invariant:** *a service is not its decisions — it is the loops that
keep them running.* Four loops, one cancel, nothing left behind.

---

### Task 1: The two Kafka handlers

**Files:**
- Create: `internal/inventory/consumers.go`
- Create: `internal/inventory/consumers_test.go`

**Interfaces:**
- Consumes: `Handler.Handle(ctx, envelope.Envelope, proto.Message) error`,
  `Projector.Project(ctx, seatID) error`, `kafka.Handler`, `kafka.Record`,
  `kafka.ErrPermanent`, `envelope.Parse`, `ErrUnknownCommand`.
- Produces: `ProjectionConsumer` (const), `Decoder` (interface),
  `Commands(h *Handler, dec Decoder) kafka.Handler`,
  `Projections(p *Projector) kafka.Handler` — all used by Task 2's `wire.go`.

- [ ] **Step 1: Write the failing tests**

Create `internal/inventory/consumers_test.go`. It reuses the package's existing
helpers: `db(t, name)`, `seat`, `hold`, `booking`, `expires`, `holdSeat(holdID)`,
`jsonEncoder{}`, `outboxRows(t, ctx, pool)`.

```go
package inventory_test

import (
	"context"
	"errors"
	"testing"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// jsonDecoder is jsonEncoder's mirror: it reads back what that wrote. The type
// name travels in ce_type rather than in the bytes, so the record's own header
// says which message to unmarshal into — which is what a registry-framed payload
// carries in its schema id.
type jsonDecoder struct{ headers map[string]string }

func (d jsonDecoder) Decode(b []byte) (proto.Message, error) {
	var m proto.Message
	switch d.headers["ce_type"] {
	case "sagaflow.inventory.v1.HoldSeat":
		m = &inventoryv1.HoldSeat{}
	case "sagaflow.inventory.v1.ReleaseSeatHold":
		m = &inventoryv1.ReleaseSeatHold{}
	case "sagaflow.inventory.v1.SeatHeld":
		m = &inventoryv1.SeatHeld{}
	default:
		return nil, errors.New("no schema for " + d.headers["ce_type"])
	}
	return m, protojson.Unmarshal(b, m)
}

// record frames one message the way the wire would: protojson bytes plus the
// CloudEvents headers a producer sets.
func record(t *testing.T, topic string, incoming envelope.Envelope, m proto.Message) kafka.Record {
	t.Helper()
	payload, err := protojson.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	incoming.Type = string(m.ProtoReflect().Descriptor().FullName())
	return kafka.Record{Topic: topic, Key: incoming.Subject, Value: payload, Headers: incoming.Headers()}
}

// commandEnvelope is what booking puts on inventory.commands.
func commandEnvelope() envelope.Envelope {
	return envelope.Envelope{
		ID: envelope.NewID(), Source: "/sagaflow/booking",
		Subject: seat, CorrelationID: "saga-1",
	}
}

func commandsHandler(pool *pgxpool.Pool, headers map[string]string) kafka.Handler {
	return inventory.Commands(inventory.NewHandler(pool, jsonEncoder{}), jsonDecoder{headers: headers})
}

func TestAHoldSeatOffTheWireHoldsTheSeat(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_consume_hold")

	incoming := commandEnvelope()
	r := record(t, inventory.CommandsTopic, incoming, holdSeat(hold))

	if err := commandsHandler(pool, r.Headers)(ctx, r); err != nil {
		t.Fatalf("handle: %v", err)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 1 || rows[0].ceTyp != "sagaflow.inventory.v1.SeatHeld" {
		t.Fatalf("want one SeatHeld queued for publication, got %+v", rows)
	}
}

func TestTheSameCommandDeliveredTwiceHoldsTheSeatOnce(t *testing.T) {
	// The inbox is what makes this true, and it is reached through the handler
	// rather than around it: the record arriving twice is Kafka's ordinary
	// behaviour, not an exceptional case.
	ctx := t.Context()
	pool := db(t, "inventory_consume_twice")

	incoming := commandEnvelope()
	r := record(t, inventory.CommandsTopic, incoming, holdSeat(hold))
	handle := commandsHandler(pool, r.Headers)

	for range 2 {
		if err := handle(ctx, r); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	if rows := outboxRows(t, ctx, pool); len(rows) != 1 {
		t.Fatalf("a redelivered command must produce nothing the second time, got %+v", rows)
	}
}

func TestARecordWithNoCloudEventHeadersIsDeadLetteredImmediately(t *testing.T) {
	// Permanent, not transient: redelivery cannot add headers, so retrying five
	// times would spend the whole budget on a record that was never going to
	// parse — and the partition behind it waits for all of it.
	ctx := t.Context()
	pool := db(t, "inventory_consume_unparseable")

	r := kafka.Record{Topic: inventory.CommandsTopic, Value: []byte(`{}`)}

	err := commandsHandler(pool, nil)(ctx, r)
	if !errors.Is(err, kafka.ErrPermanent) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
}

func TestAMessageThatIsNotACommandIsDeadLetteredImmediately(t *testing.T) {
	// A SeatHeld on inventory.commands is somebody's mistake. It decodes, so the
	// refusal has to come from the decision layer, and it must still be settled
	// rather than retried.
	ctx := t.Context()
	pool := db(t, "inventory_consume_not_a_command")

	incoming := commandEnvelope()
	r := record(t, inventory.CommandsTopic, incoming, &inventoryv1.SeatHeld{
		HoldId: hold, BookingId: booking, SeatId: seat, ExpiresAt: expires,
	})

	err := commandsHandler(pool, r.Headers)(ctx, r)
	if !errors.Is(err, kafka.ErrPermanent) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
	if !errors.Is(err, inventory.ErrUnknownCommand) {
		t.Fatalf("the cause must survive to the dead-letter header, got %v", err)
	}
}

func TestTheProjectionHandlerNeverLooksAtThePayload(t *testing.T) {
	// The claim this whole design rests on: the view is re-derived from the seat's
	// stream, so the message body is not an input. Bytes that decode to nothing at
	// all still bring the view up to date.
	ctx := t.Context()
	pool := db(t, "inventory_project_from_wire")

	incoming := commandEnvelope()
	held := record(t, inventory.CommandsTopic, incoming, holdSeat(hold))
	if err := commandsHandler(pool, held.Headers)(ctx, held); err != nil {
		t.Fatalf("hold: %v", err)
	}

	notification := kafka.Record{
		Topic: inventory.EventsTopic, Key: seat, Value: []byte("not a message at all"),
		Headers: envelope.Envelope{
			ID: envelope.NewID(), Source: inventory.Source,
			Type: "sagaflow.inventory.v1.SeatHeld", Subject: seat,
		}.Headers(),
	}
	if err := inventory.Projections(inventory.NewProjector(pool))(ctx, notification); err != nil {
		t.Fatalf("project: %v", err)
	}

	got, found, err := inventory.LoadAvailability(ctx, pool, seat)
	if err != nil || !found {
		t.Fatalf("no row for %s: found=%v err=%v", seat, found, err)
	}
	if got.Status != inventory.StatusHeld {
		t.Fatalf("the view did not follow the stream: %+v", got)
	}
}

func TestAnEventWithNoSubjectIsDeadLettered(t *testing.T) {
	// ce_subject is the only field this handler reads. Without it there is no
	// seat to re-derive, and no redelivery will supply one.
	ctx := t.Context()
	pool := db(t, "inventory_project_no_subject")

	r := kafka.Record{Topic: inventory.EventsTopic, Headers: envelope.Envelope{
		ID: envelope.NewID(), Source: inventory.Source, Type: "sagaflow.inventory.v1.SeatHeld",
	}.Headers()}

	err := inventory.Projections(inventory.NewProjector(pool))(ctx, r)
	if !errors.Is(err, kafka.ErrPermanent) {
		t.Fatalf("want a permanent failure, got %v", err)
	}
}
```

Add `"github.com/jackc/pgx/v5/pgxpool"` to the import block for `commandsHandler`.

- [ ] **Step 2: Run them and watch them fail**

Run: `go test ./internal/inventory/ -run 'OffTheWire|DeliveredTwice|DeadLettered|NeverLooks|NoSubject' -v`
Expected: build failure — `undefined: inventory.Commands`, `undefined: inventory.Projections`.

- [ ] **Step 3: Write `internal/inventory/consumers.go`**

```go
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"google.golang.org/protobuf/proto"
)

// ProjectionConsumer is the consumer group that keeps seat_availability in step
// with the seat streams.
//
// A second group, on this service's own events, deliberately separate from the
// one applying commands. The two failures are not comparable: a view that falls
// behind shows a customer a seat that is already taken, and a command that falls
// behind leaves a saga waiting forever. Separate groups mean separate offsets, so
// one can stall without stopping the other.
const ProjectionConsumer = "inventory.projection"

// Decoder reads a framed message off the wire.
//
// Encoder's mirror, and an interface for the same reason: framing is
// platform/schema's property, proven in its own tests, so a test of this file
// needs no registry.
type Decoder interface {
	Decode(b []byte) (proto.Message, error)
}

// Commands is the handler for inventory.commands: parse the envelope, decode the
// command, apply it in one transaction.
//
// The three failures it can produce itself are all permanent, and saying so is
// the point. Headers that are not a CloudEvent, bytes that are not a message
// this binary knows, and a message that is not a command will each fail
// identically on every redelivery — so each is dead-lettered on the first
// attempt instead of spending a five-attempt budget, with the rest of that
// partition waiting behind it. Everything the handler itself returns is left
// transient, because a database that is down does come back.
func Commands(h *Handler, dec Decoder) kafka.Handler {
	return func(ctx context.Context, r kafka.Record) error {
		incoming, err := envelope.Parse(r.Headers)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		cmd, err := dec.Decode(r.Value)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		if err := h.Handle(ctx, incoming, cmd); err != nil {
			if errors.Is(err, ErrUnknownCommand) {
				return fmt.Errorf("%w: %w", kafka.ErrPermanent, err)
			}
			return err
		}
		return nil
	}
}

// Projections is the handler for inventory.events: bring the changed seat's row
// up to date.
//
// It never decodes the payload. All it takes from the record is ce_subject — the
// seat — because everything else it needs is in that seat's stream, in the same
// database. So it cannot fail on an event type this binary does not recognise,
// it needs no schema at all, and a message that changed nothing costs one wasted
// read rather than a wrong row.
//
// It takes no inbox row either. An inbox stops a second delivery from applying a
// change twice; re-deriving a seat applies nothing, so there is no second
// application for one to prevent.
func Projections(p *Projector) kafka.Handler {
	return func(ctx context.Context, r kafka.Record) error {
		incoming, err := envelope.Parse(r.Headers)
		if err != nil {
			return fmt.Errorf("%w: %v", kafka.ErrPermanent, err)
		}
		if incoming.Subject == "" {
			return fmt.Errorf("%w: inventory: event %s names no seat in ce_subject",
				kafka.ErrPermanent, incoming.ID)
		}
		return p.Project(ctx, incoming.Subject)
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/inventory/ -count=1`
Expected: PASS.

- [ ] **Step 5: Mutation-check the permanent/transient split**

The classification is the load-bearing part and a passing test proves nothing
unless it can fail. Temporarily change the `ErrUnknownCommand` branch in
`Commands` to `return err`, run
`go test ./internal/inventory/ -run NotACommand -count=1`, and confirm it FAILS.
Restore the file (`git checkout internal/inventory/consumers.go` after copying it
aside, or re-apply the branch) and confirm `git diff` is clean apart from the
intended change.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/consumers.go internal/inventory/consumers_test.go
git commit -m "feat(inventory): kafka handlers for commands and the view"
```

---

### Task 2: `wire.go` — the four loops

**Files:**
- Create: `internal/inventory/wire.go`
- Create: `internal/integration/inventory_service_test.go`
- Modify: `internal/integration/delivery_test.go` (TestMain gains a registry)

**Interfaces:**
- Consumes: Task 1's `Commands`/`Projections`/`ProjectionConsumer`, plus
  `NewHandler`, `NewProjector`, `NewExpirer`, `pg.Migrate`, `pg.Open`,
  `schema.NewTopicSerde`, `kafka.EnsureTopics`, `kafka.NewProducer`,
  `kafka.NewConsumer`, `outbox.NewPoller`, `timers.NewScheduler`.
- Produces: `Config{DSN, Brokers, Registry}`, `New(ctx, Config) (*Service, error)`,
  `(*Service).Run(ctx) error`, `(*Service).Close()` — used by Task 3's `cmd/inventory`.

- [ ] **Step 1: Give the integration package a registry**

`internal/integration/delivery_test.go` holds the package's `TestMain`. Add the
registry beside the two containers already there, releasing in reverse order:

```go
	stopRegistry, err := srtest.Start()
	if err != nil {
		stopKafka()
		stopPG()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stopRegistry()
	stopKafka()
	stopPG()
```

Import `"github.com/AymanKastali/sagaflow/internal/testsupport/srtest"`. Extend
that `TestMain`'s comment: the registry is here because one of this package's
tests starts a whole service, and a service resolves its schema ids at startup.

- [ ] **Step 2: Write the failing service test**

Create `internal/integration/inventory_service_test.go`:

```go
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
// registry — the same command the Makefile documents, rather than a table
// copied out of it. A copy could drift; running the real thing means this test
// fails the day registration stops covering what the service needs.
func registerSchemas(t *testing.T, registry string) {
	t.Helper()
	cmd := exec.Command("go", "run", "./cmd/schemactl", "-registry", registry)
	cmd.Dir = "../.."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("schemactl: %v\n%s", err, out)
	}
}

// start builds the service and runs it in the background, returning a kill
// function that cancels its context — the crash this test is about — and reports
// what Run returned.
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

// watch consumes inventory.events into a channel of envelopes, so a test waits
// for the event it cares about instead of sleeping.
func watch(t *testing.T, ctx context.Context, brokers []string, group string) <-chan envelope.Envelope {
	t.Helper()
	seen := make(chan envelope.Envelope, 32)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers, Group: group, Topics: []string{inventory.EventsTopic},
		Handler: func(_ context.Context, r kafka.Record) error {
			incoming, err := envelope.Parse(r.Headers)
			if err != nil {
				return nil // not ours to judge; the service's own DLQ covers it
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
func await(t *testing.T, ctx context.Context, seen <-chan envelope.Envelope, typ, subject string) envelope.Envelope {
	t.Helper()
	for {
		select {
		case incoming := <-seen:
			if incoming.Type == typ && incoming.Subject == subject {
				return incoming
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
		ID: envelope.NewID(), Source: "/sagaflow/booking",
		Type: string(cmd.ProtoReflect().Descriptor().FullName()),
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
// Polling rather than waiting on a signal, because the view is the one thing in
// this system that is allowed to lag: there is no message announcing "the
// projection caught up", and inventing one would be building the thing this
// design exists to avoid.
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
// seat is held by a process, that process is killed by cancelling its context,
// and a second one picks the flow up from the database it left behind.
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

	// The crash. Cancelling is the whole of it: no container to restart, and Run
	// returns cleanly rather than reporting the shutdown as a failure.
	if err := kill(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled service must stop cleanly, got %v", err)
	}

	// A second process, same database, same topics. It knows nothing except what
	// the first one committed.
	kill = start(t, ctx, cfg)
	defer func() { _ = kill() }()

	send(t, ctx, producer, commands, &inventoryv1.ReleaseSeatHold{
		HoldId: "hold-service-1", BookingId: "booking-service-1", SeatId: heldSeat,
		Reason: "the customer changed their mind",
	})
	await(t, ctx, seen, "sagaflow.inventory.v1.SeatHoldReleased", heldSeat)
	awaitStatus(t, ctx, dsn, heldSeat, inventory.StatusFree)
}
```

- [ ] **Step 3: Run it and watch it fail**

Run: `go test ./internal/integration/ -run Kills -count=1`
Expected: build failure — `undefined: inventory.Config`, `inventory.New`.

- [ ] **Step 4: Write `internal/inventory/wire.go`**

```go
package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory/migrations"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"github.com/AymanKastali/sagaflow/internal/platform/outbox"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/schema"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/sr"
)

// Config is everything inventory needs from outside itself: three addresses.
//
// There are no behaviour switches here on purpose. Every failure this system
// demonstrates is triggered by the data in a message — an unavailable seat, a
// declining card — so a service has no flag that makes it behave differently,
// and no test needs one.
type Config struct {
	DSN      string
	Brokers  []string
	Registry string
}

// replication is the topic replication factor. One, because this repository runs
// a single broker; a real cluster would set it to three and would not take it
// from a service's source file.
const replication int16 = 1

// Service is inventory assembled: a database, a broker connection, and the four
// loops that keep the seats moving.
type Service struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
	commands *kafka.Consumer
	events   *kafka.Consumer
	outbox   *outbox.Poller
	timers   *timers.Scheduler
}

// New builds the service and returns it ready to Run. Everything it can fail at
// — a database it cannot reach, a schema nobody registered, a broker that is not
// there — fails here, before a single message is consumed.
//
// It applies its own migrations, which schemactl deliberately does not do for
// schemas, and the difference is worth stating. A migration touches one database
// that belongs to this service alone, so getting it wrong breaks only the thing
// that ran it. A schema is a contract other services decode against, so
// registering one is somebody's reviewed decision rather than a side effect of a
// process starting.
func New(ctx context.Context, cfg Config) (*Service, error) {
	if err := pg.Migrate(ctx, cfg.DSN, migrations.FS); err != nil {
		return nil, err
	}
	pool, err := pg.Open(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	service := &Service{pool: pool}
	if err := service.connect(ctx, cfg); err != nil {
		service.Close()
		return nil, err
	}
	return service, nil
}

// connect builds everything that speaks to Kafka: the two serdes, the producer,
// and the two consumer groups.
func (s *Service) connect(ctx context.Context, cfg Config) error {
	commandSerde, eventSerde, err := serdes(ctx, cfg.Registry)
	if err != nil {
		return err
	}
	// A service creates its own topic pair and the dead-letter topics beside them:
	// a consumer with nowhere to put a settled failure stops advancing instead.
	if err := kafka.EnsureTopics(ctx, cfg.Brokers, kafka.Partitions, replication,
		CommandsTopic, CommandsTopic+".dlq", EventsTopic, EventsTopic+".dlq"); err != nil {
		return err
	}
	if s.producer, err = kafka.NewProducer(cfg.Brokers); err != nil {
		return err
	}
	if s.commands, err = kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.Brokers, Group: Consumer, Topics: []string{CommandsTopic},
		Handler: Commands(NewHandler(s.pool, eventSerde), commandSerde), DLQ: s.producer,
	}); err != nil {
		return err
	}
	if s.events, err = kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.Brokers, Group: ProjectionConsumer, Topics: []string{EventsTopic},
		Handler: Projections(NewProjector(s.pool)), DLQ: s.producer,
	}); err != nil {
		return err
	}
	s.outbox = outbox.NewPoller(s.pool, s.producer)
	s.timers = timers.NewScheduler(s.pool, NewExpirer(eventSerde))
	return nil
}

// serdes resolves this service's schema ids: commands to decode, events to
// encode.
//
// Both resolve now, at startup, so a subject nobody registered stops the service
// here — visibly, at rollout, pointing at whoever skipped the registration step
// — rather than surfacing as a decode failure against live traffic later.
func serdes(ctx context.Context, registry string) (commands, events *schema.Serde, err error) {
	client, err := sr.NewClient(sr.URLs(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("inventory: schema registry client: %w", err)
	}
	commands, err = schema.NewTopicSerde(ctx, client, CommandsTopic,
		&inventoryv1.HoldSeat{}, &inventoryv1.ReleaseSeatHold{})
	if err != nil {
		return nil, nil, err
	}
	events, err = schema.NewTopicSerde(ctx, client, EventsTopic,
		&inventoryv1.SeatHeld{}, &inventoryv1.SeatHoldReleased{},
		&inventoryv1.SeatHoldExpired{}, &inventoryv1.SeatUnavailable{})
	if err != nil {
		return nil, nil, err
	}
	return commands, events, nil
}

// loop is one of the service's four long-running jobs, with a name for the error
// it might produce.
type loop struct {
	name string
	run  func(context.Context) error
}

// Run starts the four loops and returns when the context is cancelled or one of
// them fails.
//
// Cancelling is the entire shutdown story, and that is the point: "the service
// died in the middle of a saga" becomes a cancel() call in a test rather than a
// container restart, and what the next process finds is exactly what a crash
// would have left — whatever committed, and nothing else.
//
// The first failure cancels the other three. A service running three of its four
// loops is worse than one that stopped: holds would still be taken while nothing
// expired them, and the symptom would be seats that never come back rather than
// a process that exited.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	loops := []loop{
		{"commands consumer", s.commands.Run},
		{"events consumer", s.events.Run},
		{"outbox poller", s.outbox.Run},
		{"timer scheduler", s.timers.Run},
	}
	failures := make(chan error, len(loops))
	var running sync.WaitGroup
	for _, l := range loops {
		running.Add(1)
		go func() {
			defer running.Done()
			if err := l.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				failures <- fmt.Errorf("inventory: %s: %w", l.name, err)
				cancel()
			}
		}()
	}
	running.Wait()

	close(failures)
	return <-failures // nil when nothing failed: reading a closed, empty channel
}

// Close releases what New acquired.
//
// Consumers first: closing one leaves its group and commits the offsets it
// finished, and the producer has to still be open for a dead letter in flight to
// land. The pool goes last, because both of those may still be writing.
func (s *Service) Close() {
	for _, consumer := range []*kafka.Consumer{s.commands, s.events} {
		if consumer != nil {
			consumer.Close()
		}
	}
	if s.producer != nil {
		s.producer.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
}
```

- [ ] **Step 5: Run the service test**

Run: `go test ./internal/integration/ -run Kills -count=1 -timeout 15m`
Expected: PASS. Then the whole package: `go test ./internal/integration/ -count=1 -timeout 15m`.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/wire.go internal/integration/
git commit -m "feat(inventory): wire.go — a service you can start, and kill"
```

---

### Task 3: `cmd/inventory`

**Files:**
- Create: `cmd/inventory/main.go`
- Create: `cmd/inventory/doc.go`
- Modify: `Makefile` (a `run-inventory` target)

**Interfaces:**
- Consumes: `inventory.Config`, `inventory.New`, `(*Service).Run`, `(*Service).Close`.

- [ ] **Step 1: Write `cmd/inventory/main.go`**

Defaults match `docker-compose.yml`: the inventory database is published on
5434, Kafka on 9092, Apicurio's Confluent-compatible API under
`/apis/ccompat/v7` on 8080.

```go
// Command inventory runs the inventory service: seat holds, their deadlines,
// and the availability view.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/AymanKastali/sagaflow/internal/inventory"
)

func main() {
	var cfg inventory.Config
	flag.StringVar(&cfg.DSN, "dsn",
		"postgres://sagaflow:sagaflow@localhost:5434/inventory?sslmode=disable",
		"Postgres connection string for inventory's own database")
	brokers := flag.String("brokers", "localhost:9092",
		"comma-separated Kafka bootstrap servers")
	flag.StringVar(&cfg.Registry, "registry", "http://localhost:8080/apis/ccompat/v7",
		"schema registry ccompat base URL — must include the /apis/ccompat/v7 path")
	flag.Parse()
	cfg.Brokers = strings.Split(*brokers, ",")

	// SIGINT and SIGTERM cancel the context rather than killing the process, so
	// stopping this binary takes the same code path a test takes with cancel().
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		slog.Error("inventory stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg inventory.Config) error {
	service, err := inventory.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer service.Close()
	slog.Info("inventory running",
		"commands", inventory.CommandsTopic, "events", inventory.EventsTopic)
	return service.Run(ctx)
}
```

- [ ] **Step 2: Write `cmd/inventory/doc.go`**

The chapter. Six headings in order, 60–120 comment lines, package comment only.
Its subject is *what turns a package of decisions into a process*: the problem
(nothing polls the outbox, nothing fires a timer, nothing consumes — and there is
no way to crash it in a test); the obvious fixes that fail (assembling the
service inside `main` makes the assembly unreachable from a test, so "crash
mid-saga" becomes a container restart; a test-only harness assembles a different
service from the one that ships); what it does (three flags, `inventory.New`,
`Run`, signals cancel the context); what it deliberately does not do (no HTTP, no
schema registration — that is schemactl's job — no saga, no topology decisions
beyond its own topic pair); reading order (`main.go`); and the citations.

- [ ] **Step 3: Add a Makefile target**

```make
run-inventory:
	go run ./cmd/inventory
```

Add `run-inventory` to the `.PHONY` line.

- [ ] **Step 4: Verify**

Run: `make lint && go test ./internal/docs/ -count=1`
Expected: lint OK; the chapter checks pass for the new `cmd/inventory` package.

Then, against a running stack (`make up && make schemas-register`), start it once
by hand and stop it with Ctrl-C to confirm it logs `inventory running` and exits 0.

- [ ] **Step 5: Commit**

```bash
git add cmd/inventory Makefile
git commit -m "feat(inventory): cmd/inventory, and make run-inventory"
```

---

### Task 4: The chapter, the map, and the diagram

**Files:**
- Modify: `internal/inventory/doc.go`
- Modify: `README.md`
- Modify: `docs/architecture.md`

- [ ] **Step 1: Correct the inventory chapter**

Two claims in it stop being true this phase, and one section needs an addition.
The chapter is currently at exactly 120 lines — the C7 ceiling — so every line
added has to come out of another paragraph.

1. "This package does not know Kafka exists" is now false: `consumers.go` and
   `wire.go` both import it. Replace it with the sharper, still-true statement:
   *the decision path* stops at the outbox row — `seat.go`, `store.go`,
   `commands.go`, `expiry.go` and `projection.go` never mention a broker — and
   the two files that do speak to Kafka contain no decisions, which is what keeps
   the rest testable against nothing but Postgres.
2. "What this package does" gains a sentence: the same package also assembles
   itself into a service — two consumer groups, an outbox poller and a timer
   scheduler — that stops when its context is cancelled.
3. Reading order gains two entries, after `projection.go`:

```
//	consumers.go   The two Kafka handlers: which failures are permanent, and why
//	               the projection one never decodes a payload.
//	wire.go        New, Run, Close — the four loops. Last: it only assembles
//	               what every file above already does.
```

Confirm the count afterwards:
`awk '/^\/\//' internal/inventory/doc.go | wc -l` must be between 60 and 120.

- [ ] **Step 2: Update README.md**

- Status table: 5d becomes **built**, with `internal/inventory/wire.go`,
  `cmd/inventory` in the *Where* column.
- The paragraph under the topology diagram: inventory is now a service you can
  start, not only a package.
- The paragraph under the status table: what is still missing is the saga and the
  HTTP entry point, not the ability to run anything.
- Reading order: a twelfth item — `internal/inventory/consumers.go` and
  `wire.go`, the four loops a service is made of.
- Repository map: the `cmd/` entry lists `inventory` beside `schemactl`.
- Running it: `make run-inventory`, after `make up` and `make schemas-register`.

- [ ] **Step 3: Add "What a service is made of" to docs/architecture.md**

A short section — this spans packages, so it belongs here rather than in any
chapter — with a diagram of the four loops around one database, and two
sentences on why the outbox poller and the timer scheduler are loops rather than
things the request path does. Place it after the delivery-path section and
before the saga.

- [ ] **Step 4: Verify everything**

```bash
make lint
go test -short ./...
go test ./internal/inventory/ ./internal/docs/ -count=1
```

- [ ] **Step 5: Commit**

```bash
git add internal/inventory/doc.go README.md docs/architecture.md
git commit -m "docs(inventory): the four loops, and what the chapter can no longer claim"
```

---

## Self-review notes

- **Spec coverage:** §7.1's `wire.go` requirement (Task 2), its `consumers.go`
  slot (Task 1), `cmd/<service>/main.go` at ~20 lines (Task 3); §6.4's
  "projections consume from Kafka using committed consumer-group offsets"
  (Task 1, `ProjectionConsumer`); §12.3's mid-flow crash as a `cancel()`
  (Task 2's test); §10.2's permanent-versus-transient split (Task 1).
- **Deliberately not covered:** the phase-5 end state in §13 — "two concurrent
  holds producing one 409" — needs the HTTP layer, which is phase 8. The
  concurrency itself is already proven in `TestAppendAtAStaleVersionConflicts`.
- **Type consistency:** `Commands`/`Projections` return `kafka.Handler` and are
  consumed only by `connect`; `Config` has exactly the three fields `cmd` sets;
  `Decoder` mirrors the existing `Encoder`.
