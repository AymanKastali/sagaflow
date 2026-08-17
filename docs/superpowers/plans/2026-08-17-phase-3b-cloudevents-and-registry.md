# SagaFlow Phase 3b — CloudEvents Envelope and Registry Framing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every message a CloudEvents identity carried in Kafka headers, and put protobuf payloads on the wire in Confluent framing against schemas that only CI may register.

**Architecture:** Two independent halves. The envelope is pure data — a struct that maps to and from a `map[string]string` of Kafka headers with `ce_` prefixes, testable with no infrastructure at all, and it is where `ce_source` + `ce_id` becomes the inbox deduplication key. The serde is the opposite: it holds a schema-registry client, resolves each subject's latest schema id at construction time, and fails closed when a subject is unregistered — which is how spec D14's "services never auto-register" is enforced in code rather than in a runbook.

**Tech Stack:** Go 1.26.6, franz-go/pkg/sr v1.8.0, Protobuf v1.36.12, Apicurio Registry 3.3.1, google/uuid v1.6.0, testcontainers-go v0.44.0.

**Spec:** [docs/superpowers/specs/2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) — §8.1 (envelope), §8.3 (compatibility enforcement), §8.5 (registry), §13 phase 3

**Plan sequence:** this is plan 4 of 6. See [README.md](README.md). **Depends on Phase 3a** (generated types, `codec.TypeName`). Phase 4b depends on this one for the producer and consumer serde.

**Deliverable that ends this phase:** `make schemas-register` pins the registry to `BACKWARD` and registers both subjects with a real Apicurio, a serde built afterwards encodes and decodes a `SeatHeld` through Confluent framing, a serde built against an *unregistered* subject fails to construct rather than silently defining the contract, and an incompatible schema change is rejected by the registry rather than accepted.

## Global Constraints

Copied verbatim from spec §5 and §3. Every task's requirements implicitly include this section.

- **Go 1.26.6.** Module path `github.com/kptac/sagaflow`.
- **Pinned:** franz-go/pkg/sr v1.8.0, protobuf v1.36.12, google/uuid v1.6.0, `apicurio/apicurio-registry:3.3.1`. **Add no dependency not listed in spec §5.**
- **The registry URL is path-scoped** (spec §8.5): `http://host:8080/apis/ccompat/v7`, never the bare host. Pointed at the root, every call 404s.
- **Subject naming is `TopicRecordNameStrategy`** (spec §8.3): `<topic>-<fully.qualified.MessageName>`. The default `TopicNameStrategy` permits one schema per topic and would break on the second event type.
- **Services never auto-register schemas** (spec D14). Registration is `make schemas-register` only. A service that meets an unregistered subject must fail to start.
- **`ce_source` + `ce_id` is the deduplication key** (spec §8.1). CloudEvents specifies that pair as unique, so the inbox does not invent its own identity scheme.
- **`ce_id` is UUIDv7, generated in Go** (spec §8.1), never by the database — the enqueuing code needs the value before it commits.
- **Binary content mode only** (spec §8.1): attributes travel as headers prefixed `ce_`, the payload is the message body, and `content-type` carries the CloudEvents `datacontenttype`.
- **The Kafka record key is the target stream id** (spec §8.1/§9.1). It is what gives per-stream ordering, the only ordering guarantee the system relies on.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/platform/envelope/envelope.go` | `Envelope`, `Headers()`, `Parse()`, `NewID()` |
| `internal/platform/envelope/envelope_test.go` | Round-trip, missing attributes, prefix correctness |
| `internal/platform/srtest/srtest.go` | `apicurio/apicurio-registry:3.3.1` container for tests |
| `internal/platform/kafka/serde.go` | `Subject`, `NewTopicSerde`, Confluent framing over protobuf |
| `internal/platform/kafka/compat.go` | `EnsureBackwardCompatibility` — layer two of spec §8.3 |
| `internal/platform/kafka/serde_test.go` | Encode/decode round trip, framing bytes, BACKWARD enforcement, fail-closed on unregistered subject |
| `cmd/schemactl/main.go` | Registers `.proto` schemas. The only writer to the registry |

One serde per topic, not one global serde. Under `TopicRecordNameStrategy` the same message type on two topics has two subjects and two schema ids, and franz-go's `sr.Serde` keys its registrations by Go type — so a single serde holding both would be ambiguous. Per-topic also matches how producers and consumers are already scoped.

---

## Phase 3b Tasks

### Task 1: `platform/envelope` — CloudEvents in Kafka headers

**Files:**
- Create: `internal/platform/envelope/envelope.go`, `internal/platform/envelope/envelope_test.go`

**Interfaces:**
- Consumes: `github.com/google/uuid`.
- Produces:
  - `type Envelope struct { ID, Source, Type, Subject, CorrelationID, CausationID, TraceParent string }`
  - `func (e Envelope) Headers() map[string]string`
  - `func Parse(h map[string]string) (Envelope, error)`
  - `func NewID() string` — UUIDv7
  - `var ErrMissingAttribute = errors.New(...)`
  - `const SpecVersion = "1.0"`, `const ContentType = "application/protobuf"`

No infrastructure in this task. It is pure data mapping, so the tests are microseconds and can cover every field.

- [ ] **Step 1: Write the failing test**

Create `internal/platform/envelope/envelope_test.go`:

```go
package envelope_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/kptac/sagaflow/internal/platform/envelope"
)

func full() envelope.Envelope {
	return envelope.Envelope{
		ID:            "01920000-0000-7000-8000-000000000001",
		Source:        "/sagaflow/inventory",
		Type:          "sagaflow.inventory.v1.SeatHeld",
		Subject:       "seat-BA117-2026-09-01-14A",
		CorrelationID: "saga-1",
		CausationID:   "01920000-0000-7000-8000-000000000000",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

func TestHeadersUseCloudEventsBinaryModeNames(t *testing.T) {
	h := full().Headers()

	want := map[string]string{
		"ce_specversion":   "1.0",
		"ce_id":            "01920000-0000-7000-8000-000000000001",
		"ce_source":        "/sagaflow/inventory",
		"ce_type":          "sagaflow.inventory.v1.SeatHeld",
		"ce_subject":       "seat-BA117-2026-09-01-14A",
		"ce_correlationid": "saga-1",
		"ce_causationid":   "01920000-0000-7000-8000-000000000000",
		"traceparent":      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"content-type":     "application/protobuf",
	}
	if len(h) != len(want) {
		t.Fatalf("want %d headers, got %d: %v", len(want), len(h), h)
	}
	for k, v := range want {
		if h[k] != v {
			t.Errorf("header %s: want %q, got %q", k, v, h[k])
		}
	}
	// traceparent is a W3C header and must NOT be ce_-prefixed.
	if _, bad := h["ce_traceparent"]; bad {
		t.Error("traceparent must not carry the ce_ prefix")
	}
}

func TestRoundTrip(t *testing.T) {
	want := full()
	got, err := envelope.Parse(want.Headers())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != want {
		t.Fatalf("round trip changed the envelope:\n want %+v\n got  %+v", want, got)
	}
}

func TestOptionalAttributesAreOmittedNotEmpty(t *testing.T) {
	e := envelope.Envelope{
		ID:     "id-1",
		Source: "/sagaflow/inventory",
		Type:   "sagaflow.inventory.v1.SeatHeld",
	}
	h := e.Headers()
	for _, k := range []string{"ce_subject", "ce_correlationid", "ce_causationid", "traceparent"} {
		if _, present := h[k]; present {
			t.Errorf("optional header %s should be absent when unset, got %q", k, h[k])
		}
	}
}

func TestParseRejectsMissingRequiredAttributes(t *testing.T) {
	for _, missing := range []string{"ce_id", "ce_source", "ce_type", "ce_specversion"} {
		h := full().Headers()
		delete(h, missing)
		_, err := envelope.Parse(h)
		if !errors.Is(err, envelope.ErrMissingAttribute) {
			t.Errorf("without %s: want ErrMissingAttribute, got %v", missing, err)
		}
	}
}

func TestParseRejectsUnknownSpecVersion(t *testing.T) {
	h := full().Headers()
	h["ce_specversion"] = "0.3"
	if _, err := envelope.Parse(h); err == nil {
		t.Fatal("want an error for specversion 0.3, got nil")
	}
}

func TestNewIDIsUUIDv7AndSortsByTime(t *testing.T) {
	a := envelope.NewID()
	b := envelope.NewID()

	ua, err := uuid.Parse(a)
	if err != nil {
		t.Fatalf("parse %q: %v", a, err)
	}
	if ua.Version() != 7 {
		t.Fatalf("want UUID version 7, got %d", ua.Version())
	}
	// v7 is time-ordered, which is why it is worth preferring over v4 for an id
	// that lands in a Postgres primary key and a Kafka header.
	if !(a < b) {
		t.Fatalf("want time-ordered ids, got %q then %q", a, b)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/platform/envelope/ -v`
Expected: FAIL to build — `undefined: envelope.Envelope` and friends.

- [ ] **Step 3: Implement `envelope.go`**

```go
// Package envelope maps CloudEvents v1.0.2 attributes to and from Kafka
// headers in binary content mode, per the CNCF Kafka protocol binding.
//
// The binding puts attributes in headers prefixed "ce_" and leaves the payload
// as the message body. traceparent is a W3C header, not a CloudEvents
// attribute, so it is deliberately unprefixed.
package envelope

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const (
	// SpecVersion is the only CloudEvents version this system speaks.
	SpecVersion = "1.0"
	// ContentType is the CloudEvents datacontenttype, carried in the
	// content-type header as the binding requires.
	ContentType = "application/protobuf"
)

// ErrMissingAttribute means a required CloudEvents attribute was absent. It is a
// permanent technical failure: the message can never become valid, so it
// dead-letters without retrying (spec §10.2).
var ErrMissingAttribute = errors.New("envelope: missing required attribute")

// Envelope is the identity of one message.
//
// ID and Source together are specified by CloudEvents to be unique, which is
// exactly the property idempotent consumption needs — so that pair becomes the
// inbox deduplication key rather than something we define (spec §8.1).
type Envelope struct {
	ID            string // ce_id — UUIDv7, generated at outbox enqueue time
	Source        string // ce_source — e.g. /sagaflow/inventory
	Type          string // ce_type — the fully qualified protobuf message name
	Subject       string // ce_subject — the stream id
	CorrelationID string // ce_correlationid — the saga id (extension)
	CausationID   string // ce_causationid — the ce_id that caused this (extension)
	TraceParent   string // traceparent — W3C trace context, no ce_ prefix
}

// NewID returns a UUIDv7 for use as ce_id.
func NewID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// Headers renders the envelope as Kafka headers. Optional attributes are omitted
// entirely rather than written empty, because an empty ce_subject is a different
// statement from an absent one.
func (e Envelope) Headers() map[string]string {
	h := map[string]string{
		"ce_specversion": SpecVersion,
		"ce_id":          e.ID,
		"ce_source":      e.Source,
		"ce_type":        e.Type,
		"content-type":   ContentType,
	}
	set := func(k, v string) {
		if v != "" {
			h[k] = v
		}
	}
	set("ce_subject", e.Subject)
	set("ce_correlationid", e.CorrelationID)
	set("ce_causationid", e.CausationID)
	set("traceparent", e.TraceParent)
	return h
}

// Parse reads an envelope out of Kafka headers, rejecting anything that is not a
// well-formed CloudEvent this system can handle.
func Parse(h map[string]string) (Envelope, error) {
	need := func(k string) (string, error) {
		v, ok := h[k]
		if !ok || v == "" {
			return "", fmt.Errorf("%w: %s", ErrMissingAttribute, k)
		}
		return v, nil
	}

	sv, err := need("ce_specversion")
	if err != nil {
		return Envelope{}, err
	}
	if sv != SpecVersion {
		return Envelope{}, fmt.Errorf("envelope: unsupported ce_specversion %q, want %q", sv, SpecVersion)
	}

	var e Envelope
	for _, f := range []struct {
		key string
		dst *string
	}{
		{"ce_id", &e.ID},
		{"ce_source", &e.Source},
		{"ce_type", &e.Type},
	} {
		v, err := need(f.key)
		if err != nil {
			return Envelope{}, err
		}
		*f.dst = v
	}

	e.Subject = h["ce_subject"]
	e.CorrelationID = h["ce_correlationid"]
	e.CausationID = h["ce_causationid"]
	e.TraceParent = h["traceparent"]
	return e, nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/platform/envelope/ -v`
Expected: all six PASS, in milliseconds — no containers.

- [ ] **Step 5: Confirm the fast suite still covers this**

Run: `make test`
Expected: PASS, with `internal/platform/envelope` running rather than skipping. This package must never need infrastructure.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/envelope
git commit -m "feat(envelope): CloudEvents binary-mode attributes in Kafka headers"
```

---

### Task 2: Schema registration and Confluent wire framing

**Files:**
- Create: `internal/platform/srtest/srtest.go`, `internal/platform/kafka/serde.go`, `internal/platform/kafka/serde_test.go`, `cmd/schemactl/main.go`
- Modify: `Makefile` (the `schemas-register` target already exists from Phase 1; verify it matches)

**Interfaces:**
- Consumes: `codec.TypeName` from Phase 3a; the generated `inventoryv1` types.
- Produces:
  - `func srtest.Start() (stop func(), err error)`, `func srtest.Shared(t *testing.T) *srtest.Registry` and `func (r *Registry) URL() string`
  - `func kafka.Subject(topic, typeName string) string`
  - `func kafka.NewTopicSerde(ctx context.Context, cl *sr.Client, topic string, prototypes ...proto.Message) (*kafka.Serde, error)`
  - `func (s *Serde) Encode(m proto.Message) ([]byte, error)`
  - `func (s *Serde) Decode(b []byte) (proto.Message, error)`
  - `var kafka.ErrSubjectNotRegistered = errors.New(...)`
  - `func kafka.EnsureBackwardCompatibility(ctx context.Context, cl *sr.Client) error`

**One rule this task imposes on every future `.proto` file:** exactly one top-level message per file. The Confluent protobuf format carries a message-index array identifying which message in the file the payload is, and this serde registers index `[0]`. A second top-level message in a file would silently be framed as the first.

- [ ] **Step 1: Write the Apicurio test container**

Create `internal/platform/srtest/srtest.go`:

```go
// Package srtest starts an Apicurio Registry for integration tests.
//
// The URL it returns already includes the Confluent-compatibility path prefix,
// because that is the single most common way to waste an hour here: Apicurio
// serves the Confluent-shaped API under /apis/ccompat/v7, not at the root, and a
// client pointed at the bare host gets 404 on every call (spec §8.5).
package srtest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const Image = "apicurio/apicurio-registry:3.3.1"

type Registry struct{ url string }

// URL is the ccompat base URL, ready to pass to sr.URLs.
func (r *Registry) URL() string { return r.url }

var shared *Registry

// Shared returns the registry Start brought up, skipping the test in -short mode.
func Shared(t *testing.T) *Registry {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	if shared == nil {
		t.Fatal("srtest: no registry running — this package needs a TestMain that calls srtest.Start")
	}
	return shared
}

// Start brings up the package's registry and returns a stop function. Call it
// from TestMain, exactly as with pgtest.Start (spec §12.4).
//
// One registry per package means tests share subjects, so a test that needs an
// *unregistered* subject asks about a topic no other test registers rather than
// starting a second registry.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithEnv(map[string]string{
			// Apicurio 3.x has no "mem" storage kind; sql over in-memory H2 is
			// its ephemeral store. Set explicitly so a registry started for a
			// test cannot quietly pick up persistent storage.
			"APICURIO_STORAGE_KIND":     "sql",
			"APICURIO_STORAGE_SQL_KIND": "h2",
		}),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForHTTP("/apis/registry/v3/system/info").WithPort("8080/tcp")),
	)
	if err != nil {
		return nil, fmt.Errorf("srtest: start %s: %w", Image, err)
	}

	fail := func(format string, args ...any) (func(), error) {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("srtest: "+format, args...)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return fail("host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return fail("mapped port: %w", err)
	}

	shared = &Registry{url: fmt.Sprintf("http://%s:%s/apis/ccompat/v7", host, port.Port())}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "srtest: terminate: %v\n", err)
		}
	}, nil
}
```

- [ ] **Step 2: Write the failing serde test**

Create `internal/platform/kafka/serde_test.go`:

```go
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	inventoryv1 "github.com/kptac/sagaflow/internal/platform/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/kptac/sagaflow/internal/platform/srtest"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// One registry for the package (spec §12.4). Phase 4b adds kafkatest.Start to
// this same function when this package gains a producer and a consumer.
func TestMain(m *testing.M) {
	stopRegistry, err := srtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stopRegistry()
	os.Exit(code)
}

const topic = "inventory.events"

func client(t *testing.T, url string) *sr.Client {
	t.Helper()
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		t.Fatalf("sr client: %v", err)
	}
	return cl
}

// register puts the schema in the registry the way cmd/schemactl does, so the
// test exercises the same subject naming the tool uses.
func register(t *testing.T, cl *sr.Client, topic, protoFile string, msg proto.Message) {
	t.Helper()
	text, err := os.ReadFile(protoFile)
	if err != nil {
		t.Fatalf("read %s: %v", protoFile, err)
	}
	subject := kafka.Subject(topic, string(msg.ProtoReflect().Descriptor().FullName()))
	if _, err := cl.CreateSchema(context.Background(), subject, sr.Schema{
		Schema: string(text),
		Type:   sr.TypeProtobuf,
	}); err != nil {
		t.Fatalf("register %s: %v", subject, err)
	}
}

func TestSubjectUsesTopicRecordNameStrategy(t *testing.T) {
	got := kafka.Subject("inventory.events", "sagaflow.inventory.v1.SeatHeld")
	const want = "inventory.events-sagaflow.inventory.v1.SeatHeld"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestSerdeRoundTripsThroughConfluentFraming(t *testing.T) {
	ctx := context.Background()
	cl := client(t, srtest.Shared(t).URL())
	register(t, cl, topic, "../../../proto/sagaflow/inventory/v1/events.proto", &inventoryv1.SeatHeld{})

	s, err := kafka.NewTopicSerde(ctx, cl, topic, &inventoryv1.SeatHeld{})
	if err != nil {
		t.Fatalf("new serde: %v", err)
	}

	want := &inventoryv1.SeatHeld{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    "seat-BA117-2026-09-01-14A",
		ExpiresAt: timestamppb.New(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)),
	}

	b, err := s.Encode(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Assert the framing byte by byte rather than just the magic byte. Encode and
	// Decode are symmetric, so a serde that omitted the protobuf message-index
	// array entirely would still round-trip through itself here while emitting
	// payloads no Confluent or Java consumer could read. The index is the whole
	// difference between Confluent *protobuf* framing and protobuf bytes behind an
	// Avro-shaped header, so it has to be checked against the wire, not the
	// round trip.
	if len(b) < 6 {
		t.Fatalf("framed payload too short: %d bytes", len(b))
	}
	if b[0] != 0x00 {
		t.Fatalf("want magic byte 0x00, got 0x%02x", b[0])
	}
	id := int(b[1])<<24 | int(b[2])<<16 | int(b[3])<<8 | int(b[4])
	ss, err := cl.SchemaByVersion(ctx, kafka.Subject(topic, "sagaflow.inventory.v1.SeatHeld"), -1)
	if err != nil {
		t.Fatalf("look up the registered id: %v", err)
	}
	if id != ss.ID {
		t.Fatalf("framed schema id %d is not the registered id %d", id, ss.ID)
	}
	// One top-level message per .proto file means the index path is [0], which the
	// Confluent format shortens to a single zero byte.
	if b[5] != 0x00 {
		t.Fatalf("want the [0] message-index shortcut at byte 5, got 0x%02x", b[5])
	}
	// Everything after the header must be the bare protobuf encoding.
	var bare inventoryv1.SeatHeld
	if err := proto.Unmarshal(b[6:], &bare); err != nil {
		t.Fatalf("payload after the header is not protobuf: %v", err)
	}
	if !proto.Equal(want, &bare) {
		t.Fatalf("payload after the header lost data:\n want %v\n got  %v", want, &bare)
	}

	msg, err := s.Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := msg.(*inventoryv1.SeatHeld)
	if !ok {
		t.Fatalf("want *SeatHeld, got %T", msg)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("round trip lost data:\n want %v\n got  %v", want, got)
	}
}

func TestBackwardCompatibilityRejectsAFieldTypeChange(t *testing.T) {
	ctx := context.Background()
	cl := client(t, srtest.Shared(t).URL())
	const file = "../../../proto/sagaflow/inventory/v1/events.proto"
	register(t, cl, topic, file, &inventoryv1.SeatHeld{})

	if err := kafka.EnsureBackwardCompatibility(ctx, cl); err != nil {
		t.Fatalf("ensure backward compatibility: %v", err)
	}

	// The assertion that matters is a rejected registration, not a config value
	// read back. A registry left on its NONE default accepts this change happily,
	// so this is what tells us layer two of spec §8.3 is actually switched on.
	text, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	incompatible := strings.Replace(string(text), "string hold_id = 1;", "int32 hold_id = 1;", 1)
	if incompatible == string(text) {
		t.Fatal("the field this test mutates has been renamed; update the test")
	}

	subject := kafka.Subject(topic, "sagaflow.inventory.v1.SeatHeld")
	if _, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: incompatible,
		Type:   sr.TypeProtobuf,
	}); err == nil {
		t.Fatal("the registry accepted a field type change on an existing subject — " +
			"BACKWARD compatibility is not being enforced")
	}
}

func TestNewTopicSerdeFailsClosedOnUnregisteredSubject(t *testing.T) {
	ctx := context.Background()
	cl := client(t, srtest.Shared(t).URL())

	// A topic no test registers anything for. Under TopicRecordNameStrategy the
	// subject is derived from the topic, so this is an unregistered subject on a
	// registry that is otherwise healthy and populated — which is the situation a
	// misconfigured service actually meets.
	_, err := kafka.NewTopicSerde(ctx, cl, "inventory.events.typo", &inventoryv1.SeatHeld{})
	if !errors.Is(err, kafka.ErrSubjectNotRegistered) {
		t.Fatalf("want ErrSubjectNotRegistered, got %v", err)
	}
}
```

`TestNewTopicSerdeFailsClosedOnUnregisteredSubject` is the executable form of spec D14. If a serde could construct itself against a missing subject, a service would define the contract by accident on first produce, and the registry would record whatever shipped rather than what was reviewed.

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/platform/kafka/ -run TestSubject -v`
Expected: FAIL to build — `undefined: kafka.Subject`.

- [ ] **Step 4: Implement `serde.go`**

```go
// Package kafka holds the franz-go plumbing: wire framing against the schema
// registry, an acks=all producer, and a consumer whose offsets are committed
// only after the handler's transaction commits.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
)

// ErrSubjectNotRegistered means a schema this service needs is absent from the
// registry. Services never auto-register (spec D14), so this is fatal at
// startup rather than something to paper over at produce time.
var ErrSubjectNotRegistered = errors.New("kafka: subject not registered")

// Subject implements TopicRecordNameStrategy: <topic>-<fully.qualified.Name>.
//
// The default TopicNameStrategy allows only one schema per topic, and our topics
// carry several event types, so it would break on the second one (spec §8.3).
func Subject(topic, typeName string) string {
	return topic + "-" + typeName
}

// Serde encodes and decodes one topic's messages in the Confluent wire format:
// magic byte 0x00, big-endian schema id, protobuf message index, payload.
type Serde struct {
	inner *sr.Serde
	topic string
}

// NewTopicSerde resolves each prototype's schema id from the registry and builds
// a serde for that topic.
//
// Ids are resolved once, at construction, so the steady-state encode path makes
// no network call and a registry outage cannot stall publishing — it can only
// prevent a restart, which is the failure mode you want.
func NewTopicSerde(ctx context.Context, cl *sr.Client, topic string, prototypes ...proto.Message) (*Serde, error) {
	if len(prototypes) == 0 {
		return nil, errors.New("kafka: NewTopicSerde needs at least one prototype")
	}
	inner := sr.NewSerde(sr.Header(&sr.ConfluentHeader{}))

	for _, p := range prototypes {
		name := string(p.ProtoReflect().Descriptor().FullName())
		subject := Subject(topic, name)

		ss, err := cl.SchemaByVersion(ctx, subject, -1) // -1 is "latest"
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrSubjectNotRegistered, subject, err)
		}

		// Index [0] identifies the first top-level message in the .proto file,
		// which is why every file in this project holds exactly one.
		prototype := p
		inner.Register(ss.ID, prototype,
			sr.Index(0),
			sr.EncodeFn(func(v any) ([]byte, error) {
				return proto.Marshal(v.(proto.Message))
			}),
			sr.DecodeFn(func(b []byte, v any) error {
				return proto.Unmarshal(b, v.(proto.Message))
			}),
			sr.GenerateFn(func() any {
				return prototype.ProtoReflect().New().Interface()
			}),
		)
	}
	return &Serde{inner: inner, topic: topic}, nil
}

// Encode frames a message for the wire.
func (s *Serde) Encode(m proto.Message) ([]byte, error) {
	b, err := s.inner.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("kafka: encode %s for %s: %w",
			m.ProtoReflect().Descriptor().FullName(), s.topic, err)
	}
	return b, nil
}

// Decode reads a framed payload back into a fresh message of the registered type.
func (s *Serde) Decode(b []byte) (proto.Message, error) {
	v, err := s.inner.DecodeNew(b)
	if err != nil {
		return nil, fmt.Errorf("kafka: decode from %s: %w", s.topic, err)
	}
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("kafka: decoded %T is not a proto.Message", v)
	}
	return m, nil
}
```

- [ ] **Step 5: Implement `compat.go`**

Spec §8.3 asks for **three** layers of compatibility enforcement, and the middle one is the registry
rejecting an incompatible schema at registration time. A registry defaults to `NONE`, so without this
step that layer is silently absent: `buf breaking` and fail-closed produce still work, but an
incompatible schema that reaches `make schemas-register` is accepted.

Create `internal/platform/kafka/compat.go`:

```go
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// EnsureBackwardCompatibility pins the registry's global compatibility level to
// BACKWARD. It is the second of the three layers spec §8.3 asks for, sitting
// between `buf breaking` in CI and a producer that fails closed on a missing
// schema id.
//
// The level is global rather than per-subject for two reasons. Subjects added in
// later phases inherit it with no extra call, and setting a per-subject override
// requires the subject to exist — which it does not on a registry's first run.
// Enforcement is inherited: a subject with no override of its own is still
// checked against the global level.
//
// This is deliberately weaker than it sounds, and §8.3 is explicit about why:
// the registry checks a schema when it is *registered*, not when a message is
// produced. Nothing in an open-source Kafka can stop a determined producer from
// publishing arbitrary bytes, so this is defence against mistakes, not bypass.
func EnsureBackwardCompatibility(ctx context.Context, cl *sr.Client) error {
	// With no subjects, SetCompatibility and Compatibility both address the global
	// config, which is exactly what is wanted here.
	for _, res := range cl.SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatBackward}) {
		if res.Err != nil {
			return fmt.Errorf("kafka: set global BACKWARD compatibility: %w", res.Err)
		}
	}

	// Read the level back instead of trusting what SetCompatibility reports.
	// SetCompatibility unmarshals the registry's response *over the value it sent*,
	// so a response that omitted the field would still echo BACKWARD — the check
	// would pass without observing anything. The GET decodes a different JSON field
	// name, which makes it an independent observation.
	//
	// The read is global on purpose: Apicurio answers GET /config/{subject} for a
	// subject with no override with NONE rather than the inherited level, so a
	// per-subject read-back would report failure while enforcement was in fact
	// active.
	for _, res := range cl.Compatibility(ctx) {
		if res.Err != nil {
			return fmt.Errorf("kafka: read back global compatibility: %w", res.Err)
		}
		if res.Level != sr.CompatBackward {
			return fmt.Errorf("kafka: global compatibility is %s, want BACKWARD", res.Level)
		}
	}
	return nil
}
```

- [ ] **Step 6: Run the serde tests**

Run: `go test ./internal/platform/kafka/ -race -v -timeout 10m`
Expected: all four PASS.

If registration fails with a reference error mentioning `google/protobuf/timestamp.proto`, the registry did not resolve the well-known import implicitly. Fix it by passing the import as a `sr.SchemaReference` in the `register` helper and in `schemactl`; do not remove the `Timestamp` field. (Verified against Apicurio 3.3.1: the well-known import *is* resolved implicitly, so this fallback was not needed.)

Prove both new guards have teeth rather than trusting that they passed — each of them would pass for the
wrong reason under a plausible implementation:

- Delete `sr.Index(0)` from `serde.go` and rerun. Expected: FAIL at `want the [0] message-index shortcut
  at byte 5, got 0x0a` (0x0a is protobuf's field-1 tag, i.e. the payload started with no index byte).
- Delete the `EnsureBackwardCompatibility` call from the compatibility test and rerun. Expected: FAIL at
  `the registry accepted a field type change`.

Restore both afterwards.

- [ ] **Step 7: Implement `cmd/schemactl/main.go`**

```go
// Command schemactl registers .proto schemas with the schema registry.
//
// It is the only writer to the registry (spec D14). Services resolve ids and
// fail closed when a subject is missing, so registration is a reviewed,
// explicit step rather than a side effect of the first produce.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	inventoryv1 "github.com/kptac/sagaflow/internal/platform/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/platform/kafka"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
)

// binding ties one message type on one topic to the .proto file that defines it.
// Adding an event type means adding a row here; there is deliberately no
// reflection-driven discovery, because what gets registered should be reviewable
// as a diff.
type binding struct {
	topic string
	file  string
	msg   proto.Message
}

var bindings = []binding{
	{"inventory.events", "proto/sagaflow/inventory/v1/events.proto", &inventoryv1.SeatHeld{}},
	{"inventory.commands", "proto/sagaflow/inventory/v1/commands.proto", &inventoryv1.HoldSeat{}},
}

func main() {
	registry := flag.String("registry", "http://localhost:8080/apis/ccompat/v7",
		"schema registry ccompat base URL — must include the /apis/ccompat/v7 path")
	flag.Parse()

	if err := run(context.Background(), *registry); err != nil {
		slog.Error("schema registration failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, registry string) error {
	cl, err := sr.NewClient(sr.URLs(registry))
	if err != nil {
		return fmt.Errorf("sr client: %w", err)
	}

	// Pin compatibility before registering anything, so an incompatible change in
	// this very run is rejected rather than accepted and enforced only next time.
	// A registry defaults to NONE, which would quietly drop one of the three
	// layers spec §8.3 asks for.
	if err := kafka.EnsureBackwardCompatibility(ctx, cl); err != nil {
		return err
	}
	slog.Info("compatibility pinned", "level", "BACKWARD", "scope", "global")

	for _, b := range bindings {
		text, err := os.ReadFile(b.file)
		if err != nil {
			return fmt.Errorf("read %s: %w", b.file, err)
		}
		name := string(b.msg.ProtoReflect().Descriptor().FullName())
		subject := kafka.Subject(b.topic, name)

		ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
			Schema: string(text),
			Type:   sr.TypeProtobuf,
		})
		if err != nil {
			return fmt.Errorf("register %s: %w", subject, err)
		}
		slog.Info("registered", "subject", subject, "id", ss.ID, "version", ss.Version)
	}
	return nil
}
```

`CreateSchema` is idempotent for identical content: re-registering the same schema returns the existing id and version rather than creating a new one, so `make schemas-register` is safe to run repeatedly.

- [ ] **Step 8: Register against the Compose registry end to end**

```bash
make up
curl -s http://localhost:8080/apis/ccompat/v7/config   # {"compatibilityLevel":"NONE"} on a fresh registry
make schemas-register
curl -s http://localhost:8080/apis/ccompat/v7/config   # now BACKWARD
curl -s http://localhost:8080/apis/ccompat/v7/subjects | tr ',' '\n'
```

Expected: a `compatibility pinned` line, then two registration lines with ids, and the subjects list containing `inventory.events-sagaflow.inventory.v1.SeatHeld` and `inventory.commands-sagaflow.inventory.v1.HoldSeat`.

- [ ] **Step 9: Verify re-registration is a no-op**

```bash
make schemas-register
```

Expected: the same ids and versions as Step 8. A bumped version means the `.proto` text changed between runs, or `CreateSchema` is being handed differently-formatted content — check for a trailing-newline difference.

- [ ] **Step 10: Commit**

```bash
make down
git add internal/platform/srtest internal/platform/kafka cmd/schemactl
git commit -m "feat(kafka): Confluent framing over registry-resolved ids, plus schemactl"
```

---
