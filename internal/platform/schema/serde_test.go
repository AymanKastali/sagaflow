package schema_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/platform/schema"
	"github.com/kptac/sagaflow/internal/testsupport/srtest"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// One registry for the whole package (spec §12.4). Starting one per test would
// dominate the suite's runtime.
//
// No broker: framing is about schema ids and message bytes, so nothing here needs
// one. That is the split platform/kafka and platform/schema exist to make.
func TestMain(m *testing.M) {
	stop, err := srtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

const (
	topic = "inventory.events"
	// latestVersion is the registry's selector for "whatever is current".
	latestVersion = -1
)

// assertConfluentFraming checks a framed payload's header byte by byte, rather than
// trusting the round trip.
//
// Encode and Decode are symmetric, so a serde that omitted the protobuf
// message-index array entirely would still round-trip through itself while emitting
// payloads no Confluent or Java consumer could read. That index is the whole
// difference between Confluent *protobuf* framing and protobuf bytes behind an
// Avro-shaped header, so it has to be checked against the wire.
func assertConfluentFraming(t *testing.T, framed []byte, wantID int, want proto.Message) {
	t.Helper()
	const headerLen = 6 // magic byte, 4-byte schema id, one-byte index path

	if len(framed) < headerLen {
		t.Fatalf("framed payload too short: %d bytes", len(framed))
	}
	if framed[0] != 0x00 {
		t.Fatalf("want magic byte 0x00, got 0x%02x", framed[0])
	}
	if id := int(binary.BigEndian.Uint32(framed[1:5])); id != wantID {
		t.Fatalf("framed schema id %d is not the registered id %d", id, wantID)
	}
	// One top-level message per .proto file means the index path is [0], which the
	// Confluent format shortens to a single zero byte.
	if framed[5] != 0x00 {
		t.Fatalf("want the [0] message-index shortcut at byte 5, got 0x%02x", framed[5])
	}

	// Everything after the header must be the bare protobuf encoding.
	bare := want.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(framed[headerLen:], bare); err != nil {
		t.Fatalf("payload after the header is not protobuf: %v", err)
	}
	if !proto.Equal(want, bare) {
		t.Fatalf("payload after the header lost data:\n want %v\n got  %v", want, bare)
	}
}

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
	subject := schema.Subject(topic, string(msg.ProtoReflect().Descriptor().FullName()))
	if _, err := cl.CreateSchema(context.Background(), subject, sr.Schema{
		Schema: string(text),
		Type:   sr.TypeProtobuf,
	}); err != nil {
		t.Fatalf("register %s: %v", subject, err)
	}
}

func TestSubjectUsesTopicRecordNameStrategy(t *testing.T) {
	got := schema.Subject("inventory.events", "sagaflow.inventory.v1.SeatHeld")
	const want = "inventory.events-sagaflow.inventory.v1.SeatHeld"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestSerdeRoundTripsThroughConfluentFraming(t *testing.T) {
	ctx := t.Context()
	cl := client(t, srtest.Shared(t).URL())
	register(t, cl, topic, "../../../proto/sagaflow/inventory/v1/events.proto", &inventoryv1.SeatHeld{})

	s, err := schema.NewTopicSerde(ctx, cl, topic, &inventoryv1.SeatHeld{})
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
	ss, err := cl.SchemaByVersion(ctx, schema.Subject(topic, "sagaflow.inventory.v1.SeatHeld"), latestVersion)
	if err != nil {
		t.Fatalf("look up the registered id: %v", err)
	}
	assertConfluentFraming(t, b, ss.ID, want)

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
	ctx := t.Context()
	cl := client(t, srtest.Shared(t).URL())
	const file = "../../../proto/sagaflow/inventory/v1/events.proto"
	register(t, cl, topic, file, &inventoryv1.SeatHeld{})

	if err := schema.EnsureBackwardCompatibility(ctx, cl); err != nil {
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

	subject := schema.Subject(topic, "sagaflow.inventory.v1.SeatHeld")
	if _, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: incompatible,
		Type:   sr.TypeProtobuf,
	}); err == nil {
		t.Fatal("the registry accepted a field type change on an existing subject — " +
			"BACKWARD compatibility is not being enforced")
	}
}

func TestNewTopicSerdeFailsClosedOnUnregisteredSubject(t *testing.T) {
	ctx := t.Context()
	cl := client(t, srtest.Shared(t).URL())

	// A topic no test registers anything for. Under TopicRecordNameStrategy the
	// subject is derived from the topic, so this is an unregistered subject on a
	// registry that is otherwise healthy and populated — which is the situation a
	// misconfigured service actually meets.
	_, err := schema.NewTopicSerde(ctx, cl, "inventory.events.typo", &inventoryv1.SeatHeld{})
	if !errors.Is(err, schema.ErrSubjectNotRegistered) {
		t.Fatalf("want ErrSubjectNotRegistered, got %v", err)
	}
}
