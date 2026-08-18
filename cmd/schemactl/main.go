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

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/platform/schema"
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
	{"inventory.commands", "proto/sagaflow/inventory/v1/hold_seat.proto", &inventoryv1.HoldSeat{}},
	{"inventory.commands", "proto/sagaflow/inventory/v1/release_seat_hold.proto", &inventoryv1.ReleaseSeatHold{}},
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_held.proto", &inventoryv1.SeatHeld{}},
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_hold_released.proto", &inventoryv1.SeatHoldReleased{}},
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_unavailable.proto", &inventoryv1.SeatUnavailable{}},
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
	if err := schema.EnsureBackwardCompatibility(ctx, cl); err != nil {
		return err
	}
	slog.Info("compatibility pinned", "level", "BACKWARD", "scope", "global")

	for _, b := range bindings {
		if err := register(ctx, cl, b); err != nil {
			return err
		}
	}
	return nil
}

// register uploads one binding's .proto under its TopicRecordNameStrategy subject.
func register(ctx context.Context, cl *sr.Client, b binding) error {
	text, err := os.ReadFile(b.file)
	if err != nil {
		return fmt.Errorf("read %s: %w", b.file, err)
	}
	subject := schema.Subject(b.topic, string(b.msg.ProtoReflect().Descriptor().FullName()))

	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: string(text),
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return fmt.Errorf("register %s: %w", subject, err)
	}
	slog.Info("registered", "subject", subject, "id", ss.ID, "version", ss.Version)
	return nil
}
