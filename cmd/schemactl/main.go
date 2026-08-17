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
