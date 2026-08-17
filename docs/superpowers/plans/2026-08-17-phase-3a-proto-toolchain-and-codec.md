# SagaFlow Phase 3a — Proto Toolchain and Event Codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `.proto` files the single source of truth for event payloads, and build the codec that turns a generated protobuf message into a stored event and back.

**Architecture:** One `.proto` source, two encodings (spec §8.4). `buf generate` produces Go types; the codec marshals them with `protojson` for the `events.data` column and looks types back up through `protoregistry` by their fully qualified name — which is also the `ce_type` header value, so one string identifies an event everywhere in the system. Protobuf-on-the-wire framing is Phase 3b's job; this phase deliberately stops at storage so the event store never depends on the schema registry being reachable.

**Tech Stack:** Go 1.26.6, Protobuf v1.36.12, buf 1.72.0.

**Spec:** [docs/superpowers/specs/2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) — §8.2 (payload), §8.4 (one schema, two encodings), §13 phase 3

**Plan sequence:** this is plan 3 of 6. See [README.md](README.md). **Depends on Phase 1** (module, Makefile, pinned `go tool` binaries) and **Phase 2** (`eventstore.Event`, `eventstore.Meta`). Phase 3b depends on this one.

**Deliverable that ends this phase:** a generated `SeatHeld` round-trips message → `eventstore.Event` → database → `eventstore.Event` → message with every field intact, and `make lint` passes `buf lint`.

## Global Constraints

Copied verbatim from spec §5 and §3. Every task's requirements implicitly include this section.

- **Go 1.26.6.** `go.mod` declares `go 1.26.6`.
- **Module path:** `github.com/kptac/sagaflow`. One module at the repository root.
- **Pinned tools:** `buf` 1.72.0 and `protoc-gen-go` v1.36.12, both resolved through `go tool`, never a system install.
- **Add no dependency not listed in spec §5.**
- **Events are persisted forever** (spec §8.2): never reuse a field number, never repurpose a field, always `reserved` on removal. Adding a field is backward compatible; anything else needs a new message (`SeatHeldV2`) plus an upcaster.
- **`protojson` output is not byte-stable across library versions** (spec §8.4), so it is never hashed, never compared byte-wise, and never used as a cache key.
- **The event store must not depend on the registry** (spec §8.4). Nothing in this phase may require a network call to read a stored event.
- **Proto files live at `proto/sagaflow/<service>/v1/`**, not `proto/<service>/v1/` as spec §7's tree shows. §8.1 requires `ce_type` values like `sagaflow.inventory.v1.SeatHeld`, so the proto package is `sagaflow.inventory.v1`, and buf's `STANDARD` lint rule `PACKAGE_DIRECTORY_MATCH` requires the directory to match. §8.1's names are load-bearing at runtime — they are the `ce_type` header and the `protoregistry` lookup key — so the directory yields.

---

## File Structure

| File | Responsibility |
|---|---|
| `buf.yaml` | Module path, `STANDARD` lint, `FILE` breaking-change detection |
| `buf.gen.yaml` | Codegen: managed mode + `protoc-gen-go` via `go tool` |
| `proto/sagaflow/inventory/v1/events.proto` | `SeatHeld` |
| `proto/sagaflow/inventory/v1/commands.proto` | `HoldSeat` |
| `internal/platform/contracts/sagaflow/inventory/v1/*.pb.go` | Generated. Never hand-edited, always committed |
| `internal/platform/codec/codec.go` | `Encode` / `Decode` between `proto.Message` and `eventstore.Event` |
| `internal/platform/codec/codec_test.go` | Round-trip, unknown type, type-name stability |

Generated code is committed rather than gitignored: it makes `go build` work on a clean checkout without the proto toolchain, and it makes a contract change visible in review as a diff.

---

## Phase 3a Tasks

### Task 1: buf toolchain and the first two contracts

**Files:**
- Create: `buf.yaml`, `buf.gen.yaml`, `proto/sagaflow/inventory/v1/events.proto`, `proto/sagaflow/inventory/v1/commands.proto`
- Generated: `internal/platform/contracts/sagaflow/inventory/v1/events.pb.go`, `commands.pb.go`

**Interfaces:**
- Consumes: the pinned `go tool buf` and `go tool protoc-gen-go` from Phase 1.
- Produces: Go package `inventoryv1` at import path `github.com/kptac/sagaflow/internal/platform/contracts/sagaflow/inventory/v1`, containing `*SeatHeld` and `*HoldSeat`, whose full names are `sagaflow.inventory.v1.SeatHeld` and `sagaflow.inventory.v1.HoldSeat`.

Only two messages, and neither carries domain logic. Seat holds, rooms, payments and the saga are phases 5–8; this task needs exactly enough contract surface to prove the toolchain and the codec.

- [ ] **Step 1: Write `buf.yaml`**

```yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

`FILE` is the strictest breaking-change category: it forbids changes that break wire *or* generated-code compatibility. For events persisted forever, that is the right default.

- [ ] **Step 2: Write `buf.gen.yaml`**

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/kptac/sagaflow/internal/platform/contracts
plugins:
  - local: ["go", "tool", "protoc-gen-go"]
    out: internal/platform/contracts
    opt: paths=source_relative
```

Managed mode computes `go_package` as the prefix plus the file's directory, so no `.proto` file carries a `go_package` option and none of them mention the module path. `local` as a list invokes `go tool protoc-gen-go`, which is how the pinned version gets used without anything on `PATH`.

- [ ] **Step 3: Write `proto/sagaflow/inventory/v1/events.proto`**

```protobuf
syntax = "proto3";

package sagaflow.inventory.v1;

import "google/protobuf/timestamp.proto";

// SeatHeld records that one specific seat is held for one booking until
// expires_at. The stream is the seat, so this event is the whole invariant:
// a seat holds at most one live hold at a time.
message SeatHeld {
  string hold_id = 1;
  string booking_id = 2;
  string seat_id = 3;
  google.protobuf.Timestamp expires_at = 4;
}
```

- [ ] **Step 4: Write `proto/sagaflow/inventory/v1/commands.proto`**

```protobuf
syntax = "proto3";

package sagaflow.inventory.v1;

import "google/protobuf/timestamp.proto";

// HoldSeat asks inventory to hold a seat. hold_id is supplied by the caller and
// is deterministic, so a redelivered command produces the same hold rather than
// a second one.
message HoldSeat {
  string hold_id = 1;
  string booking_id = 2;
  string seat_id = 3;
  google.protobuf.Timestamp expires_at = 4;
}
```

`expires_at` is carried in the command rather than computed by inventory, per the spec §10.5 rule that timing is application-supplied so tests can control it.

- [ ] **Step 5: Lint, then generate**

```bash
go tool buf lint
make generate
```

Expected: `buf lint` prints nothing (success). If it reports `PACKAGE_DIRECTORY_MATCH`, the directory does not match the package — check the path is `proto/sagaflow/inventory/v1/`, not `proto/inventory/v1/`.

- [ ] **Step 6: Verify the generated code landed where the plan says**

```bash
ls internal/platform/contracts/sagaflow/inventory/v1/
grep -m1 'package ' internal/platform/contracts/sagaflow/inventory/v1/events.pb.go
```

Expected: `commands.pb.go` and `events.pb.go`, and package `inventoryv1`.

- [ ] **Step 7: Prove the full names match the spec's `ce_type` values**

Create a temporary check and run it:

```bash
cat > /tmp/fullname_check.go <<'EOF'
package main

import (
	"fmt"

	inventoryv1 "github.com/kptac/sagaflow/internal/platform/contracts/sagaflow/inventory/v1"
)

func main() {
	fmt.Println((&inventoryv1.SeatHeld{}).ProtoReflect().Descriptor().FullName())
	fmt.Println((&inventoryv1.HoldSeat{}).ProtoReflect().Descriptor().FullName())
}
EOF
go run /tmp/fullname_check.go && rm /tmp/fullname_check.go
```

Expected exactly:

```
sagaflow.inventory.v1.SeatHeld
sagaflow.inventory.v1.HoldSeat
```

These strings become the `ce_type` header and the `events.type` column. If they differ, the proto `package` is wrong and every later phase inherits the error.

- [ ] **Step 8: Commit**

```bash
git add buf.yaml buf.gen.yaml proto internal/platform/contracts
git commit -m "feat(contracts): buf toolchain and inventory v1 SeatHeld/HoldSeat"
```

---

### Task 2: `platform/codec` — protobuf ⇄ stored event

**Files:**
- Create: `internal/platform/codec/codec.go`, `internal/platform/codec/codec_test.go`

**Interfaces:**
- Consumes: `eventstore.Event`, `eventstore.Meta` from Phase 2; the generated types from Task 1.
- Produces:
  - `func Encode(m proto.Message, meta eventstore.Meta) (eventstore.Event, error)`
  - `func Decode(e eventstore.Event) (proto.Message, error)`
  - `func TypeName(m proto.Message) string`
  - `var ErrUnknownType = errors.New(...)`

- [ ] **Step 1: Write the failing test**

Create `internal/platform/codec/codec_test.go`:

```go
package codec_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	inventoryv1 "github.com/kptac/sagaflow/internal/platform/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/platform/codec"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func seatHeld() *inventoryv1.SeatHeld {
	return &inventoryv1.SeatHeld{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    "seat-BA117-2026-09-01-14A",
		ExpiresAt: timestamppb.New(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)),
	}
}

func TestEncodeSetsTypeToFullProtoName(t *testing.T) {
	got, err := codec.Encode(seatHeld(), eventstore.Meta{TraceID: "t-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const want = "sagaflow.inventory.v1.SeatHeld"
	if got.Type != want {
		t.Fatalf("want type %q, got %q", want, got.Type)
	}
	if got.Meta.TraceID != "t-1" {
		t.Fatalf("meta not carried through: %+v", got.Meta)
	}
}

func TestEncodeProducesReadableJSONWithProtoFieldNames(t *testing.T) {
	got, err := codec.Encode(seatHeld(), eventstore.Meta{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Readability in psql during an incident is the reason storage is protojson
	// and not protobuf bytes (spec §8.4), so assert on the shape a human sees.
	s := string(got.Data)
	for _, want := range []string{`"hold_id"`, `"booking_id"`, `"seat_id"`, `"expires_at"`, "hold-1"} {
		if !strings.Contains(s, want) {
			t.Fatalf("stored JSON %s does not contain %s", s, want)
		}
	}
}

func TestRoundTripPreservesEveryField(t *testing.T) {
	want := seatHeld()

	e, err := codec.Encode(want, eventstore.Meta{CorrelationID: "saga-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg, err := codec.Decode(e)
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

func TestDecodeUnknownTypeIsErrUnknownType(t *testing.T) {
	_, err := codec.Decode(eventstore.Event{
		Type: "sagaflow.inventory.v1.NotAThing",
		Data: []byte(`{}`),
	})
	if !errors.Is(err, codec.ErrUnknownType) {
		t.Fatalf("want ErrUnknownType, got %v", err)
	}
}

func TestDecodeMalformedJSONIsAnError(t *testing.T) {
	_, err := codec.Decode(eventstore.Event{
		Type: "sagaflow.inventory.v1.SeatHeld",
		Data: []byte(`{"hold_id":`),
	})
	if err == nil {
		t.Fatal("want an error for truncated JSON, got nil")
	}
	if errors.Is(err, codec.ErrUnknownType) {
		t.Fatal("malformed data must not be reported as an unknown type")
	}
}
```

The last two tests are separated because they become different retry policies in spec §10.2: an unknown type is a permanent technical failure that goes straight to the DLQ, and so is malformed data — but conflating them would hide a registry-generation bug behind a parse error.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/platform/codec/ -v`
Expected: FAIL to build — `undefined: codec.Encode`, `undefined: codec.Decode`, `undefined: codec.ErrUnknownType`.

- [ ] **Step 3: Implement `codec.go`**

```go
// Package codec converts between generated protobuf messages and the rows the
// event store holds.
//
// Storage is protojson, not protobuf bytes, for two reasons from spec §8.4: a
// registry outage must not block replay, and psql must show something readable
// during an incident. Wire framing for Kafka is a separate concern and lives in
// platform/kafka.
package codec

import (
	"errors"
	"fmt"

	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ErrUnknownType means the event's type name is not in the generated registry.
// It is a permanent technical failure — a message the consumer will never be
// able to handle, so it dead-letters immediately rather than retrying (§10.2).
var ErrUnknownType = errors.New("codec: unknown event type")

// marshal uses proto field names so the stored JSON matches the .proto file
// rather than lowerCamelCase. protojson output is not byte-stable across
// library versions, so it must never be hashed or compared byte-wise (§8.4).
var marshal = protojson.MarshalOptions{UseProtoNames: true}

// unmarshal rejects unknown fields. A field present in the data but absent from
// the compiled schema means the reader is older than the writer, which under the
// BACKWARD compatibility rule in §8.3 should be impossible — so it is better to
// fail loudly than to silently drop data during a replay.
var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: false}

// TypeName is the fully qualified protobuf message name. It is simultaneously
// the events.type column, the ce_type header, and the protoregistry lookup key,
// so there is exactly one identifier for an event type in the whole system.
func TypeName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// Encode turns a message into an event ready for eventstore.Append.
func Encode(m proto.Message, meta eventstore.Meta) (eventstore.Event, error) {
	if m == nil {
		return eventstore.Event{}, errors.New("codec: nil message")
	}
	data, err := marshal.Marshal(m)
	if err != nil {
		return eventstore.Event{}, fmt.Errorf("codec: marshal %s: %w", TypeName(m), err)
	}
	return eventstore.Event{Type: TypeName(m), Data: data, Meta: meta}, nil
}

// Decode resolves the event's type name through the global registry and
// unmarshals into a fresh message of that type.
//
// The registry is populated by the generated code's package initialisers, so
// resolution is a local map lookup and needs no network — which is what keeps
// replay independent of the schema registry.
func Decode(e eventstore.Event) (proto.Message, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(e.Type))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownType, e.Type)
	}
	msg := mt.New().Interface()
	if err := unmarshal.Unmarshal(e.Data, msg); err != nil {
		return nil, fmt.Errorf("codec: unmarshal %s: %w", e.Type, err)
	}
	return msg, nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/platform/codec/ -v`
Expected: all five PASS.

If `TestDecodeUnknownTypeIsErrUnknownType` fails because the type *was* found, you have a stale generated file naming a message that no longer exists in the `.proto` — re-run `make generate`.

- [ ] **Step 5: Prove the codec survives a real database round trip**

Add to `internal/platform/codec/codec_test.go`:

```go
func TestRoundTripThroughPostgres(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.Shared(t).DSN(t, "codec_roundtrip")
	if err := pg.Migrate(ctx, dsn, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pg.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	want := seatHeld()
	e, err := codec.Encode(want, eventstore.Meta{TraceID: "t-1"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return eventstore.Append(ctx, tx, "seat-14A", 0, []eventstore.Event{e})
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	var loaded []eventstore.Recorded
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		loaded, err = eventstore.Load(ctx, tx, "seat-14A")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("want 1 event, got %d", len(loaded))
	}

	msg, err := codec.Decode(loaded[0].Event)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !proto.Equal(want, msg) {
		t.Fatalf("postgres round trip lost data:\n want %v\n got  %v", want, msg)
	}
	if loaded[0].Meta.TraceID != "t-1" {
		t.Fatalf("meta lost: %+v", loaded[0].Meta)
	}
}
```

Add these imports to the test file: `context`, `fmt`, `os`, `github.com/jackc/pgx/v5`, `github.com/kptac/sagaflow/internal/platform/pg`, `github.com/kptac/sagaflow/internal/platform/pgtest`, and `migrations "github.com/kptac/sagaflow/internal/inventory/migrations"`.

Add the package's `TestMain` too — one container for the package, per spec §12.4. The five tests above need no infrastructure and `pgtest.Start` is a no-op under `-short`, so this costs them nothing:

```go
func TestMain(m *testing.M) {
	stop, err := pgtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}
```

This matters because JSONB is not a byte-preserving store — Postgres reparses and reorders keys. A round trip that passes in memory can still fail here, and this is the assertion that catches it.

- [ ] **Step 6: Run it**

Run: `go test ./internal/platform/codec/ -race -v`
Expected: all six PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/platform/codec
git commit -m "feat(codec): protojson encoding between proto messages and stored events"
```

---
