# Phase 5a — Seat Streams and Holds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `internal/inventory` decides `HoldSeat` and `ReleaseSeatHold` against a per-seat event stream, so that two concurrent holds on one seat produce exactly one `SeatHeld` and one `SeatUnavailable`.

**Architecture:** A seat is a stream (spec §6.3), so the hold invariant is `UNIQUE(stream_id, version)` and nothing else. Pure decision functions fold the stream and return an `Outcome`; a single transaction dedupes on the inbox, loads, decides, appends, and enqueues to the outbox (spec §7.2). The decision functions have no clock, no context and no database.

**Tech Stack:** Go 1.26.6, protobuf via buf v2, pgx v5, `internal/platform/{eventstore,codec,envelope,inbox,outbox,pg,schema}`.

**Spec:** [2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) §6.3, §7.1, §7.2, §9.1, §9.3, §10.3, §12.2 — as amended by [2026-08-18-platform-package-restructure-design.md](../specs/2026-08-18-platform-package-restructure-design.md).

## Global Constraints

- Go 1.26.6; module `github.com/kptac/sagaflow`; contracts are the second module `github.com/kptac/sagaflow/contracts`.
- Proto package `sagaflow.inventory.v1` never changes — it is simultaneously `events.type`, `ce_type` and the registry subject.
- **One top-level message per `.proto` file.** `platform/schema.register` hardcodes `sr.Index(0)`, the Confluent message-index shortcut for "first message in its file". A second message in a file would be framed under the wrong index and no other Confluent client could read it.
- A single transaction writes **exactly one stream**, plus its outbox rows, plus its inbox row (spec §7.2).
- Version-conflict retries: 3 immediate reload-retries (spec §10.3).
- Seat hold TTL 15 min in production, 200 ms–2 s in tests (spec §10.3). 5a does not expire anything; it only records `expires_at` as supplied by the command.
- `expires_at` is **application-supplied, never `DEFAULT now()`** (spec §10.5).
- `make test` starts no container; integration tests skip under `-short`. One container per package, started in `TestMain` (spec §12.4).
- No `time.Sleep` in assertions (spec §12.4).
- Every task's last step is its commit.

## Scope

**In:** the `HoldSeat` and `ReleaseSeatHold` commands, the `SeatHeld`, `SeatHoldReleased` and `SeatUnavailable` contracts, the folded seat state, the decision functions, inventory's typed store, and the §7.2 command handler with its conflict retry.

**Out, and where each lands:**

| Deferred | Where |
|---|---|
| `SeatHoldExpired`, the `timers` table, `platform/timers` | Phase 5b |
| Availability projection, `wire.go`, `cmd/inventory` | Phase 5b |
| `ConfirmSeatHold` / `SeatConfirmed` and the pivot's failure reply | Phase 7 — the pivot's semantics are a saga decision, not an inventory one |
| Mapping `SeatUnavailable` to HTTP 409 | Phase 8 |

## The one invariant this phase introduces

**Every command gets a reply; only a change gets an event.**

- A decision that changes the seat produces an **event**: appended to the stream *and* published.
- A decision that finds the seat already in the asked-for state produces the same message as a **reply only** — nothing about the seat changed, so nothing is appended.
- A decision that refuses produces a **refusal reply**.
- Nothing produces silence, because a saga step that hears nothing re-dispatches forever (spec §9.3).

This is why `Outcome` has two fields rather than one. Appending re-announcements and refusals would make a seat's history grow with every failed race and every saga re-dispatch, for events no fold would ever act on.

**Corollary: a hold is live until an event ends it, never until a clock says so.** Expiry is `SeatHoldExpired`, appended by inventory's own timer in 5b. The stream is the single source of truth, so a new hold can never race an expiry, and the decision functions need no `now`.

---

### Task 1: The seat command and event contracts

**Files:**
- Rename: `proto/sagaflow/inventory/v1/events.proto` → `seat_held.proto`
- Rename: `proto/sagaflow/inventory/v1/commands.proto` → `hold_seat.proto`
- Create: `proto/sagaflow/inventory/v1/{release_seat_hold,seat_hold_released,seat_unavailable}.proto`
- Modify: `cmd/schemactl/main.go` (the `bindings` table)
- Modify: `internal/platform/schema/serde_test.go:117,157` (the `.proto` path strings only)
- Test: `contracts/sagaflow/inventory/v1/fullname_test.go`

**Interfaces:**
- Produces: `inventoryv1.{HoldSeat,ReleaseSeatHold,SeatHeld,SeatHoldReleased,SeatUnavailable}`, all in package `sagaflow.inventory.v1`.

**Why the renames.** Every file holds exactly one message (see Global Constraints), so `events.proto` holding only `SeatHeld` alongside a `seat_hold_released.proto` would be an inconsistency that grows with every event. `make breaking` will report the two moved messages for the life of this branch — that is a file-layout change, not a wire change: the proto package and every message's fully qualified name are untouched, and each file still holds one message so the Confluent index stays `[0]`. Record the actual `buf breaking` output in the commit message.

- [ ] **Step 1: Extend the pinned-name test to the five types**

In `contracts/sagaflow/inventory/v1/fullname_test.go`, replace the two-row table body with:

```go
		{&inventoryv1.HoldSeat{}, "sagaflow.inventory.v1.HoldSeat"},
		{&inventoryv1.ReleaseSeatHold{}, "sagaflow.inventory.v1.ReleaseSeatHold"},
		{&inventoryv1.SeatHeld{}, "sagaflow.inventory.v1.SeatHeld"},
		{&inventoryv1.SeatHoldReleased{}, "sagaflow.inventory.v1.SeatHoldReleased"},
		{&inventoryv1.SeatUnavailable{}, "sagaflow.inventory.v1.SeatUnavailable"},
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd contracts && go test ./sagaflow/inventory/v1/`
Expected: FAIL — `undefined: inventoryv1.ReleaseSeatHold` (and the other two).

- [ ] **Step 3: Rename the two existing files**

```bash
git mv proto/sagaflow/inventory/v1/events.proto   proto/sagaflow/inventory/v1/seat_held.proto
git mv proto/sagaflow/inventory/v1/commands.proto proto/sagaflow/inventory/v1/hold_seat.proto
```

- [ ] **Step 4: Create the three new contracts**

`proto/sagaflow/inventory/v1/release_seat_hold.proto`:

```proto
syntax = "proto3";

package sagaflow.inventory.v1;

// ReleaseSeatHold is the compensation for HoldSeat (spec §9.3). Compensations
// retry forever, so it must be safe to apply to a seat that is already free:
// inventory answers a released hold with SeatHoldReleased either way.
message ReleaseSeatHold {
  string hold_id = 1;
  string booking_id = 2;
  string seat_id = 3;
  string reason = 4;
}
```

`proto/sagaflow/inventory/v1/seat_hold_released.proto`:

```proto
syntax = "proto3";

package sagaflow.inventory.v1;

// SeatHoldReleased records that a live hold ended without being confirmed. It is
// how the saga learns its compensation landed, which is why inventory emits it
// even when the hold was already gone.
message SeatHoldReleased {
  string hold_id = 1;
  string booking_id = 2;
  string seat_id = 3;
  string reason = 4;
}
```

`proto/sagaflow/inventory/v1/seat_unavailable.proto`:

```proto
syntax = "proto3";

package sagaflow.inventory.v1;

// SeatUnavailable refuses a HoldSeat because the seat is already held.
//
// It is a reply, not a fact about the seat, so it is never appended to the seat
// stream — nothing happened to the seat. The saga turns it into BookingRejected
// (spec §9.3) and the HTTP layer turns it into a 409 (phase 8).
message SeatUnavailable {
  string hold_id = 1;
  string booking_id = 2;
  string seat_id = 3;
  string reason = 4;
}
```

- [ ] **Step 5: Regenerate and fix the moved paths**

```bash
rm contracts/sagaflow/inventory/v1/commands.pb.go contracts/sagaflow/inventory/v1/events.pb.go
make generate
sed -i 's#/v1/events\.proto#/v1/seat_held.proto#g' internal/platform/schema/serde_test.go
```

Only the two path strings change in `serde_test.go`. **No assertion in that file may be edited** — if one needs editing, this task changed behaviour, which is a stop-and-report.

- [ ] **Step 6: Update the schemactl bindings**

In `cmd/schemactl/main.go`, replace the `bindings` value with:

```go
var bindings = []binding{
	{"inventory.commands", "proto/sagaflow/inventory/v1/hold_seat.proto", &inventoryv1.HoldSeat{}},
	{"inventory.commands", "proto/sagaflow/inventory/v1/release_seat_hold.proto", &inventoryv1.ReleaseSeatHold{}},
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_held.proto", &inventoryv1.SeatHeld{}},
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_hold_released.proto", &inventoryv1.SeatHoldReleased{}},
	{"inventory.events", "proto/sagaflow/inventory/v1/seat_unavailable.proto", &inventoryv1.SeatUnavailable{}},
}
```

- [ ] **Step 7: Verify**

```bash
cd contracts && go test ./sagaflow/inventory/v1/ && cd ..
make lint
make test
make breaking   # expected red: two moved messages, recorded in the commit message
```

Expected: the fullname test PASSes, `make lint` clean, `make test` green with no container.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(contracts): seat hold, release and refusal contracts

One top-level message per file, because platform/schema frames every
message under sr.Index(0) -- the Confluent shortcut for 'first message in
its file'. A second message in a file would be framed under the wrong
index and no other Confluent client could read it. events.proto and
commands.proto are renamed to match.

SeatUnavailable is a reply rather than an event: nothing happens to a seat
when a hold is refused, so appending it would make a seat's history grow
with every failed race.

buf breaking reports the two moved messages for the life of this branch.
The proto package and every fully qualified name are unchanged, and each
file still holds exactly one message, so the wire is untouched."
```

---

### Task 2: The seat stream

**Files:**
- Create: `internal/inventory/seat.go`
- Create: `internal/inventory/errors.go`
- Test: `internal/inventory/seat_test.go`

**Interfaces:**
- Consumes: `inventoryv1.{HoldSeat,ReleaseSeatHold,SeatHeld,SeatHoldReleased,SeatUnavailable}` from Task 1.
- Produces:
  - `type Status int`, `StatusFree`, `StatusHeld`, `func (Status) String() string`
  - `type SeatState struct { Version int; Status Status; HoldID, BookingID string; ExpiresAt time.Time }`
  - `func (SeatState) Apply(proto.Message) (SeatState, error)`
  - `func Fold([]proto.Message) (SeatState, error)`
  - `type Outcome struct { Events, Replies []proto.Message }`, `func (Outcome) Messages() []proto.Message`
  - `func (SeatState) Hold(*inventoryv1.HoldSeat) Outcome`
  - `func (SeatState) Release(*inventoryv1.ReleaseSeatHold) Outcome`
  - `func Decide(SeatState, proto.Message) (Outcome, error)`
  - `func SeatID(proto.Message) (string, error)`
  - `var ErrUnknownEvent, ErrUnknownCommand error`

This task touches no database, no context and no clock — spec §7.1's "pure decision functions. No I/O, no ctx". Its tests are §12.1 level 1 and run in `make test`.

- [ ] **Step 1: Write the failing test**

Create `internal/inventory/seat_test.go`:

```go
// Package inventory_test exercises the seat stream as spec §12.1 level 1: no
// container, no context, no clock. Everything here is a fold and a decision.
package inventory_test

import (
	"errors"
	"testing"
	"time"

	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/inventory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	seat    = "seat-BA117-2026-09-01-14A"
	booking = "booking-1"
	hold    = "hold-1"
)

var expires = timestamppb.New(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC))

func holdSeat(holdID string) *inventoryv1.HoldSeat {
	return &inventoryv1.HoldSeat{
		HoldId: holdID, BookingId: booking, SeatId: seat, ExpiresAt: expires,
	}
}

// state folds the events onto a fresh seat, failing the test rather than
// returning an error, so every case below reads as state-then-decision.
func state(t *testing.T, evts ...proto.Message) inventory.SeatState {
	t.Helper()
	s, err := inventory.Fold(evts)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	return s
}

func heldEvent(holdID string) *inventoryv1.SeatHeld {
	return &inventoryv1.SeatHeld{
		HoldId: holdID, BookingId: booking, SeatId: seat, ExpiresAt: expires,
	}
}

// only asserts an outcome carries exactly one message of the wanted type in the
// wanted field, and reports what it found when it does not.
func only(t *testing.T, got []proto.Message, want proto.Message) proto.Message {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("want exactly 1 message, got %d: %v", len(got), got)
	}
	if a, b := got[0].ProtoReflect().Descriptor().FullName(), want.ProtoReflect().Descriptor().FullName(); a != b {
		t.Fatalf("want a %s, got a %s", b, a)
	}
	return got[0]
}

func TestHoldOnAFreeSeatAppendsSeatHeld(t *testing.T) {
	out := state(t).Hold(holdSeat(hold))

	got := only(t, out.Events, &inventoryv1.SeatHeld{}).(*inventoryv1.SeatHeld)
	if got.HoldId != hold || got.BookingId != booking || got.SeatId != seat {
		t.Fatalf("SeatHeld does not carry the command's identity: %v", got)
	}
	if !got.ExpiresAt.AsTime().Equal(expires.AsTime()) {
		t.Fatalf("expires_at must come from the command, not a clock: got %v", got.ExpiresAt.AsTime())
	}
	if len(out.Replies) != 0 {
		t.Fatalf("a change is published as its event, not also as a reply: %v", out.Replies)
	}
}

func TestHoldOnAHeldSeatRefusesWithoutAppending(t *testing.T) {
	out := state(t, heldEvent(hold)).Hold(holdSeat("hold-2"))

	if len(out.Events) != 0 {
		t.Fatalf("a refused hold changes nothing about the seat, so it must append "+
			"nothing; got %v", out.Events)
	}
	got := only(t, out.Replies, &inventoryv1.SeatUnavailable{}).(*inventoryv1.SeatUnavailable)
	if got.HoldId != "hold-2" {
		t.Fatalf("the refusal must name the refused hold, got %q", got.HoldId)
	}
	if got.Reason == "" {
		t.Fatal("the refusal must say why, so the saga can log it without reloading the stream")
	}
}

func TestHoldRedispatchedForTheLiveHoldReannouncesIt(t *testing.T) {
	// A saga step timeout re-dispatches the same command under a new ce_id, so the
	// inbox does not deduplicate it. Silence would make the saga re-dispatch
	// forever, so the answer is the hold that already exists.
	out := state(t, heldEvent(hold)).Hold(holdSeat(hold))

	if len(out.Events) != 0 {
		t.Fatalf("nothing changed, so nothing may be appended; got %v", out.Events)
	}
	got := only(t, out.Replies, &inventoryv1.SeatHeld{}).(*inventoryv1.SeatHeld)
	if got.HoldId != hold {
		t.Fatalf("want the live hold %q re-announced, got %q", hold, got.HoldId)
	}
}

func TestReleaseOfTheLiveHoldAppendsSeatHoldReleased(t *testing.T) {
	out := state(t, heldEvent(hold)).Release(&inventoryv1.ReleaseSeatHold{
		HoldId: hold, BookingId: booking, SeatId: seat, Reason: "payment declined",
	})

	got := only(t, out.Events, &inventoryv1.SeatHoldReleased{}).(*inventoryv1.SeatHoldReleased)
	if got.Reason != "payment declined" {
		t.Fatalf("the release must carry the compensation's reason, got %q", got.Reason)
	}
}

func TestReleaseOfAHoldThatIsAlreadyGoneStillReplies(t *testing.T) {
	// Compensations retry forever and never dead-letter (spec §9.3), so a second
	// ReleaseSeatHold must terminate rather than go unanswered.
	s := state(t, heldEvent(hold), &inventoryv1.SeatHoldReleased{HoldId: hold, SeatId: seat})
	out := s.Release(&inventoryv1.ReleaseSeatHold{HoldId: hold, SeatId: seat})

	if len(out.Events) != 0 {
		t.Fatalf("the hold is already released, so nothing may be appended; got %v", out.Events)
	}
	only(t, out.Replies, &inventoryv1.SeatHoldReleased{})
}

func TestReleasedSeatCanBeHeldAgain(t *testing.T) {
	s := state(t, heldEvent(hold), &inventoryv1.SeatHoldReleased{HoldId: hold, SeatId: seat})
	if s.Status != inventory.StatusFree {
		t.Fatalf("a released seat is free, got %v", s.Status)
	}
	if s.Version != 2 {
		t.Fatalf("two events folded, so the expected version is 2, got %d", s.Version)
	}
	only(t, s.Hold(holdSeat("hold-2")).Events, &inventoryv1.SeatHeld{})
}

func TestFoldRejectsAnEventTypeThisServiceDoesNotOwn(t *testing.T) {
	// Only inventory appends to a seat stream, so an unrecognised type means the
	// binary is older than its own data. Folding past it would silently produce
	// the wrong state.
	if _, err := inventory.Fold([]proto.Message{&inventoryv1.HoldSeat{}}); !errors.Is(err, inventory.ErrUnknownEvent) {
		t.Fatalf("want ErrUnknownEvent, got %v", err)
	}
}

func TestDecideRejectsAnUnknownCommand(t *testing.T) {
	if _, err := inventory.Decide(inventory.SeatState{}, &inventoryv1.SeatHeld{}); !errors.Is(err, inventory.ErrUnknownCommand) {
		t.Fatalf("want ErrUnknownCommand, got %v", err)
	}
}

func TestSeatIDIsTheCommandsSeatID(t *testing.T) {
	// Spec §6.3: the seat id *is* the stream id, so there is nothing to derive.
	got, err := inventory.SeatID(holdSeat(hold))
	if err != nil {
		t.Fatalf("seat id: %v", err)
	}
	if got != seat {
		t.Fatalf("want %q, got %q", seat, got)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -short ./internal/inventory/`
Expected: FAIL — the package does not compile, `undefined: inventory.Fold`.

- [ ] **Step 3: Write the errors**

Create `internal/inventory/errors.go`:

```go
package inventory

import "errors"

// ErrUnknownEvent means a stored seat event has a type this binary does not
// know. Only inventory appends to a seat stream, so this is a binary older than
// its own data, not a message from a stranger — folding past it would silently
// produce the wrong state, so it fails instead.
var ErrUnknownEvent = errors.New("inventory: unknown seat event")

// ErrUnknownCommand means a message arrived on inventory.commands that is not a
// command. It is permanent: redelivery cannot make it a command, so the consumer
// dead-letters it rather than retrying (spec §10.2).
var ErrUnknownCommand = errors.New("inventory: unknown command")
```

- [ ] **Step 4: Write the seat stream**

Create `internal/inventory/seat.go`:

```go
// Package inventory owns the seat streams: one stream per seat, so that "this
// seat is held at most once" is enforced by UNIQUE(stream_id, version) and by
// nothing else (spec §6.3).
//
// The decision functions here are pure — no context, no database, and
// deliberately no clock. A hold is live until an event ends it, never until a
// clock says so, which is what stops a new hold racing an expiry: expiry is
// SeatHoldExpired, appended by inventory's own timer (spec §10.5).
package inventory

import (
	"fmt"
	"time"

	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Status is what a seat is doing. Phase 7 adds StatusConfirmed with the saga's
// pivot; until there is a SeatConfirmed event there is nothing to fold into it.
type Status int

const (
	StatusFree Status = iota
	StatusHeld
)

func (s Status) String() string {
	switch s {
	case StatusFree:
		return "free"
	case StatusHeld:
		return "held"
	}
	return "unknown"
}

// SeatState is one seat stream folded. Seat streams are 3–5 events long
// (spec §6.3), so rebuilding is always cheap and snapshots are unnecessary
// rather than deferred.
type SeatState struct {
	Version   int
	Status    Status
	HoldID    string // the live hold; empty when the seat is free
	BookingID string
	ExpiresAt time.Time
}

// Apply folds one event. Version advances for every event, including ones that
// leave the seat free, because it is the expected version for the next append
// rather than a count of anything.
func (s SeatState) Apply(e proto.Message) (SeatState, error) {
	switch e := e.(type) {
	case *inventoryv1.SeatHeld:
		s.Status, s.HoldID, s.BookingID = StatusHeld, e.HoldId, e.BookingId
		s.ExpiresAt = e.ExpiresAt.AsTime()
	case *inventoryv1.SeatHoldReleased:
		s.Status, s.HoldID, s.BookingID, s.ExpiresAt = StatusFree, "", "", time.Time{}
	default:
		return s, fmt.Errorf("%w: %s", ErrUnknownEvent, e.ProtoReflect().Descriptor().FullName())
	}
	s.Version++
	return s, nil
}

// Fold replays a whole stream.
func Fold(evts []proto.Message) (SeatState, error) {
	var s SeatState
	for i, e := range evts {
		next, err := s.Apply(e)
		if err != nil {
			return SeatState{}, fmt.Errorf("inventory: fold event %d: %w", i, err)
		}
		s = next
	}
	return s, nil
}

// Outcome is what a decision produces: events to append to the seat stream, and
// replies to publish without appending.
//
// The split is the phase's one invariant — every command gets a reply, only a
// change gets an event. Appending refusals and re-announcements would make a
// seat's history grow with every failed race and every saga re-dispatch, for
// events no fold would ever act on.
type Outcome struct {
	Events  []proto.Message
	Replies []proto.Message
}

// Messages is everything the outcome publishes. Events go out too: an append
// nobody hears about is a saga waiting forever.
func (o Outcome) Messages() []proto.Message {
	out := make([]proto.Message, 0, len(o.Events)+len(o.Replies))
	return append(append(out, o.Events...), o.Replies...)
}

// Hold decides a HoldSeat command.
func (s SeatState) Hold(cmd *inventoryv1.HoldSeat) Outcome {
	// The live hold is this one: a saga step timed out and re-dispatched under a
	// new ce_id, so the inbox did not deduplicate it. Re-announce rather than stay
	// silent, because the re-dispatch is waiting for a reply — and rather than
	// append, because nothing about the seat changed.
	if s.Status == StatusHeld && s.HoldID == cmd.HoldId {
		return Outcome{Replies: []proto.Message{s.held(cmd.SeatId)}}
	}
	if s.Status != StatusFree {
		return Outcome{Replies: []proto.Message{&inventoryv1.SeatUnavailable{
			HoldId:    cmd.HoldId,
			BookingId: cmd.BookingId,
			SeatId:    cmd.SeatId,
			Reason:    "seat is " + s.Status.String(),
		}}}
	}
	return Outcome{Events: []proto.Message{&inventoryv1.SeatHeld{
		HoldId:    cmd.HoldId,
		BookingId: cmd.BookingId,
		SeatId:    cmd.SeatId,
		ExpiresAt: cmd.ExpiresAt,
	}}}
}

// Release decides a ReleaseSeatHold command, the compensation for HoldSeat.
//
// A hold that is already gone still gets a SeatHoldReleased reply. Compensations
// retry forever and never dead-letter (spec §9.3), so silence here would mean a
// compensation that never terminates.
func (s SeatState) Release(cmd *inventoryv1.ReleaseSeatHold) Outcome {
	released := &inventoryv1.SeatHoldReleased{
		HoldId:    cmd.HoldId,
		BookingId: cmd.BookingId,
		SeatId:    cmd.SeatId,
		Reason:    cmd.Reason,
	}
	if s.Status != StatusHeld || s.HoldID != cmd.HoldId {
		return Outcome{Replies: []proto.Message{released}}
	}
	return Outcome{Events: []proto.Message{released}}
}

// held rebuilds the SeatHeld describing the live hold, for re-announcement.
func (s SeatState) held(seatID string) *inventoryv1.SeatHeld {
	return &inventoryv1.SeatHeld{
		HoldId:    s.HoldID,
		BookingId: s.BookingID,
		SeatId:    seatID,
		ExpiresAt: timestamppb.New(s.ExpiresAt),
	}
}

// Decide routes a command to its decision function.
func Decide(s SeatState, cmd proto.Message) (Outcome, error) {
	switch c := cmd.(type) {
	case *inventoryv1.HoldSeat:
		return s.Hold(c), nil
	case *inventoryv1.ReleaseSeatHold:
		return s.Release(c), nil
	}
	return Outcome{}, fmt.Errorf("%w: %s", ErrUnknownCommand,
		cmd.ProtoReflect().Descriptor().FullName())
}

// SeatID is the stream a command targets. Per spec §6.3 the seat id *is* the
// stream id, so there is nothing to derive — but the lookup still belongs here,
// because the handler must not know which field of which command holds it.
func SeatID(cmd proto.Message) (string, error) {
	switch c := cmd.(type) {
	case *inventoryv1.HoldSeat:
		return c.SeatId, nil
	case *inventoryv1.ReleaseSeatHold:
		return c.SeatId, nil
	}
	return "", fmt.Errorf("%w: %s", ErrUnknownCommand,
		cmd.ProtoReflect().Descriptor().FullName())
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -short -race ./internal/inventory/ -v`
Expected: all nine PASS, no container started.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/seat.go internal/inventory/errors.go internal/inventory/seat_test.go
git commit -m "feat(inventory): the seat stream and its pure decision functions

Every command gets a reply; only a change gets an event. A refused hold and
a re-announced one both leave the seat untouched, so both are replies --
appending them would grow a seat's history with every failed race and every
saga re-dispatch, for events no fold would act on.

No clock in the decision functions. A hold is live until an event ends it,
which is what stops a new hold racing the expiry timer that arrives in 5b."
```

---

### Task 3: Inventory's typed store

**Files:**
- Create: `internal/inventory/store.go`
- Test: `internal/inventory/store_test.go`
- Create: `internal/inventory/main_test.go`

**Interfaces:**
- Consumes: `Fold`, `SeatState` from Task 2; `eventstore.{Load,Append,Event,Meta}`, `codec.{Encode,Decode}`, `pgtest`.
- Produces:
  - `func LoadSeat(ctx context.Context, tx pgx.Tx, seatID string) (SeatState, error)`
  - `func AppendSeat(ctx context.Context, tx pgx.Tx, seatID string, expectedVersion int, evts []proto.Message, meta eventstore.Meta) error`

This is spec §7.1's `store.go`: "typed wrapper over platform/eventstore". Its tests are §12.2 level 2 and need a real Postgres, so the package gains a `TestMain`.

- [ ] **Step 1: Write the package TestMain**

Create `internal/inventory/main_test.go`:

```go
package inventory_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/kptac/sagaflow/internal/testsupport/pgtest"
)

// One Postgres for the whole package (spec §12.4). Isolation comes from database
// names and stream ids derived from the test, not from a container per test.
//
// No broker and no registry: everything in this package stops at the outbox
// table. Publishing is platform/outbox's property, already proven there.
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

- [ ] **Step 2: Write the failing test**

Create `internal/inventory/store_test.go`:

```go
package inventory_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/inventory"
	"github.com/kptac/sagaflow/internal/inventory/migrations"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"github.com/kptac/sagaflow/internal/testsupport/pgtest"
	"google.golang.org/protobuf/proto"
)

func db(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	return pgtest.Shared(t).Migrated(t, name, migrations.FS)
}

func TestSeatStreamRoundTripsThroughPostgres(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_store")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return inventory.AppendSeat(ctx, tx, seat, 0,
			[]proto.Message{heldEvent(hold)}, eventstore.Meta{CorrelationID: "saga-1"})
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	var got inventory.SeatState
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = inventory.LoadSeat(ctx, tx, seat)
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.Status != inventory.StatusHeld || got.HoldID != hold {
		t.Fatalf("state did not survive the round trip: %+v", got)
	}
	if got.Version != 1 {
		t.Fatalf("one event stored, so the expected version is 1, got %d", got.Version)
	}
	if !got.ExpiresAt.Equal(expires.AsTime()) {
		t.Fatalf("expires_at lost precision or zone: want %v, got %v",
			expires.AsTime(), got.ExpiresAt)
	}
}

func TestAppendAtAStaleVersionConflicts(t *testing.T) {
	// The atomic hold *is* this constraint (spec §6.3): two writers folding from
	// version 0 both try version 1 and Postgres rejects one. Asserted here so
	// Task 4's retry has something proven to retry on.
	ctx := t.Context()
	pool := db(t, "inventory_store_conflict")

	appendFrom := func(version int) error {
		return pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
			return inventory.AppendSeat(ctx, tx, seat, version,
				[]proto.Message{heldEvent(hold)}, eventstore.Meta{})
		})
	}

	if err := appendFrom(0); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := appendFrom(0); !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict on a stale expected version, got %v", err)
	}
}

func TestLoadOfAnEmptyStreamIsAFreeSeatAtVersionZero(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_store_empty")

	var got inventory.SeatState
	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		var err error
		got, err = inventory.LoadSeat(ctx, tx, "seat-never-touched")
		return err
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Status != inventory.StatusFree || got.Version != 0 {
		t.Fatalf("an unwritten seat is free at version 0, got %+v", got)
	}
}

func TestStoredEventIsReadableProtoJSON(t *testing.T) {
	// Spec §8.4: storage is protojson so a replay survives a registry outage and
	// psql shows something readable during an incident. Asserted against the
	// column rather than through the codec, which would prove only symmetry.
	ctx := t.Context()
	pool := db(t, "inventory_store_json")

	if err := pg.WithTx(ctx, pool, func(tx pgx.Tx) error {
		return inventory.AppendSeat(ctx, tx, seat, 0,
			[]proto.Message{heldEvent(hold)}, eventstore.Meta{})
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	var typ, holdID string
	if err := pool.QueryRow(ctx,
		`SELECT type, data->>'hold_id' FROM events WHERE stream_id = $1`, seat,
	).Scan(&typ, &holdID); err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if typ != "sagaflow.inventory.v1.SeatHeld" {
		t.Fatalf("events.type must be the fully qualified name, got %q", typ)
	}
	if holdID != hold {
		t.Fatalf("data must use proto field names, so data->>'hold_id' is %q, got %q", hold, holdID)
	}
}
```

`store_test.go` imports `"errors"` alongside the rest.

- [ ] **Step 3: Run it to verify it fails**

Run: `go test -run TestSeatStream ./internal/inventory/`
Expected: FAIL — `undefined: inventory.AppendSeat`.

- [ ] **Step 4: Write the store**

Create `internal/inventory/store.go`:

```go
package inventory

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/kptac/sagaflow/internal/platform/codec"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"google.golang.org/protobuf/proto"
)

// LoadSeat folds a seat stream into its state, inside the caller's transaction.
//
// Reading in the same transaction as the later append is what makes the returned
// Version a safe expected version: nothing can interleave between the two.
func LoadSeat(ctx context.Context, tx pgx.Tx, seatID string) (SeatState, error) {
	recorded, err := eventstore.Load(ctx, tx, seatID)
	if err != nil {
		return SeatState{}, err
	}
	msgs := make([]proto.Message, len(recorded))
	for i, r := range recorded {
		m, err := codec.Decode(r.Event)
		if err != nil {
			return SeatState{}, fmt.Errorf("inventory: decode %s v%d: %w", seatID, r.Version, err)
		}
		msgs[i] = m
	}
	return Fold(msgs)
}

// AppendSeat encodes evts and appends them at expectedVersion+1 …
//
// It returns eventstore.ErrVersionConflict untouched rather than wrapping it,
// because the handler's retry branches on it and a wrap here would be one more
// place for that to silently stop matching.
func AppendSeat(ctx context.Context, tx pgx.Tx, seatID string, expectedVersion int, evts []proto.Message, meta eventstore.Meta) error {
	if len(evts) == 0 {
		return nil
	}
	stored := make([]eventstore.Event, len(evts))
	for i, m := range evts {
		e, err := codec.Encode(m, meta)
		if err != nil {
			return fmt.Errorf("inventory: encode for %s: %w", seatID, err)
		}
		stored[i] = e
	}
	return eventstore.Append(ctx, tx, seatID, expectedVersion, stored)
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
go test -race ./internal/inventory/ -v
go test -short ./internal/inventory/    # must skip everything needing Postgres
```

Expected: four store tests PASS against a real Postgres; the `-short` run skips them and still runs Task 2's nine.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/store.go internal/inventory/store_test.go internal/inventory/main_test.go
git commit -m "feat(inventory): typed seat store over eventstore and codec

Load folds inside the caller's transaction, so the version it returns is a
safe expected version for the append in that same transaction.

ErrVersionConflict passes through unwrapped: the handler's retry branches
on it, and wrapping would be one more place for that match to break."
```

---

### Task 4: The command handler

**Files:**
- Create: `internal/inventory/commands.go`
- Test: `internal/inventory/commands_test.go`

**Interfaces:**
- Consumes: `Decide`, `SeatID`, `Outcome` (Task 2), `LoadSeat`, `AppendSeat` (Task 3), `inbox.MarkConsumed`, `outbox.Enqueue`, `envelope.{Envelope,NewID,Message}`, `pg.WithTx`.
- Produces:
  - `const EventsTopic = "inventory.events"`, `const CommandsTopic = "inventory.commands"`, `const Source = "/sagaflow/inventory"`, `const Consumer = "inventory.commands"`, `const ConflictRetries = 3`
  - `type Encoder interface { Encode(proto.Message) ([]byte, error) }`
  - `type Handler struct{ ... }`, `func NewHandler(pool *pgxpool.Pool, enc Encoder) *Handler`
  - `func (*Handler) Handle(ctx context.Context, env envelope.Envelope, cmd proto.Message) error`

**This task carries the phase's deliverable:** two concurrent `HoldSeat` commands on one seat produce one `SeatHeld` and one `SeatUnavailable`.

**Why `Encoder` is an interface.** The wire payload is registry-framed protobuf (spec §8.4), which `platform/schema.Serde` produces — and building one needs a live registry. Framing is `platform/schema`'s property and is proven in its own tests, so the handler takes the narrowest interface `*schema.Serde` already satisfies and its tests need no registry container.

- [ ] **Step 1: Write the failing test**

Create `internal/inventory/commands_test.go`:

```go
package inventory_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/kptac/sagaflow/internal/inventory"
	"github.com/kptac/sagaflow/internal/platform/envelope"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// jsonEncoder stands in for platform/schema.Serde. Registry framing is that
// package's property and is proven in its tests; what matters here is that the
// handler puts the encoder's bytes in the outbox row.
type jsonEncoder struct{}

func (jsonEncoder) Encode(m proto.Message) ([]byte, error) { return protojson.Marshal(m) }

// outboxRow is one published-to-be message, read back from the table.
type outboxRow struct {
	topic string
	key   string
	ceTyp string
}

func outboxRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []outboxRow {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT topic, key, headers->>'ce_type' FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()

	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.topic, &r.key, &r.ceTyp); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox: %v", err)
	}
	return out
}

func command(cmd proto.Message) envelope.Envelope {
	return envelope.Envelope{
		ID:            envelope.NewID(),
		Source:        "/sagaflow/booking",
		Type:          string(cmd.ProtoReflect().Descriptor().FullName()),
		Subject:       seat,
		CorrelationID: "saga-1",
	}
}

func TestHoldAppendsTheEventAndEnqueuesItInOneTransaction(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_hold")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := holdSeat(hold)
	if err := h.Handle(ctx, command(cmd), cmd); err != nil {
		t.Fatalf("handle: %v", err)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 1 {
		t.Fatalf("want 1 outbox row, got %d: %+v", len(rows), rows)
	}
	if rows[0].topic != inventory.EventsTopic {
		t.Fatalf("want topic %q, got %q", inventory.EventsTopic, rows[0].topic)
	}
	if rows[0].key != seat {
		t.Fatalf("the key must be the stream id, which is what keeps a seat's "+
			"events in one partition; want %q, got %q", seat, rows[0].key)
	}
	if rows[0].ceTyp != "sagaflow.inventory.v1.SeatHeld" {
		t.Fatalf("want ce_type SeatHeld, got %q", rows[0].ceTyp)
	}
}

func TestARedeliveredCommandIsAppliedOnce(t *testing.T) {
	// Spec §12.2: handle the same ce_id twice, state advances once.
	ctx := t.Context()
	pool := db(t, "inventory_handler_dedupe")
	h := inventory.NewHandler(pool, jsonEncoder{})

	cmd := holdSeat(hold)
	env := command(cmd)
	for range 2 {
		if err := h.Handle(ctx, env, cmd); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}

	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("the inbox must absorb the redelivery; got %d events", events)
	}
	if rows := outboxRows(t, ctx, pool); len(rows) != 1 {
		t.Fatalf("a deduplicated command must publish nothing the second time; got %d rows", len(rows))
	}
}

func TestARefusedHoldWritesNoEventAndStillReplies(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_refused")
	h := inventory.NewHandler(pool, jsonEncoder{})

	first := holdSeat(hold)
	if err := h.Handle(ctx, command(first), first); err != nil {
		t.Fatalf("first hold: %v", err)
	}
	second := holdSeat("hold-2")
	if err := h.Handle(ctx, command(second), second); err != nil {
		t.Fatalf("second hold: %v", err)
	}

	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("a refusal appends nothing, so the seat has 1 event; got %d", events)
	}

	rows := outboxRows(t, ctx, pool)
	if len(rows) != 2 {
		t.Fatalf("want SeatHeld then SeatUnavailable, got %d rows: %+v", len(rows), rows)
	}
	if rows[1].ceTyp != "sagaflow.inventory.v1.SeatUnavailable" {
		t.Fatalf("the refusal must reach the saga; got ce_type %q", rows[1].ceTyp)
	}
}

// TestTwoConcurrentHoldsProduceOneHoldAndOneRefusal is spec §13 phase 5's
// deliverable. The refusal is what phase 8's HTTP layer renders as a 409; there
// is no HTTP yet, so the assertion is on the refusal itself.
//
// Both goroutines fold from version 0 and both attempt version 1. The loser gets
// ErrVersionConflict, reloads, sees SeatHeld, and re-decides to a refusal — so
// this proves the retry re-decides rather than merely re-appending.
func TestTwoConcurrentHoldsProduceOneHoldAndOneRefusal(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_race")
	h := inventory.NewHandler(pool, jsonEncoder{})

	var (
		wg    sync.WaitGroup
		errs  [2]error
		start = make(chan struct{})
	)
	for i, holdID := range [2]string{"hold-a", "hold-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := holdSeat(holdID)
			env := command(cmd)
			<-start // release both at once, so they really do race
			errs[i] = h.Handle(ctx, env, cmd)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("hold %d failed instead of being decided: %v", i, err)
		}
	}

	var events int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE stream_id = $1`, seat).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("a seat holds at most one live hold, so exactly one event may be "+
			"appended; got %d", events)
	}

	rows := outboxRows(t, ctx, pool)
	var held, unavailable int
	for _, r := range rows {
		switch r.ceTyp {
		case "sagaflow.inventory.v1.SeatHeld":
			held++
		case "sagaflow.inventory.v1.SeatUnavailable":
			unavailable++
		}
	}
	if held != 1 || unavailable != 1 {
		t.Fatalf("want exactly one SeatHeld and one SeatUnavailable, got %d and %d: %+v",
			held, unavailable, rows)
	}
}

func TestAnUnknownCommandIsRejected(t *testing.T) {
	ctx := t.Context()
	pool := db(t, "inventory_handler_unknown")
	h := inventory.NewHandler(pool, jsonEncoder{})

	// A SeatHeld is an event, not a command. Redelivery cannot change that.
	ev := heldEvent(hold)
	if err := h.Handle(ctx, command(ev), ev); !errors.Is(err, inventory.ErrUnknownCommand) {
		t.Fatalf("want ErrUnknownCommand, got %v", err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -run TestTwoConcurrentHolds ./internal/inventory/`
Expected: FAIL — `undefined: inventory.NewHandler`.

- [ ] **Step 3: Write the handler**

Create `internal/inventory/commands.go`:

```go
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kptac/sagaflow/internal/platform/codec"
	"github.com/kptac/sagaflow/internal/platform/envelope"
	"github.com/kptac/sagaflow/internal/platform/eventstore"
	"github.com/kptac/sagaflow/internal/platform/inbox"
	"github.com/kptac/sagaflow/internal/platform/outbox"
	"github.com/kptac/sagaflow/internal/platform/pg"
	"google.golang.org/protobuf/proto"
)

const (
	// CommandsTopic and EventsTopic are spec §9.1's topology: commands and events
	// are separate topics, one pair per service.
	CommandsTopic = "inventory.commands"
	EventsTopic   = "inventory.events"

	// Source is this service's ce_source. With ce_id it forms the inbox key that
	// makes redelivery a no-op.
	Source = "/sagaflow/inventory"

	// Consumer is the inbox consumer name. Consumer groups are per purpose, not
	// per service (spec §9.1), which is why it is part of the inbox primary key.
	Consumer = "inventory.commands"
)

// ConflictRetries is spec §10.3: three immediate reload-retries before giving
// up. Immediate rather than backed off, because a version conflict means the
// winning write is already committed — there is nothing to wait for.
const ConflictRetries = 3

// Encoder frames an event for the wire.
//
// An interface, not *schema.Serde, so the handler's tests need no registry
// container: framing is platform/schema's property and is proven in its tests.
type Encoder interface {
	Encode(m proto.Message) ([]byte, error)
}

// Handler applies inventory commands. One command, one seat, one transaction.
type Handler struct {
	pool *pgxpool.Pool
	enc  Encoder
}

func NewHandler(pool *pgxpool.Pool, enc Encoder) *Handler {
	return &Handler{pool: pool, enc: enc}
}

// Handle applies one command, retrying a version conflict by reloading and
// re-deciding.
//
// Re-deciding is the point: the loser of a race folded from state that no longer
// exists, so replaying its old decision would append a second hold. Reloading
// turns it into a refusal instead.
func (h *Handler) Handle(ctx context.Context, env envelope.Envelope, cmd proto.Message) error {
	var err error
	for range ConflictRetries + 1 {
		if err = h.handleOnce(ctx, env, cmd); !errors.Is(err, eventstore.ErrVersionConflict) {
			return err
		}
	}
	return fmt.Errorf("inventory: %s on %s after %d retries: %w",
		env.Type, env.Subject, ConflictRetries, err)
}

// handleOnce is spec §7.2's invariant: one transaction writes exactly one
// stream, plus its outbox rows, plus its inbox row.
//
// The inbox mark is inside the transaction so a conflict rolls it back too —
// otherwise the retry would find its own mark and treat the command as already
// handled.
func (h *Handler) handleOnce(ctx context.Context, env envelope.Envelope, cmd proto.Message) error {
	seatID, err := SeatID(cmd)
	if err != nil {
		return err
	}
	return pg.WithTx(ctx, h.pool, func(tx pgx.Tx) error {
		fresh, err := inbox.MarkConsumed(ctx, tx, Consumer, env.Source, env.ID)
		if err != nil || !fresh {
			return err // not fresh: already applied, commit nothing, ack
		}
		state, err := LoadSeat(ctx, tx, seatID)
		if err != nil {
			return err
		}
		out, err := Decide(state, cmd)
		if err != nil {
			return err
		}
		// No TraceID: it is the trace id, not the whole W3C traceparent header,
		// and nothing extracts one until phase 9 wires OTel.
		meta := eventstore.Meta{
			CorrelationID: env.CorrelationID,
			CausationID:   env.ID,
		}
		if err := AppendSeat(ctx, tx, seatID, state.Version, out.Events, meta); err != nil {
			return err
		}
		msgs, err := h.messages(out.Messages(), env, seatID)
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, msgs)
	})
}

// messages frames each outgoing message and wraps it in its own envelope.
//
// Each gets a fresh ce_id because each is a distinct message, keeps the
// incoming correlation id so the saga can route the reply, and takes the
// incoming ce_id as its causation id so the chain is walkable (spec §8.1).
func (h *Handler) messages(msgs []proto.Message, in envelope.Envelope, seatID string) ([]envelope.Message, error) {
	out := make([]envelope.Message, 0, len(msgs))
	for _, m := range msgs {
		payload, err := h.enc.Encode(m)
		if err != nil {
			return nil, fmt.Errorf("inventory: frame %s: %w", codec.TypeName(m), err)
		}
		env := envelope.Envelope{
			ID:            envelope.NewID(),
			Source:        Source,
			Type:          codec.TypeName(m),
			Subject:       seatID,
			CorrelationID: in.CorrelationID,
			CausationID:   in.ID,
			TraceParent:   in.TraceParent,
		}
		out = append(out, envelope.Message{
			Topic:   EventsTopic,
			Key:     seatID, // the stream id, which is what preserves per-seat ordering
			Payload: payload,
			Headers: env.Headers(),
		})
	}
	return out, nil
}
```

- [ ] **Step 3b: Prove the retry deterministically**

*Added during execution — the plan missed it.* `TestTwoConcurrentHoldsProduceOneHoldAndOneRefusal` can pass without ever exercising the retry: if the two goroutines do not overlap, the second simply loads the committed `SeatHeld` and refuses, no conflict involved. So it does not prove what its own comment claims.

Append to `commands_test.go` a test that arranges the conflict instead of hoping for it: open a transaction, `AppendSeat` a `SeatHeld` at version 1 and **do not commit**, then run the handler in a goroutine. Its insert blocks on the unique index. Wait for `pg_stat_activity.wait_event_type = 'Lock'` — a polled condition with a deadline, not a sleep before an assertion — commit the winner, and assert the handler produced exactly one `SeatUnavailable` row and appended nothing.

Verify it discriminates by mutating `ConflictRetries` to `0`: the test must fail with `after 0 retries: eventstore: version conflict`. Restore it to `3`.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
go test -race ./internal/inventory/ -v
```

Expected: all six handler tests PASS, including `TestTwoConcurrentHoldsProduceOneHoldAndOneRefusal`.

- [ ] **Step 5: Verify the whole tree**

```bash
make lint
make test              # no container, Task 2's nine tests only
make test-integration  # everything, both modules
```

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/commands.go internal/inventory/commands_test.go
git commit -m "feat(inventory): command handler, one seat per transaction

Spec §7.2: one transaction writes exactly one stream, plus its outbox rows,
plus its inbox row. The inbox mark is inside it, so a version conflict rolls
the mark back too -- otherwise the retry would find its own mark and treat
the command as already handled.

A conflict retry reloads and re-decides rather than re-appending. That is
what turns the loser of a race into a refusal instead of a second hold, and
it is what spec §13 phase 5's deliverable asserts: two concurrent holds on
one seat produce one SeatHeld and one SeatUnavailable."
```

---

## Done when

- [ ] Two concurrent `HoldSeat` commands on one seat produce exactly one `SeatHeld` event and exactly one `SeatUnavailable` reply.
- [ ] `internal/inventory`'s decision functions import no `context`, no `pgx`, and no `time.Now` — verified by `grep -n 'context\|pgx\|time\.Now' internal/inventory/seat.go` returning nothing.
- [ ] A redelivered command with the same `ce_id` advances state once.
- [ ] Every outbox row's key is the seat id.
- [ ] `make test` starts no container and stays under 5 s.
- [ ] `make test-integration` green, uncached, in both modules.
- [ ] `make lint` clean, including `buf lint`.
- [ ] `make breaking`'s output on the two renamed files is recorded in Task 1's commit message.

## Deliberately not done here

`SeatHoldExpired` and the timer, the availability projection, `wire.go`, and `cmd/inventory` are phase 5b. `ConfirmSeatHold` is phase 7, with the pivot. Nothing in this phase publishes to Kafka — the outbox row is the boundary, and `platform/outbox` already proves what happens past it.
