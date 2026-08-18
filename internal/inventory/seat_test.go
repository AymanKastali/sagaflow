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

func heldEvent(holdID string) *inventoryv1.SeatHeld {
	return &inventoryv1.SeatHeld{
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

// only asserts a slice carries exactly one message of the wanted type, and
// reports what it found when it does not.
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
