// Package inventory_test exercises the seat stream at its purest level: no
// container, no context, no clock. Everything here is a fold and a decision.
package inventory_test

import (
	"errors"
	"testing"
	"time"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
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

func releasedEvent(holdID string) *inventoryv1.SeatHoldReleased {
	return &inventoryv1.SeatHoldReleased{
		HoldId: holdID, BookingId: booking, SeatId: seat, Reason: "compensating",
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
	// Compensations retry with backoff forever and never dead-letter, so a
	// second ReleaseSeatHold must still get an answer rather than go unanswered
	// — an unanswered retry here would never stop retrying.
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
	// Each seat has a stream of its own, so the seat id *is* the stream id and
	// there is nothing to derive.
	got, err := inventory.SeatID(holdSeat(hold))
	if err != nil {
		t.Fatalf("seat id: %v", err)
	}
	if got != seat {
		t.Fatalf("want %q, got %q", seat, got)
	}
}

func TestAnExpiryOfTheLiveHoldFreesTheSeat(t *testing.T) {
	held := state(t, heldEvent(hold))

	got := held.Expire(seat, hold)

	if len(got.Events) != 1 || len(got.Replies) != 0 {
		t.Fatalf("expiry changes the seat, so it is one event and no reply: %+v", got)
	}
	expired, ok := got.Events[0].(*inventoryv1.SeatHoldExpired)
	if !ok {
		t.Fatalf("wrong event type: %T", got.Events[0])
	}
	if expired.HoldId != hold || expired.BookingId != booking || expired.SeatId != seat {
		t.Fatalf("the expiry lost the identity of what it freed: %+v", expired)
	}

	after, err := held.Apply(expired)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if after.Status != inventory.StatusFree || after.HoldID != "" {
		t.Fatalf("an expired hold leaves the seat free: %+v", after)
	}
}

func TestAnExpiryOfAHoldThatIsGoneProducesNothingAtAll(t *testing.T) {
	// The only decision in this package that is allowed to be silent. Nothing
	// sent this and nothing is waiting on it, so there is nobody to answer.
	free := state(t, heldEvent(hold), releasedEvent(hold))

	got := free.Expire(seat, hold)

	if len(got.Events) != 0 || len(got.Replies) != 0 {
		t.Fatalf("a timer for a hold that ended has nothing to say: %+v", got)
	}
}

func TestAnExpiryDoesNotTouchASupersededHold(t *testing.T) {
	// The token fence. hold-1's timer fires after hold-1 was released and the
	// seat was taken by hold-2; expiring hold-2 here would free a seat someone
	// is actively holding.
	current := state(t, heldEvent(hold), releasedEvent(hold), heldEvent("hold-2"))

	got := current.Expire(seat, hold)

	if len(got.Events) != 0 || len(got.Replies) != 0 {
		t.Fatalf("a stale timer must not disturb the live hold: %+v", got)
	}
}

func TestHoldingASeatAsksForATimerAtTheDeadline(t *testing.T) {
	free := inventory.SeatState{Version: 0, Status: inventory.StatusFree}

	got := free.Hold(holdSeat(hold))

	if len(got.Timers) != 1 {
		t.Fatalf("a hold with a deadline asks for exactly one timer, got %d", len(got.Timers))
	}
	if !got.Timers[0].FireAt.Equal(expires.AsTime()) {
		t.Fatalf("the timer must fire at the command's deadline: want %v, got %v",
			expires.AsTime(), got.Timers[0].FireAt)
	}
	if got.Timers[0].Token != hold {
		t.Fatalf("the token is the hold it expires: want %q, got %q", hold, got.Timers[0].Token)
	}
}

func TestReAnnouncingAHoldAsksForNoSecondTimer(t *testing.T) {
	// Timers accompany events, never replies. A re-dispatched HoldSeat for the
	// hold that is already live changes nothing, and its timer already exists.
	held := state(t, heldEvent(hold))

	got := held.Hold(holdSeat(hold))

	if len(got.Replies) != 1 || len(got.Events) != 0 {
		t.Fatalf("setup: expected a bare re-announcement, got %+v", got)
	}
	if len(got.Timers) != 0 {
		t.Fatalf("the timer was scheduled when the hold was taken, got %d more", len(got.Timers))
	}
}

func TestAHoldWithNoDeadlineExpiresImmediately(t *testing.T) {
	// A HoldSeat with no expires_at is malformed. Of the two ways to fail, the
	// timer still gets scheduled — at the zero time, which is already past — so
	// the seat is freed on the next pass and the saga is told. Refusing to
	// schedule would instead produce the one thing this phase exists to prevent:
	// a hold no clock will ever end.
	free := inventory.SeatState{Version: 0, Status: inventory.StatusFree}

	got := free.Hold(&inventoryv1.HoldSeat{HoldId: hold, BookingId: booking, SeatId: seat})

	if len(got.Timers) != 1 {
		t.Fatalf("a hold always gets a deadline, got %d timers", len(got.Timers))
	}
	if !got.Timers[0].FireAt.Before(time.Now()) {
		t.Fatalf("a missing deadline is already past, got %v", got.Timers[0].FireAt)
	}
}
