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
// The split is this phase's one invariant — every command gets a reply, only a
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
