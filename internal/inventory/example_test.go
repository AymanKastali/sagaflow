package inventory_test

import (
	"fmt"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
)

// ExampleDecide_freeSeat shows a free seat accepting a hold.
//
// One event comes back, which is appended to the seat's stream and also
// published. Nothing here touches a database or a broker: Decide is a pure
// function of the folded state and the command, which is why this example can
// run under `go test -short` with no container.
func ExampleDecide_freeSeat() {
	free := inventory.SeatState{Version: 0, Status: inventory.StatusFree}

	decision, err := inventory.Decide(free, &inventoryv1.HoldSeat{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    "seat-BA117-2026-09-01-14A",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("events: ", len(decision.Events))
	fmt.Println("replies:", len(decision.Replies))
	for _, e := range decision.Events {
		fmt.Printf("event:   %s\n", e.ProtoReflect().Descriptor().FullName())
	}

	// Output:
	// events:  1
	// replies: 0
	// event:   sagaflow.inventory.v1.SeatHeld
}

// ExampleDecide_seatAlreadyHeld shows the refusal, and the distinction this
// whole package turns on: SeatUnavailable is a reply, not an event.
//
// Nothing happened to the seat, so nothing is appended to its stream — a seat
// that lost a hundred races has a history of length one, not a hundred and one.
// The reply still goes out, because a saga step that hears nothing re-dispatches
// forever.
func ExampleDecide_seatAlreadyHeld() {
	held := inventory.SeatState{
		Version:   1,
		Status:    inventory.StatusHeld,
		HoldID:    "hold-1",
		BookingID: "booking-1",
	}

	decision, err := inventory.Decide(held, &inventoryv1.HoldSeat{
		HoldId:    "hold-2",
		BookingId: "booking-2",
		SeatId:    "seat-BA117-2026-09-01-14A",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("events: ", len(decision.Events))
	fmt.Println("replies:", len(decision.Replies))
	for _, r := range decision.Replies {
		fmt.Printf("reply:   %s\n", r.ProtoReflect().Descriptor().FullName())
	}

	// Output:
	// events:  0
	// replies: 1
	// reply:   sagaflow.inventory.v1.SeatUnavailable
}

// ExampleDecide_releasingAHoldThatIsAlreadyGone shows why compensation is safe
// to retry.
//
// The hold has already been released, so there is nothing left to undo and no
// event is produced. The reply still comes back, because compensations retry
// forever and never dead-letter — a compensation that gets no answer is a stuck
// saga holding real inventory.
func ExampleDecide_releasingAHoldThatIsAlreadyGone() {
	free := inventory.SeatState{Version: 2, Status: inventory.StatusFree}

	decision, err := inventory.Decide(free, &inventoryv1.ReleaseSeatHold{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    "seat-BA117-2026-09-01-14A",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("events: ", len(decision.Events))
	fmt.Println("replies:", len(decision.Replies))
	for _, r := range decision.Replies {
		fmt.Printf("reply:   %s\n", r.ProtoReflect().Descriptor().FullName())
	}

	// Output:
	// events:  0
	// replies: 1
	// reply:   sagaflow.inventory.v1.SeatHoldReleased
}
