package inventoryv1_test

import (
	"testing"

	inventoryv1 "github.com/kptac/sagaflow/contracts/sagaflow/inventory/v1"
	"google.golang.org/protobuf/proto"
)

// TestFullNamesMatchTheSpec pins the fully qualified message names.
//
// These strings are load-bearing in three places at once (spec §8.1): the
// ce_type header on the wire, the events.type column in Postgres, and the
// protoregistry lookup key that turns a stored row back into a message. Editing
// a .proto file's `package` line would change all three silently — generated
// code would still compile, the codec would still round-trip in a single
// process, and only cross-service delivery and replay of already-stored events
// would break. So the names are asserted here rather than trusted.
func TestFullNamesMatchTheSpec(t *testing.T) {
	for _, tc := range []struct {
		msg  proto.Message
		want string
	}{
		{&inventoryv1.HoldSeat{}, "sagaflow.inventory.v1.HoldSeat"},
		{&inventoryv1.ReleaseSeatHold{}, "sagaflow.inventory.v1.ReleaseSeatHold"},
		{&inventoryv1.SeatHeld{}, "sagaflow.inventory.v1.SeatHeld"},
		{&inventoryv1.SeatHoldReleased{}, "sagaflow.inventory.v1.SeatHoldReleased"},
		{&inventoryv1.SeatUnavailable{}, "sagaflow.inventory.v1.SeatUnavailable"},
	} {
		got := string(tc.msg.ProtoReflect().Descriptor().FullName())
		if got != tc.want {
			t.Errorf("full name = %q, want %q — the proto package line is wrong, "+
				"and every later phase inherits the error", got, tc.want)
		}
	}
}
