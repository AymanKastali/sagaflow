// Package toolchain guards the Go version the build actually runs on. It holds
// only a test because a guard has nothing to export.
package toolchain

import (
	"go/version"
	"runtime"
	"testing"
)

// Minimum is the toolchain floor spec §5 pins. It matches the `go` directive in
// go.mod, which is what actually causes the toolchain to be fetched.
const Minimum = "go1.26.6"

// meetsFloor reports whether a Go release version is at least Minimum.
//
// go/version.Compare understands Go's release ordering, including that go1.26.10
// is newer than go1.26.9 — which string comparison gets wrong.
func meetsFloor(v string) bool {
	return version.Compare(v, Minimum) >= 0
}

// TestToolchainMeetsTheFloor guards the pin for the build actually running.
func TestToolchainMeetsTheFloor(t *testing.T) {
	got := runtime.Version()
	if !version.IsValid(got) {
		// Devel and gccgo builds report versions Compare cannot order; say so
		// rather than failing on something this test cannot judge.
		t.Skipf("runtime.Version() = %q is not a comparable release version", got)
	}
	if !meetsFloor(got) {
		t.Fatalf("built with %s, want %s or newer — see Global Constraints; "+
			"GOTOOLCHAIN=auto should fetch it from the go.mod directive", got, Minimum)
	}
}

// TestFloorRejectsOlderReleases proves the guard has teeth.
//
// Without this, TestToolchainMeetsTheFloor is a test whose only observed outcome
// is "pass" — it would look identical if the comparison were inverted or the
// floor were unreachable. This pins the rejection behaviour without needing an
// older toolchain installed to demonstrate it.
func TestFloorRejectsOlderReleases(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want bool
	}{
		{"go1.25.13", false}, // previous minor, however high its patch
		{"go1.26.4", false},
		{"go1.26.5", false}, // what was installed when this was written
		{"go1.26.6", true},  // the floor itself
		{"go1.26.7", true},
		{"go1.26.10", true}, // string comparison would call this older than 1.26.6
		{"go1.27.0", true},
	} {
		if got := meetsFloor(tc.v); got != tc.want {
			t.Errorf("meetsFloor(%q) = %v, want %v", tc.v, got, tc.want)
		}
	}
}
