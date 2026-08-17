package version

import (
	"runtime"
	"strings"
	"testing"
)

func TestGoVersionIsAtLeast1_26_6(t *testing.T) {
	got := runtime.Version()
	if !strings.HasPrefix(got, "go1.26.") {
		t.Fatalf("built with %s, want go1.26.x — see Global Constraints", got)
	}
	if got == "go1.26.5" {
		t.Fatalf("built with %s, want go1.26.6 or newer", got)
	}
}
