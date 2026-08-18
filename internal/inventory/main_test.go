package inventory_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
)

// One Postgres for the whole package. Isolation comes from database names and
// stream ids derived from the test, not from a container per test — a fresh
// container per test would make the suite far slower for no added safety.
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
