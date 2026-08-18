package timers_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/AymanKastali/sagaflow/internal/testsupport/pgtest"
)

// One Postgres for the whole package. This package owns no migrations of its
// own — the timers table is per-service schema — so the tests borrow
// inventory's, exactly as the outbox and inbox tests do.
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
