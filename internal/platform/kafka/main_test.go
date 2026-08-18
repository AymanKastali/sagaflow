package kafka_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/AymanKastali/sagaflow/internal/testsupport/kafkatest"
)

// One broker for the whole package. Isolation comes from topic names and
// consumer groups derived from the test, not from a container per test.
//
// This package needs no registry: framing against one is platform/schema's job,
// and these tests only produce and consume bytes.
func TestMain(m *testing.M) {
	stop, err := kafkatest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}
