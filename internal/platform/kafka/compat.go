package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// EnsureBackwardCompatibility pins the registry's global compatibility level to
// BACKWARD. It is the second of the three layers spec §8.3 asks for, sitting
// between `buf breaking` in CI and a producer that fails closed on a missing
// schema id.
//
// The level is global rather than per-subject for two reasons. Subjects added in
// later phases inherit it with no extra call, and setting a per-subject override
// requires the subject to exist — which it does not on a registry's first run.
// Enforcement is inherited: a subject with no override of its own is still
// checked against the global level.
//
// This is deliberately weaker than it sounds, and §8.3 is explicit about why:
// the registry checks a schema when it is *registered*, not when a message is
// produced. Nothing in an open-source Kafka can stop a determined producer from
// publishing arbitrary bytes, so this is defence against mistakes, not bypass.
func EnsureBackwardCompatibility(ctx context.Context, cl *sr.Client) error {
	// With no subjects, SetCompatibility and Compatibility both address the global
	// config, which is exactly what is wanted here.
	for _, res := range cl.SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatBackward}) {
		if res.Err != nil {
			return fmt.Errorf("kafka: set global BACKWARD compatibility: %w", res.Err)
		}
	}

	// Read the level back instead of trusting what SetCompatibility reports.
	// SetCompatibility unmarshals the registry's response *over the value it sent*,
	// so a response that omitted the field would still echo BACKWARD — the check
	// would pass without observing anything. The GET decodes a different JSON field
	// name, which makes it an independent observation.
	//
	// The read is global on purpose: Apicurio answers GET /config/{subject} for a
	// subject with no override with NONE rather than the inherited level, so a
	// per-subject read-back would report failure while enforcement was in fact
	// active.
	for _, res := range cl.Compatibility(ctx) {
		if res.Err != nil {
			return fmt.Errorf("kafka: read back global compatibility: %w", res.Err)
		}
		if res.Level != sr.CompatBackward {
			return fmt.Errorf("kafka: global compatibility is %s, want BACKWARD", res.Level)
		}
	}
	return nil
}
