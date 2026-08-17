package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// EnsureBackwardCompatibility pins the registry's global compatibility level to
// BACKWARD and confirms it took.
//
// This is the second of the three layers spec §8.3 asks for, sitting between `buf
// breaking` in CI and a producer that fails closed on a missing schema id. It is
// deliberately weaker than it sounds, and §8.3 is explicit about why: the registry
// checks a schema when it is *registered*, not when a message is produced. Nothing
// in an open-source Kafka can stop a determined producer from publishing arbitrary
// bytes, so this is defence against mistakes, not against bypass.
//
// The level is global rather than per-subject because subjects added in later
// phases then inherit it with no extra call, and because setting a per-subject
// override requires the subject to already exist — which it does not on a
// registry's first run. Enforcement is inherited: a subject with no override of its
// own is still checked against the global level.
func EnsureBackwardCompatibility(ctx context.Context, cl *sr.Client) error {
	// With no subjects passed, both calls address the global config.
	if _, err := globalLevel(cl.SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatBackward})); err != nil {
		return fmt.Errorf("kafka: set global BACKWARD compatibility: %w", err)
	}

	// Read the level back rather than trusting what SetCompatibility reported: it
	// unmarshals the registry's response *over the value it sent*, so a response
	// that omitted the field would still echo BACKWARD. The GET decodes a different
	// JSON field name, which makes it an independent observation.
	//
	// Global on purpose: Apicurio answers GET /config/{subject} with NONE for a
	// subject that has no override of its own, so a per-subject read-back would
	// report failure while enforcement was in fact active.
	level, err := globalLevel(cl.Compatibility(ctx))
	if err != nil {
		return fmt.Errorf("kafka: read back global compatibility: %w", err)
	}
	if level != sr.CompatBackward {
		return fmt.Errorf("kafka: global compatibility is %s, want BACKWARD", level)
	}
	return nil
}

// globalLevel reduces franz-go's one-result-per-registry-URL slice to a single
// answer: the first error, or the level the registry reported.
func globalLevel(results []sr.CompatibilityResult) (sr.CompatibilityLevel, error) {
	var level sr.CompatibilityLevel
	for _, res := range results {
		if res.Err != nil {
			return level, res.Err
		}
		level = res.Level
	}
	return level, nil
}
