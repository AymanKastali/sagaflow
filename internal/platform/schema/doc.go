// Package schema frames protobuf messages for the wire against a Confluent-
// compatible schema registry, so a consumer can tell exactly which message
// type and which version of it a sequence of bytes was encoded from.
//
// # The problem
//
// A consumer reading a record off Kafka gets a slice of bytes and nothing
// else. Nothing in the record itself says what type those bytes are, which
// version of that type produced them, or whether decoding them with today's
// compiled types is still safe. Meanwhile the producer's schema keeps
// evolving: fields get added, and a saga that spans inventory, booking and
// payment services deployed on different days needs the consumer half of
// that change to keep working against messages the producer half already
// started sending.
//
// # Why the obvious fixes do not work
//
// One schema per topic — Kafka's and most registries' default assumption —
// breaks the moment a topic needs to carry a second message type, and these
// topics deliberately carry several: SeatHeld and SeatHoldReleased both travel
// on inventory.events, because splitting one topic per event type would lose
// the ordering guarantee a single partition gives events about the same seat.
//
// Trusting a type-name header works only halfway. A header saying
// "SeatHeld" tells a consumer what the message claims to be, but not which
// *version* of SeatHeld — a field added upstream by a producer that has
// already redeployed changes what those bytes mean without changing the one
// name a header carries.
//
// Auto-registering a schema the first time a producer publishes sounds like
// it solves both problems for free, but it moves the contract's definition
// to the worst possible moment: whichever service happens to start first
// writes the schema the registry then holds, and a malformed or badly
// evolved schema reaches the registry at runtime, in production, instead of
// being rejected by CI before anything shipped.
//
// # What this package does
//
// Subject implements TopicRecordNameStrategy: the subject a schema is
// registered and looked up under is <topic>-<fully.qualified.MessageName>,
// not just <topic>. Each event type on a topic then versions independently,
// which is what the default strategy cannot do.
//
// NewTopicSerde resolves every prototype's schema id from the registry once,
// at construction, and Encode never looks it up again. A registry outage
// during steady-state running therefore cannot stall a single publish — the
// id is already in hand — it can only block a service that is trying to
// start for the first time, which is the failure mode worth having: a
// dependency being down should stop a rollout, not a system already running.
//
// A framed payload is six bytes of header followed by the payload: a magic
// byte 0x00, a four-byte big-endian schema id, and a message-index path,
// which is where the one-message-per-file rule in this repository matters.
// The index path names which message inside a .proto file this is; every
// .proto file here holds exactly one, so the path is always [0], which the
// Confluent format shortens to a single zero byte. Add a second message to
// one of those files and the framing would still round-trip perfectly
// through this package's own Encode and Decode — but every other Confluent
// client, in any other language, would be reading the wrong index and would
// fail. That is what makes it a trap and not a bug: nothing here would ever
// tell you it happened.
//
// EnsureBackwardCompatibility pins the registry's global compatibility level
// so an incompatible schema is rejected when someone tries to register it.
// That check runs at *registration*, not when a message is produced, so it
// is defence against a mistake reaching the registry — not a guarantee that
// every byte on the wire matches a registered schema, which no open-source
// broker can enforce.
//
// # What it deliberately does not do
//
// This package knows nothing about brokers, partitions or offsets — that
// plumbing is platform/kafka's, and a serde here is handed to it as an
// argument rather than reaching for it itself. It does not decide what
// happens when a message fails to decode; that retry-or-dead-letter policy
// belongs to the consumer that calls Decode, not to Decode itself. And it
// never registers a schema on a producer's behalf: nothing in this package
// calls a create-subject endpoint, so the only way a subject gets into the
// registry is the reviewed path outside this package that puts it there
// before any service starts.
//
// # Reading order
//
//	serde.go   Subject, Serde, NewTopicSerde, the Confluent framing. Start here.
//	compat.go  EnsureBackwardCompatibility and the registry's global setting.
//
// # Where this comes from
//
// Design spec §8.3 (the three enforcement layers and why
// TopicRecordNameStrategy was chosen), §8.5 (Apicurio as the registry and its
// path-scoped compatibility API), and decision D14 — services never
// auto-register a schema.
package schema
