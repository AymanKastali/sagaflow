// Package codec converts between generated protobuf messages and the rows
// the event store persists them as.
//
// # The problem
//
// An event written today has to be readable years from now: replayed to
// rebuild a projection, inspected by a human during an incident, decoded by
// a service that may not even exist yet. Whatever format the event store
// keeps has to survive all three without depending on anything that might be
// down at the time — including a schema registry.
//
// # Why the obvious fixes do not work
//
// Store the raw protobuf bytes: compact, but opaque. A row in the events
// table becomes a wall of binary that psql cannot show and a human cannot
// read during an incident, and decoding it back into a message ties every
// replay to a protobuf descriptor being available — in practice, to a schema
// registry being reachable at read time, not just at write time.
//
// Store hand-written JSON: readable, but it drifts. Nothing forces the JSON
// shape to track the .proto file, so the day someone adds a field to the
// message and forgets to update the hand-written mirror, the stored events
// and the current schema disagree and nobody notices until a replay breaks.
//
// Store the Confluent-framed wire bytes as they arrive from Kafka: this
// couples the database to the transport. The magic byte, the schema id and
// the message index are framing for Kafka, not properties of the event
// itself, and if that framing scheme ever changes — a different registry, a
// different header — every previously stored row would need rewriting to
// match, which is not something you can do to history.
//
// # What this package does
//
// One schema, two encodings, both generated from the same .proto file so
// they cannot drift apart: protojson for storage, binary protobuf for the
// wire. Encode marshals a message to protojson with UseProtoNames, so the
// stored JSON uses the same field names as the .proto file — hold_id, not
// holdId — rather than protojson's default lowerCamelCase, which would give
// the database a different vocabulary than the schema it is supposed to
// mirror. Decode reverses this: it looks the event's type name up in
// protobuf's global registry and unmarshals into a fresh instance of that
// type.
//
// The sharp edge: protojson's output is not byte-stable across library
// versions. A dependency bump can reorder fields or change whitespace
// without changing what the JSON means. Stored JSON must therefore never be
// hashed, signed, or compared byte-for-byte — only decoded and compared as
// values. A checksum computed today and re-verified after a routine
// `go get -u` is not testing the data, it is testing the exact bytes one
// version of protojson happened to produce, and it will fail for a reason
// that has nothing to do with the event.
//
// An event's type name is looked up in protobuf's global type registry,
// which is populated at process start by the generated packages' own init
// functions — so resolution is a local map lookup, not a network call, and
// replay does not depend on a registry being reachable. But that also means
// an unknown type name is a permanent failure, not a transient one: no
// amount of retrying or waiting adds a message type to a compiled binary's
// registry. The type is either linked in or it never will be, so Decode
// fails immediately with ErrUnknownType and the caller dead-letters the
// message rather than retrying it.
//
// # What it deliberately does not do
//
// This package does not put anything on the wire and does not know Kafka
// exists. Framing bytes — the magic byte, the schema id, the message index
// — belong to platform/schema, which wraps this package's binary protobuf
// output in the Confluent format before a message is produced, and strips
// that framing back off before a message reaches this package's Decode.
// codec only ever sees a bare protobuf message on one side and a JSONB row
// on the other.
//
// # Reading order
//
//	codec.go   Encode, Decode, TypeName, and the package's errors. The whole
//	           package.
//
// # Where this comes from
//
// Design spec §8.2 (protobuf as the payload format and its field-evolution
// discipline), §8.4 (one schema, two encodings, and the byte-stability
// warning), §10.2 (why an unknown type dead-letters instead of retrying).
package codec
