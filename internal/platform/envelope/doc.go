// Package envelope puts a message's identity, type, and causation into Kafka
// headers instead of its payload, using CloudEvents v1.0.2 in binary content
// mode.
//
// # The problem
//
// A message crossing from one service to another needs metadata that has
// nothing to do with what it says: who sent it, what kind of thing it is,
// what it is about, and what caused it. A consumer needs the type before it
// can decode the body, needs an identity before it can tell "delivery
// attempt two" from "a new message", and needs a causation chain before it
// can explain, during an incident, why this message exists at all. If every
// service invents its own shape for that metadata, no consumer can route or
// deduplicate a message it did not produce itself — a topic that carries
// several event types becomes unreadable to anything that was not built by
// the same team on the same day.
//
// # Why the obvious fixes do not work
//
// Putting the metadata inside the payload means a consumer must decode the
// body, and already know its schema, before it can decide what to do with
// the message — routing and deduplication end up coupled to schema
// evolution, since a field renamed or moved inside the payload silently
// breaks whatever was reading it from there.
//
// Inventing a private header set works fine right up until the second team,
// the second language, or the first tool that expects a standard — a tracing
// system, a schema registry client, an off-the-shelf Kafka Connect sink —
// none of which will recognize headers this system made up.
//
// Using Kafka's own record metadata gives a timestamp and a key and nothing
// else: no type, no causation, and no identity distinct from the key, which
// is the target stream, not the message.
//
// # What this package does
//
// CloudEvents v1.0.2 defines a binary content mode: attributes travel as
// headers, and the payload is left alone as the message body. Envelope holds
// one message's identity (ID, Source), type, routing subject, and causation
// (CorrelationID, CausationID); Headers renders it onto the wire and Parse
// reads it back, rejecting anything that is not a well-formed CloudEvent
// this system can handle.
//
// ID and Source are specified by CloudEvents to be unique together, which is
// exactly the property idempotent consumption needs — so the deduplication
// key is something the format already guarantees rather than something this
// system invented and must defend. NewID supplies that ID as a UUIDv7, which
// sorts by creation time.
//
// traceparent is deliberately not ce_-prefixed: it is a W3C trace-context
// header, not a CloudEvents attribute, and giving it the ce_ prefix would
// claim it as one.
//
// Optional attributes — subject, correlation id, causation id, traceparent —
// are omitted entirely when unset rather than written as empty strings,
// because an absent ce_subject and an empty one are different statements:
// one says "not applicable", the other says "applies to nothing".
//
// # What it deliberately does not do
//
// This package knows nothing about brokers and nothing about payload
// encoding. It is a pure mapping between a struct and a map of strings, which
// is why it needs no infrastructure to test. Producing and consuming the
// headers it renders is platform/kafka's job; encoding the payload it leaves
// untouched is platform/codec's job.
//
// # Reading order
//
//	envelope.go   Envelope, Headers, Parse, NewID, and the Message the outbox
//	              and the DLQ both share. One file; start at the top.
//
// # Where this comes from
//
// Design spec §8.1 (the CloudEvents envelope and the ce_id/ce_source
// uniqueness that becomes the inbox deduplication key), §10.2 (why a message
// missing a required attribute is a permanent failure that dead-letters
// without retrying).
package envelope
