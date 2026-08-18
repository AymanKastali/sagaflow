// Command schemactl registers every message type's .proto schema with the
// schema registry, once, before any service that depends on it starts.
//
// # The problem
//
// Every message type on the wire needs a schema in the registry before a
// service can produce or consume it — a serde resolves every schema id when
// it is constructed, and refuses to construct at all if the registry has no
// such subject, so a missing schema stops a service at startup. That
// registration has to happen somewhere, run by someone, at some point in the
// deployment sequence, and it has to have happened by the time the first
// service that needs it starts.
//
// # Why the obvious fixes do not work
//
// Auto-registration on first publish looks free: a service that meets a
// schema it does not recognise just registers it and moves on, no separate
// tool required. But then whichever service happens to start first is the
// one that defines the contract — an implementation detail of deployment
// order, not a decision anyone reviewed. And the schema that reaches the
// registry this way reaches it at runtime, in production, the first time
// that code path executes — not in CI, where a bad one could still be
// rejected before it shipped anywhere.
//
// # What this package does
//
// schemactl is run by an operator or a deployment pipeline (make
// schemas-register), never by a service. It pins the registry's
// compatibility level to BACKWARD before registering anything, then walks a
// fixed table of topic-message-.proto-file bindings and registers each one
// under its TopicRecordNameStrategy subject. The binding table is
// hand-written, not discovered by reflection, so what gets registered is
// visible as a diff in this file rather than inferred from whatever types
// happen to be linked into the binary.
//
// Because registration is explicit and separate from every service's
// startup path, a service that finds its subject missing fails to start.
// That is the failure mode this tool exists to produce: caught at rollout,
// pointing at the operator who forgot to run it, rather than surfacing as a
// decode failure against production traffic sometime later.
//
// # What it deliberately does not do
//
// schemactl does not run inside any service, is not imported by one, and is
// not a library — its only reader is an operator or a CI job invoking the
// binary. It does not decide compatibility policy beyond pinning the one
// level this repository uses; and it does not touch anything on the
// consuming side of a schema id lookup, which is internal/platform/schema's
// job, not this command's.
//
// # Reading order
//
//	main.go   The binding table, compatibility pinning, then registration.
//
// # Where this comes from
//
// Design spec §8.3 (the three enforcement layers, and why services never
// auto-register), §8.5 (Apicurio and its path-scoped compatibility API,
// which is what the -registry flag's default URL points at), decision D14
// (schemas registered by CI only).
package main
