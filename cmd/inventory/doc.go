// Command inventory is the process that makes the inventory package a running
// service: it holds seats, expires the holds nobody released, publishes what it
// decided, and keeps the availability view in step.
//
// # The problem
//
// internal/inventory is a library. It can fold a seat's history, decide a hold,
// write events and an outbox row in one transaction, and re-derive a row in the
// availability view — but nothing in it ever runs. Nobody is polling the outbox
// table, so the messages it wrote sit there and the saga that asked for the seat
// waits forever. Nobody is watching the timer table, so a hold taken by a
// booking that then crashed keeps that seat out of circulation permanently.
// Nobody is consuming inventory.commands, so no command ever arrives to begin
// with.
//
// There is a second, quieter problem. A system whose failure modes are the
// interesting part has to be able to fail on demand, and "the service died in
// the middle of a saga" is the failure worth demonstrating most. If the only
// way to run this service is to start a process, then the only way to kill it
// mid-flow is to stop a container — from a test, with the timing of the crash
// left to whatever the shutdown took.
//
// # Why the obvious fixes do not work
//
// Assemble everything inside main: open the pool, build the consumers, start
// the poller, block. It runs, and nothing else can. A test that wants a running
// inventory has to shell out to a binary, and the assembly itself — which
// consumer group, which handler, whether the timer scheduler was started at all
// — is the one part of the service no test ever executes. Forgetting a loop
// there produces a service that looks healthy and quietly stops expiring holds.
//
// Give tests their own assembly instead, a small harness that wires up the
// pieces it needs. Now two arrangements exist, and the tested one is not the
// shipped one. The failure this invites is specific: a handler wired to the
// wrong consumer group, or a poller nobody starts, is invisible to a suite that
// builds its own topology.
//
// Start the loops lazily, on the first message, to avoid the assembly question
// entirely. Then a service with no traffic expires no holds — and expiry exists
// precisely for the case where the traffic stopped because the other side died.
//
// # What this package does
//
// It reads three addresses from flags — the database, the brokers, the schema
// registry — and hands them to inventory.New, which does all of the assembly
// and all of the failing: a database it cannot reach, a schema nobody
// registered, a broker that is not there. Then it calls Run, which is the
// service's four loops, and blocks.
//
// SIGINT and SIGTERM cancel the context rather than killing the process. That
// is what makes the crash reproducible: stopping this binary and cancelling a
// context in a test are the same code path, so a test can kill the service at
// any instant it chooses and then start another one against the database the
// first one left behind. What that second process finds is exactly what a real
// crash leaves: whatever committed, and nothing else.
//
// The defaults match the local Compose stack, so running it with no arguments
// against a started stack works.
//
// # What it deliberately does not do
//
// It serves no HTTP. Nothing here answers a customer; the entry point to the
// whole system is booking's API, and inventory only ever hears from Kafka.
//
// It registers no schemas. A service that met an unregistered subject and
// registered it would become the author of a contract other services decode
// against, by accident of deployment order. schemactl does that, deliberately,
// as its own reviewed step — and a service started before it has run fails at
// startup, which is the intended outcome.
//
// It makes no decisions of its own. Which failures are permanent, what a hold
// does to a seat, when a deadline fires: all of that is internal/inventory's,
// and this file would be the wrong place to read any of it.
//
// # Reading order
//
//	main.go   Flags, signals, New, Run. Twenty lines, and all of the interest
//	          is behind inventory.New — read internal/inventory/wire.go next.
//
// # Where this comes from
//
// Design spec §7.1 (wire.go returning a runnable service is a hard requirement,
// and cmd/<service>/main.go is config plus New plus Run), §12.3 (cancel a
// service's context mid-saga, construct it again, and assert the flow resumes —
// which is why shutdown is a cancellation rather than an exit), §8.3 (services
// never auto-register a schema, so this one fails to start instead), §5 (the
// Compose ports the flag defaults point at).
package main
