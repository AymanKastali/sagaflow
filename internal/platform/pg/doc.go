// Package pg is the Postgres plumbing shared by every service: a connection
// pool, a transaction helper every handler runs inside, and a migration
// runner.
//
// # The problem
//
// Every service needs the same three things — a configured pool, a way to run
// work inside a transaction that cannot leak a connection, and a way to apply
// schema migrations — and getting any one of them subtly wrong shows up as a
// production incident, not a test failure. A leaked transaction exhausts the
// pool under load. A migration applied by hand on one machine can drift from
// what the code running elsewhere expects.
//
// # Why the obvious fixes do not work
//
// A bare `defer tx.Rollback()`, with the commit's error dropped, silently
// swallows a failed commit: `Rollback` after a successful `Commit` just
// returns `pgx.ErrTxClosed` and is ignored, so nothing distinguishes that case
// from a `Commit` that failed and was never checked — the function returns as
// if the write landed. Running migrations from a separate tool lets the
// schema and the code that needs it deploy apart, so a service can start
// against a schema the code assumes but the database has not reached yet.
// Using a second database driver purely for migrations doubles the
// connection configuration — DSN, TLS, pool limits — that has to be kept in
// step, and the two copies can drift out from under each other.
//
// # What this package does
//
// Open builds a pool from a DSN with MaxConns 10 and MinConns 2, then calls
// Ping before returning, so a bad DSN or an unreachable database fails at
// startup rather than on the first query.
//
// WithTx makes the transaction's lifetime the function's lifetime. Rollback
// is deferred unconditionally rather than called on the error path, so it
// also runs if the function panics, releasing the connection before the
// panic propagates. Its own error is discarded: after a successful Commit it
// is a harmless ErrTxClosed, and a genuine rollback failure means the
// connection is already gone. The result: a nil return commits, an error
// return or a panic rolls back, and the caller has nowhere to forget to
// close the transaction.
//
// Migrate runs tern against a single connection rather than the pool,
// because tern holds a session-level advisory lock for the migration's
// duration — the mechanism that makes two replicas racing to migrate on
// startup safe: the second one blocks, then finds nothing left to do. tern
// is used because it is pgx-native, so migrations and application code share
// one driver and one DSN, leaving exactly one place connection configuration
// lives instead of two that can disagree.
//
// # What it deliberately does not do
//
// It knows nothing about events, messages or domains. This is the one
// package in the platform layer that would look the same in a project with
// no Kafka in it at all, and that is worth saying plainly: a reader chasing
// the distributed-systems content can skip this file and lose none of it.
//
// # Reading order
//
//	pg.go       Open and WithTx — start with WithTx.
//	migrate.go  Migrate and the advisory-lock reasoning.
//
// # Where this comes from
//
// Design spec §5 (infrastructure and pinned versions) and §12.4 (test
// determinism, the reasoning behind Open's pool settings).
package pg
