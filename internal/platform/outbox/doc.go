// Package outbox makes "the state changed" and "the message was sent" the
// same commit.
//
// # The problem
//
// A service that changes its own database and also needs to tell other
// services about that change has two separate systems to update, and no way
// to update them as one atomic step. Say the inventory service holds a seat:
// it writes the hold to its own Postgres database, then needs to publish a
// "seat held" message to Kafka so the saga can move on to the next step. If the
// process dies, or the network to the broker drops, at the instant between
// those two operations, one of two things happens depending on which came
// first: the hold is durable but nobody downstream was ever told, so it sits
// invisible until something else notices; or a message went out announcing a
// hold that the database transaction then failed to commit, so downstream
// now believes something that isn't true. Either way, the database and the
// broker disagree about what happened, and nothing in the system is watching
// for the disagreement.
//
// # Why the obvious fixes do not work
//
// Publish before committing: if the surrounding transaction then rolls back
// — a conflict, a validation failure, a serialization retry — the message
// already on the broker describes a change that never happened, and there is
// no way to recall it.
//
// Commit before publishing: the risky order is now reversed. If the process
// is killed in the gap between the commit and the publish call, the state
// change is durable but the announcement is gone, and nothing on disk
// records that one was owed.
//
// Wrap both in one distributed transaction: not available. Kafka has no
// participant for two-phase commit, so there is no protocol-level way to
// enlist it alongside Postgres. Even ignoring that, 2PC would mean every
// database write in the service now blocks on the broker being reachable and
// voting — a Kafka outage would stop writes that have nothing to do with
// messaging. Only once these three have failed does writing the message into
// the same transaction as the change look like the obvious move.
//
// # What this package does
//
// Enqueue inserts each outgoing message as a row in the outbox table, inside
// the caller's own transaction, so the message and the change it describes
// commit or roll back together by construction. A separate Poller later
// reads committed rows and publishes them, turning "publish" into a step
// that happens outside the original transaction and can be retried on its
// own.
//
// The poller claims rows by a published_at flag rather than by stepping a
// cursor over id. Ids come from a BIGSERIAL, handed out when a row is
// inserted but not visible to another transaction until that insert commits.
// Two concurrent writers can therefore commit out of insertion order, so a
// row with a lower id can become visible after one with a higher id already
// has — a cursor tracking "everything past id N" would step over the
// late-arriving row and never come back for it. A flag has no such gap:
// whatever is still unpublished gets claimed on the next pass, regardless of
// where its id falls. FOR UPDATE SKIP LOCKED on that claim exists for
// failover, not for spreading load across pollers — only one poller is ever
// meant to be active, and SKIP LOCKED just lets a newly elected one make
// progress instead of blocking on rows the previous, dying poller still
// holds.
//
// The NOTIFY that follows an enqueue is transactional: Postgres only
// delivers it if the transaction commits, and drops it if the transaction
// rolls back. A poller woken by it is therefore never woken to go looking
// for a row that turned out not to exist.
//
// # What it deliberately does not do
//
// This package does not deliver a message exactly once. The poller can
// publish successfully and then die before the row is marked published;
// on the next pass it finds the row still unmarked and publishes it again.
// The guarantee here stops at at-least-once, on purpose — correcting a
// redelivered message into an applied-exactly-once effect is the job of
// internal/platform/inbox, not this package.
//
// # Reading order
//
//	outbox.go   Enqueue and its SQL. Start here.
//	poller.go   Poller, Drain's claim-publish-mark cycle, election, pruning.
//
// # Where this comes from
//
// Design spec §10.1 (the outbox table and the poller loop), §10.2 (why the
// consumer on the other end has to deduplicate), §6.4 (the BIGSERIAL
// visibility reasoning behind claim-by-flag).
package outbox
