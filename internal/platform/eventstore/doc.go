// Package eventstore is an append-only log of events grouped into streams,
// where each event claims the next version number in its own stream and the
// database refuses any second event that claims the one already taken.
//
// # The problem
//
// A row updated in place destroys the only evidence of how it got there: after
// the third update there is no way to ask what the first two were, only what
// the row is now. Two writers updating that same row concurrently make it
// worse — both read the current value, both compute a new one, and whichever
// writes last simply replaces the other's write, with no error, no warning,
// and no trace that the lost write ever happened. A held seat can silently
// un-hold itself, or a released seat can look held, because one update
// overwrote the other and nothing recorded that it happened.
//
// # Why the obvious fixes do not work
//
// SELECT … FOR UPDATE looks like the fix: lock the row, read, decide, write,
// release. It works, but the lock is held for as long as the caller's
// business logic takes, not as long as the write takes, so every other
// request touching that row — including ones that only want to read it —
// queues behind whatever the lock holder is doing.
//
// Comparing a last_updated timestamp before writing avoids the lock, but
// trades it for a race of its own: writers on different application instances
// can have clocks skewed by more than the gap between their reads, so the
// "newer" write is decided by whose clock is ahead, not who actually went
// second. Two writes issued inside the same timestamp's resolution look
// identical, so the comparison passes when it should fail — the fix silently
// stops working exactly when writes are frequent enough to matter.
//
// Retrying on any error avoids both, but throws away the one fact a caller
// needs: whether the error was another writer got there first or the network
// blipped. Retrying the first case as if it were the second resends a
// decision made against state that no longer exists; giving up on the second
// case as if it were the first abandons a write that would have succeeded. A
// bare error cannot tell you which one you are in.
//
// # What this package does
//
// Append writes a batch of events to a stream at versions expectedVersion+1
// … +n in a single INSERT built from unnest, so the database numbers the rows
// and no loop can miscount. expectedVersion is the version the caller's
// in-memory state was folded from. The enforcement is UNIQUE(stream_id,
// version), not code: if another writer already claimed expectedVersion+1,
// the INSERT hits that constraint, Postgres reports it as SQLSTATE 23505, and
// Append translates that into ErrVersionConflict. There is no window between
// checking and acting for a second writer to land in, because nothing checks
// — the constraint either lets the row exist or it does not.
//
// Everything this buys depends on what a stream is chosen to be, and that
// choice is this package's whole design. Here, each seat is its own stream, so
// two attempts to hold the same seat are two attempts to append version 1 to
// the same stream id, and the constraint lets exactly one through — "held at
// most once" is not a rule anyone enforces, it falls out of the schema. A
// stream per flight instead would make booking seat 3A collide with booking
// seat 22F, because both would append into the same version sequence — every
// seat serialised against every other seat on the aircraft. A stream per
// booking instead would let two bookings racing for the same seat each start
// at version 0 in their own stream, so their INSERTs would never collide —
// the conflict is real but invisible to keys that never compare against each
// other.
//
// # What it deliberately does not do
//
// It does not decide anything. Append does not know a stream is a seat, and
// Load does not know that a returned event means held rather than released —
// that judgment is folded from the events by the caller. It does not know
// what an event payload means beyond a type name and a JSON blob.
//
// It takes a pgx.Tx, never a pool, and never opens its own transaction: the
// caller's transaction is what lets an event append share one commit with an
// outbox row and an inbox mark, and a transaction opened inside this package
// could not be joined to those from outside it.
//
// global_seq is for a human running ad-hoc SQL, never for a consumer to
// track. It is a BIGSERIAL, assigned when a row is inserted but visible to
// other transactions only once its own commits — and commit order need not
// match assignment order. A transaction assigned a lower number can commit
// after one assigned a higher number has already committed, so a consumer
// scanning WHERE global_seq > cursor reads past the higher number, advances,
// and never comes back for the lower one. That loss appears only under
// concurrency, so it passes on a quiet machine and fails in production.
//
// # Reading order
//
//	eventstore.go  Append and Load. Start with Append's SQL.
//	errors.go      ErrVersionConflict, and the Postgres code it is mapped from.
//
// # Where this comes from
//
// Design spec §6.1 (schema), §6.2 (append and optimistic concurrency), §6.3
// (stream boundaries), §6.4 (why global_seq is diagnostic only).
package eventstore
