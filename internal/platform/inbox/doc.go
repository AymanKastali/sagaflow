// Package inbox makes "have I handled this message" and "did I apply its
// effects" the same fact, decided by one row written inside one transaction.
//
// # The problem
//
// Kafka delivers at least once, not exactly once. A consumer that crashes
// after applying a message but before its offset commits will be handed that
// same message again on restart or rebalance, and a producer retry after a
// timeout can put a duplicate on the topic in the first place. A handler that
// reacts to a redelivered SeatHeld by holding a seat, charging a payment, or
// appending an event does not error on the second delivery — it holds two
// seats, charges twice, or writes the same event into a stream that assumed
// each message arrived once. Nothing about the failure looks like a failure;
// the system appears to have worked, twice.
//
// # Why the obvious fixes do not work
//
// Checking "have I seen this event_id?" before starting the transaction that
// applies the effects splits the check from the apply into two separate
// statements, leaving a window between them. A crash after the check passes
// but before the mark is written loses the mark entirely, so redelivery walks
// straight through the same check again; a crash the other way round leaves
// the mark recorded but the effect never applied. Either order, under
// concurrency, two deliveries can interleave through the same gap.
//
// Writing every handler to be naturally idempotent — an upsert by id instead
// of an increment, "set held=true" instead of "add one hold" — works only for
// operations that happen to have that shape. Charging a payment does not;
// appending an event to a log does not. The property also cannot be checked:
// nothing stops a later edit to the handler removing it quietly, and the loss
// is silent — tests keep passing until a redelivery reaches production.
//
// Kafka's own exactly-once semantics guarantee that a chain of reads and
// writes confined to Kafka itself — consume, transform, produce — commits
// atomically or not at all. Applying a message here means writing to
// Postgres. Kafka's transaction coordinator has no reach into a Postgres
// transaction, so this guarantee covers a problem this package does not have
// and leaves the one it does have untouched.
//
// # What this package does
//
// MarkConsumed inserts (consumer, source, event_id) as the first statement of
// the same transaction that goes on to apply the message's effects, using
// INSERT ... ON CONFLICT (consumer, source, event_id) DO NOTHING. Because the
// mark and the effect share one transaction, they cannot come apart in either
// direction: a redelivered message finds its own row already there and is
// skipped before anything else runs, and a handler that fails partway rolls
// its mark back along with everything else, so the next delivery finds no row
// and can retry cleanly.
//
// A conflict is detected by rows-affected rather than by catching a unique
// violation, because a raised error would abort the whole transaction — every
// later statement fails, and COMMIT silently degrades into a rollback.
// MarkConsumed's bool return distinguishes "already handled" from "freshly
// recorded"; an error return is reserved for something actually going wrong.
//
// source and event_id are a message's CloudEvents ce_source and ce_id, a pair
// the specification already guarantees is unique. The deduplication key is
// therefore not invented here — it is an identity the producer already had to
// establish.
//
// consumer sits inside the primary key, not beside it as a filter, because
// more than one consumer inside a single service can read the same message
// for different purposes — a saga and a projection can both react to the
// same SeatHeld — and each must decide independently whether it has already
// handled this particular event. Keying on (source, event_id) alone would
// make the second consumer's insert collide with the first's and reject its
// own first delivery as a duplicate.
//
// Prune deletes rows older than olderThan, which must exceed Kafka's own
// retention window: a pruned row that is still within the topic's retention
// leaves a message that can still be redelivered with no record of ever
// having been handled.
//
// # What it deliberately does not do
//
// This package makes delivery duplicates harmless — the same message,
// redelivered by Kafka. It does not make business retries idempotent: two
// separate HoldSeat commands carrying two different ids are two commands as
// far as this package can see, whatever button click produced them. Telling
// "the same request, retried" apart from "a new request that looks similar"
// is a business idempotency key carried in the command itself, a different
// mechanism from this one.
//
// inbox is the delivery-side counterpart to outbox: outbox gets a message out
// of a committed transaction and onto Kafka without ever losing it; inbox
// gets a message back off Kafka and applied without ever double-applying it.
// The two are one idea split across the two directions a message travels.
//
// # Reading order
//
//	inbox.go   MarkConsumed, then Prune.
//
// # Where this comes from
//
// Design spec §10.2 (schema and the transaction-ordering rule this package
// encodes), §8.1 (the CloudEvents uniqueness guarantee behind source and
// event_id), §10.4 (business idempotency keys — the boundary this package
// does not cross).
package inbox
