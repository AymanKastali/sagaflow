// Package inventory decides who gets a seat when two requests for it arrive
// at the same instant, by treating each seat as an append-only stream rather
// than a row that can be overwritten.
//
// # The problem
//
// Two customers browsing the same flight both tap "hold" on seat 14A close
// enough together that neither request can see the other's effect before
// committing to its own. Exactly one of them must get the seat; the other
// must be told no, not eventually and not after a support ticket, but as the
// direct answer to the request it just made. And no number of retries by the
// loser may ever produce a second hold on 14A — a retried decision that has
// been overtaken by events has to come back a refusal, not a duplicate.
//
// # Why the obvious fixes do not work
//
// A seats table with a held_by column, read then write: read whether the
// column is set, and if not, write your own id into it. Between the read and
// the write is a window with no lock in it, and two requests that both read
// "empty" both proceed to write — the second write simply overwrites the
// first, with no error and no trace that a collision happened.
//
// Wrap that same read-then-write in SELECT … FOR UPDATE: the window closes,
// but the lock is held for as long as the transaction takes, not as long as
// the write takes. Every other request touching that row — including a
// second customer who only wants to check whether 14A is still free — queues
// behind whatever the lock holder's application code is doing, for exactly
// as long as that code takes to decide.
//
// An application-level mutex around the hold logic: this works, right up
// until there are two instances of the service, which is the normal
// deployment shape for anything that needs to survive one instance dying.
// Two processes, two independent mutexes, guarding nothing against each
// other.
//
// # What this package does
//
// A seat is a stream, so the conflict is caught by UNIQUE(stream_id,
// version) at the instant of the append — no lock, no window, because
// nothing is checked and then acted on separately. Two holds for 14A both
// fold from version 0 and both attempt to append version 1; Postgres accepts
// one and rejects the other with ErrVersionConflict. The loser does not retry
// its old decision — it reloads the stream, sees the SeatHeld that beat it,
// and re-decides. Re-deciding is what turns the loss into a refusal: replaying
// the original decision would append a second hold onto a seat that is no
// longer free.
//
// Every command gets a reply; only a change to the seat gets an event.
// SeatUnavailable answers a losing racer but is never appended, because
// nothing happened to the seat — appending it would grow 14A's history by one
// row for every customer who asked and lost. Nor may a decision answer with
// silence: a saga step that hears nothing back re-dispatches the same command
// forever, so even a no-op release still produces a reply.
//
// The decision functions have no clock. A hold is live until an event ends
// it — a release, eventually an expiry — never until a clock says the TTL has
// passed. That absence is what stops a new hold racing an expiry: if "held"
// meant "held, unless now() has passed expires_at", two goroutines evaluating
// now() a millisecond apart could disagree about whether the seat was free.
// Expiry will itself be an event, SeatHoldExpired, appended by a timer this
// package owns and has not built yet — a clock is still involved, just not
// inside the decision.
//
// # What it deliberately does not do
//
// This package does not know Kafka exists. Its dependency list ends at the
// outbox row it writes in the same transaction as the seat's events —
// confirmed by `go list -deps ./internal/inventory`, which names eventstore,
// inbox, outbox, envelope, codec and pg and nothing that speaks the wire
// protocol. Turning that row into a Kafka message is the outbox poller's job;
// the boundary is deliberate, because it is what lets this package be tested
// against nothing but Postgres.
//
// # Reading order
//
//	seat.go      SeatState, Fold, Decide, Hold, Release — the pure decision
//	             functions. No context, no database, no clock. Start here.
//	store.go     LoadSeat and AppendSeat — the same fold and append, now
//	             wrapped around a real transaction.
//	errors.go     ErrUnknownEvent and ErrUnknownCommand.
//	commands.go  Handler.Handle — the one-stream-per-transaction glue that
//	             calls both. It only makes sense once seat.go's decisions and
//	             store.go's transaction boundary are already understood.
//
// # Where this comes from
//
// Design spec §6.3 (a seat is one stream, and why that is safe), §7.2 (one
// transaction writes exactly one stream, plus its outbox rows, plus its inbox
// row), §9.3 (a compensation such as ReleaseSeatHold must never dead-letter,
// so it always gets a reply), §10.3 (three immediate reload-retries before a
// conflict is given up on), §10.5 (why the seat's own timer, not the saga's
// step timeout, has to own expiry).
package inventory
