// Package inventory decides who gets a seat when two requests for it arrive
// at the same instant, by treating each seat as an append-only stream rather
// than a row that can be overwritten.
//
// # The problem
//
// Two customers browsing the same flight both tap "hold" on seat 14A close
// enough together that neither request can see the other's effect before
// committing to its own. Exactly one must get the seat; the other must be told
// no, not eventually and not after a support ticket, but as the direct answer
// to the request it just made. And no number of retries by the loser may ever
// produce a second hold on 14A: a decision overtaken by events has to come
// back a refusal, not a duplicate.
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
// the write takes. Every other request touching that row — including a second
// customer who only wants to know whether 14A is free — queues behind whatever
// the lock holder's application code is doing, for as long as it takes.
//
// An application-level mutex around the hold logic: this works right up until
// there are two instances of the service, which is the normal shape for anything
// that survives one instance dying. Two processes, two mutexes, guarding nothing.
//
// # What this package does
//
// A seat is a stream, so the conflict is caught by UNIQUE(stream_id, version)
// at the instant of the append — no lock, no window, because nothing is checked
// and then acted on separately. Two holds for 14A both fold from version 0 and
// both attempt to append version 1; Postgres accepts one and rejects the other
// with ErrVersionConflict. The loser does not retry its old decision — it
// reloads, sees the SeatHeld that beat it, and re-decides. That is what turns
// the loss into a refusal: replaying it would hold a seat that is no longer free.
//
// Every command gets a reply; only a change to the seat gets an event.
// SeatUnavailable answers a losing racer but is never appended, because nothing
// happened. Nor may a decision answer with silence: a saga step that hears
// nothing re-dispatches forever, so even a no-op release gets a reply.
//
// The decision functions have no clock. A hold is live until an event ends it,
// never until a clock says its TTL has passed: if "held" meant "held, unless
// now() is past expires_at", two goroutines reading now() a millisecond apart
// could disagree about whether the seat was free. Expiry is itself an event,
// SeatHoldExpired, appended by a timer this package owns.
//
// A hold therefore ends in one of three ways, all of them events on the seat's
// own stream: the saga releases it, the saga confirms it, or its deadline passes
// and inventory expires it. The third exists because the first two need the
// booking service alive, and 14A must come back even when it is not.
//
// Alongside the streams is one derived table, seat_availability, so that
// showing a customer a seat map does not mean folding three hundred streams. It
// is allowed to be out of date, because nothing decides from it: the worst a
// stale row can do is offer a seat that has just gone, and the answer to that is
// the refusal above. It is brought up to date by re-reading the seat whose
// stream changed, so the same notification twice, or two out of order, both
// land on the current state.
//
// The package also assembles itself into a running service: two consumer groups
// — one applying commands, one keeping the view in step — plus the outbox
// poller and the timer scheduler, four loops that stop together when their
// context is cancelled. Cancelling is the whole of shutdown, so a test can kill
// this service mid-flow exactly when it wants to.
//
// # What it deliberately does not do
//
// It does not treat a passed deadline as making a hold false. The hold is held
// until SeatHoldExpired is appended, which is why nothing here needs a now().
//
// It does not cancel a timer when a hold is released early. The release and the
// deadline can arrive in either order, so a late fire has to be harmless
// regardless — and once it is harmless, cancelling it is dead code that can fail.
//
// It does not know which seats exist. A seat nobody has ever held has no stream
// and so no row in the view: what is derived from events can only describe what
// has happened. A seat map is reference data, and no event here carries it.
//
// It does not decide anything in the two files that speak to Kafka. Every
// decision stops at the outbox row written beside the seat's events — seat.go,
// store.go, commands.go, expiry.go and projection.go never mention a broker —
// while consumers.go and wire.go carry no decisions. That split is what lets
// everything worth testing be tested against nothing but Postgres.
//
// # Reading order
//
//	seat.go        SeatState, Fold, Decide, Hold, Release, Expire — the pure
//	               decision functions. No context, no database, no clock.
//	store.go       LoadSeat and AppendSeat — the same fold and append, now
//	               wrapped around a real transaction.
//	errors.go      ErrUnknownEvent and ErrUnknownCommand.
//	commands.go    Handler.Handle — the one-stream-per-transaction glue that
//	               calls both, and only makes sense after the two above.
//	expiry.go      Expirer.Fire — commands.go's shape with one thing missing.
//	               Read them side by side: the absent inbox row is the lesson.
//	projection.go  Projector and the read side. The only file here that is
//	               allowed to be wrong for a moment.
//	consumers.go   The two Kafka handlers: which failures are permanent, and
//	               why the projection one never decodes a payload.
//	wire.go        New, Run, Close — the four loops. Last, because it only
//	               assembles what every file above already does.
//
// # Where this comes from
//
// Design spec §6.3 (a seat is one stream, and why browsing availability is a
// deliberately stale projection), §6.4 (why a rebuild enumerates streams rather
// than scanning by a cursor, and why the live view consumes from Kafka), §7.1
// (wire.go returning a runnable service is a hard requirement), §7.2 (one
// transaction writes exactly one stream, plus its outbox rows, its inbox row and
// any deadline it scheduled), §9.3 (a compensation must never dead-letter),
// §10.2 (an inbox row is for an effect that cannot be repeated, which is why the
// projection has none), §10.3 (three immediate reload-retries), §10.5 (the
// seat's own timer owns expiry), §12.3 (kill a service by cancelling it).
package inventory
