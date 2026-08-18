// Package timers makes "wake me at this time" a row in the same transaction as
// the thing that needs waking.
//
// # The problem
//
// A seat is held for fifteen minutes. Something has to free it when the booking
// that took the hold never finishes — and that something cannot be the booking,
// because the case that matters is precisely the one where the booking process
// is dead. Nothing failed, no error was logged, no message arrived: seat 14A is
// simply unsellable forever.
//
// A time.AfterFunc solves this for exactly as long as the process lives. Restart
// the service and every pending deadline is gone, silently, with no way even to
// enumerate what was lost.
//
// # Why the obvious fixes do not work
//
// Give the hold an expiry column and treat an expired hold as free. Then the
// hold ends without an event, so nothing is published and the saga waiting on
// that seat waits forever. Worse, "is this hold live?" becomes a question about
// the current time rather than about the stream, so two readers a millisecond
// apart can disagree, and replaying that history next year produces different
// state than it did on the day.
//
// Schedule the wake-up right after the transaction commits. The gap between the
// commit and the schedule is small and it is fatal: a crash inside it leaves a
// hold that exists with no deadline attached, which is the original problem with
// extra steps.
//
// Cancel the timer when the hold is released. A release at 14:59:59 and a
// deadline at 15:00:00 race, and no lock removes that race — so a late fire has
// to be harmless regardless. Once it is harmless, cancelling is dead code that
// can fail.
//
// Elect a leader, as the outbox poller does. The outbox needs one because its
// effect, the Kafka publish, happens outside the row's transaction. A timer's
// whole effect is inside it, so two schedulers racing one row settle it between
// themselves: the loser's claim reports no rows and it rolls back.
//
// # What this package does
//
// One table. A row is scheduled inside the caller's transaction, so the deadline
// and the event that created it commit together or not at all. A scheduler reads
// rows the clock has reached and, for each, opens one transaction that claims the
// row and applies its effect together.
//
// The claim is an UPDATE reporting rows-affected rather than raising, so a timer
// another scheduler already took is an ordinary branch instead of an error that
// would poison the rest of the commit.
//
// Two columns carry all the meaning. Subject names the stream the timer is
// about; token records what that stream looked like when the timer was set. A
// handler compares the token against the stream it just loaded, and a mismatch
// means the world moved on.
//
// # What it deliberately does not do
//
// It does not know what a timer means. Firing hands the row to a Firer, which
// decides; this package never appends an event and never publishes anything.
//
// It has no clock of its own. Deadlines are passed in, so a test controls one
// with a value rather than by waiting.
//
// It does not cancel, reschedule, or deduplicate fires. A row fires once, and
// whether that fire does anything is the Firer's business.
//
// It is not a cron. There is no recurrence and no schedule expression.
//
// # Reading order
//
// Start in timers.go with Schedule, Due and MarkFired. Those three are the whole
// data model, and MarkFired is where the safety lives. Then scheduler.go for the
// loop that calls them, whose only subtlety is how long it waits between passes.
//
// # Where this comes from
//
// Design spec §10.5, two timers deliberately separate — the seat-hold TTL owned
// by inventory, the saga step timeout owned by booking — and §7.2, where a
// scheduled timer joins the outbox row and the inbox row in one commit.
package timers
