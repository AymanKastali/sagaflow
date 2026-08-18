// Package kafka wraps franz-go with the specific set of non-default settings
// a system that commits work to a database needs from a broker built for
// throughput, not for exactly-once bookkeeping.
//
// # The problem
//
// Kafka's client defaults are tuned to move records fast, and several of them
// quietly lose messages once "consuming a record" means "doing real work with
// it" — writing to a database, calling another service — rather than just
// reading it. A consumer that commits an offset before that work is durably
// done, or that lets a partition move to another member mid-handler, or that
// leaves a broken message parked at the head of a partition forever, is not
// misconfigured. It is running Kafka exactly the way Kafka expects to be run.
// That default behavior is wrong for a system whose whole point is that "the
// message was handled" and "the database changed" happen together.
//
// # Why the obvious fixes do not work
//
// Auto-commit on a timer — franz-go's default — commits the offset of every
// polled record on a schedule, whether or not the handler has finished with
// it. A crash between the poll and the handler's own completion advances the
// group past work that was never done, and nothing redelivers a committed
// offset: the message is simply gone.
//
// Commit right after the handler returns, but allow a rebalance at any point
// during the poll: a partition can be reassigned to another member while its
// records are still mid-handler on this one. The new owner resumes from the
// last committed offset, which lags behind what this handler is about to
// commit, so the same work either runs twice somewhere it wasn't expected or
// vanishes depending on which side loses the race.
//
// Retry a failing message forever: it never leaves the front of its
// partition, so every later record on that partition queues up behind it
// indefinitely. One malformed message now stalls an entire stream, not just
// itself.
//
// # What this package does
//
// Three franz-go options, none of them the default, are the entire
// difference between at-least-once delivery and silent loss:
//
//   - AutoCommitMarks commits only offsets this package explicitly marks,
//     never merely polled ones — this is what stops the auto-commit-on-a-
//     timer loss above.
//   - BlockRebalanceOnPoll refuses to let a rebalance start between a poll
//     and this package marking that poll's offsets — this is what stops a
//     partition moving out from under a handler still in flight.
//   - Committing marked offsets from OnPartitionsRevoked flushes whatever
//     work finished before the partitions are handed away, so a rebalance
//     never discards completed work that was only waiting to be committed.
//
// Each record is retried in place with exponential backoff and jitter, up to
// a bounded attempt count; a failure that outlives the whole budget is
// dead-lettered to <topic>.dlq rather than retried without end, which is what
// keeps one bad record from blocking everything behind it. An error wrapping
// ErrPermanent skips the budget and dead-letters immediately, because an
// undecodable payload or an unknown type will not become readable by
// waiting — retrying it would only spend the backoff before reaching the
// same DLQ anyway. A business outcome is not an error: a handler that looks
// at a record and correctly decides there is nothing to do returns nil,
// because returning an error there would retry a message that needed no
// retry and then dead-letter one that was already handled correctly.
//
// RebalanceTimeout has to exceed the slowest handler transaction. Under
// BlockRebalanceOnPoll a pending rebalance is stuck behind whatever this
// consumer is doing with the current batch, so a timeout shorter than that
// work pulls the partitions away before the handler can finish and mark its
// offset. Close leaves the group through CloseAllowingRebalance rather than
// Close: leaving a group is itself a rebalance, and franz-go's own Close
// hangs under BlockRebalanceOnPoll unless rebalances are allowed again first.
//
// # What it deliberately does not do
//
// This package knows nothing about outboxes, inboxes, sagas, or any other
// domain concept — it moves record bytes and headers and settles offsets.
// Producer happens to satisfy outbox.Publisher's method set structurally, so
// the outbox poller can hold one without either package importing the other,
// but that assertion lives in the test rather than here: writing it as a
// compile-time check would require this package to import outbox, which
// would reintroduce exactly the dependency the platform restructure removed.
// What a record's payload and headers mean, and what redelivery of one
// implies, is platform/envelope's, platform/codec's and platform/schema's
// concern, plus whatever the caller's own Handler decides.
//
// # Reading order
//
//	producer.go   NewProducer and Publish — why acks=all and idempotent
//	              production need no configuration here, only naming.
//	consumer.go   NewConsumer's three options, the retry-then-DLQ policy,
//	              and Close. Start here; it is most of the package.
//	admin.go      EnsureTopics — explicit topic creation instead of relying
//	              on the broker's auto-create defaults.
//
// # Where this comes from
//
// Design spec §9.1 (topics, keys and per-stream ordering), §10.2 (the retry
// and DLQ policy and the offset-commit reasoning this package implements),
// §10.3 (concrete limits: partition count and RebalanceTimeout).
package kafka
