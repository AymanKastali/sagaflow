# Legibility L3 — Chapters for the Storage and Delivery Core

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `go doc ./internal/platform/{eventstore,outbox,inbox,pg}` each returns
a chapter that teaches its concept to someone who has never met it, without the
design spec open.

**Architecture:** Four `doc.go` files, one per package, each holding only the
package comment. The existing package comments move into them and are expanded
to the six-heading chapter standard already written in `docs/conventions.md`.
No logic changes; the only Go edits are removing the old package comment from
wherever it currently sits and adding `doc.go` beside it.

**Tech Stack:** Go doc comments, using godoc heading syntax (`// # Heading`),
rendered by `go doc`.

**Spec:** [docs/superpowers/specs/2026-08-18-legibility-design.md](../specs/2026-08-18-legibility-design.md) §6.
The operative standard is [docs/conventions.md](../../conventions.md), which is
already written and committed — rules C1–C7 govern every chapter here.

---

## Global Constraints

- **The chapter standard is rules C1–C7 of `docs/conventions.md`.** Six
  headings, in this order, using godoc heading syntax:
  `# The problem`, `# Why the obvious fixes do not work`,
  `# What this package does`, `# What it deliberately does not do`,
  `# Reading order`, `# Where this comes from`.
- **C3 is the heart of it.** "Why the obvious fixes do not work" is mandatory
  and is the most valuable section on the page. A reader who has not been shown
  why the simple thing fails has learned nothing from being shown the complex
  thing. A chapter whose C3 section is thin has failed even if every other
  heading is present.
- **C6:** "Where this comes from" is the **only** place in Go source where `§`
  may appear. Every other `§` in the package must be removed and its content
  written out.
- **C7:** 60–120 lines per chapter. Shorter means the concept was not explained;
  longer means it belongs in `docs/`.
- **C1:** the first sentence is a complete definition of the concept in plain
  language, true out of context — it is what `go doc` lists.
- **`doc.go` holds the package comment and nothing else** — no code, no imports.
- **The reader** is a competent Go programmer who has never seen this
  repository, does not know what a transactional outbox is, and has not read the
  design spec.
- **No behavior change.** No function body, no SQL, no signature is altered.
  **Test files may be edited for comment text only** — rule C6 makes `doc.go` the
  sole home for `§`, and several test files carry citations, so the sweep has to
  reach them. No assertion, no fixture and no test name changes.
- **Nothing is explained in two places.** These chapters do not re-explain what
  `docs/architecture.md` or `docs/glossary.md` already cover; where they need a
  term they use it and let the glossary define it.
- Module path is `github.com/AymanKastali/sagaflow`.

---

## Scope

| In this plan | |
|---|---|
| `internal/platform/eventstore/doc.go` | Event sourcing, streams, optimistic concurrency |
| `internal/platform/outbox/doc.go` | The dual-write problem and the transactional outbox |
| `internal/platform/inbox/doc.go` | At-least-once delivery turned into applied-exactly-once |
| `internal/platform/pg/doc.go` | Pool, transactions and migrations |

| Deferred | Where |
|---|---|
| `envelope`, `codec`, `schema`, `kafka` chapters | L4 |
| `inventory`, `contracts`, `testsupport`, `cmd` chapters, plus the comment and naming rewrite across all files | L5 |
| `internal/docs/docs_test.go` | L6 |

**Declaration comments inside these four packages are not rewritten here.** Only
the package comment moves and grows. The full comment and naming rewrite is L5,
which touches every file in the repository at once so the voice stays uniform.
The one exception: a `§` appearing in a declaration comment inside these four
packages must be removed now, because rule C6 makes `doc.go` its only legal home
and leaving it would contradict the chapter that has just been written.

---

## File structure

```
internal/platform/
├── eventstore/
│   ├── doc.go        ← new. the chapter.
│   ├── eventstore.go    package comment removed from here
│   └── errors.go
├── outbox/
│   ├── doc.go        ← new
│   ├── outbox.go        package comment removed from here
│   └── poller.go
├── inbox/
│   ├── doc.go        ← new
│   └── inbox.go         package comment removed from here
└── pg/
    ├── doc.go        ← new
    ├── pg.go            package comment removed from here
    └── migrate.go
```

Each `doc.go` is the only place its package is introduced. The `.go` file that
previously carried the package comment keeps everything else unchanged and loses
only those comment lines and the blank line under them.

---

## What each chapter must land

This is the substance of the plan. Each package has one insight that a reader
will not arrive at alone, and the chapter exists to deliver it. A chapter that
covers the headings but misses its insight has not done its job.

### `eventstore`

- **The problem.** A row that is overwritten cannot answer "how did it get like
  this", and two writers overwriting it concurrently silently lose one of the
  changes.
- **The obvious fixes that fail.** `SELECT … FOR UPDATE` serialises every reader
  of the row and holds a lock across application logic. A `last_updated`
  timestamp compare loses to clock skew and to two writes inside the same
  timestamp resolution. A retry-on-any-error loop cannot tell a conflict from a
  network blip.
- **The insight.** **Choosing what counts as a stream is the whole design.** A
  seat is a stream, so `UNIQUE (stream_id, version)` makes "held at most once"
  structurally impossible to violate — no lock, no check-then-act, no window. A
  stream per flight would have serialised every seat in the aircraft; a stream
  per booking would have left the constraint unable to see the conflict at all.
- **The boundary.** It does not decide anything, does not know what an event
  means, and takes a `pgx.Tx` rather than a pool — because the caller's
  transaction is what makes the co-commit with outbox and inbox possible.
- Must also explain why `global_seq` is diagnostic only: `BIGSERIAL` values are
  handed out at insert and become visible at commit, so a late-committing row can
  carry a lower number than one already read, and any consumer tracking a cursor
  over it would silently skip rows under concurrency.
- Spec refs for C6: §6.1, §6.2, §6.3, §6.4.

### `outbox`

- **The problem.** A service that changes its database and then publishes to
  Kafka has two systems to update and no way to update both atomically. A crash
  between them leaves one wrong forever: either the seat is held and nobody was
  told, or the world was told about a hold that does not exist.
- **The obvious fixes that fail.** Publishing before committing publishes changes
  that then roll back. Publishing after committing loses the message if the
  process dies in the gap. A distributed transaction would solve it and is not
  available: Kafka has no XA participant, and two-phase commit would put a broker
  outage in the path of every database write.
- **The insight.** The message is written into the same database, in the same
  transaction, as the change it announces — one commit, one system, atomic by
  construction. Publishing becomes a separate, retryable step that can fail
  safely.
- Must explain **claim-by-flag rather than a cursor**, with the same `BIGSERIAL`
  visibility reasoning as `eventstore`, and why `SKIP LOCKED` is for failover
  rather than throughput.
- Must explain that the `NOTIFY` is transactional — delivered on commit,
  discarded on rollback — so a woken poller never chases a row that was never
  written.
- **The boundary.** It does not deliver exactly once. The poller can publish and
  die before marking, and will publish again. That is why `inbox` exists.
- Spec refs for C6: §10.1, §10.2, §6.4 (the cursor reasoning).

### `inbox`

- **The problem.** Kafka delivers at least once. A handler that applies a
  redelivered message a second time holds two seats, charges two payments, or
  appends the same event twice.
- **The obvious fixes that fail.** Checking "have I seen this?" before the
  transaction leaves a window in which a crash loses the mark or applies twice.
  Making every handler naturally idempotent works only for some operations and
  cannot be verified. Kafka's own exactly-once semantics cover Kafka-to-Kafka
  transactions and do not extend to a Postgres write.
- **The insight.** Record the message id **inside the transaction that applies
  its effects.** They cannot come apart, so a duplicate finds its own record and
  is skipped, and a rolled-back attempt takes its record with it and can be
  retried. `(source, event_id)` is unique by the CloudEvents spec, so the key is
  something the format already guarantees rather than something invented here.
- Must explain why `consumer` is part of the primary key: several consumers in
  one service read the same message for different purposes and must deduplicate
  independently.
- **The boundary.** It makes delivery-duplicates harmless. It does not make a
  *business* retry idempotent — two different `HoldSeat` commands with different
  ids are two commands, and that is what business idempotency keys are for.
- Spec refs for C6: §10.2, §8.1 (the uniqueness guarantee), §10.4 (business keys).

### `pg`

- **The problem.** Every service needs the same three things — a configured
  pool, a way to run work in a transaction that cannot leak, and schema
  migrations — and getting any of them subtly wrong is a production incident
  rather than a test failure.
- **The obvious fixes that fail.** `defer tx.Rollback()` alone silently swallows
  the commit error. Running migrations from a separate tool means the schema and
  the code that needs it can be deployed apart. A second database driver just for
  migrations doubles the connection configuration that has to be kept in step.
- **The insight.** `WithTx` makes the transaction's lifetime the function's
  lifetime: it commits on a nil return, rolls back on error or panic, and gives
  the caller nowhere to forget. `tern` is used because it is pgx-native, so
  migrations and application code share one driver and one DSN.
- **The boundary.** It knows nothing about events, messages or domains. It is
  the only package here that would look the same in a project with no Kafka in
  it at all.
- Spec refs for C6: §5, §12.4.

---

## Task 1: The `eventstore` chapter

**Files:**
- Create: `internal/platform/eventstore/doc.go`
- Modify: `internal/platform/eventstore/eventstore.go` — remove the existing
  package comment lines above `package eventstore`

**Interfaces:**
- Consumes: nothing.
- Produces: the pattern the other three tasks follow. The exact heading text
  established here is what `internal/docs/docs_test.go` will assert in L6:
  `# The problem`, `# Why the obvious fixes do not work`, `# What this package
  does`, `# What it deliberately does not do`, `# Reading order`, `# Where this
  comes from`.

- [ ] **Step 1: Read the package before writing about it**

```bash
cat internal/platform/eventstore/eventstore.go internal/platform/eventstore/errors.go
cat internal/inventory/migrations/001_events.sql
sed -n '174,275p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

The chapter must be true. Read `Append`'s SQL and its conflict detection, read
the `UNIQUE` constraint, and read `global_seq`'s column comment before writing a
word about any of them.

- [ ] **Step 2: Write `internal/platform/eventstore/doc.go`**

It holds the package comment and nothing else. Structure:

```go
// Package eventstore is the append-only event log every service owns a copy of.
//
// # The problem
//
// [the overwrite problem and the lost update, concretely]
//
// # Why the obvious fixes do not work
//
// [SELECT FOR UPDATE, last_updated compare, retry-on-any-error — each named
// and each refuted with the specific failure it produces]
//
// # What this package does
//
// [append with an expected version; UNIQUE(stream_id, version) does the
// enforcing; and the stream-boundary insight, which is the centre of the page]
//
// # What it deliberately does not do
//
// [no decisions, no domain knowledge; takes a pgx.Tx not a pool, and why that
// matters; global_seq is diagnostic only, and why a cursor over it loses rows]
//
// # Reading order
//
//	eventstore.go  Append and Load. Start with Append's SQL.
//	errors.go      ErrVersionConflict, and the Postgres code it is mapped from.
//
// # Where this comes from
//
// Design spec §6.1 (schema), §6.2 (append and optimistic concurrency),
// §6.3 (stream boundaries), §6.4 (why global_seq is diagnostic only).
package eventstore
```

Fill every bracketed section with real prose against the specification in
"What each chapter must land" above. Keep the whole file between 60 and 120
lines. Indent the reading-order file list with a leading tab so godoc renders it
as a code block.

- [ ] **Step 3: Remove the old package comment from `eventstore.go`**

The file currently opens with a package comment above `package eventstore`.
Delete those comment lines and the blank line beneath them, so the file starts
directly with `package eventstore`. Change nothing else in the file.

- [ ] **Step 4: Verify the chapter renders and the package still builds**

```bash
go doc ./internal/platform/eventstore | head -40
gofmt -l internal/platform/eventstore/
go build ./... && echo "build OK"
```

Expected: `go doc` prints the chapter with its headings; `gofmt -l` prints
nothing; `build OK`.

- [ ] **Step 5: Verify the six headings are present, in order**

```bash
grep -n '^// # ' internal/platform/eventstore/doc.go
```

Expected, in exactly this order: `# The problem`, `# Why the obvious fixes do
not work`, `# What this package does`, `# What it deliberately does not do`,
`# Reading order`, `# Where this comes from`.

- [ ] **Step 6: Verify `§` appears only in `doc.go` (rule C6)**

```bash
grep -rn '§' internal/platform/eventstore/ | grep -v 'doc.go' || echo "no stray section refs"
```

Expected: `no stray section refs`. If any appear, rewrite those comments to
explain themselves rather than cite — that is rule R1, and it applies now
because the chapter has just claimed `doc.go` as the only home for citations.

- [ ] **Step 7: Verify the length bound (rule C7)**

```bash
wc -l internal/platform/eventstore/doc.go
```

Expected: between 60 and 120.

- [ ] **Step 8: Run the tests**

```bash
make lint && make test 2>&1 | tail -3
```

Expected: lint clean, tests green. No test file was edited, so any failure means
the package comment removal took a line it should not have.

- [ ] **Step 9: Commit**

```bash
git add internal/platform/eventstore/
git commit -m "docs(eventstore): the chapter — why a seat is a stream

go doc on this package returned three sentences. It now returns a chapter
that explains what an append-only log buys, refutes SELECT FOR UPDATE and
timestamp comparison as the obvious alternatives, and lands the insight the
package exists for: choosing what counts as a stream is the whole design.
A seat is a stream, so UNIQUE(stream_id, version) makes held-at-most-once
structurally impossible to violate.

Also states why global_seq is diagnostic only — BIGSERIAL is handed out at
insert and becomes visible at commit, so a cursor over it silently skips
rows under concurrency.

No behavior change: the package comment moved to doc.go and grew.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: The `outbox` chapter

**Files:**
- Create: `internal/platform/outbox/doc.go`
- Modify: `internal/platform/outbox/outbox.go` — remove the existing package
  comment above `package outbox`

**Interfaces:**
- Consumes: the six exact headings established in Task 1.
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Read the package before writing about it**

```bash
cat internal/platform/outbox/outbox.go internal/platform/outbox/poller.go
cat internal/inventory/migrations/002_outbox.sql
sed -n '565,610p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

`poller.go` already carries the claim-by-flag reasoning in a comment above
`claimSQL`. Read it: the chapter must agree with it and must not duplicate it
word for word — the chapter states the principle, the comment states it at the
point of use.

- [ ] **Step 2: Write `internal/platform/outbox/doc.go`**

Same six headings, same 60–120 line bound. The content specification is under
"What each chapter must land → `outbox`" above. The C3 section must name and
refute all three obvious fixes: publish-before-commit, publish-after-commit, and
a distributed transaction — the third with the concrete reason that Kafka has no
XA participant and that 2PC would put a broker outage in the path of every
database write.

Its C4 section must be explicit that this package does **not** deliver exactly
once and must name `inbox` as where that is handled. That sentence is what turns
two packages into one idea in the reader's head.

Spec refs for the C6 section: §10.1 (outbox), §10.2 (why the consumer
deduplicates), §6.4 (the `BIGSERIAL` visibility reasoning behind claim-by-flag).

- [ ] **Step 3: Remove the old package comment from `outbox.go`**

Delete the comment lines above `package outbox` and the blank line beneath them.
Change nothing else.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/outbox | head -40
grep -n '^// # ' internal/platform/outbox/doc.go
wc -l internal/platform/outbox/doc.go
grep -rn '§' internal/platform/outbox/ | grep -v 'doc.go' || echo "no stray section refs"
gofmt -l internal/platform/outbox/
go build ./... && echo "build OK"
```

Expected: six headings in the standard order; 60–120 lines; `no stray section
refs`; `gofmt -l` silent; `build OK`.

- [ ] **Step 5: Run the tests**

```bash
make lint && make test 2>&1 | tail -3
```

Expected: lint clean, tests green.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/outbox/
git commit -m "docs(outbox): the chapter — the dual-write problem, stated

The C3 section is the point: publish-before-commit publishes changes that
then roll back, publish-after-commit loses the message if the process dies in
the gap, and a distributed transaction is not available because Kafka has no
XA participant and 2PC would put a broker outage in the path of every
database write. Only after all three fail does writing the message into the
same transaction look obvious.

Says plainly that this package does not deliver exactly once and names inbox
as where that is handled, which is what turns two packages into one idea.

No behavior change: the package comment moved to doc.go and grew.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: The `inbox` chapter

**Files:**
- Create: `internal/platform/inbox/doc.go`
- Modify: `internal/platform/inbox/inbox.go` — remove the existing package
  comment above `package inbox`

**Interfaces:**
- Consumes: the six exact headings from Task 1; the `outbox` chapter from Task 2
  names this package, so this chapter must name `outbox` back.
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Read the package before writing about it**

```bash
cat internal/platform/inbox/inbox.go
cat internal/inventory/migrations/003_inbox.sql
sed -n '602,690p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

Note that `MarkConsumed` returns a bool rather than an error on duplication, and
that the SQL names its conflict target explicitly. The chapter must be accurate
about both.

- [ ] **Step 2: Write `internal/platform/inbox/doc.go`**

Same six headings, same 60–120 line bound. Content specification is under "What
each chapter must land → `inbox`" above.

The C3 section must refute: a check-before-the-transaction (leaves a window),
relying on naturally-idempotent handlers (works for some operations, cannot be
verified, and silently stops being true when a handler grows), and Kafka's own
exactly-once semantics (covers Kafka-to-Kafka transactions and does not extend
to a Postgres write).

The C4 section must draw the line between *delivery* duplicates, which this
package makes harmless, and *business* retries, which it does not — two
different `HoldSeat` commands with different ids are two commands, and business
idempotency keys are the separate answer to that.

Spec refs for C6: §10.2, §8.1, §10.4.

- [ ] **Step 3: Remove the old package comment from `inbox.go`**

Delete the comment lines above `package inbox` and the blank line beneath them.
Change nothing else.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/inbox | head -40
grep -n '^// # ' internal/platform/inbox/doc.go
wc -l internal/platform/inbox/doc.go
grep -rn '§' internal/platform/inbox/ | grep -v 'doc.go' || echo "no stray section refs"
gofmt -l internal/platform/inbox/
go build ./... && echo "build OK"
```

Expected: six headings in order; 60–120 lines; `no stray section refs`; `gofmt
-l` silent; `build OK`.

- [ ] **Step 5: Run the tests**

```bash
make lint && make test 2>&1 | tail -3
```

Expected: lint clean, tests green.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/inbox/
git commit -m "docs(inbox): the chapter — where at-least-once becomes once

Refutes the three things a reader would try first: checking before the
transaction leaves a window, naturally-idempotent handlers work only for some
operations and stop being true silently when a handler grows, and Kafka's
exactly-once semantics cover Kafka-to-Kafka transactions and do not reach a
Postgres write.

Then the insight: record the message id inside the transaction that applies
its effects, so they cannot come apart in either direction — a duplicate
finds its own record, and a rolled-back attempt takes its record with it.

Draws the line between delivery duplicates, which this handles, and business
retries, which it does not.

No behavior change: the package comment moved to doc.go and grew.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: The `pg` chapter

**Files:**
- Create: `internal/platform/pg/doc.go`
- Modify: `internal/platform/pg/pg.go` — remove the existing package comment
  above `package pg`

**Interfaces:**
- Consumes: the six exact headings from Task 1.
- Produces: nothing.

- [ ] **Step 1: Read the package before writing about it**

```bash
cat internal/platform/pg/pg.go internal/platform/pg/migrate.go
```

`WithTx` is the piece that matters. Read exactly what it does on error, on panic
and on a nil return before describing it.

- [ ] **Step 2: Write `internal/platform/pg/doc.go`**

Same six headings. This chapter will sit at the lower end of the 60–120 range:
it is the least conceptual package here, and padding it to look like the others
would be worse than letting it be short. It must still refute its obvious fixes
— a bare `defer tx.Rollback()` swallowing the commit error, migrations run from
a separate tool letting schema and code deploy apart, and a second driver purely
for migrations doubling the connection configuration.

Its C4 section should make the point that this is the one package here that
would look the same in a project with no Kafka in it at all — which tells the
reader they can skip it and lose nothing about the distributed-systems content.

Spec refs for C6: §5 (infrastructure and pinned versions), §12.4 (test
determinism, which is why `Open` is configured as it is).

- [ ] **Step 3: Remove the old package comment from `pg.go`**

Delete the comment lines above `package pg` and the blank line beneath them.
Change nothing else.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/pg | head -40
grep -n '^// # ' internal/platform/pg/doc.go
wc -l internal/platform/pg/doc.go
grep -rn '§' internal/platform/pg/ | grep -v 'doc.go' || echo "no stray section refs"
gofmt -l internal/platform/pg/
go build ./... && echo "build OK"
```

Expected: six headings in order; 60–120 lines; `no stray section refs`; `gofmt
-l` silent; `build OK`.

- [ ] **Step 5: Run the full suite, including integration**

This is the last task in the plan, and it is the first point at which all four
packages have been touched. `pg` in particular is used by every integration
test, so a botched comment removal would show up here and nowhere earlier.

```bash
make lint && make test-integration 2>&1 | tail -20
```

Expected: lint clean, every package `ok`.

- [ ] **Step 6: Commit**

```bash
git add internal/platform/pg/
git commit -m "docs(pg): the chapter — the plumbing, and why it is boring

Short on purpose: this is the one package in platform/ that would look the
same in a project with no Kafka in it at all, and the chapter says so, so a
reader can skip it knowing they have lost nothing about the distributed
systems content.

Still refutes its obvious fixes: a bare defer tx.Rollback() swallows the
commit error, migrations run from a separate tool let schema and code deploy
apart, and a second driver purely for migrations doubles the connection
configuration that has to be kept in step.

No behavior change: the package comment moved to doc.go and grew.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Done when

- [ ] `doc.go` exists in `eventstore`, `outbox`, `inbox` and `pg`, each holding
      the package comment and nothing else
- [ ] Each has all six headings, in the standard order:
      `for p in eventstore outbox inbox pg; do echo "== $p"; grep -c '^// # ' internal/platform/$p/doc.go; done`
      prints `6` four times
- [ ] Each is between 60 and 120 lines (rule C7)
- [ ] `grep -rn '§' internal/platform/{eventstore,outbox,inbox,pg}/ | grep -v doc.go`
      is empty (rule C6)
- [ ] No `.go` file outside these four packages changed, and no test file changed
      at all: `git diff --name-only <plan base>..HEAD` lists only the eight
      expected files
- [ ] `go doc` on each package prints a chapter a stranger could learn from
- [ ] `make lint` clean, `make test-integration` green
- [ ] Four commits, one per task

## Deliberately not done here

- **`envelope`, `codec`, `schema`, `kafka`** — L4. `kafka` has no package
  comment at all today, so it is the one that changes most.
- **Declaration comments and names inside these packages** — L5, which does the
  whole repository at once so the voice stays uniform. The only exception taken
  here is removing a stray `§`, which rule C6 forbids the moment these chapters
  land.
- **`internal/docs/docs_test.go`** — L6, which turns the heading list above into
  an assertion that runs on every build.
