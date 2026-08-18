# SagaFlow — Legibility Design Spec

**Status:** proposed
**Amends:** [2026-08-17-sagaflow-design.md](2026-08-17-sagaflow-design.md) (the design spec) and
[2026-08-18-platform-package-restructure-design.md](2026-08-18-platform-package-restructure-design.md).
Neither is contradicted here. This spec adds a requirement neither of them stated.

---

## 1. Purpose

SagaFlow exists to teach a system design: event sourcing, the transactional
outbox, inbox deduplication, Kafka delivery semantics, sagas and compensation.
That is its only product. There is no user, no deadline, no feature to ship.

The code currently does not serve that purpose. It is correct — 6,800 lines,
a green suite, sound package boundaries — and it is unreadable by anyone who
has not read the 888-line design spec first. Four specific failures:

1. **No entry point.** There is no `README.md` anywhere in the repository. The
   only prose is a design spec and eight implementation plans, filed under
   `docs/superpowers/`, a directory named after the tool that produced them
   rather than after anything a reader is looking for.

2. **The code cites instead of explains.** Comments say "spec §7.2's invariant",
   "§9.3", "spec D14". These are footnotes to a book the reader is not holding.

3. **The code explains *why this choice* and never *what this thing is*.** Every
   comment is addressed to a reviewer who already knows the system. A reader
   meeting the transactional outbox for the first time is told why the `NOTIFY`
   is transactional before being told what an outbox is.

4. **No concrete example and no diagram.** For a system whose entire subject is
   messages moving between services, there is not one picture and not one
   worked trace showing an actual message actually moving.

**This spec defines the standard that fixes all four, and the enforcement that
keeps it fixed.**

---

## 2. The reader we are writing for

One reader, assumed throughout:

> A competent Go programmer who has never seen this repository, does not know
> what a transactional outbox is, and has not read the design spec.

They should be able to clone the repo and learn the system from it. They are
not assumed to be a beginner in Go, in SQL, or in distributed systems
generally — we do not explain what a transaction is or what a goroutine is.
We do explain every term that is specific to this system or to this class of
system: stream, envelope, outbox, inbox, pivot, compensation, saga.

**This reader is the acceptance test.** Every rule below exists because of a
question this reader would be unable to answer.

---

## 3. Non-goals

- **No layout change.** The package boundaries are already one concept each and
  survived a deliberate restructure. `internal/platform/outbox` stays where it
  is. Nothing moves at all: `docs/` gains files (§5.1) but loses none.
- **No behavior change.** Not one test assertion changes meaning. This is
  commentary, naming, documentation and diagrams. Where a rename touches code,
  the suite must stay green with no test edits beyond the rename itself.
- **No runnable demo, no UI, no logging showcase.** The reader learns by
  reading. Making the system observable at runtime is a separate concern and
  is not addressed here.
- **The design spec is not rewritten.** It remains the authority on decisions.
  This spec changes who has to read it: currently everyone, afterwards nobody.

---

## 4. Success criteria

Checkable, in order of importance.

| # | Criterion | How it is verified |
|---|---|---|
| SC1 | A reader can answer "what is this concept, and why does it exist" for any package from `go doc ./internal/platform/<pkg>` alone, without opening the spec | Read-through, plus SC4's structural test |
| SC2 | No source file outside a `doc.go` cites a spec section | `grep -rn '§' --include='*.go' . \| grep -v doc.go` returns nothing |
| SC3 | The README routes a cold reader to every concept in the system | Every package under `internal/platform` is linked from `README.md` |
| SC4 | Every package carries a chapter with the required headings | `internal/docs/docs_test.go` (§9) |
| SC5 | Every worked example is compiled and run | Examples are Go `Example` functions, not comment blocks |
| SC6 | Every domain term is defined once, findable | `docs/glossary.md`, linked from the README and from each `doc.go` that uses a term it does not define |

---

## 5. The four surfaces, and the rule that divides them

Documentation lives in exactly four places. The division is a rule, not a
preference, and it exists so that nothing is explained twice — duplicated
explanation is what rots.

```
┌────────────────────────────────────────────────────────────────────┐
│ README.md            the syllabus. what this is, what's built,     │
│                      one diagram, concept→package table,           │
│                      reading order. ~150 lines. no explanations.   │
├────────────────────────────────────────────────────────────────────┤
│ docs/                cross-cutting only. anything true of more     │
│                      than one package: the message lifecycle,      │
│                      the topology, the compensation matrix,        │
│                      the diagrams, the glossary, the conventions.  │
├────────────────────────────────────────────────────────────────────┤
│ <pkg>/doc.go         the chapter for ONE concept. what it is,      │
│                      the naive approach, how the naive approach    │
│                      fails, what we do, what we deliberately       │
│                      don't, the file map. 60–120 lines.            │
├────────────────────────────────────────────────────────────────────┤
│ declaration comments what this identifier is, then why it is       │
│                      this way. self-contained. no citations.       │
└────────────────────────────────────────────────────────────────────┘
```

**The routing rule:**

- Concerns one package → `doc.go`.
- Spans packages → `docs/`.
- Is a concrete trace → an executable `Example` or an annotated test, linked
  from the doc that describes it in prose.
- Names the system's status, structure, or reading order → `README.md`.

**Nothing is explained in two places.** Where a second place needs the concept,
it links.

### 5.1 What `docs/` gains

`docs/superpowers/` is named after the tool that generated it. A reader looking
for architecture does not look there — so the reader-facing documents go one
level up, where they are the first thing in `docs/`. Nothing is moved or
renamed; four files are added alongside:

```
README.md                          ← new, the entry point
docs/
├── architecture.md                ← new: topology, service map, diagrams
├── message-lifecycle.md           ← new: one message's full journey, traced
├── glossary.md                    ← new: every domain term, defined once
├── conventions.md                 ← new: the standard in this spec, for contributors
└── superpowers/                   ← unchanged: specs/ and plans/ stay put
    ├── specs/
    └── plans/
```

`docs/superpowers/` keeps its name and contents. It is the record of how the
project was designed and built; it is not the manual, and after this work
nobody needs it to read the code.

---

## 6. The chapter standard (`doc.go`)

Every **concept-bearing** package gets a `doc.go` holding only the package
comment: everything under `internal/platform/`, the service packages such as
`internal/inventory`, and `contracts/sagaflow/*/v1`. Trivial packages (the
migrations embeds) and test-only packages are exempt — see §9's D1/D2. Six headings, in this order, using
Go's godoc heading syntax so `go doc` renders them. The example below is
abridged to keep this spec readable; a real chapter runs longer (C7):

```go
// Package outbox makes "the state changed" and "the message was sent" the
// same commit.
//
// # The problem
//
// A service that writes to its database and then publishes to Kafka has two
// systems to update and no way to update both together. If the process dies
// between the two, one of them is wrong forever: either the seat is held and
// nobody was told, or the world was told about a hold that does not exist.
//
// # Why the obvious fixes do not work
//
// Publishing before committing means publishing changes that then roll back.
// Publishing after committing means a crash in the gap loses the message.
// A distributed transaction across Postgres and Kafka would solve it, and is
// not available: Kafka has no XA participant, and two-phase commit would put
// a broker outage in the path of every database write.
//
// # What this package does
//
// The message is written into the same database, in the same transaction, as
// the change it announces. One commit, one system, atomic by construction.
// A separate poller reads committed rows and publishes them afterwards.
//
// # What it deliberately does not do
//
// It does not deliver exactly once. The poller can publish a row and die
// before marking it, and will then publish it again. That is why inbox
// exists: deduplication happens at the consumer, and cannot happen here.
//
// # Reading order
//
//   outbox.go  Enqueue — the tx-local write. Start here.
//   poller.go  the publish loop, its claim strategy and its failure handling.
//
// # Where this comes from
//
// Design spec §10.1 (outbox), §10.2 (why the consumer deduplicates).
package outbox
```

Rules for the chapter:

- **C1.** The first sentence is a complete definition of the concept in plain
  language. It must be true out of context — it is what `go doc` lists.
- **C2.** "The problem" describes the failure the reader would hit *without*
  this package. Concrete, not abstract: name what ends up wrong.
- **C3.** "Why the obvious fixes do not work" is mandatory and is the most
  valuable section. A reader who does not know why the simple thing fails has
  not learned anything by being shown the complex thing.
- **C4.** "What it deliberately does not do" names the boundary and points at
  whatever handles the rest. This is how the reader builds a map instead of a
  pile.
- **C5.** "Reading order" lists the package's own files with one clause each,
  and names where to start.
- **C6.** "Where this comes from" carries the spec citations. This is the only
  place in Go source where `§` may appear.
- **C7.** 60–120 lines. Shorter means the concept was not explained; longer
  means it belongs in `docs/`.

`doc.go` contains the package comment and nothing else — no code, no imports.

---

## 7. The comment standard

### 7.1 Rules

- **R1 — No citations outside `doc.go`.** A comment may not defer its
  explanation to a document. If a design decision needs justifying, justify it
  in the comment.
- **R2 — What before why.** A declaration comment's first sentence says what
  the identifier is or does. Justification follows. A comment that opens with
  a rationale is addressed to a reviewer, not a reader.
- **R3 — Define on first use.** The first time a package's own code uses a
  domain term, the term is defined — in `doc.go` if the package is about it,
  inline if it is borrowed from elsewhere. Terms defined elsewhere link to
  `docs/glossary.md`.
- **R4 — One line inside a body.** Comments inside a function body are single
  lines marking a non-obvious step. Anything longer belongs on the declaration.
  *(Existing project convention; restated here because it is part of the
  standard.)*
- **R5 — Functions stay under ~40 lines.** *(Existing project convention.)*

### 7.2 Worked example, from real code

Current, [internal/inventory/commands.go:73](../../../internal/inventory/commands.go#L73):

```go
// handleOnce is spec §7.2's invariant: one transaction writes exactly one
// stream, plus its outbox rows, plus its inbox row.
//
// The inbox mark is inside the transaction so a conflict rolls it back too —
// otherwise the retry would find its own mark and treat the command as already
// handled.
func (h *Handler) handleOnce(ctx context.Context, env envelope.Envelope, cmd proto.Message) error {
```

Fails R1 (cites §7.2), R2 (opens with a rationale for a rule the reader has
not been told), and R3 (uses "stream", "outbox rows", "inbox row" and "conflict"
undefined).

After:

```go
// applyInOneTransaction handles a single command in a single database
// transaction, which either commits all of its effects or none of them.
//
// Three things are written together and must not come apart: the events the
// command produced, the outgoing messages announcing them, and the record
// that this command was consumed. If the events committed but the messages
// did not, the world would never hear about a hold that exists. If the
// consumed-record committed but the events did not, redelivery would be
// ignored and the command would be lost.
//
// The consumed-record is marked inside the transaction rather than before it
// for the same reason. On a version conflict the whole transaction rolls back,
// the mark with it — so the retry sees an unconsumed command. Marking outside
// would leave the retry looking at its own mark, concluding the command was
// already handled, and silently dropping it.
func (h *Handler) applyInOneTransaction(
	ctx context.Context,
	incoming envelope.Envelope,
	cmd proto.Message,
) error {
```

Longer, and it stands alone. The spec citation moves to
`internal/inventory/doc.go` under "Where this comes from".

---

## 8. The naming standard

- **N1 — Domain terms are never abbreviated.** `envelope`, `outbox`, `command`,
  `correlation`. Not `env`, `ob`, `cmd`… with one exception: `cmd` is retained
  because it is unambiguous in this codebase and appears in a hundred places.
- **N2 — Short names need short lives.** A one- or two-letter name is allowed
  only when the variable's whole life is within about three lines. `ctx`, `tx`,
  `err`, `t *testing.T`, `i`, and `b` in a two-line byte helper are conventional
  and stay.
- **N3 — A name that reads as something else gets replaced**, even if correct.

Concrete renames this mandates:

| Current | Becomes | Why |
|---|---|---|
| `env envelope.Envelope` | `incoming envelope.Envelope` | `env` reads as "environment" |
| `out Outcome` | `decision Outcome` | `out` reads as output parameter |
| `fresh bool` | `firstDelivery bool` | "fresh" does not say fresh *what* |
| `handleOnce` | `applyInOneTransaction` | the name hid the entire point |

The full rename list is produced per-package during implementation; these four
are binding examples of the rule, not the complete set.

---

## 9. Enforcement

A standard nobody checks is a standard that decays. `internal/docs/docs_test.go`
is a level-1 test (no infrastructure, runs under `make test`) asserting the
mechanical parts of this spec:

- **D1.** Every package under `internal/` and `contracts/` **that has at least
  one non-test `.go` file** has a non-empty package comment.
- **D2.** Every **concept-bearing** package — everything under
  `internal/platform/` plus `internal/inventory` and the later service packages
  — has that comment in a file named `doc.go` containing only the package
  comment.

  Two refinements found while surveying the tree, both recorded here because
  the original wording was unimplementable:

  *Test-only packages are exempt from D1 and D2.* `internal/integration` and
  `internal/toolchain` contain no non-test `.go` file, and their package clause
  is `integration_test`. Go forbids a non-`_test.go` file from declaring an
  external test package, so a `doc.go` for them cannot be written at all. Their
  package comment stays in the test file that carries it.

  *Trivial packages get D1 but not D2 or D3.* `internal/inventory/migrations`
  and `internal/booking/migrations` are nine lines that embed a directory of
  SQL. A six-heading chapter for them would be padding, and §6's C7 bound of
  60–120 lines would force it. They keep a short, honest package comment in the
  file that already has one.
- **D3.** Each such `doc.go` contains all six §6 headings, in order.
- **D4.** No `.go` file other than a `doc.go` contains `§`.
- **D5.** `README.md` links to every package directory under
  `internal/platform/`.

It walks the tree, so a package added in phase 6 that skips its chapter fails
the suite the day it is written. That is the mechanism that makes this stick
through phases 5b–10 without anyone remembering to care.

D3 checks headings, not quality — no test can check whether an explanation is
good. The headings are chosen so that filling them in dishonestly is more work
than filling them in honestly.

---

## 10. Diagrams

Mermaid, fenced in Markdown. GitHub renders it natively; there is no toolchain,
no generated image to fall out of date, and the source is readable as text.

Four diagrams, all in `docs/architecture.md` except where noted:

1. **Service and topic topology** — the four services, the topics between them,
   which service owns which database. Also reproduced in the README.
2. **The outbox/inbox delivery path** — sequence diagram: transaction commits,
   poller wakes on `NOTIFY`, publishes, consumer deduplicates, handler commits.
   This is the diagram that makes at-least-once concrete.
3. **The saga's happy path and its pivot** — state diagram, marking the point
   after which the saga rolls forward instead of back.
4. **The compensation matrix as a flow** — which failure at which step triggers
   which compensation. In `docs/architecture.md`, sourced from spec §9.3.

Diagrams 3 and 4 describe behavior that does not exist yet (phases 7 and 10).
They are written from the spec and labeled with what is built and what is not —
see §12.

---

## 11. The worked example

Two artifacts, because one cannot cover both scales.

**`docs/message-lifecycle.md`** — one message traced end to end in prose, with
the actual table rows and the actual header values at each step: a command is
enqueued, the row that lands in `outbox`, the `NOTIFY`, the poller's claim,
the CloudEvents headers on the Kafka record, the `inbox` row at the consumer,
the events appended to the seat stream, the reply enqueued. Every value shown
is a real value the existing tests produce. It links to the test that produces
each one.

**Go `Example` functions** — for anything pure enough to run without
infrastructure. `inventory.Decide` is the important one: a reader sees a free
seat plus a `HoldSeat` produce a `SeatHeld`, and a held seat plus a `HoldSeat`
produce a `SeatUnavailable`, as compiled, executed, output-checked Go. Also
`envelope` (headers in, envelope out) and `codec` (message to row and back).

These are `Example` functions rather than comment blocks specifically so that a
worked example which stops being true fails `make test`.

The full booking-with-compensation walkthrough — the thing that would best
teach the saga — cannot be written yet, because the saga does not exist. It is
scheduled with phase 10 and named in `docs/architecture.md` as pending.

---

## 12. Honesty about status

A repository that describes unbuilt things as though they exist teaches the
reader something false and wastes their time when they go looking. Two rules:

- **H1.** The README carries a build-status table: every phase from spec §13,
  marked built or not built, with the package that holds it.
- **H2.** Any document describing behavior that is not yet implemented says so
  at the point of description, not only in a status section.

---

## 13. What changes, file by file

**New:**

```
README.md
docs/architecture.md
docs/message-lifecycle.md
docs/glossary.md
docs/conventions.md
internal/docs/docs_test.go
internal/platform/{codec,envelope,eventstore,inbox,kafka,outbox,pg,schema}/doc.go
internal/inventory/doc.go
internal/testsupport/{pgtest,kafkatest,srtest}/doc.go
contracts/sagaflow/inventory/v1/doc.go
cmd/schemactl/doc.go
internal/inventory/example_test.go
internal/platform/envelope/example_test.go
internal/platform/codec/example_test.go
```

**Modified:** every existing `.go` file — declaration comments rewritten to
§7, names corrected to §8. `docs/superpowers/plans/README.md` gains the new
plan set.

**Unchanged:** every package boundary, every SQL migration, every test
assertion's meaning, the two design specs, `Makefile`, `docker-compose.yml`,
`buf.yaml`, `buf.gen.yaml`, all `.proto` files.

---

## 14. Build order

Six plans, each independently reviewable, each ending green.

| # | Plan | Ends when |
|---|---|---|
| L1 | Conventions, glossary, README skeleton | `docs/conventions.md` and `docs/glossary.md` exist; `README.md` carries the status table and topology diagram |
| L2 | Architecture doc, diagrams, message lifecycle | The four diagrams render; `docs/message-lifecycle.md` traces a real message with real values |
| L3 | Chapters: the storage and delivery core — `eventstore`, `outbox`, `inbox`, `pg` | Those four packages read correctly from `go doc` alone |
| L4 | Chapters: the wire — `envelope`, `codec`, `schema`, `kafka` | Same, for those four |
| L5 | Chapters: the service — `inventory`, `contracts`, `testsupport`, `cmd` | Same; `Example` functions compile and pass |
| L6 | Enforcement | `internal/docs/docs_test.go` passes with no exemptions |

L6 last, deliberately: written earlier it would fail for every package not yet
converted and leave the suite red across five plans.

Phase 5b resumes after L6, written to this standard from its first line.

---

## 15. Risks

- **The rewrite touches every file, so the diff is large.** Mitigated by
  splitting into six plans by package group, and by the constraint that no test
  assertion changes meaning: a rename that breaks a test is a rename that was
  wrong.
- **Chapters can become essays.** The 60–120 line bound in C7 is the check;
  overflow goes to `docs/`.
- **Diagrams 3 and 4 describe unbuilt behavior** and could drift from what
  phases 7 and 10 actually produce. Accepted: they are sourced from the spec,
  and phases 7 and 10 include updating them.

---

## 16. Deferred

- **A runnable demo** — starting the services and watching a booking succeed and
  then fail and compensate. This is the strongest possible teaching artifact and
  is explicitly out of scope here, because the reader said reading is what is
  blocked. Revisit after phase 10, when there is a saga to watch.
- **The full compensation walkthrough test** — phase 10, per §11.
- **Godoc hosting** — nothing is published; `go doc` locally is the interface.
