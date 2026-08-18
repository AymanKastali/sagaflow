# Conventions

How code and documentation are written in this repository.

These rules exist because SagaFlow's product is the system design it
demonstrates. Code that is correct but unreadable has not delivered that
product. The reasoning behind each rule is in the
[legibility design spec](superpowers/specs/2026-08-18-legibility-design.md);
this page is the operative version, for someone about to write something.

**The reader every rule is written for:**

> A competent Go programmer who has never seen this repository, does not know
> what a transactional outbox is, and has not read the design spec.

Do not explain what a transaction, a goroutine or a foreign key is. Do explain
every term specific to this system or to this class of system — or link it to
the [glossary](glossary.md).

---

## Where things are written

Documentation lives in exactly four places, and the boundaries are rules rather
than preferences. Duplicated explanation is what goes stale, so nothing is
explained twice; a second place that needs the concept links to the first.

| Surface | Holds | Does not hold |
|---|---|---|
| `README.md` | What this is, what is built, one diagram, the reading order | Any explanation of any mechanism |
| `docs/*.md` | Anything true of more than one package: the lifecycle, the topology, the compensation matrix, the diagrams, this page, the glossary | Anything about a single package |
| `<pkg>/doc.go` | One concept's chapter | Anything spanning packages |
| Declaration comments | What this identifier is, then why it is this way | Citations, deferrals, references to a document |

**Routing:** concerns one package → `doc.go`. Spans packages → `docs/`. Is a
concrete trace → an executable `Example` or an annotated test, linked from the
prose that describes it. Names status, structure or reading order → `README.md`.

---

## The chapter standard

Every package under `internal/` and `contracts/` has a `doc.go` containing the
package comment and nothing else — no code, no imports. It uses these six
headings, in this order, in Go's godoc heading syntax so that `go doc` renders
them:

```go
// Package outbox makes "the state changed" and "the message was sent" the
// same commit.
//
// # The problem
//
// # Why the obvious fixes do not work
//
// # What this package does
//
// # What it deliberately does not do
//
// # Reading order
//
// # Where this comes from
package outbox
```

- **C1.** The first sentence is a complete definition of the concept in plain
  language, true out of context — it is what `go doc` lists.
- **C2.** *The problem* describes the failure a reader would hit **without** this
  package. Concrete, not abstract: name what ends up wrong.
- **C3.** *Why the obvious fixes do not work* is mandatory, and is the most
  valuable section on the page. A reader who has not been shown why the simple
  thing fails has learned nothing from being shown the complex thing.
- **C4.** *What it deliberately does not do* names the boundary and points at
  whatever handles the rest. This is how a reader builds a map instead of a pile.
- **C5.** *Reading order* lists the package's own files, one clause each, and
  names where to start.
- **C6.** *Where this comes from* carries the design-spec citations. **This is
  the only place in Go source where `§` may appear.**
- **C7.** 60–120 lines. Shorter means the concept was not explained; longer means
  it belongs in `docs/`.

---

## The comment standard

- **R1 — No citations outside `doc.go`.** A comment may not defer its
  explanation to a document. If a decision needs justifying, justify it here.
- **R2 — What before why.** A declaration comment's first sentence says what the
  identifier is or does. Justification follows. A comment that opens with a
  rationale is addressed to a reviewer, not to a reader.
- **R3 — Define on first use.** The first time a package's own code uses a
  domain term, define it: in `doc.go` if the package is about it, inline if it
  is borrowed. Terms owned elsewhere link to the [glossary](glossary.md).
- **R4 — One line inside a body.** Comments inside a function body are single
  lines marking a non-obvious step. Anything longer belongs on the declaration.
- **R5 — Functions stay under about 40 lines.**

### What this looks like

Before — fails R1 (cites a section), R2 (opens with a rationale for a rule the
reader has not been told), and R3 (uses *stream*, *outbox row*, *inbox row* and
*conflict* undefined):

```go
// handleOnce is spec §7.2's invariant: one transaction writes exactly one
// stream, plus its outbox rows, plus its inbox row.
//
// The inbox mark is inside the transaction so a conflict rolls it back too —
// otherwise the retry would find its own mark and treat the command as already
// handled.
```

After — self-contained, what before why, no citation:

```go
// applyInOneTransaction handles a single command in a single database
// transaction, which either commits all of its effects or none of them.
//
// Three things are written together and must not come apart: the events the
// command produced, the outgoing messages announcing them, and the record that
// this command was consumed. If the events committed but the messages did not,
// the world would never hear about a hold that exists. If the consumed-record
// committed but the events did not, redelivery would be ignored and the command
// would be lost.
```

---

## The naming standard

- **N1 — Domain terms are never abbreviated.** `envelope`, `outbox`, `command`,
  `correlation`. The single exception is `cmd`, retained because it is
  unambiguous here and appears in a hundred places.
- **N2 — Short names need short lives.** A one- or two-letter name is allowed
  only when the variable's whole life is within about three lines. `ctx`, `tx`,
  `err`, `t *testing.T`, `i`, and `b` in a two-line byte helper are conventional
  and stay.
- **N3 — A name that reads as something else gets replaced,** even when it is
  technically correct.

| Instead of | Write | Because |
|---|---|---|
| `env envelope.Envelope` | `incoming envelope.Envelope` | `env` reads as "environment" |
| `out Outcome` | `decision Outcome` | `out` reads as an output parameter |
| `fresh bool` | `firstDelivery bool` | "fresh" does not say fresh *what* |
| `handleOnce` | `applyInOneTransaction` | the name hid the entire point |

---

## Worked examples are executable

A worked example written in a comment can quietly stop being true. Worked
examples are therefore Go [`Example` functions](https://go.dev/blog/examples),
which are compiled and run by `go test` and whose printed output is checked. An
example that stops being true fails `make test`.

Use them wherever a function is pure enough to run without infrastructure.

---

## Testing conventions

Inherited from design spec §12, restated here because they are part of the
standard:

- **`make test` never starts a container.** Integration tests call
  `testing.Short()` and skip. `make test-integration` runs everything.
- **One container per package, started in `TestMain`.** `pgtest`, `kafkatest`
  and `srtest` each expose `Start()` for `TestMain` and `Shared(t)` for tests.
  None can start a container for a single test. Isolation comes from database
  names, topic names and consumer groups derived from the test.
- **No `time.Sleep` in assertions.** Tests wait on a signal — a channel, a
  terminal event — with a context deadline as the failure mode.

---

## Enforcement

`internal/docs/docs_test.go` checks the mechanical parts, so the standard
survives without anyone remembering to care. It walks the tree: a package added
later that skips its chapter fails the suite the day it is written.

- **D1.** Every package under `internal/` and `contracts/` has a non-empty
  package comment.
- **D2.** That comment is in a file named `doc.go` containing only the package
  comment.
- **D3.** Each `doc.go` contains all six chapter headings, in order.
- **D4.** No `.go` file other than a `doc.go` contains `§`.
- **D5.** `README.md` links to every package directory under
  `internal/platform/`.

D3 checks headings, not quality — no test can judge whether an explanation is
good. The headings are chosen so that filling them in dishonestly is more work
than filling them in honestly.
