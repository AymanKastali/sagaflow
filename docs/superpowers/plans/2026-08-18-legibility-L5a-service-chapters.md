# Legibility L5a — Chapters for the Service Layer

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every remaining package that carries a concept has a chapter, so that
`go doc` is a complete tour of the repository rather than a tour of its platform
layer.

**Architecture:** Four `doc.go` chapters to the established six-heading standard
(`internal/inventory`, the three `testsupport` packages as one shared chapter
each, `contracts/sagaflow/inventory/v1`, `cmd/schemactl`), plus a real package
comment for the two trivial `migrations` packages that get D1 but not a chapter.

**Tech Stack:** Go doc comments, godoc heading syntax.

**Spec:** [docs/superpowers/specs/2026-08-18-legibility-design.md](../specs/2026-08-18-legibility-design.md)
§6 and §9, **as amended** to scope D1/D2 — test-only packages are exempt, and
trivial packages get a package comment but not a chapter.

---

## Global Constraints

- **Rules C1–C7 of [docs/conventions.md](../../conventions.md)**, as in L3 and
  L4. Six headings, in order: `# The problem`,
  `# Why the obvious fixes do not work`, `# What this package does`,
  `# What it deliberately does not do`, `# Reading order`,
  `# Where this comes from`.
- **The exemplars are `internal/platform/outbox/doc.go` and
  `internal/platform/eventstore/doc.go`.** New chapters must read as though the
  same person wrote them.
- **C6:** `§` only under "Where this comes from".
- **C7:** 60–120 lines for a chapter.
- **Scope, per the amended spec:** `internal/integration` and
  `internal/toolchain` are **exempt** — they have no non-test `.go` file and
  declare `package integration_test`, and Go forbids a non-`_test.go` file from
  declaring an external test package, so a `doc.go` for them is impossible.
  `internal/inventory/migrations` and `internal/booking/migrations` get D1 only.
- **No behavior change.** No function body, no signature, no SQL is altered.
- Module path is `github.com/AymanKastali/sagaflow`.

---

## Scope

| In this plan | Treatment |
|---|---|
| `internal/inventory/doc.go` | Full chapter — the only service that exists |
| `internal/testsupport/{pgtest,kafkatest,srtest}/doc.go` | Full chapter each |
| `contracts/sagaflow/inventory/v1/doc.go` | Full chapter — it is a public module |
| `cmd/schemactl/doc.go` | Full chapter, short end of the range |
| `internal/{inventory,booking}/migrations/migrations.go` | D1 only — improve the existing comment, no chapter |

| Deferred | Where |
|---|---|
| The repository-wide comment and naming rewrite, and the `Example` functions | L5b |
| `internal/docs/docs_test.go` | L6 |

---

## What each chapter must land

### `internal/inventory`

The one place where every platform concept meets in a single transaction. This
chapter is the most valuable in the repository after `outbox`, because it is
where a reader sees the machinery used rather than described.

- **The problem.** Two customers ask for the same seat at the same instant. One
  must get it and the other must be told no, and no amount of retrying may
  produce two holds.
- **The obvious fixes that fail.** A `seats` table with a `held_by` column and a
  read-then-write is a race with a window between the read and the write.
  Wrapping that in `SELECT … FOR UPDATE` closes the race but serialises writers
  and holds a lock across application logic. An application-level mutex works
  until there are two instances of the service, which is the normal case.
- **The insight.** A seat is a stream, so the conflict is detected by
  `UNIQUE (stream_id, version)` at the moment of the append — no lock, no
  window. The loser reloads and *re-decides*, which is what converts it into a
  refusal rather than replaying a decision that is no longer valid.
- **The other insight, equally load-bearing.** Every command gets a reply; only
  a change gets an event. `SeatUnavailable` is a reply and is never appended,
  because nothing happened to the seat — appending it would grow the seat's
  history by one row for every losing racer. Nothing may produce silence,
  because a saga step that hears nothing re-dispatches forever.
- **The third.** The decision functions have no clock. A hold is live until an
  event ends it, never until a clock says so, which is what stops a new hold
  racing an expiry.
- **The boundary.** It does not know about Kafka. Its dependency list ends at
  the outbox row, and that is deliberate — the outbox is the seam.
- Spec refs for C6: §6.3, §7.2, §9.3, §10.3, §10.5.

### `internal/testsupport/pgtest`, `kafkatest`, `srtest`

One chapter each, all making the same argument from different angles.

- **The problem.** Tests need real Postgres, real Kafka and a real registry —
  mocks of these would test the mock — but a container per test would make the
  suite take longer than anyone will wait for.
- **The obvious fixes that fail.** A shared long-lived container outside the
  test run makes tests order-dependent and unrunnable on a fresh machine. A
  container per test is correct and unusably slow. Mocking the client tests the
  mock's behaviour, and the failures that matter here — rebalances, conflicts,
  visibility ordering — are precisely the ones a mock does not have.
- **The insight.** One container per *package*, started in `TestMain`, with
  isolation coming from names derived from the test: a database per test, a
  topic per test, a consumer group per test. `Start()` is for `TestMain` and
  `Shared(t)` is for tests, and neither can start a container for a single test
  — the API makes the slow thing impossible rather than discouraged.
- Each chapter must say what its own isolation key is: database name for
  `pgtest`, topic and group name for `kafkatest`, subject name for `srtest`.
- Spec refs for C6: §12.4.

### `contracts/sagaflow/inventory/v1`

- **The problem.** A consumer in another repository needs the message types, and
  must not need the services' internals to get them.
- **The obvious fixes that fail.** Putting the generated code inside `internal/`
  makes it unimportable by anything outside this module — which is what
  `internal/` is for. Publishing the whole service module drags every dependency
  along, including the database driver. Copying the `.proto` files into each
  consumer guarantees they drift.
- **The insight.** The contracts are their own Go module, so a consumer depends
  on the messages and nothing else. The full names are load-bearing in three
  places at once — the `ce_type` header, the `type` column in Postgres, and the
  protoregistry lookup key — which is why `fullname_test.go` asserts them rather
  than trusting them.
- Must state the **one message per `.proto` file** rule and why breaking it is a
  silent wire failure, not a compile failure.
- Spec refs for C6: §8.1, §8.2.

### `cmd/schemactl`

Short — the low end of the range is right here.

- **The problem.** Every message type's schema must be in the registry before
  any service starts, and a service that registers its own schemas can publish
  an incompatible one at runtime.
- **The obvious fix that fails.** Auto-registration on first publish: the first
  service to start defines the contract, and a bad schema reaches the registry
  in production rather than in CI.
- **The insight.** Registration is an explicit operation run by an operator or a
  pipeline. A service meeting an unregistered subject fails at startup, which is
  the failure mode you want.
- Spec refs for C6: §8.3, §8.5, decision D14.

---

## Task 1: The `inventory` chapter

**Files:**
- Create: `internal/inventory/doc.go`
- Modify: `internal/inventory/seat.go` — remove the package comment above
  `package inventory`; rewrite any comment containing `§`
- Modify: `internal/inventory/{commands.go,store.go,seat_test.go,store_test.go,commands_test.go}`
  — rewrite comments containing `§` only. No logic, no names, no assertions.

**Interfaces:**
- Consumes: the six headings from L3.
- Produces: nothing.

- [ ] **Step 1: Read the package**

```bash
cat internal/inventory/seat.go internal/inventory/store.go internal/inventory/commands.go
grep -rn '§' internal/inventory/
sed -n '212,250p;373,400p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

- [ ] **Step 2: Write `internal/inventory/doc.go`**

Six headings, 60–120 lines, content per "What each chapter must land →
`internal/inventory`". All three insights must land: the stream-as-conflict-
detector, reply-versus-event, and the absent clock.

Its "Reading order" section should send the reader to `seat.go` first — the pure
decision functions, no infrastructure — then `store.go`, then `commands.go`
last, because `commands.go` only makes sense once the other two are understood.

Spec refs for the final section: §6.3, §7.2, §9.3, §10.3, §10.5.

- [ ] **Step 3: Remove the old package comment and clear every `§` in the package**

Delete the comment lines above `package inventory` in `seat.go`. Then rewrite
every remaining `§` comment in the package to state its reasoning. There are
several, including in the test files. Change only comment text.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/inventory | head -40
grep -n '^// # ' internal/inventory/doc.go
wc -l internal/inventory/doc.go
grep -rn '§' internal/inventory/ | grep -v doc.go || echo "no stray refs"
gofmt -l internal/inventory/
go build ./... && go test -short ./internal/inventory/ && echo OK
```

Expected: six headings in order; 60–120 lines; `no stray refs`; gofmt silent; OK.

- [ ] **Step 5: Commit**

```bash
git add internal/inventory/
git commit -m "docs(inventory): the chapter — where the machinery is used

The one place every platform concept meets in a single transaction, so it is
the chapter where a reader sees the machinery used rather than described.

Three insights, all load-bearing. A seat is a stream, so two racing holds
collide on UNIQUE(stream_id, version) with no lock and no window, and the
loser re-decides rather than replaying a decision that is no longer valid.
Every command gets a reply but only a change gets an event, which is why
SeatUnavailable is never appended — nothing happened to the seat. And the
decision functions have no clock, because a hold is live until an event ends
it, which is what stops a new hold racing an expiry.

Section citations across the package, tests included, are written out.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: The three `testsupport` chapters

**Files:**
- Create: `internal/testsupport/pgtest/doc.go`
- Create: `internal/testsupport/kafkatest/doc.go`
- Create: `internal/testsupport/srtest/doc.go`
- Modify: the corresponding `pgtest.go`, `kafkatest.go`, `srtest.go` — remove
  each package comment; clear any `§`

**Interfaces:**
- Consumes: the six headings.
- Produces: nothing.

These three are one task because they make the same argument and must not
contradict each other on it. Writing them separately invites three different
accounts of why one container per package is the right trade.

- [ ] **Step 1: Read all three**

```bash
cat internal/testsupport/pgtest/pgtest.go internal/testsupport/kafkatest/kafkatest.go internal/testsupport/srtest/srtest.go
sed -n '825,840p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

- [ ] **Step 2: Write the three chapters**

Six headings each, 60–120 lines each, content per "What each chapter must land →
testsupport". The shared argument is the same; the **isolation key differs and
each chapter must name its own**: database name for `pgtest`, topic and consumer
group for `kafkatest`, subject name for `srtest`.

Each must make the point that `Start()` is for `TestMain` and `Shared(t)` is for
tests, and that neither can start a container for a single test — the API makes
the slow thing impossible rather than merely discouraged.

Spec refs for each final section: §12.4.

- [ ] **Step 3: Remove the old package comments and clear any `§`**

- [ ] **Step 4: Verify**

```bash
for p in pgtest kafkatest srtest; do
  f=internal/testsupport/$p/doc.go
  printf '%-10s %3s lines  %s headings\n' "$p" "$(wc -l < $f)" "$(grep -c '^// # ' $f)"
done
grep -rn '§' internal/testsupport/ | grep -v doc.go || echo "no stray refs"
gofmt -l internal/testsupport/
go build ./... && echo "build OK"
```

Expected: each 60–120 lines with 6 headings; `no stray refs`; gofmt silent.

- [ ] **Step 5: Commit**

```bash
git add internal/testsupport/
git commit -m "docs(testsupport): three chapters, one argument

Mocks of Postgres and Kafka would test the mock, and the failures that matter
here — rebalances, version conflicts, commit-visibility ordering — are exactly
the ones a mock does not have. A container per test is correct and unusably
slow. So: one container per package, started in TestMain, with isolation from
names derived from the test.

Each chapter names its own isolation key — database for pgtest, topic and
consumer group for kafkatest, subject for srtest — and each makes the point
that Start() is for TestMain and Shared(t) is for tests, so the API makes the
slow thing impossible rather than merely discouraged.

Written as one task because three separate accounts of the same trade-off
would eventually disagree.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: The `contracts` and `schemactl` chapters, and the migrations comments

**Files:**
- Create: `contracts/sagaflow/inventory/v1/doc.go`
- Create: `cmd/schemactl/doc.go`
- Modify: `contracts/sagaflow/inventory/v1/fullname_test.go` — clear its `§`
- Modify: `internal/inventory/migrations/migrations.go` and
  `internal/booking/migrations/migrations.go` — improve the package comment
  (D1 only, no chapter, no six headings)

**Interfaces:**
- Consumes: the six headings, for the two real chapters only.
- Produces: nothing. This is the last task of L5a.

- [ ] **Step 1: Read them**

```bash
cat contracts/sagaflow/inventory/v1/fullname_test.go cmd/schemactl/main.go
cat internal/inventory/migrations/migrations.go internal/booking/migrations/migrations.go
head -5 contracts/sagaflow/inventory/v1/seat_held.pb.go
```

Confirm the generated files declare `package inventoryv1`, so a hand-written
`doc.go` in that directory must declare the same package. It is not generated
and `buf generate` will not overwrite it — confirm that by running
`make generate` and checking `git status` in Step 5.

- [ ] **Step 2: Write `contracts/sagaflow/inventory/v1/doc.go`**

Six headings, 60–120 lines, content per "What each chapter must land →
contracts". Must state the one-message-per-`.proto`-file rule and that breaking
it is a **silent wire failure, not a compile failure** — which is what makes it
worth a paragraph.

Spec refs: §8.1, §8.2.

- [ ] **Step 3: Write `cmd/schemactl/doc.go`**

Six headings. This is a `package main`, and the chapter belongs to the command
rather than to a library, so it should read as "what this tool is for and when
you run it". Short — 60 to 75 lines.

Spec refs: §8.3, §8.5, decision D14.

- [ ] **Step 4: Improve the two `migrations` package comments — D1 only**

These get a real package comment and **no chapter**: no six headings, no length
bound. Each should say what the package is (an `embed.FS` of the SQL files that
`tern` applies), and the one thing worth knowing — that migrations are embedded
into the binary rather than shipped alongside it, so the schema a binary expects
travels with it and cannot be deployed apart from the code that needs it.

- [ ] **Step 5: Verify, including that codegen does not clobber the new file**

```bash
make generate
git status --porcelain contracts/
```

Expected: `doc.go` still present and unmodified — `buf generate` writes only
`.pb.go` files. If `doc.go` disappears or changes, stop: the chapter must move
somewhere `buf` does not manage.

```bash
go doc ./contracts/... 2>/dev/null | head -20
cd contracts && go doc . | head -20 && cd ..
grep -n '^// # ' contracts/sagaflow/inventory/v1/doc.go cmd/schemactl/doc.go
grep -rn '§' contracts/ cmd/ | grep -v doc.go || echo "no stray refs"
gofmt -l cmd/ && (cd contracts && gofmt -l .)
go build ./... && (cd contracts && go build ./...) && echo "build OK"
```

- [ ] **Step 6: Verify every concept-bearing package now has a chapter**

```bash
missing=0
for d in internal/platform/*/ internal/inventory internal/testsupport/*/ \
         contracts/sagaflow/inventory/v1 cmd/schemactl; do
  d=${d%/}
  if [ -f "$d/doc.go" ]; then
    printf '%-45s %3s lines  %s headings\n' "$d" "$(wc -l < $d/doc.go)" "$(grep -c '^// # ' $d/doc.go)"
  else
    printf '%-45s MISSING doc.go\n' "$d"; missing=1
  fi
done
[ $missing -eq 0 ] && echo "every concept-bearing package has a chapter"
```

Expected: fourteen rows, each `6 headings`, each 60–120 lines, no `MISSING`.

- [ ] **Step 7: Run the full suite**

```bash
make lint && make test-integration 2>&1 | tail -20
```

Expected: lint clean, every package `ok`.

- [ ] **Step 8: Commit**

```bash
git add contracts/ cmd/ internal/inventory/migrations/ internal/booking/migrations/
git commit -m "docs(contracts,schemactl,migrations): the last chapters

contracts is its own Go module so a consumer depends on the messages and not
on any service's internals — nor on its database driver. Its chapter states
the one-message-per-.proto-file rule and why breaking it is a silent wire
failure rather than a compile failure, which is the only reason it needs
saying at all.

schemactl exists because services never auto-register: the first service to
start would otherwise define the contract, and a bad schema would reach the
registry in production rather than in CI.

The two migrations packages get a real package comment and deliberately no
chapter — nine lines embedding a directory of SQL cannot fill six headings
without padding, which the spec amendment now says explicitly.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Done when

- [ ] Fourteen concept-bearing packages each have a `doc.go` with six headings
      and 60–120 lines — verified by the loop in Task 3 Step 6
- [ ] Both `migrations` packages have a real package comment and no chapter
- [ ] `grep -rn '§' internal/ cmd/ contracts/ --include='*.go' | grep -v doc.go`
      is empty across the whole repository
- [ ] `make generate` does not touch `contracts/sagaflow/inventory/v1/doc.go`
- [ ] No behavior change: no signature, no SQL, no assertion altered
- [ ] `make lint` clean, `make test-integration` green
- [ ] Three commits, one per task

## Deliberately not done here

- **The repository-wide comment and naming rewrite** — L5b. This plan removes
  `§` citations because rule C6 forces it the moment a chapter lands, but it does
  **not** rewrite declaration comments to what-before-why, and does not rename
  `env`, `out`, `fresh` or `handleOnce`. That is one sweep in one commit so the
  voice comes out uniform.
- **Go `Example` functions** — L5b.
- **`internal/docs/docs_test.go`** — L6.
