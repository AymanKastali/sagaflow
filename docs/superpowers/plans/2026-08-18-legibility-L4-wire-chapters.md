# Legibility L4 — Chapters for the Wire

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `go doc ./internal/platform/{envelope,codec,schema,kafka}` each returns
a chapter explaining what is actually on the wire and in the database, and why.

**Architecture:** Four `doc.go` files to the same six-heading standard L3
established. Three packages have a package comment today and it moves and grows;
`kafka` has **none at all**, so `go doc` on it currently prints a bare symbol
dump — that is the largest single gap this plan closes.

**Tech Stack:** Go doc comments using godoc heading syntax (`// # Heading`).

**Spec:** [docs/superpowers/specs/2026-08-18-legibility-design.md](../specs/2026-08-18-legibility-design.md) §6.
Operative standard: [docs/conventions.md](../../conventions.md), rules C1–C7.

---

## Global Constraints

- **Rules C1–C7 of `docs/conventions.md`**, identical to L3. Six headings, in
  order: `# The problem`, `# Why the obvious fixes do not work`,
  `# What this package does`, `# What it deliberately does not do`,
  `# Reading order`, `# Where this comes from`.
- **C3 is the heart of it.** "Why the obvious fixes do not work" must name each
  alternative a reader would reach for and refute it with the specific failure it
  produces.
- **C6:** `§` may appear only under "Where this comes from". Every other `§` in
  these four packages must be removed and its content written out — and there are
  many here, more than in L3's packages.
- **C7:** 60–120 lines per chapter.
- **`doc.go` holds the package comment and nothing else.**
- **No behavior change.** No function body, no signature, no configuration value
  is altered. No test file is edited.
- Module path is `github.com/AymanKastali/sagaflow`.

---

## Scope

| In this plan | |
|---|---|
| `internal/platform/envelope/doc.go` | CloudEvents as Kafka headers |
| `internal/platform/codec/doc.go` | One schema, two encodings |
| `internal/platform/schema/doc.go` | The registry and Confluent framing |
| `internal/platform/kafka/doc.go` | Broker plumbing — **new package comment, none exists** |

| Deferred | Where |
|---|---|
| `inventory`, `contracts`, `testsupport`, `cmd` chapters, and the repository-wide comment and naming rewrite | L5 |
| `internal/docs/docs_test.go` | L6 |

**One correction lands here.** `internal/platform/codec/codec.go`'s package
comment currently ends: *"Wire framing for Kafka is a separate concern and lives
in platform/kafka."* That has been false since the platform restructure moved
framing to `platform/schema`. The new chapter must say `platform/schema`.

---

## What each chapter must land

### `envelope`

- **The problem.** A message crossing services needs metadata — who sent it,
  what it is, what it is about, what caused it — and if every service invents its
  own shape, nothing can route or deduplicate a message it did not produce.
- **The obvious fixes that fail.** Putting the metadata inside the payload means
  you must decode the body, and know its schema, before you can decide what to do
  with it — and it couples routing to schema evolution. Inventing a private header
  set works until the second team, the second language, or the first tool that
  expects a standard. Using Kafka's own record metadata gives you a timestamp and
  a key and nothing else.
- **The insight.** CloudEvents v1.0.2 in *binary content mode*: attributes are
  headers, the body is the payload alone. **`ce_id` and `ce_source` are specified
  to be unique together**, which is exactly the property idempotent consumption
  needs — so the deduplication key is something the format already guarantees
  rather than something this system invented and must defend.
- Must explain that `traceparent` is deliberately **not** `ce_`-prefixed: it is
  a W3C header, not a CloudEvents attribute.
- Must explain that optional attributes are omitted entirely rather than written
  empty, because an absent `ce_subject` is a different statement from an empty one.
- **The boundary.** It knows nothing about brokers and nothing about payload
  encoding. It is a pure mapping, which is why it needs no infrastructure to test.
- Spec refs for C6: §8.1, §10.2 (why a missing attribute dead-letters).

### `codec`

- **The problem.** Events must be stored so they can be replayed years later,
  read during an incident, and decoded without depending on anything that might
  be down at the time.
- **The obvious fixes that fail.** Storing raw protobuf bytes makes the log
  opaque to `psql` and ties every replay to a schema registry being reachable.
  Storing hand-written JSON drifts from the `.proto` file the moment someone edits
  one and not the other. Storing the Confluent-framed wire bytes couples the
  database to the transport's framing, so changing how messages are framed would
  rewrite history.
- **The insight.** **One schema, two encodings.** protojson in Postgres, binary
  protobuf on the wire, both generated from the same `.proto` file so they cannot
  drift. `UseProtoNames` so the stored JSON matches the `.proto` rather than
  lowerCamelCase.
- Must state the sharp edge: **protojson output is not byte-stable across library
  versions**, so stored JSON must never be hashed, signed, or compared byte-wise.
  A reader who misses this will write a perfectly reasonable checksum and it will
  fail on a dependency bump.
- Must explain why an unknown type name is a *permanent* failure that
  dead-letters immediately rather than retrying — no amount of waiting adds a type
  to a compiled binary's registry.
- **The boundary.** Storage only. Wire framing lives in `platform/schema` — and
  the current comment saying `platform/kafka` is wrong and is corrected here.
- Spec refs for C6: §8.2, §8.4, §10.2.

### `schema`

- **The problem.** A consumer holding a sequence of bytes has to know which
  schema produced them, and has to keep working when the producer's schema
  changes.
- **The obvious fixes that fail.** One type per topic is Kafka's default
  assumption and breaks on the second message type — and these topics deliberately
  carry several. Trusting a type-name header tells you what it *claims* to be but
  not which *version*, so a field added upstream silently changes meaning.
  Auto-registering on first publish means the first service to start defines the
  contract, and a bad schema reaches the registry at runtime rather than in CI.
- **The insight.** *TopicRecordNameStrategy* — the subject is
  `<topic>-<fully.qualified.MessageName>` — because the default strategy allows
  only one schema per topic. Schema ids are resolved **once, at construction**, so
  the steady-state encode path makes no network call: a registry outage cannot
  stall publishing, it can only prevent a restart, which is the failure mode you
  want.
- Must explain the six framing bytes and, critically, **`sr.Index(0)` and the
  one-message-per-file rule**: the message-index path names which message inside
  the `.proto` file this is, every file here holds exactly one so the path is
  always `[0]`, and a second message in a file would be framed under the wrong
  index — unreadable by any other Confluent client while still round-tripping
  perfectly through our own code. That last clause is what makes it dangerous.
- Must be honest that registry compatibility checking happens at *registration*,
  not at produce time, so it is defence against mistakes and not against bypass.
- Spec refs for C6: §8.3, §8.5, D14 (never auto-register).

### `kafka`

**This package has no package comment at all.** `go doc` on it prints a symbol
dump today. This chapter is written from nothing.

- **The problem.** Kafka's defaults are tuned for throughput, and several of them
  quietly lose messages in a system that commits work to a database.
- **The obvious fixes that fail.** Auto-commit on a timer — franz-go's default —
  commits offsets for records still in flight, so a crash skips work that was
  never done. Committing after the handler but allowing rebalance during the poll
  lets a partition be reassigned while its records are mid-handler. Retrying a
  failing message forever blocks every message behind it in that partition.
- **The insight.** Three non-default options are the entire difference between
  at-least-once and silent loss: `AutoCommitMarks` (commit only explicitly marked
  offsets), `BlockRebalanceOnPoll` (no rebalance between polling and marking), and
  committing marked offsets on revoke (flush finished work before losing the
  partitions). Name all three and say what each prevents.
- Must explain the retry policy and the DLQ: retried with backoff up to a limit,
  then dead-lettered; an error wrapping `ErrPermanent` skips the retries because
  waiting cannot help; and a business outcome is **not** an error — a handler that
  decides "nothing to do" returns nil.
- Must explain `RebalanceTimeout` needing to exceed the slowest handler
  transaction, and why `CloseAllowingRebalance` rather than `Close`.
- **The boundary.** It knows nothing about outboxes, inboxes or domains. It
  satisfies `outbox.Publisher` structurally and deliberately does not import
  `outbox` to say so — the assertion lives in the test, because importing it for a
  compile-time check would reintroduce exactly the dependency the platform
  restructure removed.
- Spec refs for C6: §9.1, §10.2, §10.3.

---

## Task 1: The `envelope` chapter

**Files:**
- Create: `internal/platform/envelope/doc.go`
- Modify: `internal/platform/envelope/envelope.go` — remove the package comment
  above `package envelope`; rewrite any comment containing `§` to explain itself

**Interfaces:**
- Consumes: the six headings established in L3 Task 1.
- Produces: nothing.

- [ ] **Step 1: Read the package**

```bash
cat internal/platform/envelope/envelope.go
sed -n '400,435p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

- [ ] **Step 2: Write `internal/platform/envelope/doc.go`**

Six headings, 60–120 lines, content per "What each chapter must land →
`envelope`". Spec refs for the final section: §8.1 and §10.2.

- [ ] **Step 3: Remove the old package comment from `envelope.go`, and clear its `§`**

Delete the comment lines above `package envelope`. Then find every remaining
`§` in the file — `ErrMissingAttribute` cites §10.2 and `Envelope` cites §8.1 —
and rewrite those comments to say the thing rather than cite it. For example, the
`ErrMissingAttribute` comment should explain that a message missing a required
attribute can never become valid, so it dead-letters without retrying, rather than
pointing at a section number.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/envelope | head -40
grep -n '^// # ' internal/platform/envelope/doc.go
wc -l internal/platform/envelope/doc.go
grep -rn '§' internal/platform/envelope/ | grep -v doc.go || echo "no stray refs"
gofmt -l internal/platform/envelope/
go build ./... && go test -short ./internal/platform/envelope/ && echo OK
```

Expected: six headings in order; 60–120 lines; `no stray refs`; gofmt silent; OK.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/envelope/
git commit -m "docs(envelope): the chapter — metadata that survives the crossing

Refutes the three things a reader would try: metadata inside the payload
forces you to decode the body before you can route it, a private header set
works until the second team or language, and Kafka's own record metadata
gives you a timestamp and a key and nothing else.

The insight: ce_id and ce_source are specified to be unique together, which
is exactly what idempotent consumption needs — so the deduplication key is
something the format guarantees rather than something we invented and must
defend.

Section citations removed from the declaration comments and written out.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: The `codec` chapter

**Files:**
- Create: `internal/platform/codec/doc.go`
- Modify: `internal/platform/codec/codec.go` — remove the package comment;
  rewrite the comments containing `§`

**Interfaces:**
- Consumes: the six headings.
- Produces: nothing.

- [ ] **Step 1: Read the package**

```bash
cat internal/platform/codec/codec.go
sed -n '460,495p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

- [ ] **Step 2: Write `internal/platform/codec/doc.go`**

Six headings, 60–120 lines, content per "What each chapter must land → `codec`".

**Do not repeat the current comment's error.** The existing package comment says
wire framing "lives in platform/kafka". It lives in `platform/schema`. The
chapter must say `platform/schema`.

Spec refs for the final section: §8.2, §8.4, §10.2.

- [ ] **Step 3: Remove the old package comment from `codec.go`, and clear its `§`**

Delete the comment lines above `package codec`. Then rewrite the comments on
`ErrUnknownType` and on the `marshal` variable so they explain rather than cite
— in particular, the byte-stability warning must survive the rewrite intact,
because it is the one thing in this package that will bite someone.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/codec | head -40
grep -n '^// # ' internal/platform/codec/doc.go
wc -l internal/platform/codec/doc.go
grep -rn '§' internal/platform/codec/ | grep -v doc.go || echo "no stray refs"
grep -rn 'platform/kafka' internal/platform/codec/ && echo "STALE REFERENCE REMAINS" || echo "stale reference gone"
gofmt -l internal/platform/codec/
go build ./... && go test -short ./internal/platform/codec/ && echo OK
```

Expected: six headings; 60–120 lines; `no stray refs`; `stale reference gone`;
gofmt silent; OK.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/codec/
git commit -m "docs(codec): the chapter — one schema, two encodings

protojson in Postgres so the log is readable in an incident and replay does
not depend on a registry being up; binary protobuf on the wire so it is
compact. Both generated from the same .proto file, so they cannot drift.

States the sharp edge plainly: protojson output is not byte-stable across
library versions, so stored JSON must never be hashed, signed or compared
byte-wise. A reader who misses that writes a perfectly reasonable checksum
and it fails on a dependency bump.

Also corrects a claim that has been false since the platform restructure:
wire framing lives in platform/schema, not platform/kafka.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: The `schema` chapter

**Files:**
- Create: `internal/platform/schema/doc.go`
- Modify: `internal/platform/schema/serde.go` — remove the package comment;
  rewrite comments containing `§`
- Modify: `internal/platform/schema/compat.go` — rewrite comments containing `§`

**Interfaces:**
- Consumes: the six headings.
- Produces: nothing.

- [ ] **Step 1: Read the package**

```bash
cat internal/platform/schema/serde.go internal/platform/schema/compat.go
sed -n '438,495p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

`compat.go`'s comment on `EnsureBackwardCompatibility` is unusually good and
unusually long — it already explains that the registry checks at registration
rather than at produce time. Keep that reasoning; only remove the citations.

- [ ] **Step 2: Write `internal/platform/schema/doc.go`**

Six headings, 60–120 lines, content per "What each chapter must land →
`schema`". The `sr.Index(0)` explanation is the most consequential paragraph in
this plan: a second message in a `.proto` file breaks other Confluent clients
while still round-tripping through our own code, which is what makes it a trap
rather than a bug.

Spec refs for the final section: §8.3, §8.5, and decision D14 (services never
auto-register).

- [ ] **Step 3: Remove the old package comment and clear the `§` from both files**

Delete the comment lines above `package schema` in `serde.go`. Then rewrite the
comments on `ErrSubjectNotRegistered`, `Subject`, and
`EnsureBackwardCompatibility` so they explain rather than cite.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/schema | head -40
grep -n '^// # ' internal/platform/schema/doc.go
wc -l internal/platform/schema/doc.go
grep -rn '§' internal/platform/schema/ | grep -v doc.go || echo "no stray refs"
gofmt -l internal/platform/schema/
go build ./... && go test -short ./internal/platform/schema/ && echo OK
```

Expected: six headings; 60–120 lines; `no stray refs`; gofmt silent; OK.

- [ ] **Step 5: Commit**

```bash
git add internal/platform/schema/
git commit -m "docs(schema): the chapter — and the trap in the framing

Schema ids are resolved once, at construction, so the steady-state encode
path makes no network call: a registry outage cannot stall publishing, only
prevent a restart, which is the failure mode you want.

The paragraph that matters most is sr.Index(0). The message-index path names
which message inside the .proto file this is; every file here holds exactly
one, so the path is always [0]. A second message in a file would be framed
under the wrong index — unreadable by any other Confluent client while still
round-tripping perfectly through our own code. That last clause is what makes
it a trap rather than a bug.

Also honest that compatibility is checked at registration and not at produce
time, so it defends against mistakes and not against bypass.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: The `kafka` chapter

**Files:**
- Create: `internal/platform/kafka/doc.go` — **the package has no package
  comment today; this is written from nothing**
- Modify: `internal/platform/kafka/consumer.go` — rewrite comments containing `§`

**Interfaces:**
- Consumes: the six headings.
- Produces: nothing. This is the last chapter in L4.

- [ ] **Step 1: Read the package**

```bash
cat internal/platform/kafka/producer.go internal/platform/kafka/consumer.go internal/platform/kafka/admin.go
sed -n '493,520p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
sed -n '602,690p' docs/superpowers/specs/2026-08-17-sagaflow-design.md
```

Confirm which file currently holds a package comment:

```bash
grep -l '^// Package kafka' internal/platform/kafka/*.go || echo "none — write from nothing"
```

Expected: `none — write from nothing`.

- [ ] **Step 2: Write `internal/platform/kafka/doc.go`**

Six headings, 60–120 lines, content per "What each chapter must land →
`kafka`". The C3 section must name the three defaults that lose messages, and
the C2/C3 pair must make clear that this is not a "how to use franz-go" page —
it is a page about which of its defaults are wrong for a system that commits
work to a database.

Spec refs for the final section: §9.1 (topology), §10.2 (retry and DLQ policy),
§10.3 (the concrete limits).

- [ ] **Step 3: Clear the `§` from `consumer.go`**

There is no package comment to remove here. Rewrite the comments on
`ErrPermanent`, `RebalanceTimeout` and `Handler` so they explain rather than
cite. The `NewConsumer` comment listing the three franz-go options is already
excellent — leave its substance alone.

- [ ] **Step 4: Verify**

```bash
go doc ./internal/platform/kafka | head -40
grep -n '^// # ' internal/platform/kafka/doc.go
wc -l internal/platform/kafka/doc.go
grep -rn '§' internal/platform/kafka/ | grep -v doc.go || echo "no stray refs"
gofmt -l internal/platform/kafka/
go build ./... && echo "build OK"
```

Expected: `go doc` now leads with a chapter instead of a symbol list; six
headings; 60–120 lines; `no stray refs`; gofmt silent; `build OK`.

- [ ] **Step 5: Run the full suite, including integration**

This is the last task of the plan and the first point at which all four wire
packages have been touched. The Kafka and schema integration tests are the ones
that would catch a botched comment removal.

```bash
make lint && make test-integration 2>&1 | tail -20
```

Expected: lint clean, every package `ok`.

- [ ] **Step 6: Verify every platform package now has a chapter**

```bash
for p in internal/platform/*/; do
  n=$(basename "$p")
  if [ -f "$p/doc.go" ]; then
    c=$(grep -c '^// # ' "$p/doc.go")
    printf '%-12s doc.go  %s headings  %s lines\n' "$n" "$c" "$(wc -l < "$p/doc.go")"
  else
    printf '%-12s MISSING doc.go\n' "$n"
  fi
done
```

Expected: all eight packages listed, each with `6 headings` and a line count
between 60 and 120. No `MISSING`.

- [ ] **Step 7: Commit**

```bash
git add internal/platform/kafka/
git commit -m "docs(kafka): the chapter — which defaults lose messages

This package had no package comment at all, so go doc printed a symbol dump.
It now opens with the point: Kafka's defaults are tuned for throughput, and
several of them quietly lose messages in a system that commits work to a
database.

Three non-default options are the entire difference between at-least-once and
silent loss — AutoCommitMarks, BlockRebalanceOnPoll, and committing marked
offsets on revoke — and the chapter says what each one prevents rather than
that each one is set.

Also states the rule that is easiest to get wrong: a business outcome is not
an error. A handler that decides there is nothing to do returns nil, because
returning an error would retry and then dead-letter a message that was
handled correctly.

Completes the platform layer — all eight packages now have chapters.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Done when

- [ ] `doc.go` exists in `envelope`, `codec`, `schema` and `kafka`
- [ ] All eight platform packages have a `doc.go` with six headings and 60–120
      lines — verified by the loop in Task 4 Step 6
- [ ] `grep -rn '§' internal/platform/ | grep -v doc.go` is empty across the
      whole platform layer
- [ ] `grep -rn 'platform/kafka' internal/platform/codec/` is empty — the stale
      cross-reference is gone
- [ ] `go doc ./internal/platform/kafka` leads with prose, not a symbol list
- [ ] No test file changed
- [ ] `make lint` clean, `make test-integration` green
- [ ] Four commits, one per task

## Deliberately not done here

- **`inventory`, `contracts`, `testsupport`, `cmd`** — L5, together with the
  repository-wide comment and naming rewrite.
- **`internal/docs/docs_test.go`** — L6.
- **Go `Example` functions** — L5. `envelope` and `codec` are the natural
  candidates, since both are pure and need no infrastructure, but they land with
  the rest of the executable examples rather than alone.
