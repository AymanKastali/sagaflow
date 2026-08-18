# Legibility L5b — Naming, Declaration Comments and Executable Examples

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The code reads the way the chapters promise it does — names that say
what they are, declaration comments that define before they justify, and worked
examples that cannot go stale because the compiler runs them.

**Architecture:** Three passes over existing code, no new packages. A precise
naming sweep (the sites are enumerated, not left to judgment), a rewrite of the
declaration comments that open with a rationale instead of a definition, and
three `Example` test files for the packages pure enough to run without
infrastructure.

**Tech Stack:** Go. `Example` functions with `// Output:` blocks, which
`go test` compiles and executes.

**Spec:** [docs/superpowers/specs/2026-08-18-legibility-design.md](../specs/2026-08-18-legibility-design.md)
§7 (comments), §8 (naming), §11 (worked examples). Operative standard:
[docs/conventions.md](../../conventions.md) rules R1–R5 and N1–N3.

---

## Global Constraints

- **R2 — what before why.** A declaration comment's first sentence says what the
  identifier is or does. Justification follows. A comment that opens with a
  rationale is addressed to a reviewer, not a reader.
- **R4 — one line inside a body.** Comments inside a function body are single
  lines marking a non-obvious step.
- **N1 — domain terms are never abbreviated**, with the single retained
  exception of `cmd`.
- **N2 — short names need short lives.** A one- or two-letter name is allowed
  only when the variable's whole life is within about three lines. `ctx`, `tx`,
  `err`, `t *testing.T`, `i`, and `b` in a two-line byte helper are conventional
  and stay. **This is not a licence to rename every short variable** — the
  accumulator `out` in `eventstore.Load` lives for three lines and is fine.
- **No behavior change of any kind.** Renames are renames. No signature changes
  except parameter *names*, no logic, no test assertions, no SQL. The suite must
  be green before and after with identical test names and identical assertion
  counts.
- **Examples must actually run.** An `Example` function with an `// Output:`
  block is compiled and executed by `go test`; one without is only compiled.
  Every example added here has an `// Output:` block, because the point is that
  a stale example fails the build.
- Module path is `github.com/AymanKastali/sagaflow`.

---

## Scope

| In this plan | |
|---|---|
| The naming corrections | Six enumerated sites, listed in Task 1 |
| Declaration comments failing R2 | `internal/inventory`, then the platform layer |
| `internal/inventory/example_test.go` | `Decide` on a free seat and on a held one |
| `internal/platform/envelope/example_test.go` | Envelope → headers → envelope |
| `internal/platform/codec/example_test.go` | Message → stored row → message |

| Deferred | Where |
|---|---|
| `internal/docs/docs_test.go` | L6 |
| Examples for `outbox`, `inbox`, `eventstore`, `schema`, `kafka` | Not planned. All four need infrastructure, so an `Example` for them would need a container, and `go test -short` must never start one. Their behaviour is covered by the integration tests, which the chapters point at. |

**What this plan is not.** It is not a blanket rewrite of every comment in the
repository. Most declaration comments already open with a definition — a survey
found 33 that do. The work is the ones that do not, plus the enumerated names.
Rewriting comments that are already correct would churn the diff and make the
real changes impossible to review.

---

## Task 1: The naming corrections

**Files:**
- Modify: `internal/inventory/commands.go`
- Modify: `internal/integration/delivery_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `applyInOneTransaction` as the new name of `handleOnce`. Nothing
  outside `commands.go` calls it — it is unexported and used once — so no other
  file changes.

**The complete list of sites.** These were found by grep, not by judgment, and
this is all of them. Do not rename anything not on this list.

| File | Line (approx) | From | To | Why |
|---|---|---|---|---|
| `internal/inventory/commands.go` | 63 | `env envelope.Envelope` | `incoming envelope.Envelope` | `env` reads as "environment" (N3) |
| `internal/inventory/commands.go` | 80 | `env envelope.Envelope` | `incoming envelope.Envelope` | same |
| `internal/inventory/commands.go` | 74, 80 | `handleOnce` | `applyInOneTransaction` | the name hid the entire point (N3) |
| `internal/inventory/commands.go` | 86 | `fresh` | `firstDelivery` | "fresh" does not say fresh *what* (N3) |
| `internal/inventory/commands.go` | 94 | `out` (the `Outcome`) | `decision` | `out` reads as an output parameter (N3) |
| `internal/inventory/commands.go` | ~115 | `env` (the built envelope in `messages`) | `outgoing` | it is the envelope being constructed, not the one received |
| `internal/integration/delivery_test.go` | 73, 102 | `env envelope.Envelope` | `incoming` | same reasoning; keep it consistent across the repository |
| `internal/integration/delivery_test.go` | 76 | `fresh` | `firstDelivery` | same |

**Explicitly NOT renamed**, so nobody "fixes" them later:

- `out` in `eventstore.Load` (`eventstore.go:115`) and `out` in
  `commands.go:121` — both are accumulators whose whole life is three lines,
  which N2 permits.
- `fresh` in `kafka/consumer_test.go` — it means "a fresh consumer", which is
  clear and is not the `MarkConsumed` boolean.
- `cmd` anywhere — N1's stated exception.
- `ctx`, `tx`, `err`, `t`, `h`, `s`, `e` as method receivers and conventional
  short names.

- [ ] **Step 1: Record the baseline so the rename can be proved behaviour-neutral**

```bash
go test -short ./... 2>&1 > /tmp/before.txt
go test -short -v ./internal/inventory/ 2>&1 | grep -c '^=== RUN' > /tmp/before_count.txt
cat /tmp/before_count.txt
```

- [ ] **Step 2: Apply the renames in `internal/inventory/commands.go`**

Rename by hand or with a scoped `sed`; either way check each hit. `env` is a
substring of nothing else in this file, but confirm rather than assume:

```bash
grep -n '\benv\b\|\bfresh\b\|\bout\b\|handleOnce' internal/inventory/commands.go
```

Apply:
- `env envelope.Envelope` → `incoming envelope.Envelope` in both `Handle` and
  the renamed `applyInOneTransaction`, and every `env.` reference in those two
  functions becomes `incoming.`
- `handleOnce` → `applyInOneTransaction` at its declaration and its one call
  site
- `fresh, err := inbox.MarkConsumed(...)` → `firstDelivery, err := ...`, and the
  `!fresh` test becomes `!firstDelivery`
- `out, err := Decide(...)` → `decision, err := Decide(...)`, and `out.Events`
  → `decision.Events`, `out.Messages()` → `decision.Messages()`
- inside `messages`, the locally built `env := envelope.Envelope{...}` →
  `outgoing := envelope.Envelope{...}`, and `env.Headers()` → `outgoing.Headers()`

Leave the `out` accumulator in `messages` alone — it is three lines and N2
permits it.

- [ ] **Step 3: Apply the renames in `internal/integration/delivery_test.go`**

`env envelope.Envelope` → `incoming` in `applySeatHeld` and at the parse site;
`fresh` → `firstDelivery`. Every `env.` in those scopes follows.

Do not touch the test names or the assertions.

- [ ] **Step 4: Prove it changed nothing**

```bash
gofmt -l internal/
go build ./...
go test -short -v ./internal/inventory/ 2>&1 | grep -c '^=== RUN'
# strip cache markers and timings — they are never stable between runs
diff <(go test -short ./... 2>&1 | sed -E 's/\t[0-9.]+s$|\t\(cached\)$//') \
     <(sed -E 's/\t[0-9.]+s$|\t\(cached\)$//' /tmp/before.txt) && echo "identical results"
```

Expected: `gofmt -l` silent; the RUN count identical to `/tmp/before_count.txt`;
`identical results`.

- [ ] **Step 5: Verify no old name survives**

```bash
grep -rn --include='*.go' 'handleOnce' internal/ && echo "OLD NAME REMAINS" || echo "handleOnce gone"
grep -n '\benv\b' internal/inventory/commands.go && echo "ENV REMAINS" || echo "env gone from commands.go"
```

Expected: `handleOnce gone`, `env gone from commands.go`.

- [ ] **Step 6: Commit**

```bash
git add internal/inventory/commands.go internal/integration/delivery_test.go
git commit -m "refactor: name things what they are

Six sites, all found by grep rather than judgment. env reads as
'environment' and becomes incoming (or outgoing, where it is the envelope
being built rather than the one received). fresh does not say fresh what and
becomes firstDelivery. out reads as an output parameter and becomes decision.
And handleOnce becomes applyInOneTransaction, because the old name hid the
entire point of the function — that the whole thing is one transaction.

Deliberately not renamed: the three-line accumulators called out, which N2
permits; fresh in the kafka tests, where it means a fresh consumer; and cmd,
which N1 keeps as its one exception.

Behaviour-neutral by construction, and proved: identical test names, identical
RUN count, identical suite output before and after.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Declaration comments that open with a rationale

**Files:**
- Modify: `internal/inventory/commands.go`, `store.go`, `seat.go`
- Modify: any declaration in `internal/platform/**` whose comment opens with a
  justification rather than a definition

**Interfaces:**
- Consumes: Task 1's names — the rewritten comments must use them.
- Produces: nothing.

**How to find them.** A comment fails R2 when its first sentence explains *why a
choice was made* rather than *what the identifier is*. Openings like "It is an
interface so that…", "This is here because…", "Rather than X, we…" are the tell.
Read every exported declaration's comment and judge it; there is no grep for
this.

**The worked example, which is the whole reason this plan exists.** Before:

```go
// handleOnce is spec §7.2's invariant: one transaction writes exactly one
// stream, plus its outbox rows, plus its inbox row.
//
// The inbox mark is inside the transaction so a conflict rolls it back too —
// otherwise the retry would find its own mark and treat the command as already
// handled.
```

After — self-contained, what before why, no citation, and using Task 1's names:

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
```

- [ ] **Step 1: Rewrite the comments in `internal/inventory`**

`commands.go` is the priority: it is the file a reader reaches last and
understands least. Apply the before/after above verbatim for
`applyInOneTransaction`. Then check `Handle`, `Encoder`, `Handler`,
`NewHandler`, `messages`, and the `const` blocks, and rewrite any that open with
a rationale.

Then `store.go` and `seat.go`. `AppendSeat`'s comment explaining why
`ErrVersionConflict` passes through unwrapped is good reasoning — keep the
reasoning, but make sure the first sentence says what `AppendSeat` does.

- [ ] **Step 2: Rewrite the failing comments in `internal/platform`**

Read each package's exported declarations. Known candidates, to be judged rather
than assumed:

- `outbox.Publisher` — its comment opens "It is an interface so that…", which is
  a justification. It should first say that a Publisher sends messages, then say
  why it is an interface.
- `outbox.Claimed` — opens with what it is, then "It never leaves this package",
  which is fine.
- `kafka.Producer` — check whether the first sentence defines it.

Do not rewrite a comment that already opens with a definition. The diff should
be small and every hunk should be defensible.

- [ ] **Step 3: Verify nothing but comments changed**

```bash
gofmt -l internal/
go build ./...
go test -short ./... 2>&1 | tail -5
git diff --stat
```

Then confirm the diff is comment-only:

```bash
git diff -U0 | grep '^[+-]' | grep -v '^[+-][+-]' | grep -v '^[+-]\s*//' | grep -v '^[+-]\s*$' \
  && echo "NON-COMMENT CHANGES PRESENT — review them" || echo "comment-only diff"
```

Expected: `comment-only diff`. If any non-comment line appears, look at it: it
is either a mistake or a rename that belonged in Task 1.

- [ ] **Step 4: Commit**

```bash
git add -u internal/
git commit -m "docs: comments define before they justify

Rule R2: a declaration comment's first sentence says what the identifier is.
Justification follows. A comment that opens with a rationale is addressed to a
reviewer who already knows the system, not to a reader meeting it.

The one that mattered most is applyInOneTransaction, which used to open by
citing a spec section for an invariant the reader had never been told, and
then justified a choice using four terms it never defined. It now says what
the function does, then what would break if the three writes came apart, then
why the inbox mark sits inside the transaction rather than before it.

Only comments that failed the rule were touched; most already opened with a
definition and were left alone, so every hunk here is a real change.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Executable examples

**Files:**
- Create: `internal/inventory/example_test.go`
- Create: `internal/platform/envelope/example_test.go`
- Create: `internal/platform/codec/example_test.go`

**Interfaces:**
- Consumes: Task 1's names and Task 2's comments.
- Produces: nothing.

**Why these three and no others.** An `Example` runs under `go test`, and
`make test` must never start a container. `inventory.Decide`, the `envelope`
mapping and the `codec` round trip are pure — they need no Postgres, no Kafka
and no registry. `outbox`, `inbox`, `eventstore`, `schema` and `kafka` all do,
so an example for them would either need a container or be a lie.

Every example below has an `// Output:` block. Without one, `go test` compiles
the example but does not run it, which loses the entire guarantee.

- [ ] **Step 1: Write `internal/inventory/example_test.go`**

Two examples: a free seat accepting a hold, and a held seat refusing one. The
second is the more valuable, because it shows the refusal is a *reply* and not
an event.

```go
package inventory_test

import (
	"fmt"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory"
)

// ExampleDecide_freeSeat shows the happy path: a free seat accepts a hold and
// produces one event, which is appended to the seat's stream and also published.
func ExampleDecide_freeSeat() {
	free := inventory.SeatState{Version: 0, Status: inventory.StatusFree}

	decision, err := inventory.Decide(free, &inventoryv1.HoldSeat{
		HoldId:    "hold-1",
		BookingId: "booking-1",
		SeatId:    "seat-BA117-2026-09-01-14A",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("events: ", len(decision.Events))
	fmt.Println("replies:", len(decision.Replies))
	for _, e := range decision.Events {
		fmt.Printf("event:   %s\n", e.ProtoReflect().Descriptor().FullName())
	}

	// Output:
	// events:  1
	// replies: 0
	// event:   sagaflow.inventory.v1.SeatHeld
}

// ExampleDecide_seatAlreadyHeld shows the refusal, and the distinction the whole
// package turns on: SeatUnavailable is a reply, not an event.
//
// Nothing happened to the seat, so nothing is appended to its stream — a seat
// that lost a hundred races has a history of length one, not one hundred and
// one. The reply still goes out, because a saga step that hears nothing
// re-dispatches forever.
func ExampleDecide_seatAlreadyHeld() {
	held := inventory.SeatState{
		Version:   1,
		Status:    inventory.StatusHeld,
		HoldID:    "hold-1",
		BookingID: "booking-1",
	}

	decision, err := inventory.Decide(held, &inventoryv1.HoldSeat{
		HoldId:    "hold-2",
		BookingId: "booking-2",
		SeatId:    "seat-BA117-2026-09-01-14A",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("events: ", len(decision.Events))
	fmt.Println("replies:", len(decision.Replies))
	for _, r := range decision.Replies {
		fmt.Printf("reply:   %s\n", r.ProtoReflect().Descriptor().FullName())
	}

	// Output:
	// events:  0
	// replies: 1
	// reply:   sagaflow.inventory.v1.SeatUnavailable
}
```

**Before writing this, check the real field names.** `SeatState`'s fields and
`Decide`'s signature must match `internal/inventory/seat.go` exactly; if
`StatusHeld` or `HoldID` differ, use what is there. If `Decide` returns
something other than `(Outcome, error)`, adapt and note it.

- [ ] **Step 2: Run it**

```bash
go test -run Example -v ./internal/inventory/
```

Expected: both examples PASS. A failure here means the `// Output:` block does
not match reality — fix the block to match the code, not the code to match the
block, unless the code is genuinely wrong.

- [ ] **Step 3: Write `internal/platform/envelope/example_test.go`**

One example showing an envelope becoming Kafka headers and being parsed back.
Read `envelope.go` first for the exact function names — `Headers()` and `Parse`
are what the integration test uses, so confirm those are the exported surface.

Sort the header map before printing: Go map iteration order is randomised, and
an example that prints a map directly will fail intermittently. That is worth a
comment in the example itself, because it is the trap anyone writing the next
example will hit.

- [ ] **Step 4: Write `internal/platform/codec/example_test.go`**

One example showing a message encoded to a stored row and decoded back, printing
the `type` and the readable JSON `data`. This is where a reader sees the "one
schema, two encodings" claim made concrete — the JSON must show `hold_id`, not
`holdId`, because `UseProtoNames` is set.

Read `codec.go` for the exact signatures of `Encode` and `Decode` and for what
`eventstore.Event` contains.

- [ ] **Step 5: Verify all examples run and the suite is green**

```bash
go test -run Example -v ./internal/inventory/ ./internal/platform/envelope/ ./internal/platform/codec/
make lint && make test 2>&1 | tail -5
```

Expected: every example PASS, lint clean, `make test` green with no container
started.

- [ ] **Step 6: Confirm the examples appear in the documentation**

```bash
go doc ./internal/inventory | grep -i example
go doc -all ./internal/inventory | grep -A3 'func ExampleDecide'
```

Expected: the examples are listed. They are part of the chapter now, not
separate from it.

- [ ] **Step 7: Run the full suite**

```bash
make test-integration 2>&1 | tail -20
```

Expected: every package `ok`.

- [ ] **Step 8: Commit**

```bash
git add internal/inventory/example_test.go \
        internal/platform/envelope/example_test.go \
        internal/platform/codec/example_test.go
git commit -m "test: worked examples that cannot go stale

Three Example functions, each with an // Output: block, so go test executes
them and a claim that stops being true fails the build. A worked example in a
comment has no such property, which is why these are not comments.

The valuable one is ExampleDecide_seatAlreadyHeld: it shows the distinction
the inventory package turns on, that SeatUnavailable is a reply and not an
event. A seat that lost a hundred races has a history of length one, not one
hundred and one.

Only pure packages get examples. outbox, inbox, eventstore, schema and kafka
all need infrastructure, and make test must never start a container — an
example for them would either need one or be a lie.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Done when

- [ ] Every site in Task 1's table is renamed, and nothing outside it is
- [ ] `grep -rn --include='*.go' 'handleOnce' internal/` is empty
- [ ] No declaration comment in `internal/` opens with a rationale
- [ ] Task 2's diff is comment-only, proved by the check in its Step 3
- [ ] Three `example_test.go` files exist, each example has an `// Output:`
      block, and `go test -run Example ./...` passes
- [ ] Test names and assertion counts are unchanged from before this plan
- [ ] `make lint` clean, `make test-integration` green
- [ ] Three commits, one per task

## Deliberately not done here

- **`internal/docs/docs_test.go`** — L6, the last pass, which turns this whole
  standard into assertions that run on every build.
- **Examples for the infrastructure packages** — they would need containers.
  Their behaviour is covered by `internal/integration`, which the chapters point
  at by name.
- **Renaming conventional short identifiers** — `ctx`, `tx`, `err`, `t`, method
  receivers, and three-line accumulators all stay. N2 permits them, and churning
  them would bury the six renames that matter.
