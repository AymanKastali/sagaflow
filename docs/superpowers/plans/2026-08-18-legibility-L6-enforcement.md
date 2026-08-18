# Legibility L6 — Enforcement

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The documentation standard becomes a test, so that a package written
in phase 6 that skips its chapter fails the suite the day it is written rather
than the day someone notices.

**Architecture:** One level-1 test package, `internal/docs`, containing only
`docs_test.go`. It walks the module tree with `go/parser`, finds every package,
and asserts the mechanical parts of the standard. It starts no container, needs
no network, and runs under `make test`.

**Tech Stack:** Go, `go/parser` and `go/ast` from the standard library, plus
`path/filepath.WalkDir`. No new dependency.

**Spec:** [docs/superpowers/specs/2026-08-18-legibility-design.md](../specs/2026-08-18-legibility-design.md)
§9, as amended to scope D1/D2. Operative standard:
[docs/conventions.md](../../conventions.md).

---

## Global Constraints

- **The five checks are D1–D5, exactly as `docs/conventions.md` states them.**
  The test is the standard made executable; if a check here disagrees with the
  conventions document, one of them is wrong and it must be resolved rather than
  papered over.
- **Scope, per the amended spec.** D1 applies to every package under `internal/`
  and `contracts/` that has **at least one non-test `.go` file**. D2 and D3 apply
  to **concept-bearing** packages: everything under `internal/platform/`, the
  service packages, `internal/testsupport/*`, `contracts/sagaflow/*/v1` and
  `cmd/*`. Test-only packages (`internal/integration`, `internal/toolchain`) are
  exempt from all of D1–D3, because Go forbids a non-`_test.go` file from
  declaring an external test package. The two `migrations` packages get D1 only.
- **The exemptions live in the test as a named, commented list**, not as a
  silent path filter. A future reader must be able to see what is exempt and why
  without reverse-engineering a glob.
- **A failure must say what to do.** "docs_test.go:41: FAIL" teaches nobody. Each
  assertion's message names the package, the rule, and the fix.
- **`make test` must not start a container**, so this test does no I/O beyond
  reading files in the repository.
- **No production code changes.** This plan adds one test file and, if anything
  it finds is a genuine gap, the minimum fix for that gap.
- Module path is `github.com/AymanKastali/sagaflow`.

---

## Scope

| In this plan | |
|---|---|
| `internal/docs/docs_test.go` | D1–D5, walking the tree |
| `README.md`, `docs/conventions.md` | Only if D5 or a rule statement turns out to be wrong |

| Not in this plan | |
|---|---|
| Checking that an explanation is *good* | No test can. D3 checks that the headings are present; the headings are chosen so that filling them in dishonestly is more work than filling them in honestly. |
| Enforcing the 60–120 line bound (C7) | Deliberately omitted — see the ruling below. |

**Ruling on C7.** The conventions document bounds a chapter at 60–120 lines, but
D1–D5 as specified do not include that check and this plan does not add it. A
line-count assertion would fail on a chapter that is genuinely short and correct
— `pg` is 66 lines and padding it would make it worse — and would encourage
padding to clear a floor. The bound stays advisory, enforced by review. If a
future chapter is three lines, D1's "non-empty" catches the pathological case.

---

## Task 1: The enforcement test

**Files:**
- Create: `internal/docs/docs_test.go`

**Interfaces:**
- Consumes: every `doc.go` written in L3, L4 and L5a, and the `README.md`
  written in L1.
- Produces: the guarantee that phases 5b–10 cannot silently skip a chapter.

- [ ] **Step 1: Confirm the current tree passes before writing the test**

The test must be written against a tree that already satisfies it, so that its
first run being green means something. Check by hand first:

```bash
echo "--- concept-bearing packages and their chapters ---"
for d in internal/platform/*/ internal/inventory internal/testsupport/*/ \
         contracts/sagaflow/inventory/v1 cmd/schemactl; do
  d=${d%/}
  [ -f "$d/doc.go" ] && printf '%-45s %s headings\n' "$d" "$(grep -c '^// # ' $d/doc.go)" \
                     || printf '%-45s MISSING\n' "$d"
done
echo "--- stray section refs ---"
grep -rn '§' --include='*.go' internal/ cmd/ contracts/ | grep -v doc.go || echo "none"
echo "--- platform packages linked from README ---"
for d in internal/platform/*/; do
  grep -q "(${d%/})" README.md || echo "NOT LINKED: ${d%/}"
done; echo "D5 checked"
```

Every concept-bearing package must show `6 headings`, stray refs must be `none`,
and D5 must report nothing. **If any of these fail, fix the tree first.** Writing
the test against a failing tree makes its first green run meaningless.

- [ ] **Step 2: Write the failing test first — assert something false**

TDD applies even here. Write the file with D1 only, but temporarily assert that
every package must have a package comment **of at least 10,000 characters**, so
it fails:

```bash
go test ./internal/docs/ 2>&1 | head -20
```

Expected: FAIL, listing real package names. That proves the walker actually
finds packages, which is the part most likely to be silently broken — a walker
with a wrong root finds nothing and passes every assertion vacuously.

- [ ] **Step 3: Write `internal/docs/docs_test.go` properly**

```go
// Package docs holds no code. It exists so that the documentation standard in
// docs/conventions.md is a test rather than a habit.
//
// A standard nobody checks decays at the first deadline. This walks the module
// and asserts the mechanical parts of the standard, so a package added in a
// later phase that skips its chapter fails the suite the day it is written.
package docs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot is two levels up from internal/docs.
const repoRoot = "../.."

// requiredHeadings are the six chapter headings from docs/conventions.md rule
// C1–C7, in the order a chapter must present them.
var requiredHeadings = []string{
	"# The problem",
	"# Why the obvious fixes do not work",
	"# What this package does",
	"# What it deliberately does not do",
	"# Reading order",
	"# Where this comes from",
}

// exemptFromChapter lists packages that get a package comment (D1) but not a
// doc.go chapter (D2, D3), with the reason each is exempt.
//
// Keeping this as a named list rather than a path glob means a reader can see
// what is exempt and why without reverse-engineering a pattern, and adding to it
// is a visible decision rather than a quiet one.
var exemptFromChapter = map[string]string{
	"internal/inventory/migrations": "nine lines embedding a directory of SQL; six headings would be padding",
	"internal/booking/migrations":   "nine lines embedding a directory of SQL; six headings would be padding",
}

// exemptEntirely lists packages with no non-test .go file at all. Go forbids a
// non-_test.go file from declaring an external test package, so a doc.go for
// these cannot be written — their package comment lives in the test file.
var exemptEntirely = map[string]string{
	"internal/integration": "test-only package, declares package integration_test",
	"internal/toolchain":   "test-only package, guards the Go version floor",
}
```

Then the walker and the five checks. The walker must:

- start at `repoRoot`, walk `internal/`, `cmd/` and `contracts/`
- skip `testdata`, `.git`, `.superpowers`, and any directory with no `.go` files
- for each directory, parse with `parser.ParseDir(..., parser.ParseComments)`
- classify: test-only (every file ends `_test.go`), exempt-from-chapter, or
  concept-bearing

And the checks:

- **D1** — the package has a non-empty package comment somewhere. Use
  `ast.Package`'s doc, or read the `*ast.File` whose `Doc` is non-nil.
- **D2** — for concept-bearing packages, that comment is in a file named
  `doc.go`, and `doc.go` declares no types, functions, constants, variables or
  imports. Check the parsed file's `Decls` is empty and `Imports` is empty.
- **D3** — the `doc.go` package comment contains all six `requiredHeadings`, in
  order. Find each heading's index in the comment text and assert the indices
  increase.
- **D4** — no `.go` file other than a `doc.go` contains `§`. Walk files, read
  bytes, `strings.Contains`.
- **D5** — `README.md` contains `(internal/platform/<name>)` for every directory
  under `internal/platform/`.

Every failure message must name the package, the rule, and the fix. For example:

```go
t.Errorf("%s: rule D3 — chapter is missing the heading %q.\n"+
	"Every chapter needs all six headings from docs/conventions.md, in order.\n"+
	"See internal/platform/outbox/doc.go for the shape.", pkgPath, heading)
```

- [ ] **Step 4: Run it and watch it pass**

```bash
go test -v ./internal/docs/
```

Expected: every subtest PASS. If anything fails, the tree is wrong, not the
test — fix the tree.

- [ ] **Step 5: Mutation-test every check**

A green test proves nothing until you have watched it go red. Break each rule in
turn, confirm the specific check catches it, and restore. **Do all five.**

```bash
# D1/D2: hide a chapter
mv internal/platform/inbox/doc.go /tmp/doc.go.bak
go test ./internal/docs/ 2>&1 | head -5
mv /tmp/doc.go.bak internal/platform/inbox/doc.go

# D3: remove a heading
sed -i '0,|^// # Reading order$|s|^// # Reading order$|// # Files|' internal/platform/inbox/doc.go
go test ./internal/docs/ 2>&1 | head -5
git checkout internal/platform/inbox/doc.go

# D4: plant a section reference outside doc.go
printf '\n// A stray reference to spec §1.\n' >> internal/platform/inbox/inbox.go
go test ./internal/docs/ 2>&1 | head -5
git checkout internal/platform/inbox/inbox.go

# D5: unlink a package from the README
sed -i 's|(internal/platform/inbox)|(internal/platform/inbox-typo)|' README.md
go test ./internal/docs/ 2>&1 | head -5
git checkout README.md

# D2's "nothing but the comment": put code in a doc.go
printf '\nconst intruder = 1\n' >> internal/platform/inbox/doc.go
go test ./internal/docs/ 2>&1 | head -5
git checkout internal/platform/inbox/doc.go
```

Each must FAIL with the message naming that specific rule, then pass again after
restoring. **Record the five failure messages in the commit body** — a check
nobody has seen fail is a check nobody should trust.

- [ ] **Step 6: Confirm the test starts no container and is fast**

```bash
go test -short -v ./internal/docs/ 2>&1 | tail -3
time go test -count=1 ./internal/docs/
```

Expected: passes under `-short` (it must, since it does no I/O beyond file
reads), and completes in well under a second.

- [ ] **Step 7: Run the whole suite**

```bash
make lint && make test 2>&1 | tail -5 && make test-integration 2>&1 | tail -20
```

Expected: lint clean, both suites green, `internal/docs` listed as `ok`.

- [ ] **Step 8: Commit**

```bash
git add internal/docs/
git commit -m "test(docs): make the documentation standard a test

A standard nobody checks decays at the first deadline. This walks the module
and asserts the mechanical parts of docs/conventions.md: every package has a
package comment, every concept-bearing package has it in a doc.go holding
nothing else, every chapter has all six headings in order, no § appears
outside a doc.go, and the README links every platform package.

The exemptions are a named list with a reason each, not a path glob, so a
reader can see what is exempt and why. Test-only packages are exempt because
Go forbids a non-_test.go file from declaring an external test package, so a
doc.go for them cannot exist. The migrations packages are exempt from the
chapter because nine lines embedding SQL cannot fill six headings without
padding.

Every check was mutation-tested — broken deliberately, watched fail with a
message naming the rule and the fix, then restored. A check nobody has seen
fail is a check nobody should trust.

Not enforced: the 60-120 line bound on a chapter. It would fail on a chapter
that is genuinely short and correct — pg is 66 lines and padding it would make
it worse — and would reward padding. It stays advisory.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Close the loop on the README and the conventions

**Files:**
- Modify: `README.md` — the status of the documentation work
- Modify: `docs/conventions.md` — the enforcement section's honesty marker

**Interfaces:**
- Consumes: the test from Task 1.
- Produces: a documentation set with no forward references left.

- [ ] **Step 1: Remove the "not yet written" marker from `docs/conventions.md`**

L1 added an italic line under the Enforcement section saying
`internal/docs/docs_test.go` was not yet written and that D1–D5 were checked by
hand until then. It exists now. Delete that line.

Verify no marker survives:

```bash
grep -n 'Not yet written' docs/conventions.md && echo "MARKER REMAINS" || echo "marker gone"
```

- [ ] **Step 2: Update the README's note under the concepts table**

L1 wrote: *"The package chapters are being written now — see the legibility
spec. Until a package has one, `go doc` shows only its existing summary — and
for `internal/platform/kafka`, which has no package comment yet, only a list of
symbols."*

Every package now has a chapter, including `kafka`. Replace that paragraph with
one that says so, and points at `docs/conventions.md` for the standard and at
`internal/docs/docs_test.go` for the fact that it is enforced.

- [ ] **Step 3: Verify the whole documentation set is internally consistent**

```bash
for f in README.md docs/conventions.md docs/glossary.md docs/architecture.md docs/message-lifecycle.md; do
  d=$(dirname "$f")
  grep -oP '\]\(\K[^)]+' "$f" | grep -v '^http' | sed 's/#.*//' | grep -v '^$' \
    | while read -r t; do [ -e "$d/$t" ] || echo "BROKEN in $f: $t"; done
done
grep -rn 'not yet written\|Not yet written\|coming in the next' README.md docs/*.md || echo "no forward references remain"
```

Expected: no `BROKEN` lines, and `no forward references remain`.

- [ ] **Step 4: Read the result as a stranger would**

Not a command — a judgment. Run these in order, as the README's reading order
tells someone to, and check that each one lands:

```bash
go doc ./internal/platform/eventstore
go doc ./internal/platform/outbox
go doc ./internal/platform/inbox
go doc ./internal/inventory
go doc -all ./internal/inventory | grep -A20 'func ExampleDecide_seatAlreadyHeld'
```

If any of them leaves an obvious question unanswered, that is a bug in the
chapter and should be fixed now rather than filed.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/conventions.md
git commit -m "docs: close the loop — every chapter exists and is enforced

Removes the last two forward references in the documentation set: the
conventions page no longer says its enforcement test is unwritten, and the
README no longer warns that internal/platform/kafka has no package comment.
Both were true when written and are not now.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Done when

- [ ] `internal/docs/docs_test.go` exists and passes
- [ ] All five checks have been watched to fail and recover — the five failure
      messages are in the commit body
- [ ] The test runs under `make test`, starts no container, and takes under a
      second
- [ ] No "not yet written" or "coming in the next pass" marker remains in
      `README.md` or any file in `docs/`
- [ ] Every relative link across the five documents resolves
- [ ] `make lint` clean, `make test` and `make test-integration` green
- [ ] Two commits

## What happens next

Phase 5b resumes: `internal/platform/timers`, the seat-hold TTL emitting
`SeatHoldExpired`, the availability projection with its fold-drop-rebuild test,
and `wire.go` and `cmd/inventory` to make the service runnable.

It is written to this standard from its first line, and `internal/docs` is what
makes that true rather than aspirational — `platform/timers` will fail the suite
until it has a chapter.

The spec §7 tree amendment moving the timer scheduler out of `saga/` into its
own `timers/` entry is still owed, and lands with that code.
