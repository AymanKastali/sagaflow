# Platform Package Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct four structural defects in `internal/platform/` before plan set 2 writes four services against the current boundaries.

**Architecture:** A pure refactor in six commits — the spec's five code changes plus the §7 amendment they oblige. A neutral `envelope.Message` replaces `outbox.Message` so the transport layer stops importing the persistence layer; the schema-registry island moves out of `kafka`; test scaffolding and two test-only packages move to self-describing homes; generated contracts become a separate public Go module. No behaviour changes, no wire-format changes, no SQL changes.

**Tech Stack:** Go 1.26.6, buf v2 managed mode, protoc-gen-go, franz-go, pgx v5, testcontainers-go.

**Spec:** [docs/superpowers/specs/2026-08-18-platform-package-restructure-design.md](../specs/2026-08-18-platform-package-restructure-design.md), which amends [2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md) §7.

## Global Constraints

- Go toolchain floor is `go1.26.6`, matching the `go` directive in `go.mod`. Reached via `GOTOOLCHAIN`, not a machine-level install.
- `make test` must never start a container. Container tests skip under `-short`.
- One Postgres / Kafka / registry container per package, started in `TestMain`. Never one per test.
- No `time.Sleep` in assertions. Completion is a channel signal with a context deadline as the failure mode.
- Every task ends in a commit. Every commit must build and pass the suite on its own.
- Proto package names (`sagaflow.inventory.v1`) must not change. They are simultaneously `events.type`, `ce_type`, and the registry subject.
- Commit messages end with `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.

## This is a refactor, so the TDD cycle is inverted

There is no new behaviour to drive out with a failing test. The existing 2977-line suite **is** the specification, and the cycle for every task is:

1. Run the suite and confirm it is green *before* touching anything.
2. Make the change.
3. Run the suite again. It must be green with **no assertion edited**.

**The one legitimate exception, in Task 1:** `fakePublisher` and `kafka.Producer` implement `outbox.Publisher`, whose signature changes. Their method signatures and the fake's field types must change — that is implementing a changed interface, not editing a test. Assertions inside test bodies must not change. If any `t.Fatalf`/`t.Errorf` condition needs editing to make the suite pass, **stop and report**: it means the refactor altered behaviour.

---

### Task 1: Neutral message type

Moves the message struct out of `outbox` into `envelope`, narrows `Publisher` so row ids stop crossing the boundary, and removes the `kafka → outbox` edge.

This task cannot be split. Narrowing `Publisher` breaks `kafka.Producer` in the same instant, so a commit containing only half of it does not build.

**Files:**
- Modify: `internal/platform/envelope/envelope.go` (append `Message` + `Validate`)
- Modify: `internal/platform/outbox/outbox.go:22-59` (delete `Message`/`validate`, redefine `Claimed`, narrow `Publisher`), `:75-93` (`Enqueue` signature + validate call)
- Modify: `internal/platform/outbox/poller.go:190` (map claims to messages before publishing)
- Modify: `internal/platform/kafka/producer.go:1-16,45-73` (drop `outbox` import, move assertion out)
- Modify: `internal/platform/kafka/consumer.go:291-298` (dead letter builds an `envelope.Message`)
- Test: `internal/platform/outbox/outbox_test.go` (fake + call sites), `internal/platform/kafka/producer_test.go` (call sites + moved assertion), `internal/platform/delivery_test.go` (call sites)

**Interfaces:**
- Consumes: nothing from earlier tasks — this is the first.
- Produces: `envelope.Message{Topic string; Key string; Payload []byte; Headers map[string]string}` with method `Validate() error`. `outbox.Claimed{ID int64; envelope.Message}`. `outbox.Publisher` with `Publish(ctx context.Context, msgs []envelope.Message) error`. `outbox.Enqueue(ctx context.Context, tx pgx.Tx, msgs []envelope.Message) error`. Tasks 2–5 do not depend on these, but plan set 2 will.

- [ ] **Step 1: Confirm the suite is green before touching anything**

```bash
make up
go test -race -count=1 -timeout 15m ./...
```

Expected: PASS across all 11 packages. If anything fails here, stop — the baseline is broken and nothing below is measurable.

- [ ] **Step 2: Add `Message` and `Validate` to envelope**

Append to `internal/platform/envelope/envelope.go`. The file already imports `errors`, so no import change is needed.

```go
// Message is one message to publish: a body, the headers an Envelope renders,
// and the routing key.
//
// It lives here rather than in outbox because it is the vocabulary two packages
// share — outbox rows and dead letters are both Messages, and a dead letter was
// never in the outbox.
type Message struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// Validate rejects a message that could not be published correctly. It is
// exported because outbox.Enqueue is now an outside caller.
//
// The strings are unprefixed because every caller wraps them with its own
// context.
func (m Message) Validate() error {
	switch {
	case m.Topic == "":
		return errors.New("no topic")
	case m.Key == "":
		// Without a key Kafka round-robins the record, which destroys the
		// per-stream ordering every consumer downstream relies on.
		return errors.New("no key")
	case len(m.Payload) == 0:
		return errors.New("no payload")
	}
	return nil
}
```

- [ ] **Step 3: Rewrite the outbox type block**

In `internal/platform/outbox/outbox.go`, replace lines 22–59 (from `// Message is one message to publish.` through the closing brace of the `Publisher` interface) with:

```go
// Claimed is a Message plus the row id the poller marks once it is published.
// It never leaves this package: Publisher takes plain messages, because the row
// ids were never the publisher's business.
type Claimed struct {
	ID int64
	envelope.Message
}

// Publisher sends messages. It is an interface so that every property of the
// poller — ordering, retention on failure, election — is testable without a
// broker. The Kafka implementation lives in platform/kafka.
type Publisher interface {
	Publish(ctx context.Context, msgs []envelope.Message) error
}
```

Then fix the imports at the top of the same file: `errors` is no longer used, and `envelope` is:

```go
import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
)
```

- [ ] **Step 4: Update `Enqueue` to take envelope messages**

In the same file, change the signature on line 75 and the validate call on line 85:

```go
func Enqueue(ctx context.Context, tx pgx.Tx, msgs []envelope.Message) error {
```

```go
		if err := m.Validate(); err != nil {
```

Everything else in `Enqueue` is unchanged — the field names are identical.

- [ ] **Step 5: Make the poller map claims to messages**

In `internal/platform/outbox/poller.go`, replace the publish call at line 190:

```go
	if err := p.pub.Publish(ctx, messages(claimed)); err != nil {
		// Rolling back releases the claim, so the rows are picked up next pass.
		return 0, fmt.Errorf("outbox: publish %d messages: %w", len(claimed), err)
	}
```

And add `messages` next to the existing `ids` helper (after the `ids` function, around line 240):

```go
// messages strips the row ids the publisher has no use for. The correspondence
// with claimed is positional, but nothing needs it: Publish is all-or-nothing,
// so a success marks the whole batch.
func messages(claimed []Claimed) []envelope.Message {
	out := make([]envelope.Message, len(claimed))
	for i, c := range claimed {
		out[i] = c.Message
	}
	return out
}
```

Add the `envelope` import to `poller.go` if `goimports` does not.

- [ ] **Step 6: Update the producer**

In `internal/platform/kafka/producer.go`, change the import block, delete the assertion on line 16, and update both signatures:

```go
import (
	"context"
	"fmt"

	"github.com/AymanKastali/sagaflow/internal/platform/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes messages to Kafka. It satisfies outbox.Publisher — asserted
// in the test rather than here, because importing outbox for a compile-time
// assertion would reintroduce a dependency this package does not otherwise need.
type Producer struct {
	cl *kgo.Client
}
```

```go
func (p *Producer) Publish(ctx context.Context, msgs []envelope.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		recs = append(recs, record(m))
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish %d records: %w", len(recs), err)
	}
	return nil
}
```

```go
// record converts one message to its wire form. The key is always the stream id,
// which is what keeps a stream's events in one partition and therefore in order.
func record(m envelope.Message) *kgo.Record {
```

Note the loop body changed from `record(m.Message)` to `record(m)` — `m` is now already an `envelope.Message`.

- [ ] **Step 7: Update the dead-letter path**

In `internal/platform/kafka/consumer.go`, replace the return at lines 291–298:

```go
	return c.cfg.DLQ.Publish(ctx, []envelope.Message{{
		Topic:   r.Topic + ".dlq",
		Key:     r.Key, // same key so a replay lands on the same partition
		Payload: r.Value,
		Headers: headers,
	}})
```

Swap the `outbox` import for `envelope` in that file's import block.

- [ ] **Step 8: Update the outbox test's fake publisher**

In `internal/platform/outbox/outbox_test.go`, change the fake's types (lines 222–248). Only types change; the body's logic is identical.

```go
type fakePublisher struct {
	mu   sync.Mutex
	got  [][]envelope.Message
	err  error
	call int
	// published, when set, receives each batch. Tests that drive Run block on it
	// instead of sleeping: spec §12.4 wants completion to be a signal with a
	// context deadline as the failure mode, not a guess at a duration.
	published chan []envelope.Message
}

func (f *fakePublisher) Publish(_ context.Context, msgs []envelope.Message) error {
```

At line 581, change the channel type:

```go
	pub := &fakePublisher{published: make(chan []envelope.Message, 4)}
```

`keys()` is unchanged — `m.Key` still resolves.

- [ ] **Step 9: Update the remaining outbox test call sites**

In the same file, every `outbox.Message` becomes `envelope.Message`. There are ten: lines 42–43 (the `msg` helper's return type and literal), 74, 101, 127, 202, 204, 205, 206, 210, 265, 466, 475, 638. Add the `envelope` import.

The `msg` helper becomes:

```go
func msg(topic, key string) envelope.Message {
	return envelope.Message{
```

- [ ] **Step 10: Update the producer test and move the interface assertion**

In `internal/platform/kafka/producer_test.go`, add the assertion that moved out of `producer.go`, near the top after the imports:

```go
// Producer must satisfy the interface the outbox poller consumes. The assertion
// lives here because kafka does not import outbox in production — a test import
// does not appear in the production dependency graph.
var _ outbox.Publisher = (*kafka.Producer)(nil)
```

Then simplify the two call sites. Line 73:

```go
	err = p.Publish(ctx, []envelope.Message{{
		Topic:   topic,
		Key:     "seat-BA117-2026-09-01-14A",
		Payload: []byte{0x00, 0x0a, 0x0b},
		Headers: wantHeaders,
	}})
```

Line 131:

```go
	var batch []envelope.Message
	for i := range total {
		batch = append(batch, envelope.Message{
			Topic:   topic,
			Key:     "seat-14A", // one key ⇒ one partition ⇒ ordered
			Payload: []byte{byte(i)},
			Headers: map[string]string{"ce_id": "id"},
		})
	}
```

The `ID:` fields are dropped because `Publish` no longer receives them. Add the `envelope` import; keep the `outbox` import for the assertion.

- [ ] **Step 11: Update the delivery test call sites**

In `internal/platform/delivery_test.go`, three sites. Line 157:

```go
		return outbox.Enqueue(ctx, tx, []envelope.Message{{
```

Lines 208–209:

```go
		if err := producer.Publish(ctx, []envelope.Message{
			{Topic: eventsTopic, Key: seat, Payload: wire, Headers: e.Headers()},
		}); err != nil {
```

Lines 275–283:

```go
	var batch []envelope.Message
	for i := range total {
		e := envelope.Envelope{
			ID: envelope.NewID(), Source: source, Type: "sagaflow.inventory.v1.SeatHeld",
			Subject: fmt.Sprintf("seat-%03d", i),
		}
		batch = append(batch, envelope.Message{
			Topic: topic, Key: e.Subject, Payload: []byte(`{}`), Headers: e.Headers(),
		})
	}
```

`envelope` is already imported in this file.

- [ ] **Step 12: Verify the wrong edge is gone**

```bash
go build ./... && go list -f '{{join .Imports "\n"}}' github.com/AymanKastali/sagaflow/internal/platform/kafka | grep sagaflow
```

Expected: exactly one line, `github.com/AymanKastali/sagaflow/internal/platform/envelope`. If `.../outbox` appears, a production file still imports it.

- [ ] **Step 13: Run the full suite**

```bash
go vet ./... && go test -race -count=1 -timeout 15m ./...
```

Expected: PASS, identical package list to Step 1. If a `t.Fatalf` or `t.Errorf` condition had to change to get here, stop and report.

- [ ] **Step 14: Commit**

```bash
git add internal/platform/envelope internal/platform/outbox internal/platform/kafka internal/platform/delivery_test.go
git commit -F - <<'EOF'
refactor(platform): move the message type to envelope, narrow Publisher

kafka imported outbox only for the struct in Publisher's signature, so the
dead-letter path had to fabricate an outbox.Claimed -- an ID it does not have,
for a row that does not exist -- to publish a record that was never in the
outbox.

envelope owns the header vocabulary and has no internal dependencies, so the
neutral Message belongs there. Publisher narrows to []envelope.Message: Drain
publishes all-or-nothing and marks the batch with ids(claimed) on success, so
the row ids were never the publisher's business. Claimed becomes internal.

The compile-time Publisher assertion moves to producer_test.go; a test import
does not appear in the production dependency graph.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 2: Extract the schema-registry package

`kafka` is five files doing four unrelated jobs. `serde.go` and `compat.go` are already an isolated island: `producer.go`, `consumer.go`, and `admin.go` reference no symbol declared in either.

**Files:**
- Create: `internal/platform/schema/serde.go` (moved from `internal/platform/kafka/serde.go`), `internal/platform/schema/compat.go` (moved from `internal/platform/kafka/compat.go`)
- Delete: `internal/platform/kafka/serde.go`, `internal/platform/kafka/compat.go`
- Modify: `cmd/schemactl/main.go:57,76`
- Test: `internal/platform/schema/serde_test.go` (moved from `internal/platform/kafka/serde_test.go`)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `schema.Subject(topic, typeName string) string`, `schema.Serde` with `Encode(proto.Message) ([]byte, error)` and `Decode([]byte) (proto.Message, error)`, `schema.NewTopicSerde(ctx context.Context, cl *sr.Client, topic string, prototypes ...proto.Message) (*Serde, error)`, `schema.EnsureBackwardCompatibility(ctx context.Context, cl *sr.Client) error`, `schema.ErrSubjectNotRegistered`.

- [ ] **Step 1: Move the three files with git**

```bash
mkdir -p internal/platform/schema
git mv internal/platform/kafka/serde.go internal/platform/schema/serde.go
git mv internal/platform/kafka/compat.go internal/platform/schema/compat.go
git mv internal/platform/kafka/serde_test.go internal/platform/schema/serde_test.go
```

`git mv` rather than copy-and-delete so the history follows the file.

- [ ] **Step 2: Change the package clauses and add a package comment**

`internal/platform/schema/serde.go` — replace the bare `package kafka` line at the top with a package doc, since this file is now the package's primary file:

```go
// Package schema frames protobuf messages for the wire against a Confluent-
// compatible registry, and pins the registry's compatibility level.
//
// It is separate from platform/kafka because it shares no symbol with the broker
// plumbing: framing is about schema ids and message bytes, not about brokers,
// partitions, or offsets.
package schema
```

`internal/platform/schema/compat.go` — change `package kafka` to `package schema`.

`internal/platform/schema/serde_test.go` — change `package kafka_test` to `package schema_test`, and update its own import of `platform/kafka` to `platform/schema` plus every `kafka.` qualifier in the file to `schema.`.

- [ ] **Step 3: Rename the error to match its package**

In `internal/platform/schema/serde.go`, line 18:

```go
var ErrSubjectNotRegistered = errors.New("schema: subject not registered")
```

Then update every other `kafka:` prefix in the two moved files to `schema:` — they appear in `fmt.Errorf` calls. Find them with:

```bash
grep -n '"kafka:\|kafka: ' internal/platform/schema/*.go
```

Expected after fixing: no matches.

- [ ] **Step 4: Update schemactl**

In `cmd/schemactl/main.go`, add the `schema` import and change two call sites:

Line 57: `if err := schema.EnsureBackwardCompatibility(ctx, cl); err != nil {`

Line 76: `subject := schema.Subject(b.topic, string(b.msg.ProtoReflect().Descriptor().FullName()))`

Then check whether `cmd/schemactl` still uses anything from `platform/kafka`:

```bash
grep -n 'kafka\.' cmd/schemactl/main.go
```

If there are no matches, remove the `platform/kafka` import.

- [ ] **Step 4b: Split the `TestMain` that moved with serde_test.go**

*Added during execution — the plan missed it.* `serde_test.go` held the whole `kafka` package's `TestMain`, so moving the file left `kafka` with no broker and every one of its tests failing on `kafkatest: no broker running`.

The split is cleaner than what was there: the combined `TestMain` started a registry **and** a broker for every test in the package, and neither half needed both. `schema` tests call only `srtest.Shared`; `kafka` tests call only `kafkatest.Shared`.

Create `internal/platform/kafka/main_test.go`:

```go
package kafka_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/AymanKastali/sagaflow/internal/platform/kafkatest"
)

// One broker for the whole package (spec §12.4). Isolation comes from topic names
// and consumer groups derived from the test, not from a container per test.
//
// This package needs no registry: framing against one is platform/schema's job,
// and these tests only produce and consume bytes.
func TestMain(m *testing.M) {
	stop, err := kafkatest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}
```

In `internal/platform/schema/serde_test.go`, narrow the moved `TestMain` to the registry alone and drop its `kafkatest` import:

```go
// One registry for the whole package (spec §12.4). Starting one per test would
// dominate the suite's runtime.
//
// No broker: framing is about schema ids and message bytes, so nothing here needs
// one. That is the split platform/kafka and platform/schema exist to make.
func TestMain(m *testing.M) {
	stop, err := srtest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}
```

- [ ] **Step 5: Verify and run the suite**

```bash
go vet ./... && go test -race -count=1 -timeout 15m ./...
```

Expected: PASS, now with `internal/platform/schema` in the package list and `internal/platform/kafka` still present. The `kafka` package should have dropped from 569 lines to 413.

- [ ] **Step 6: Commit**

```bash
git add -A internal/platform/schema internal/platform/kafka cmd/schemactl
git commit -F - <<'EOF'
refactor(platform): extract schema registry framing out of kafka

kafka was five files doing four jobs. serde.go and compat.go were already an
isolated island inside it -- producer.go, consumer.go, and admin.go reference no
symbol either one declares -- so the split costs nothing and the remaining
package is broker plumbing only.

Error prefixes follow the file: "kafka:" becomes "schema:".

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 3: Move test scaffolding out of platform

`pgtest`, `kafkatest`, and `srtest` sit as siblings of `pg` and `kafka`, so `go list ./...`, coverage, and lint treat 509 lines of container plumbing as production packages. Package names stay — renaming them to `postgres`/`kafka`/`registry` would collide with `platform/kafka` at every import site and buy nothing.

**Files:**
- Move: `internal/platform/pgtest/` → `internal/testsupport/pgtest/`, `internal/platform/kafkatest/` → `internal/testsupport/kafkatest/`, `internal/platform/srtest/` → `internal/testsupport/srtest/`
- Modify (imports only): `internal/platform/kafka/producer_test.go`, `internal/platform/kafka/consumer_test.go`, `internal/platform/schema/serde_test.go`, `internal/platform/codec/codec_test.go`, `internal/platform/inbox/inbox_test.go`, `internal/platform/eventstore/eventstore_test.go`, `internal/platform/outbox/outbox_test.go`, `internal/platform/pg/pg_test.go`, `internal/platform/delivery_test.go`, `internal/testsupport/kafkatest/kafkatest_test.go`

**Interfaces:**
- Consumes: `platform/schema` from Task 2, since `serde_test.go` now lives there and imports `srtest`.
- Produces: import paths `github.com/AymanKastali/sagaflow/internal/testsupport/{pgtest,kafkatest,srtest}`. Package names and every exported symbol are unchanged.

- [ ] **Step 1: Move the directories**

```bash
mkdir -p internal/testsupport
git mv internal/platform/pgtest internal/testsupport/pgtest
git mv internal/platform/kafkatest internal/testsupport/kafkatest
git mv internal/platform/srtest internal/testsupport/srtest
```

- [ ] **Step 2: Rewrite the import paths**

The `s|…|` form will not work here — `|` is both the sed delimiter and the alternation operator. Use `#`:

```bash
grep -rl 'internal/platform/\(pgtest\|kafkatest\|srtest\)' --include='*.go' . \
  | xargs sed -i 's#internal/platform/\(pgtest\|kafkatest\|srtest\)#internal/testsupport/\1#g'
```

Then confirm nothing was missed:

```bash
grep -rn 'internal/platform/\(pgtest\|kafkatest\|srtest\)' --include='*.go' .
```

Expected: no matches.

- [ ] **Step 3: Fix the one production import inside the moved code**

`internal/testsupport/pgtest/pgtest.go` imports `internal/platform/pg`. That path did not change, so it should still be correct — verify:

```bash
grep -n 'platform/pg"' internal/testsupport/pgtest/pgtest.go
```

Expected: one match, unchanged. This is the only production-package import any of the three helpers has.

- [ ] **Step 4: Prove no production package reaches testsupport**

```bash
go build ./... && for p in $(go list ./... | grep -v testsupport); do
  go list -f '{{join .Deps "\n"}}' $p | grep testsupport && echo "LEAK: $p"
done; echo "checked"
```

Expected: `checked` with no `LEAK:` lines. `.Deps` is the production dependency closure, so test-only imports correctly do not appear.

- [ ] **Step 5: Run the suite**

```bash
go vet ./... && go test -race -count=1 -timeout 15m ./...
```

Expected: PASS. `internal/testsupport/pgtest` and `internal/testsupport/srtest` report `[no test files]`; `internal/testsupport/kafkatest` runs its own test.

- [ ] **Step 6: Commit**

```bash
git add -A internal
git commit -F - <<'EOF'
refactor(platform): move container helpers to internal/testsupport

pgtest, kafkatest, and srtest sat as siblings of pg and kafka, so go list,
coverage, and lint counted 509 lines of container plumbing as production
packages. Nothing but a naming convention separated scaffolding from product.

Package names are unchanged -- renaming them to postgres/kafka/registry would
collide with platform/kafka at every import site.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 4: Give the two homeless tests a home

`internal/platform` is a package holding one 347-line test file and no production code, while its directory is simultaneously the namespace root for eight packages. `internal/platform/version` holds one test file and no production code behind a name that sounds like a library.

**Files:**
- Move: `internal/platform/delivery_test.go` → `internal/integration/delivery_test.go`
- Move: `internal/platform/version/version_test.go` → `internal/toolchain/toolchain_test.go`
- Delete: the now-empty `internal/platform/version/` directory

**Interfaces:**
- Consumes: `internal/testsupport/*` from Task 3, `envelope.Message` from Task 1 — both already reflected in the moved file.
- Produces: nothing. Both are leaf test packages that no other package imports.

- [ ] **Step 1: Move the delivery test**

```bash
mkdir -p internal/integration
git mv internal/platform/delivery_test.go internal/integration/delivery_test.go
```

- [ ] **Step 2: Update its package clause**

In `internal/integration/delivery_test.go`, replace the package doc and clause at the top:

```go
// Package integration holds cross-package deliverables — tests that exercise
// several platform packages together rather than any one of them.
//
// The first is spec §13 phase 4: an event committed in one service's transaction
// reaching another service's handler, applied exactly once. It lives here rather
// than in a service package because the services are phases 5–8. Two databases in
// one container, never one database with two schemas: no transaction can span
// them, which is the property being demonstrated.
package integration_test
```

The import block is unchanged — Task 3 already rewrote the `testsupport` paths.

- [ ] **Step 3: Move the toolchain guard**

```bash
mkdir -p internal/toolchain
git mv internal/platform/version/version_test.go internal/toolchain/toolchain_test.go
rmdir internal/platform/version
```

- [ ] **Step 4: Update its package clause**

In `internal/toolchain/toolchain_test.go`, change the first line from `package version` to:

```go
// Package toolchain guards the Go version the build actually runs on. It holds
// only a test because a guard has nothing to export.
package toolchain
```

Nothing else in the file changes. `Minimum`, `meetsFloor`, and both tests keep their names.

- [ ] **Step 5: Confirm platform is no longer a package**

```bash
go list ./... | grep -x 'github.com/AymanKastali/sagaflow/internal/platform'
```

Expected: no output, and a non-zero exit from grep. `internal/platform` is now a directory only.

- [ ] **Step 6: Run the suite**

```bash
go vet ./... && go test -race -count=1 -timeout 15m ./...
```

Expected: PASS. `internal/platform` is absent from the results; `internal/integration` and `internal/toolchain` appear. Confirm the deliverable specifically:

```bash
go test -race -count=1 -run TestEventCrossesServicesExactlyOnce -v ./internal/integration/
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A internal
git commit -F - <<'EOF'
refactor(platform): give the two test-only packages self-describing homes

internal/platform was simultaneously the namespace root for eight packages and a
package holding one 347-line test; internal/platform/version held one test behind
a name that reads like a library. Both were tests parked where they landed.

internal/integration and internal/toolchain are still packages holding only
tests, which is fine when the directory says so.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 5: Contracts as a separate public module

Generated contracts sit four levels below `internal/`, under a directory named for platform mechanics, unimportable from outside the module. A separate module is what makes "public" mean anything: consumers get the event types without inheriting pgx, franz-go, and testcontainers.

**Resolves the spec's open mechanical detail.** Proto package `sagaflow.inventory.v1` plus buf's `PACKAGE_DIRECTORY_MATCH` pins the source path, and `paths=source_relative` mirrors it, so generated code lands at `contracts/sagaflow/inventory/v1` and the import path stutters. **The stutter is accepted.** Removing it would need either a proto package rename (forbidden by Global Constraints — it would change `ce_type`) or a `PACKAGE_DIRECTORY_MATCH` lint exception. Neither is worth it, because every import site already aliases the package as `inventoryv1`, so the path appears once per file in an import block and nowhere else.

**Files:**
- Create: `contracts/go.mod`
- Move: `internal/platform/contracts/sagaflow/` → `contracts/sagaflow/`
- Delete: the now-empty `internal/platform/contracts/`
- Modify: `buf.gen.yaml`, `go.mod`, `Makefile:15-31`
- Modify (imports only): `cmd/schemactl/main.go`, `internal/platform/codec/codec_test.go`, `internal/integration/delivery_test.go`, `internal/platform/kafka/consumer_test.go`, `internal/platform/schema/serde_test.go`, `contracts/sagaflow/inventory/v1/fullname_test.go`

**Interfaces:**
- Consumes: `internal/platform/schema` from Task 2, `internal/integration` from Task 4 — both appear in the import-rewrite list.
- Produces: import path `github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1`, conventionally aliased `inventoryv1`. Exported types `SeatHeld` and `HoldSeat` are unchanged, as are their fully qualified proto names.

- [ ] **Step 1: Move the generated tree**

```bash
git mv internal/platform/contracts contracts
```

This leaves `contracts/sagaflow/inventory/v1/*.go` — the layout buf will regenerate into, so the move and the config agree.

- [ ] **Step 2: Create the module**

Create `contracts/go.mod`:

```
module github.com/AymanKastali/sagaflow/contracts

go 1.26.6

require google.golang.org/protobuf v1.36.12
```

The generated code imports only `protoreflect`, `protoimpl`, and `timestamppb`, all from that one module. `fullname_test.go` additionally imports `proto`, same module.

- [ ] **Step 3: Wire the root module to it**

```bash
go mod edit -require=github.com/AymanKastali/sagaflow/contracts@v0.0.0
go mod edit -replace=github.com/AymanKastali/sagaflow/contracts=./contracts
```

The `replace` is what makes local development work without a `go.work`. It is ignored by anyone importing `github.com/AymanKastali/sagaflow` from outside, which is correct — they should depend on a tagged `contracts/vX.Y.Z` instead.

- [ ] **Step 4: Rewrite the import paths**

```bash
grep -rl 'internal/platform/contracts' --include='*.go' . \
  | xargs sed -i 's|internal/platform/contracts/sagaflow|contracts/sagaflow|g'
grep -rn 'internal/platform/contracts' --include='*.go' .
```

Expected from the second command: no matches. The alias `inventoryv1` at each site is unchanged.

- [ ] **Step 5: Point buf at the new output**

Replace `buf.gen.yaml` entirely:

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/AymanKastali/sagaflow/contracts
plugins:
  - local: ["go", "tool", "protoc-gen-go"]
    out: contracts
    opt: paths=source_relative
```

Only two values changed — the `go_package_prefix` and the `out` directory — both dropping `internal/platform/`.

- [ ] **Step 6: Regenerate and confirm the output is byte-identical**

```bash
make generate
git status --short contracts/
```

Expected: the two `.pb.go` files show as modified with **only** their `go_package`-derived header comment changed, or as unmodified. Confirm the diff touches nothing else:

```bash
git diff contracts/ | grep '^[+-]' | grep -v '^[+-][+-]' | head -20
```

If any message field, type, or descriptor byte differs, stop and report — regeneration should be a no-op apart from the package path.

- [ ] **Step 7: Tidy both modules**

```bash
go mod tidy
cd contracts && go mod tidy && cd ..
```

- [ ] **Step 8: Teach the Makefile about the second module**

In `Makefile`, replace the `lint`, `test`, and `test-integration` targets. Each `cd` is confined to its own recipe line, which is its own shell.

```make
lint:
	go vet ./...
	cd contracts && go vet ./...
	@if [ -f buf.yaml ]; then go tool buf lint; \
	else echo "lint: skipping buf lint -- no buf.yaml yet (arrives in Phase 3a)"; fi

test:
	go test -race -short ./...
	cd contracts && go test -race ./...

test-integration:
	go test -race -timeout 15m ./...
	cd contracts && go test -race ./...
```

The contracts module has no container tests, so it needs no `-short` variant and no timeout bump.

- [ ] **Step 9: Verify both modules**

```bash
make lint
make test
make test-integration
```

Expected: `make lint` clean including `buf lint`; `make test` fast, starting no container, with `contracts` reporting a PASS for `TestFullNamesMatchTheSpec`; `make test-integration` green across every package in both modules.

- [ ] **Step 10: Prove the public module is genuinely light**

```bash
cd contracts && go list -deps ./... | grep -c 'jackc\|twmb\|testcontainers'; cd ..
```

Expected: `0`. This is the whole point of the separate module — a consumer of the contracts inherits none of the service's infrastructure dependencies.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -F - <<'EOF'
refactor(contracts): publish generated protobuf as its own module

The contracts were four levels below internal/, under a directory named for
platform mechanics, unimportable from outside the module -- for a system whose
subject is inter-service contracts.

A separate module is what makes "public" mean anything: consumers get the event
types without inheriting pgx, franz-go, and testcontainers. It also gives `buf
breaking` teeth, since it now guards an API someone can actually import.

The import path stutters at contracts/sagaflow/inventory/v1. Accepted: removing
it needs either a proto package rename, which would change ce_type and
events.type, or a PACKAGE_DIRECTORY_MATCH exception. Every site already aliases
the package as inventoryv1, so the path appears once per import block.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 6: Amend §7 of the design spec

The spec is this repo's authority — "where a plan and the spec disagree, the spec wins and the plan is wrong". Leaving §7 describing a tree that no longer exists would make the authority wrong.

**Files:**
- Modify: `docs/superpowers/specs/2026-08-17-sagaflow-design.md` §7 (the tree at :286 and the paragraph following it)
- Modify: `docs/superpowers/plans/README.md` (note the restructure between plan sets)

**Interfaces:**
- Consumes: the final tree produced by Tasks 1–5.
- Produces: nothing executable.

- [ ] **Step 1: Replace the tree in §7**

In `docs/superpowers/specs/2026-08-17-sagaflow-design.md`, replace the `internal/` portion of the §7 tree with the shape Tasks 1–5 produced. Keep the `proto/` and `cmd/` portions as they are, and add `contracts/` between them:

```
├── contracts/                      # generated protobuf — its own module, public
│   ├── go.mod                      # github.com/AymanKastali/sagaflow/contracts
│   └── sagaflow/<service>/v1/
├── cmd/
│   ├── booking/main.go             # ~20 lines: config, wire.New, Run
│   ├── inventory/main.go
│   ├── hotel/main.go
│   └── payment/main.go
└── internal/
    ├── platform/                   # shared mechanics — where the four topics live
    │   ├── eventstore/             # Append with expected version, Load, Replay
    │   ├── outbox/                 # tx-local Enqueue + claim-based poller
    │   ├── inbox/                  # consume-once deduplication
    │   ├── kafka/                  # franz-go producer/consumer, topic admin
    │   ├── schema/                 # registry framing, compatibility level
    │   ├── envelope/               # CloudEvents headers, the shared Message type
    │   ├── codec/                  # protojson for the event store
    │   ├── saga/                   # state-machine runtime, timer scheduler
    │   ├── pg/                     # pool, migrations, WithTx
    │   └── obs/                    # OTel setup, slog
    ├── testsupport/                # containers for tests — never reached from production
    │   ├── pgtest/  kafkatest/  srtest/
    ├── integration/                # cross-package deliverables
    ├── toolchain/                  # Go version floor guard
    ├── booking/
    ├── inventory/
    ├── hotel/
    └── payment/
```

- [ ] **Step 2: Record why contracts split three ways**

Immediately after the tree, before the existing "`internal/platform/` will feel like writing a framework" paragraph, insert:

```markdown
The single `contracts/` entry this section first described became four packages, because it
named four different boundaries: `contracts/` is the generated code and now a separate public
module; `codec/` is message ⇄ Postgres in protojson, deliberately registry-free so replay
survives a registry outage (§8.4); `envelope/` is identity ⇄ Kafka headers; `schema/` is
message ⇄ Kafka body in Confluent framing. Merging them would couple replay to the registry.

`testsupport/`, `integration/`, and `toolchain/` were not named here originally. They exist,
so the tree says so. See
[2026-08-18-platform-package-restructure-design.md](2026-08-18-platform-package-restructure-design.md).
```

- [ ] **Step 3: Note the restructure in the plans README**

In `docs/superpowers/plans/README.md`, insert a section between the "Plan set 1" table's trailing paragraph ("Plans 3a/3b and 4a/4b are independent…") and the `## Plan set 2 — the domain` heading:

```markdown
## Interlude — platform restructure

| # | Plan | Ends when | Depends on |
|---|---|---|---|
| 7 | [Platform package restructure](2026-08-18-platform-package-restructure.md) | `platform/kafka` imports only `envelope`, and the contracts are their own public module | all of plan set 1 |

Ran between the two plan sets deliberately: the boundaries it corrects gain four consumers each the
moment a service is written, and there are none yet.
```

Also update the README's `**Spec:**` line to name both specs, since the restructure amends §7 of the first:

```markdown
**Spec:** [2026-08-17-sagaflow-design.md](../specs/2026-08-17-sagaflow-design.md), as amended by
[2026-08-18-platform-package-restructure-design.md](../specs/2026-08-18-platform-package-restructure-design.md).
The spec is the authority; where a plan and the spec disagree, the spec wins and the plan is wrong.
```

Finally, correct the stale claim in the Conventions section. It names `pgtest`, `kafkatest` and `srtest` without paths, which is still true, but the bullet "**Task steps end in a commit.** Nothing in this repository has been committed yet; the first task's commit is the initial one." is no longer true — replace its second clause with "every task's last step is its commit."

- [ ] **Step 4: Verify the spec describes reality**

```bash
find internal -maxdepth 2 -type d | sort
```

Expected: every directory listed appears in the amended tree, and every non-service directory in the amended tree exists. `saga/` and `obs/` are the two exceptions — they are phases 5–8 and legitimately absent.

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -F - <<'EOF'
docs(spec): amend §7 to the restructured tree

The spec is the authority -- "where a plan and the spec disagree, the spec wins"
-- so a §7 describing a tree that no longer exists would make the authority
wrong.

Records why the one contracts entry became four, and adds testsupport,
integration, and toolchain, which existed without being named.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
```

---

## Done when

- `go list -f '{{join .Imports "\n"}}' ./internal/platform/kafka | grep sagaflow` prints only `.../envelope`.
- `go list ./...` contains no `internal/platform` package and no `internal/platform/version`.
- No package outside `internal/testsupport/` has a `testsupport` entry in its `.Deps`.
- `cd contracts && go list -deps ./... | grep -c 'jackc\|twmb\|testcontainers'` prints `0`.
- `make lint` clean, `make test` starts no container, `make test-integration` green in both modules.
- No test assertion was edited in any of the six commits.
