// Package srtest starts a real Apicurio Registry, once per test package, for
// tests that need one.
//
// # The problem
//
// Code that registers or looks up a schema needs to be tested against a real
// schema registry. Whether a proposed schema is accepted as compatible with
// what is already registered under a subject, and what a lookup for an
// unregistered subject actually returns, are properties of the registry
// itself; nothing else has them by construction. A test that passes against
// a stand-in and would fail against a real registry has told the suite
// nothing about the code it exists to check.
//
// So integration tests need a live registry to run against. The obstacle is
// cost: bringing one up takes real wall-clock time, and the naive way of
// paying that cost, repeated once per test, makes the suite take longer than
// anyone will wait for.
//
// # Why the obvious fixes do not work
//
// A shared, long-lived registry started outside the test run — by a
// developer's own docker run, or a CI step that lives apart from go test —
// makes every test depend on whatever subjects and schema versions the
// previous test left registered, in whatever order the runner happened to
// pick. That coupling stays invisible until the suite runs in a different
// order, or runs on a machine where the registry was never started, and
// fails for reasons that have nothing to do with the change under test.
//
// A registry per test is correct: each test gets a registry with nothing
// registered in it, so ordering and machine state stop mattering. It is also
// unusably slow — standing one up takes seconds, not milliseconds, and a
// suite with hundreds of tests would spend most of its time waiting for a
// registry to start rather than running assertions.
//
// Mocking the client sidesteps the wait but tests the mock instead of the
// registry: the failures that matter here — a compatibility check rejecting
// a field-type change on an existing subject, an unregistered subject
// returning the specific not-found shape a client has to detect and fail
// closed on — are precisely the ones a mock does not have unless someone
// reimplements the registry's compatibility engine a second time to give it
// them.
//
// # What this package does
//
// Start brings up one Apicurio Registry container for the whole test package
// in its ephemeral storage mode — sql over in-memory H2, the only kind
// Apicurio 3.x offers with nothing left over between runs — called once from
// TestMain before any test runs. Shared returns that registry's URL to a
// test, skipping the test whenever -short is set, and failing loudly if no
// TestMain brought a registry up.
//
// URL already carries the path prefix Apicurio serves its Confluent-compatible
// API under: a client pointed at the bare host gets 404 on every call, so
// returning the full, correct base URL is what keeps that mistake from being
// available to a caller at all.
//
// The isolation key is the subject name. Under the topic–record-name
// strategy this project's clients use — a choice made in platform/schema,
// not a registry setting — a subject is derived from a
// topic name plus a fully qualified message type name, so a test that must
// register or query a schema state no other test has touched picks a topic
// name whose derived subject is exclusive to it, rather than sharing one
// subject's version history with a test that registers something
// incompatible.
//
// # What it deliberately does not do
//
// No exported function starts a registry for a single test. That is the
// specific mistake this API exists to close off, not an oversight: Start
// belongs to TestMain and Shared belongs to a Test function, and there is no
// third way to get a *Registry that skips TestMain.
//
// It does not register any schema itself, and it does not reset the registry
// between tests. Registering, and choosing compatibility rules, is the
// caller's job through the registry client; a test that needs an
// unregistered subject picks a topic name no other test has touched rather
// than asking srtest to clear state that other tests in the package may
// still depend on.
//
// # Reading order
//
//	srtest.go   The whole package: Start and Shared bracket the container's
//	            life exactly as pgtest's do; runRegistry and hostAddress are
//	            Start's two helpers. Start here.
//
// # Where this comes from
//
// Design spec §12.4 (one container per package, started in TestMain;
// isolation from subject names derived from the test rather than from
// fresh containers).
package srtest
