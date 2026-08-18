// Package pgtest starts one Postgres container per test package and hands out
// independent databases inside it for every test that asks.
//
// # The problem
//
// Code that writes SQL needs to be tested against a real Postgres. Unique
// constraints, the locking behaviour of SELECT ... FOR UPDATE, and the exact
// visibility ordering a transaction commit gives a concurrent reader are
// properties of the database engine itself; nothing else has them by
// construction, only by however faithfully someone else reimplemented them. A
// test that passes against a stand-in and would fail against real Postgres has
// told the suite nothing about the code it exists to check.
//
// So integration tests need a live Postgres to run against. The obstacle is
// cost: bringing one up takes real wall-clock time, and the naive way of
// paying that cost, repeated once per test, makes the suite take longer than
// anyone will wait for.
//
// # Why the obvious fixes do not work
//
// A shared, long-lived container started outside the test run — by a
// developer's own docker run, or a CI step that lives apart from go test —
// makes every test depend on whatever state the previous test left behind, in
// whatever order the runner happened to pick. That coupling stays invisible
// until the suite runs in a different order, or runs on a machine where the
// container was never started, and fails for reasons that have nothing to do
// with the change under test.
//
// A container per test is correct: each test gets a Postgres with nothing in
// it, so ordering and machine state stop mattering. It is also unusably slow —
// a container takes on the order of a second to become reachable, and a suite
// with hundreds of tests would spend most of its time waiting for Postgres to
// start rather than running assertions.
//
// Mocking the client sidesteps the wait but tests the mock instead of the
// database: the failures that matter here — a UNIQUE constraint rejecting a
// racing writer, a row becoming visible out of insertion order, a lock a
// concurrent UPDATE has to wait behind — are precisely the ones a mock does
// not have unless someone reimplements Postgres a second time to give it them.
//
// # What this package does
//
// Start brings up one Postgres container for the whole test package, called
// once from TestMain before any test runs. Shared returns that container's
// handle to a test, skipping the test whenever -short is set, and failing loudly if no
// TestMain brought a container up.
//
// The isolation key is the database name. DSN creates dbName inside that one
// running container — reusing it if a prior call in the same test already
// created it — and returns a DSN pointing at it. Two tests in the same package
// pass different dbName arguments, conventionally named after the test itself,
// and get separate databases inside the one container, so neither can see the
// other's rows without either paying for a second container. Migrated does
// DSN's work plus the two steps every integration test takes next: apply a
// caller-supplied fs.FS of migrations and open a pool, both cleaned up when
// the test ends. recreate drops a database before creating it, which is what
// makes go test -count=2 start from empty rather than inheriting the first
// run's rows.
//
// # What it deliberately does not do
//
// No exported function starts a container for a single test. That is the
// specific mistake this API exists to close off, not an oversight: Start
// belongs to TestMain and Shared belongs to a Test function, and there is no
// third way to get a *PG that skips TestMain.
//
// It does not own schema content. Migrated applies whatever fs.FS the caller
// hands it; deciding what that filesystem contains, and running it against a
// live service, is internal/platform/pg's job (pg.Migrate, pg.Open), not
// this package's.
//
// # Reading order
//
//	pgtest.go   The whole package: Start and Shared bracket the container's
//	            life; DSN and Migrated bracket one test's database; recreate
//	            and replaceDBName are their helpers. Start here.
//
// # Where this comes from
//
// Design spec §12.4 (one container per package, started in TestMain;
// isolation from database names derived from the test rather than from
// fresh containers, which also preserves the property that no transaction
// spans two services).
package pgtest
