// Package kafkatest starts a real apache/kafka broker, once per test package,
// for tests that need one.
//
// # The problem
//
// Code that produces or consumes Kafka records needs to be tested against a
// real broker. Partition assignment during a rebalance, the ordering a
// consumer actually sees across partitions, and what a commit does and does
// not make visible to another consumer in the same group are properties of
// the broker itself; nothing else has them by construction. A test that
// passes against a stand-in and would fail against a real broker has told
// the suite nothing about the code it exists to check.
//
// So integration tests need a live broker to run against. The obstacle is
// cost: bringing one up takes real wall-clock time, and the naive way of
// paying that cost, repeated once per test, makes the suite take longer than
// anyone will wait for.
//
// # Why the obvious fixes do not work
//
// A shared, long-lived broker started outside the test run — by a
// developer's own docker run, or a CI step that lives apart from go test —
// makes every test depend on whatever topics and consumer-group offsets the
// previous test left behind, in whatever order the runner happened to pick.
// That coupling stays invisible until the suite runs in a different order,
// or runs on a machine where the broker was never started, and fails for
// reasons that have nothing to do with the change under test.
//
// A broker per test is correct: each test gets a Kafka with nothing on it, so
// ordering and machine state stop mattering. It is also unusably slow —
// standing up a broker takes seconds, not milliseconds, and a suite with
// hundreds of tests would spend most of its time waiting for Kafka to start
// rather than running assertions.
//
// Mocking the client sidesteps the wait but tests the mock instead of the
// broker: the failures that matter here — a rebalance reassigning partitions
// mid-batch, two consumers in one group colliding over the same partition, a
// commit's visibility ordering across concurrent readers — are precisely the
// ones a mock does not have unless someone reimplements a Kafka broker a
// second time to give it them.
//
// # What this package does
//
// Start brings up one single-node KRaft broker for the whole test package
// (apache/kafka:4.3.1, the same major version docker-compose.yml runs),
// called once from TestMain before any test runs. Shared returns that
// broker's address list to a test, skipping the test whenever -short is set,
// and failing loudly if no TestMain brought a broker up.
//
// Bringing the broker up has one wrinkle Start absorbs so no test has to
// think about it: advertised.listeners must name the host-mapped port, which
// does not exist until the container is already running, and Kafka cannot be
// told to change it afterward. Start therefore boots the container into a
// gate that blocks before Kafka starts, learns the real port, writes the
// correct listener address in, and only then releases the gate.
//
// The isolation key is the topic name and the consumer-group name. Every test
// that needs a topic names its own, distinct from every other test's, and the
// container disables auto-creation so a name has to be created on purpose
// before anything can be produced to it — a typo cannot silently land on the
// wrong topic. A test that consumes also names its own consumer group, so two
// tests reading through the one shared broker never fight over the same
// group's partition assignment or see a rebalance neither of them caused.
//
// # What it deliberately does not do
//
// No exported function starts a broker for a single test. That is the
// specific mistake this API exists to close off, not an oversight: Start
// belongs to TestMain and Shared belongs to a Test function, and there is no
// third way to get a *Kafka that skips TestMain.
//
// It does not create topics, does not manage consumer groups, and does not
// verify the broker's version at runtime — the version is guaranteed by the
// pinned Image constant at container creation, not by a check that would
// either be tautological or rest on inferring a release from the API
// versions it happens to support. Topic and consumer-group lifecycle belong
// to internal/platform/kafka (EnsureTopics, Consumer), not to this package.
//
// # Reading order
//
//	kafkatest.go   The whole package. Start, Shared and Kafka.Brokers first;
//	               then runGatedBroker, hostAddress, openStartGate and
//	               waitForBroker in the order Start calls them, since each
//	               only makes sense against the one before it.
//
// # Where this comes from
//
// Design spec §12.4 (one container per package, started in TestMain;
// isolation from topic and consumer-group names derived from the test
// rather than from fresh containers).
package kafkatest
