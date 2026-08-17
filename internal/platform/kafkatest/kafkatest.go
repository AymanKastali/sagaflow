// Package kafkatest starts a real apache/kafka broker for integration tests.
//
// It does not use testcontainers-go/modules/kafka: that module's startup script
// sources /etc/confluent/docker/bash-config and execs /etc/confluent/docker/launch,
// paths that exist only in confluentinc/confluent-local — whose default tag is a
// Kafka 3.5-era broker under the Confluent Community License. Testing against a
// different broker major than production runs is exactly the gap the offset and
// rebalance behaviour in this system cannot afford.
package kafkatest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Image = "apache/kafka:4.3.1"

type Kafka struct {
	brokers []string
}

func (k *Kafka) Brokers() []string { return k.brokers }

var shared *Kafka

// Shared returns the broker Start brought up, skipping the test in -short mode.
func Shared(t *testing.T) *Kafka {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	if shared == nil {
		t.Fatal("kafkatest: no broker running — this package needs a TestMain that calls kafkatest.Start")
	}
	return shared
}

// startGate is the file the container waits for before booting Kafka.
const startGate = "/tmp/sagaflow-advertised-listeners"

// Start brings up a single-node KRaft broker for the package and returns a stop
// function. Call it from TestMain, exactly as with pgtest.Start.
//
// The advertised listener has to name the *host-mapped* port, and that port does
// not exist until the container is running. advertised.listeners also cannot be
// changed after the broker boots: in Kafka 4.3.1,
// DynamicBrokerConfig.DynamicListenerConfig.RECONFIGURABLE_CONFIGS covers
// listeners and listener.security.protocol.map but not advertised.listeners, and
// ALL_DYNAMIC_CONFIGS is built from those same sets — so kafka-configs.sh rejects
// it as a non-dynamic config.
//
// So the boot is gated instead: the container starts with its command wrapped in
// a shell that blocks on startGate, we read the mapped port, write the real
// advertised listener into that file, and the broker then boots with a correct
// value it never has to change. The image makes this cheap — it declares
// CMD ["/etc/kafka/docker/run"] and no ENTRYPOINT, so wrapping the command is
// enough, and its Alpine base provides /bin/sh.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_NUM_PARTITIONS":                           "6",
			// Off deliberately: every topic in this system is created explicitly
			// with a known partition count, and an auto-created topic would let a
			// typo'd name silently succeed.
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
			// KAFKA_ADVERTISED_LISTENERS is deliberately absent: the wrapper
			// command below exports it from startGate once the port is known.
		}),
		testcontainers.WithCmd("/bin/sh", "-c",
			"echo 'sagaflow: awaiting advertised listeners'; "+
				"while [ ! -f "+startGate+" ]; do sleep 0.1; done; "+
				"export KAFKA_ADVERTISED_LISTENERS=\"$(cat "+startGate+")\"; "+
				"exec /etc/kafka/docker/run"),
		// Wait only for the gate message here — Kafka has not booted yet.
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForLog("sagaflow: awaiting advertised listeners")),
	)
	if err != nil {
		return nil, fmt.Errorf("kafkatest: start %s: %w", Image, err)
	}

	fail := func(format string, args ...any) (func(), error) {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("kafkatest: "+format, args...)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return fail("host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return fail("mapped port: %w", err)
	}
	broker := fmt.Sprintf("%s:%s", host, port.Port())

	// Open the gate. The broker now boots advertising the host-mapped address —
	// a value it never has to change afterwards.
	code, out, err := ctr.Exec(ctx, []string{"/bin/sh", "-c", fmt.Sprintf(
		"printf '%%s' 'PLAINTEXT://%s,BROKER://localhost:19092' > %s", broker, startGate)})
	if err != nil || code != 0 {
		buf := make([]byte, 4096)
		n, _ := out.Read(buf)
		return fail("open start gate (code %d): %v: %s", code, err, buf[:n])
	}

	// Only now is Kafka actually starting, so this is where we wait for it —
	// behaviourally, by asking it for metadata, rather than by matching a log
	// line whose wording is not part of Kafka's public contract.
	if err := waitForBroker(ctx, broker, 3*time.Minute); err != nil {
		return fail("%w", err)
	}

	shared = &Kafka{brokers: []string{broker}}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "kafkatest: terminate: %v\n", err)
		}
	}, nil
}

// waitForBroker polls until the broker answers a metadata request.
//
// kgo.Ping "returns whether any broker is reachable and that the client can
// communicate with it" — which is the property tests actually need, and unlike a
// log-line match it cannot silently start failing because Kafka reworded a
// startup message.
func waitForBroker(ctx context.Context, broker string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var last error
	for {
		cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
		if err != nil {
			return fmt.Errorf("kafkatest: client for %s: %w", broker, err)
		}
		last = cl.Ping(ctx)
		cl.Close()
		if last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafkatest: broker %s never became ready: %w", broker, last)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
