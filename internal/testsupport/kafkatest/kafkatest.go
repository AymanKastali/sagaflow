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

// Image is the broker every test in the module runs against.
//
// It is apache/kafka rather than testcontainers-go/modules/kafka. That module's
// startup script sources /etc/confluent/docker/bash-config and execs
// /etc/confluent/docker/launch, paths that exist only in
// confluentinc/confluent-local — whose default tag is a Kafka 3.5-era broker
// under the Confluent Community License. Testing against a different broker
// major than production runs is exactly the gap the offset and rebalance
// behaviour in this system cannot afford.
const Image = "apache/kafka:4.3.1"

// startGate is the file the container blocks on before booting Kafka. See
// runGatedBroker for why the boot has to be gated at all.
const startGate = "/tmp/sagaflow-advertised-listeners"

// gateOpened is the log line the container prints once it is waiting on the gate.
const gateOpened = "sagaflow: awaiting advertised listeners"

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

// Start brings up a single-node KRaft broker for the package and returns a stop
// function. Call it from TestMain, exactly as with pgtest.Start.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()

	ctr, err := runGatedBroker(ctx)
	if err != nil {
		return nil, err
	}
	abort := func(err error) (func(), error) {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, err
	}

	broker, err := hostAddress(ctx, ctr)
	if err != nil {
		return abort(err)
	}
	if err := openStartGate(ctx, ctr, broker); err != nil {
		return abort(err)
	}
	// Only now is Kafka actually starting, so this is where we wait for it.
	if err := waitForBroker(ctx, broker, 3*time.Minute); err != nil {
		return abort(err)
	}

	shared = &Kafka{brokers: []string{broker}}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "kafkatest: terminate: %v\n", err)
		}
	}, nil
}

// runGatedBroker starts the container and returns once it is blocked on startGate,
// before Kafka itself has booted.
//
// The gate exists because advertised.listeners has to name the *host-mapped* port,
// which does not exist until the container is running — and it cannot be changed
// afterwards. In Kafka 4.3.1,
// DynamicBrokerConfig.DynamicListenerConfig.RECONFIGURABLE_CONFIGS covers listeners
// and listener.security.protocol.map but not advertised.listeners, and
// ALL_DYNAMIC_CONFIGS is built from those same sets, so kafka-configs.sh rejects it
// as a non-dynamic config.
//
// So the boot waits instead: the command is wrapped in a shell that blocks until
// startGate exists, and the broker then boots with a correct value it never has to
// change. The image makes this cheap — it declares CMD ["/etc/kafka/docker/run"]
// with no ENTRYPOINT, so wrapping the command is enough, and its Alpine base
// provides /bin/sh.
func runGatedBroker(ctx context.Context) (*testcontainers.DockerContainer, error) {
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
			"echo '"+gateOpened+"'; "+
				"while [ ! -f "+startGate+" ]; do sleep 0.1; done; "+
				"export KAFKA_ADVERTISED_LISTENERS=\"$(cat "+startGate+")\"; "+
				"exec /etc/kafka/docker/run"),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute, wait.ForLog(gateOpened)),
	)
	if err != nil {
		return nil, fmt.Errorf("kafkatest: start %s: %w", Image, err)
	}
	return ctr, nil
}

// hostAddress is the host:port this test process can reach the broker on.
func hostAddress(ctx context.Context, ctr *testcontainers.DockerContainer) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("kafkatest: host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return "", fmt.Errorf("kafkatest: mapped port: %w", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port()), nil
}

// openStartGate writes the advertised listeners into startGate, releasing the boot.
//
// Written to a temporary path and renamed into place, because `> file` creates and
// truncates before printf writes anything. The waiting shell tests only for
// existence, so an in-place write leaves a window — narrow, but real — where it sees
// the file, cats it empty, and boots the broker with no advertised listener at all.
// rename(2) is atomic within a filesystem, so the gate either does not exist or
// holds the whole value.
func openStartGate(ctx context.Context, ctr *testcontainers.DockerContainer, broker string) error {
	code, out, err := ctr.Exec(ctx, []string{"/bin/sh", "-c", fmt.Sprintf(
		"printf '%%s' 'PLAINTEXT://%s,BROKER://localhost:19092' > %s.tmp && mv %s.tmp %s",
		broker, startGate, startGate, startGate)})
	if err != nil || code != 0 {
		buf := make([]byte, 4096)
		n, _ := out.Read(buf)
		return fmt.Errorf("kafkatest: open start gate (code %d): %v: %s", code, err, buf[:n])
	}
	return nil
}

// waitForBroker polls until the broker answers a metadata request.
//
// kgo.Ping "returns whether any broker is reachable and that the client can
// communicate with it" — which is the property tests actually need, and unlike a
// log-line match it cannot silently start failing because Kafka reworded a startup
// message.
func waitForBroker(ctx context.Context, broker string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// One client for the whole wait. franz-go retries metadata internally and a
	// failed Ping leaves it usable, so rebuilding it each attempt would spawn and
	// tear down goroutines hundreds of times to learn nothing extra.
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return fmt.Errorf("kafkatest: client for %s: %w", broker, err)
	}
	defer cl.Close()

	for {
		last := cl.Ping(ctx)
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
