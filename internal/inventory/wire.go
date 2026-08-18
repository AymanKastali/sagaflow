package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	inventoryv1 "github.com/AymanKastali/sagaflow/contracts/sagaflow/inventory/v1"
	"github.com/AymanKastali/sagaflow/internal/inventory/migrations"
	"github.com/AymanKastali/sagaflow/internal/platform/kafka"
	"github.com/AymanKastali/sagaflow/internal/platform/outbox"
	"github.com/AymanKastali/sagaflow/internal/platform/pg"
	"github.com/AymanKastali/sagaflow/internal/platform/schema"
	"github.com/AymanKastali/sagaflow/internal/platform/timers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/sr"
)

// Config is everything inventory needs from outside itself: three addresses.
//
// There is no behaviour switch here on purpose. Every failure this system
// demonstrates is triggered by what a message contains — an unavailable seat, a
// declining card — so a service has no flag that makes it act differently, and no
// test needs one.
type Config struct {
	DSN      string
	Brokers  []string
	Registry string
}

// replication is the topic replication factor. One, because this repository runs
// a single broker; a real cluster would use three and would not take the number
// from a service's source file.
const replication int16 = 1

// Service is inventory assembled: a database, a broker connection, and the four
// loops that keep the seats moving.
type Service struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
	commands *kafka.Consumer
	events   *kafka.Consumer
	outbox   *outbox.Poller
	timers   *timers.Scheduler
}

// New builds the service and returns it ready to Run. Everything it can fail at —
// a database it cannot reach, a schema nobody registered, a broker that is not
// there — fails here, before a single message is consumed.
//
// It applies its own migrations, which schemactl deliberately does not do for
// schemas, and the difference is worth stating. A migration touches one database
// belonging to this service alone, so getting it wrong breaks only the thing that
// ran it. A schema is a contract other services decode against, so registering
// one is somebody's reviewed decision rather than a side effect of a process
// starting.
func New(ctx context.Context, cfg Config) (*Service, error) {
	if err := pg.Migrate(ctx, cfg.DSN, migrations.FS); err != nil {
		return nil, err
	}
	pool, err := pg.Open(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	service := &Service{pool: pool}
	if err := service.connect(ctx, cfg); err != nil {
		service.Close()
		return nil, err
	}
	return service, nil
}

// connect builds everything that speaks to Kafka: the two serdes, the producer,
// and the two consumer groups.
func (s *Service) connect(ctx context.Context, cfg Config) error {
	commandSerde, eventSerde, err := serdes(ctx, cfg.Registry)
	if err != nil {
		return err
	}
	// The dead-letter topics are created alongside the pair: a consumer with
	// nowhere to put a settled failure stops advancing its partition instead.
	if err := kafka.EnsureTopics(ctx, cfg.Brokers, kafka.Partitions, replication,
		CommandsTopic, CommandsTopic+".dlq", EventsTopic, EventsTopic+".dlq"); err != nil {
		return err
	}
	if s.producer, err = kafka.NewProducer(cfg.Brokers); err != nil {
		return err
	}
	if s.commands, err = kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.Brokers, Group: Consumer, Topics: []string{CommandsTopic},
		Handler: Commands(NewHandler(s.pool, eventSerde), commandSerde), DLQ: s.producer,
	}); err != nil {
		return err
	}
	if s.events, err = kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.Brokers, Group: ProjectionConsumer, Topics: []string{EventsTopic},
		Handler: Projections(NewProjector(s.pool)), DLQ: s.producer,
	}); err != nil {
		return err
	}
	s.outbox = outbox.NewPoller(s.pool, s.producer)
	s.timers = timers.NewScheduler(s.pool, NewExpirer(eventSerde))
	return nil
}

// serdes resolves this service's schema ids: commands to decode, events to encode.
//
// Both resolve now, at startup, so a subject nobody registered stops the service
// here — visibly, at rollout, pointing at whoever skipped the registration step —
// rather than surfacing as a decode failure against live traffic later.
func serdes(ctx context.Context, registry string) (commands, events *schema.Serde, err error) {
	client, err := sr.NewClient(sr.URLs(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("inventory: schema registry client: %w", err)
	}
	commands, err = schema.NewTopicSerde(ctx, client, CommandsTopic,
		&inventoryv1.HoldSeat{}, &inventoryv1.ReleaseSeatHold{})
	if err != nil {
		return nil, nil, err
	}
	events, err = schema.NewTopicSerde(ctx, client, EventsTopic,
		&inventoryv1.SeatHeld{}, &inventoryv1.SeatHoldReleased{},
		&inventoryv1.SeatHoldExpired{}, &inventoryv1.SeatUnavailable{})
	if err != nil {
		return nil, nil, err
	}
	return commands, events, nil
}

// loop is one of the service's four long-running jobs, named for the error it
// might produce.
type loop struct {
	name string
	run  func(context.Context) error
}

// Run starts the four loops and returns when the context is cancelled or one of
// them fails.
//
// Cancelling is the entire shutdown story, and that is the point: "the service
// died in the middle of a saga" becomes a cancel() call in a test rather than a
// container restart, and what the next process finds is exactly what a crash
// would have left behind — whatever committed, and nothing else.
//
// The first failure cancels the other three. A service running three of its four
// loops is worse than one that stopped: holds would still be taken while nothing
// expired them, and the symptom would be seats that never come back rather than a
// process that exited.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	loops := []loop{
		{"commands consumer", s.commands.Run},
		{"events consumer", s.events.Run},
		{"outbox poller", s.outbox.Run},
		{"timer scheduler", s.timers.Run},
	}
	failures := make(chan error, len(loops))
	var running sync.WaitGroup
	for _, l := range loops {
		running.Add(1)
		go func() {
			defer running.Done()
			if err := l.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				failures <- fmt.Errorf("inventory: %s: %w", l.name, err)
				cancel()
			}
		}()
	}
	running.Wait()

	close(failures)
	return <-failures // nil when nothing failed: a closed, empty channel reads zero
}

// Close releases what New acquired.
//
// Consumers first: closing one leaves its group and commits the offsets it
// finished, and the producer has to still be open for a dead letter in flight to
// land. The pool goes last, because both of those may still be writing.
func (s *Service) Close() {
	for _, consumer := range []*kafka.Consumer{s.commands, s.events} {
		if consumer != nil {
			consumer.Close()
		}
	}
	if s.producer != nil {
		s.producer.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
}
