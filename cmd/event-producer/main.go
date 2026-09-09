package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	client "github.com/ghaering/core-api-task/event_client"
	ing "github.com/ghaering/core-api-task/event_ingestion"
)

const (
	instanceStartEvent = "instance.start"
	instanceStopEvent  = "instance.stop"
)

var (
	projects = []string{"project-a", "project-b", "project-c"}
	flavours = []string{"s1.small", "s1.medium", "s1.large"}
)

type config struct {
	brokers          []string
	topic            string
	count            int
	instanceCount    int
	startProbability float64
	interval         time.Duration
	eventTimeStep    time.Duration
	maxDuration      time.Duration
	seed             int64
}

type instanceState struct {
	running   bool
	projectID string
	flavour   string
}

type eventGenerator struct {
	random    *rand.Rand
	instances []instanceState
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(os.Args[1:]); err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		slog.Error("produce events", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(args []string) error {
	conf, err := parseConfig(args)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if conf.maxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, conf.maxDuration)
		defer cancel()
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(conf.brokers...))
	if err != nil {
		return fmt.Errorf("create Kafka producer: %w", err)
	}
	defer producer.Close()

	random := rand.New(rand.NewSource(conf.seed))
	generator := newEventGenerator(random, conf.instanceCount)
	encoder := &client.VmEventJsonEncoder{}
	firstEventTime := time.Now().UTC()

	slog.Info(
		"producing events",
		slog.Int("count", conf.count),
		slog.String("topic", conf.topic),
		slog.Duration("max_duration", conf.maxDuration),
		slog.Int64("seed", conf.seed),
	)

	for i := 0; conf.count == 0 || i < conf.count; i++ {
		event := generator.nextEvent(
			firstEventTime.Add(time.Duration(i)*conf.eventTimeStep),
			conf.startProbability,
		)
		value, err := encoder.Encode(event)
		if err != nil {
			return fmt.Errorf("encode event %q: %w", event.EventId, err)
		}

		record := &kgo.Record{Topic: conf.topic, Value: value}
		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			return fmt.Errorf("produce event %q: %w", event.EventId, err)
		}

		slog.Info(
			"produced event",
			slog.String("event_id", event.EventId),
			slog.String("instance_id", event.InstanceId),
			slog.String("event_type", event.EventType),
			slog.Time("occurred_at", event.OccurredAt),
		)

		if (conf.count > 0 && i == conf.count-1) || conf.interval == 0 {
			continue
		}
		timer := time.NewTimer(conf.interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	return nil
}

func parseConfig(args []string) (config, error) {
	var brokers string
	conf := config{}
	flags := flag.NewFlagSet("event-producer", flag.ContinueOnError)

	flags.StringVar(&brokers, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flags.StringVar(&conf.topic, "topic", "billing.vm.events", "Kafka topic")
	flags.IntVar(&conf.count, "count", 0, "number of events to produce; zero runs continuously")
	flags.IntVar(&conf.instanceCount, "instances", 5, "number of instance IDs to choose from")
	flags.Float64Var(&conf.startProbability, "start-probability", 0.5, "probability that an event is a start, from 0 to 1")
	flags.DurationVar(&conf.interval, "interval", 100*time.Millisecond, "delay between produced records")
	flags.DurationVar(&conf.eventTimeStep, "event-time-step", time.Minute, "event-time increase between records")
	flags.DurationVar(&conf.maxDuration, "max-duration", 30*time.Minute, "maximum runtime; zero disables the limit")
	flags.Int64Var(&conf.seed, "seed", time.Now().UnixNano(), "random seed for reproducible events")

	if err := flags.Parse(args); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}

	for _, broker := range strings.Split(brokers, ",") {
		conf.brokers = append(conf.brokers, strings.TrimSpace(broker))
	}
	conf.topic = strings.TrimSpace(conf.topic)

	if err := validateConfig(conf); err != nil {
		return config{}, err
	}
	return conf, nil
}

func validateConfig(conf config) error {
	var validationErrors []error
	for i, broker := range conf.brokers {
		if broker == "" {
			validationErrors = append(validationErrors, fmt.Errorf("kafka broker at position %d must not be empty", i+1))
		}
	}
	if conf.topic == "" {
		validationErrors = append(validationErrors, errors.New("kafka topic must not be empty"))
	}
	if conf.count < 0 {
		validationErrors = append(validationErrors, errors.New("event count must be non-negative"))
	}
	if conf.instanceCount < 1 {
		validationErrors = append(validationErrors, errors.New("instance count must be greater than zero"))
	}
	if conf.startProbability < 0 || conf.startProbability > 1 {
		validationErrors = append(validationErrors, errors.New("start probability must be between 0 and 1"))
	}
	if conf.interval < 0 {
		validationErrors = append(validationErrors, errors.New("interval must be non-negative"))
	}
	if conf.count == 0 && conf.interval == 0 {
		validationErrors = append(validationErrors, errors.New("interval must be greater than zero when producing continuously"))
	}
	if conf.eventTimeStep < 0 {
		validationErrors = append(validationErrors, errors.New("event time step must be non-negative"))
	}
	if conf.maxDuration < 0 {
		validationErrors = append(validationErrors, errors.New("max duration must be non-negative"))
	}
	return errors.Join(validationErrors...)
}

func newEventGenerator(random *rand.Rand, instanceCount int) *eventGenerator {
	return &eventGenerator{
		random:    random,
		instances: make([]instanceState, instanceCount),
	}
}

func (generator *eventGenerator) nextEvent(
	occurredAt time.Time,
	startProbability float64,
) ing.VmEvent {
	running := make([]int, 0, len(generator.instances))
	stopped := make([]int, 0, len(generator.instances))
	for i, instance := range generator.instances {
		if instance.running {
			running = append(running, i)
		} else {
			stopped = append(stopped, i)
		}
	}

	produceStart := len(running) == 0 ||
		(len(stopped) > 0 && generator.random.Float64() < startProbability)

	var instanceIndex int
	var eventType string
	if produceStart {
		instanceIndex = stopped[generator.random.Intn(len(stopped))]
		eventType = instanceStartEvent
		generator.instances[instanceIndex] = instanceState{
			running:   true,
			projectID: projects[generator.random.Intn(len(projects))],
			flavour:   flavours[generator.random.Intn(len(flavours))],
		}
	} else {
		instanceIndex = running[generator.random.Intn(len(running))]
		eventType = instanceStopEvent
	}

	instance := generator.instances[instanceIndex]
	event := ing.VmEvent{
		EventId:    uuid.NewString(),
		Flavour:    instance.flavour,
		InstanceId: fmt.Sprintf("instance-%03d", instanceIndex+1),
		OccurredAt: occurredAt,
		ProjectId:  instance.projectID,
		EventType:  eventType,
	}
	if !produceStart {
		generator.instances[instanceIndex].running = false
	}
	return event
}
