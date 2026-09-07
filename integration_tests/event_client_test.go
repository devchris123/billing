//go:build integration

package integration_tests

import (
	"context"
	"errors"
	"testing"
	"time"

	client "github.com/ghaering/core-api-task/event_client"
	event "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaFixture struct {
	brokers []string
	admin   *kadm.Client
}

func TestKafkaClient(t *testing.T) {
	fixture := newKafkaFixture(t)

	t.Run("sends and receives events", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fixture.createTopic(t, ctx, "events-topic")
		kafkaClient := fixture.newClient(t, "events-group", "events-topic", noRetry)
		expected := []event.VmEvent{{EventId: "event-1"}, {EventId: "event-2"}}
		for _, vmEvent := range expected {
			require.NoError(t, <-kafkaClient.Send(ctx, vmEvent))
		}

		received := make(chan event.Result[event.VmEvent], len(expected))
		listenCtx, stopListening := context.WithCancel(ctx)
		listenErr := make(chan error, 1)
		go func() {
			listenErr <- kafkaClient.Listen(listenCtx, func(result event.Result[event.VmEvent]) error {
				received <- result
				return nil
			})
		}()

		// Execute and assert
		for _, expectedEvent := range expected {
			select {
			case result := <-received:
				require.NoError(t, result.Err)
				require.Equal(t, expectedEvent, result.Value)
			case <-ctx.Done():
				t.Fatalf("event %s was not received", expectedEvent.EventId)
			}
		}
		stopListening()
		require.ErrorIs(t, <-listenErr, context.Canceled)
	})

	t.Run("retries failed event before processing next event", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fixture.createTopic(t, ctx, "retry-topic")
		kafkaClient := fixture.newClient(t, "retry-group", "retry-topic", immediateRetry)
		expected := []event.VmEvent{{EventId: "event-1"}, {EventId: "event-2"}, {EventId: "event-3"}}
		for _, vmEvent := range expected {
			require.NoError(t, <-kafkaClient.Send(ctx, vmEvent))
		}

		processedEventIDs := make(chan string, 5)
		listenCtx, stopListening := context.WithCancel(ctx)
		listenErr := make(chan error, 1)
		go func() {
			attempts := make(map[string]int)
			listenErr <- kafkaClient.Listen(listenCtx, func(result event.Result[event.VmEvent]) error {
				if result.Err != nil {
					return nil
				}
				processedEventIDs <- result.Value.EventId
				attempts[result.Value.EventId]++
				if result.Value.EventId == "event-1" && attempts[result.Value.EventId] < 3 {
					return errors.New("processing failed")
				}
				return nil
			})
		}()

		// Execute and assert
		for _, expectedEventID := range []string{"event-1", "event-1", "event-1", "event-2", "event-3"} {
			select {
			case actualEventID := <-processedEventIDs:
				require.Equal(t, expectedEventID, actualEventID)
			case <-ctx.Done():
				t.Fatalf("event %s was not processed", expectedEventID)
			}
		}
		stopListening()
		require.ErrorIs(t, <-listenErr, context.Canceled)
	})

	t.Run("does not replay committed event", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fixture.createTopic(t, ctx, "commit-topic")
		firstConsumer := fixture.newClient(t, "commit-group", "commit-topic", noRetry)
		first := event.VmEvent{EventId: "committed-event"}
		second := event.VmEvent{EventId: "uncommitted-event"}
		require.NoError(t, <-firstConsumer.Send(ctx, first))
		require.NoError(t, <-firstConsumer.Send(ctx, second))

		stopErr := errors.New("stop before second commit")
		err := firstConsumer.Listen(ctx, func(result event.Result[event.VmEvent]) error {
			if result.Err != nil {
				return nil
			}
			if result.Value.EventId == second.EventId {
				return stopErr
			}
			return nil
		})
		require.ErrorIs(t, err, stopErr)
		firstConsumer.Close()

		// Execute
		secondConsumer := fixture.newClient(t, "commit-group", "commit-topic", noRetry)
		replayed, listenErr, stopListening := listenForOne(ctx, secondConsumer)
		defer stopListening()

		// Assert
		select {
		case result := <-replayed:
			require.NoError(t, result.Err)
			require.Equal(t, second, result.Value)
		case <-ctx.Done():
			t.Fatal("uncommitted event was not replayed")
		}
		stopListening()
		require.ErrorIs(t, <-listenErr, context.Canceled)
	})

	t.Run("commits malformed event before processing next event", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fixture.createTopic(t, ctx, "malformed-topic")
		produceRawRecords(t, ctx, fixture.brokers, "malformed-topic", []byte("malformed"))
		encoder := &client.VmEventJsonEncoder{}
		expected := event.VmEvent{EventId: "valid-event"}
		encoded, err := encoder.Encode(expected)
		require.NoError(t, err)
		produceRawRecords(t, ctx, fixture.brokers, "malformed-topic", encoded)

		firstConsumer := fixture.newClient(t, "malformed-group", "malformed-topic", noRetry)
		decodeErrors := 0
		stopErr := errors.New("stop before valid event commit")
		err = firstConsumer.Listen(ctx, func(result event.Result[event.VmEvent]) error {
			if result.Err != nil {
				decodeErrors++
				return nil
			}
			require.Equal(t, expected, result.Value)
			return stopErr
		})
		require.ErrorIs(t, err, stopErr)
		require.Equal(t, 1, decodeErrors)
		firstConsumer.Close()

		// Execute
		secondConsumer := fixture.newClient(t, "malformed-group", "malformed-topic", noRetry)
		replayed, listenErr, stopListening := listenForOne(ctx, secondConsumer)
		defer stopListening()

		// Assert
		select {
		case result := <-replayed:
			require.NoError(t, result.Err)
			require.Equal(t, expected, result.Value)
		case <-ctx.Done():
			t.Fatal("valid event was not replayed")
		}
		stopListening()
		require.ErrorIs(t, <-listenErr, context.Canceled)
	})

	t.Run("cancellation interrupts handler retry", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fixture.createTopic(t, ctx, "cancel-retry-topic")
		kafkaClient := fixture.newClient(t, "cancel-retry-group", "cancel-retry-topic", client.RetryWithExponentialBackoff)
		require.NoError(t, <-kafkaClient.Send(ctx, event.VmEvent{EventId: "event-1"}))

		attempted := make(chan struct{}, 1)
		listenCtx, stopListening := context.WithCancel(ctx)
		listenErr := make(chan error, 1)
		go func() {
			listenErr <- kafkaClient.Listen(listenCtx, func(result event.Result[event.VmEvent]) error {
				if result.Err == nil {
					select {
					case attempted <- struct{}{}:
					default:
					}
				}
				return errors.New("database unavailable")
			})
		}()
		<-attempted

		// Execute
		stopListening()

		// Assert
		select {
		case err := <-listenErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("listener did not stop during retry backoff")
		}
		kafkaClient.Close()

		// The interrupted event was never acknowledged and must be replayed.
		replayConsumer := fixture.newClient(
			t,
			"cancel-retry-group",
			"cancel-retry-topic",
			noRetry,
		)
		replayed, replayErr, stopReplay := listenForOne(ctx, replayConsumer)
		select {
		case result := <-replayed:
			require.NoError(t, result.Err)
			require.Equal(t, "event-1", result.Value.EventId)
		case <-ctx.Done():
			t.Fatal("interrupted event was not replayed")
		}
		stopReplay()
		require.ErrorIs(t, <-replayErr, context.Canceled)
	})
}

func newKafkaFixture(t *testing.T) kafkaFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kafkaC, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("kafka-client-tests"))
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, kafkaC)
	brokers, err := kafkaC.Brokers(ctx)
	require.NoError(t, err)
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)
	return kafkaFixture{brokers: brokers, admin: kadm.NewClient(adminClient)}
}

func (fixture kafkaFixture) createTopic(t *testing.T, ctx context.Context, topic string) {
	t.Helper()
	_, err := fixture.admin.CreateTopic(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
}

func (fixture kafkaFixture) newClient(t *testing.T, group string, topic string, retry client.RetryFunc) *client.KafkaClient[event.VmEvent] {
	t.Helper()
	kafkaClient, err := client.NewKafkaClient(fixture.brokers, group, topic, &client.VmEventJsonEncoder{}, retry)
	require.NoError(t, err)
	t.Cleanup(kafkaClient.Close)
	return kafkaClient
}

func listenForOne(ctx context.Context, kafkaClient *client.KafkaClient[event.VmEvent]) (<-chan event.Result[event.VmEvent], <-chan error, context.CancelFunc) {
	resultChan := make(chan event.Result[event.VmEvent], 1)
	listenCtx, cancel := context.WithCancel(ctx)
	errChan := make(chan error, 1)
	go func() {
		errChan <- kafkaClient.Listen(listenCtx, func(result event.Result[event.VmEvent]) error {
			resultChan <- result
			return nil
		})
	}()
	return resultChan, errChan, cancel
}

func noRetry(_ context.Context, operation func() error) error { return operation() }

func immediateRetry(ctx context.Context, operation func() error) error {
	for {
		if err := operation(); err == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func produceRawRecords(t *testing.T, ctx context.Context, brokers []string, topic string, values ...[]byte) {
	t.Helper()
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()
	records := make([]*kgo.Record, 0, len(values))
	for _, value := range values {
		records = append(records, &kgo.Record{Topic: topic, Value: value})
	}
	for _, result := range producer.ProduceSync(ctx, records...) {
		require.NoError(t, result.Err)
	}
}
