//go:build integration

package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/ghaering/core-api-task/event_client"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	KAFKA_DEFAULT_PORT = "9092/tcp"
)

func TestKafkaClient(t *testing.T) {
	// Setup
	ctx := context.Background()
	kafkaC, errC := tckafka.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, errC)
	testcontainers.CleanupContainer(t, kafkaC)

	brokers, err := kafkaC.Brokers(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(adminClient.Close)

	admin := kadm.NewClient(adminClient)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = admin.CreateTopic(ctx, 1, 1, nil, "testtopic")
	require.NoError(t, err)

	kc, err := event_client.NewKafkaClient(
		brokers,
		"testgroup",
		"testtopic",
	)
	require.NoError(t, err)
	ec := event_client.NewEventClient(kc)

	// Setup some messages
	event1 := event_client.VmEvent{Event_id: "id1"}
	event2 := event_client.VmEvent{Event_id: "id2"}

	sendErrChan, err := ec.Send(ctx, event1)
	require.NoError(t, err)
	require.NoError(t, <-sendErrChan)
	sendErrChan, err = ec.Send(ctx, event2)
	require.NoError(t, err)
	require.NoError(t, <-sendErrChan)

	// Listen for messages
	eventChan, errChan := ec.Listen(ctx)

	select {
	case event := <-eventChan:
		require.Equal(t, event1.Event_id, event.Event_id)
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for first event")
	}

	select {
	case event := <-eventChan:
		require.Equal(t, event2.Event_id, event.Event_id)
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for second event")
	}

	// Cleanup
	testcontainers.CleanupContainer(t, kafkaC)
	require.NoError(t, errC)
}
