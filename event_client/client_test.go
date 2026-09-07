package event_client

import (
	"context"
	"errors"
	"testing"
	"time"

	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type failingEncoder struct {
	err error
}

func (encoder failingEncoder) Encode(ing.VmEvent) ([]byte, error) {
	return nil, encoder.err
}

func (encoder failingEncoder) Decode([]byte) (ing.VmEvent, error) {
	return ing.VmEvent{}, nil
}

func TestKafkaClientSendReturnsEncodingError(t *testing.T) {
	// Setup
	expectedErr := errors.New("encode event")
	client := &KafkaClient[ing.VmEvent]{
		encoder: failingEncoder{err: expectedErr},
	}

	// Execute
	errChan := client.Send(context.Background(), ing.VmEvent{})

	// Assert
	require.ErrorIs(t, <-errChan, expectedErr)
	_, ok := <-errChan
	require.False(t, ok)
}

func TestKafkaClientListenReturnsTerminalFetchError(t *testing.T) {
	// Setup
	kafkaClient, err := NewKafkaClient(
		[]string{"127.0.0.1:1"},
		"test-group",
		"test-topic",
		&VmEventJsonEncoder{},
		func(_ context.Context, operation func() error) error { return operation() },
	)
	require.NoError(t, err)
	kafkaClient.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenErr := make(chan error, 1)
	handlerCalled := false

	// Execute
	go func() {
		listenErr <- kafkaClient.Listen(ctx, func(ing.Result[ing.VmEvent]) error {
			handlerCalled = true
			return nil
		})
	}()

	// Assert
	select {
	case err := <-listenErr:
		require.ErrorIs(t, err, kgo.ErrClientClosed)
		require.False(t, handlerCalled)
	case <-time.After(250 * time.Millisecond):
		cancel()
		<-listenErr
		t.Fatal("listener did not return the terminal fetch error")
	}
}

func TestNewKafkaClientRejectsMissingRetryFunction(t *testing.T) {
	// Execute
	_, err := NewKafkaClient(
		[]string{"127.0.0.1:1"},
		"test-group",
		"test-topic",
		&VmEventJsonEncoder{},
		nil,
	)

	// Assert
	require.EqualError(t, err, "retry function must not be nil")
}

func TestNewKafkaClientRejectsMissingEncoder(t *testing.T) {
	// Execute
	_, err := NewKafkaClient[ing.VmEvent](
		[]string{"127.0.0.1:1"},
		"test-group",
		"test-topic",
		nil,
		func(_ context.Context, operation func() error) error { return operation() },
	)

	// Assert
	require.EqualError(t, err, "encoder must not be nil")
}

func TestKafkaClientListenRejectsMissingHandler(t *testing.T) {
	// Setup
	kafkaClient, err := NewKafkaClient(
		[]string{"127.0.0.1:1"},
		"test-group",
		"test-topic",
		&VmEventJsonEncoder{},
		func(_ context.Context, operation func() error) error { return operation() },
	)
	require.NoError(t, err)
	defer kafkaClient.Close()

	// Execute
	err = kafkaClient.Listen(context.Background(), nil)

	// Assert
	require.EqualError(t, err, "handler must not be nil")
}
