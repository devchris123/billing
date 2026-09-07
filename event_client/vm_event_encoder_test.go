package event_client

import (
	"context"
	"errors"
	"testing"
	"time"

	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/stretchr/testify/require"
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

func TestVmEventJSONEncoderRoundTrip(t *testing.T) {
	// Setup
	encoder := &VmEventJsonEncoder{}
	expected := ing.VmEvent{
		EventId:    "event-1",
		Flavour:    "small",
		InstanceId: "instance-1",
		OccurredAt: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
		ProjectId:  "project-1",
		EventType:  "start",
	}

	// Execute
	encoded, err := encoder.Encode(expected)
	require.NoError(t, err)
	actual, err := encoder.Decode(encoded)

	// Assert
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestVmEventJSONEncoderRejectsMalformedJSON(t *testing.T) {
	// Setup
	encoder := &VmEventJsonEncoder{}

	// Execute
	_, err := encoder.Decode([]byte("malformed"))

	// Assert
	require.Error(t, err)
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
