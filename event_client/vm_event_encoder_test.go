package event_client

import (
	"testing"
	"time"

	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/stretchr/testify/require"
)

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
