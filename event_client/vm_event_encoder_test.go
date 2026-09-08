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

func TestVmEventJSONEncoderDecodesCorrectly(t *testing.T) {
	// Setup
	encoder := &VmEventJsonEncoder{}
	// Sample JSON representation of a VmEvent
	jsonData := []byte(`{
		"event_id": "event-2",
		"flavor": "medium",
		"instance_id": "instance-2",
		"occurred_at": "2025-02-01T12:00:00Z",
		"project_id": "project-2",
		"type": "stop"
	}`)

	expected := ing.VmEvent{
		EventId:    "event-2",
		Flavour:    "medium",
		InstanceId: "instance-2",
		OccurredAt: time.Date(2025, 2, 1, 12, 0, 0, 0, time.UTC),
		ProjectId:  "project-2",
		EventType:  "stop",
	}

	// Execute
	actual, err := encoder.Decode(jsonData)

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

func TestVmEventJSONEncoderRejectsMissingRequiredFields(t *testing.T) {
	// Setup
	encoder := &VmEventJsonEncoder{}

	// Execute
	_, err := encoder.Decode([]byte(`{}`))

	// Assert
	require.ErrorContains(t, err, "missing required event fields")
	require.ErrorContains(t, err, "event_id")
	require.ErrorContains(t, err, "flavor")
	require.ErrorContains(t, err, "instance_id")
	require.ErrorContains(t, err, "occurred_at")
	require.ErrorContains(t, err, "project_id")
	require.ErrorContains(t, err, "type")
}
