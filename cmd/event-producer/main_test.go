package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	// Setup
	args := []string{
		"-brokers", " kafka-1:9092, kafka-2:9092 ",
		"-topic", "test-events",
		"-count", "12",
		"-instances", "3",
		"-start-probability", "0.75",
		"-interval", "20ms",
		"-event-time-step", "2m",
		"-max-duration", "5m",
		"-seed", "42",
	}

	// Execute
	conf, err := parseConfig(args)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, conf.brokers)
	require.Equal(t, "test-events", conf.topic)
	require.Equal(t, 12, conf.count)
	require.Equal(t, 3, conf.instanceCount)
	require.Equal(t, 0.75, conf.startProbability)
	require.Equal(t, 20*time.Millisecond, conf.interval)
	require.Equal(t, 2*time.Minute, conf.eventTimeStep)
	require.Equal(t, 5*time.Minute, conf.maxDuration)
	require.Equal(t, int64(42), conf.seed)
}

func TestParseConfigReportsAllValidationErrors(t *testing.T) {
	// Setup
	args := []string{
		"-brokers", ",",
		"-topic", " ",
		"-count", "-1",
		"-instances", "0",
		"-start-probability", "1.1",
		"-interval", "-1s",
		"-event-time-step", "-1m",
		"-max-duration", "-1m",
	}

	// Execute
	_, err := parseConfig(args)

	// Assert
	require.ErrorContains(t, err, "kafka broker at position 1 must not be empty")
	require.ErrorContains(t, err, "kafka broker at position 2 must not be empty")
	require.ErrorContains(t, err, "kafka topic must not be empty")
	require.ErrorContains(t, err, "event count must be non-negative")
	require.ErrorContains(t, err, "instance count must be greater than zero")
	require.ErrorContains(t, err, "start probability must be between 0 and 1")
	require.ErrorContains(t, err, "interval must be non-negative")
	require.ErrorContains(t, err, "event time step must be non-negative")
	require.ErrorContains(t, err, "max duration must be non-negative")
}

func TestParseConfigRejectsContinuousProductionWithoutInterval(t *testing.T) {
	// Setup
	args := []string{"-count", "0", "-interval", "0"}

	// Execute
	_, err := parseConfig(args)

	// Assert
	require.EqualError(t, err, "interval must be greater than zero when producing continuously")
}

func TestEventGeneratorProducesValidInstanceLifecycles(t *testing.T) {
	// Setup
	firstEventTime := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	generator := newEventGenerator(rand.New(rand.NewSource(42)), 5)
	running := make(map[string]struct {
		projectID string
		flavour   string
	})

	// Execute and assert
	for i := 0; i < 100; i++ {
		event := generator.nextEvent(firstEventTime.Add(time.Duration(i)*time.Minute), 0.5)
		require.NotEmpty(t, event.EventId)
		require.Equal(t, firstEventTime.Add(time.Duration(i)*time.Minute), event.OccurredAt)
		require.Contains(t, projects, event.ProjectId)
		require.Contains(t, flavours, event.Flavour)

		switch event.EventType {
		case instanceStartEvent:
			_, alreadyRunning := running[event.InstanceId]
			require.False(t, alreadyRunning, "instance started while already running")
			running[event.InstanceId] = struct {
				projectID string
				flavour   string
			}{projectID: event.ProjectId, flavour: event.Flavour}
		case instanceStopEvent:
			started, isRunning := running[event.InstanceId]
			require.True(t, isRunning, "instance stopped without a start")
			require.Equal(t, started.projectID, event.ProjectId)
			require.Equal(t, started.flavour, event.Flavour)
			delete(running, event.InstanceId)
		default:
			t.Fatalf("unexpected event type %q", event.EventType)
		}
	}
}
