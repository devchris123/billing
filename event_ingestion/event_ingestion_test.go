package eventingestion

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type FakeEventClient struct {
	resultChan chan Result[VmEvent]
	listenErr  error
}

func (ec *FakeEventClient) Close(ctx context.Context) {}

func (ec *FakeEventClient) Listen(ctx context.Context, handler func(Result[VmEvent]) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-ec.resultChan:
			if !ok {
				return ec.listenErr
			}
			_ = handler(result)
		}
	}
}

type FakeEventDB struct {
	appended chan VmEvent
}

func (ebd *FakeEventDB) Append(ctx context.Context, vmEvent VmEvent) error {
	ebd.appended <- vmEvent
	return nil
}

func TestWatermarkAdvances(t *testing.T) {
	// Setup
	clientResultChan := make(chan Result[VmEvent], 3)
	appended := make(chan VmEvent, 3)
	ing := newTestIngestor(clientResultChan, appended)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Execute
	resultChan := ing.Ingest(ctx)

	event1Time := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	event1 := VmEvent{EventId: "id1", OccurredAt: event1Time}
	event2Time := event1Time.Add(time.Minute)
	event2 := VmEvent{EventId: "id2", OccurredAt: event2Time}
	event3 := VmEvent{EventId: "id3", OccurredAt: event2Time.Add(-5 * time.Minute)}

	clientResultChan <- Result[VmEvent]{Value: event1}
	clientResultChan <- Result[VmEvent]{Value: event2}
	clientResultChan <- Result[VmEvent]{Value: event3}

	// Assert
	select {
	case result := <-resultChan:
		require.NoError(t, result.Err)
		require.NotNil(t, result.Value.Watermark)
		require.Equal(t, event1.OccurredAt, *result.Value.Watermark)
		require.Equal(t, event1.EventId, result.Value.Event.EventId)
	case <-time.After(1 * time.Second):
		t.Fatal("watermark timeout")
	}

	select {
	case result := <-resultChan:
		require.NoError(t, result.Err)
		require.NotNil(t, result.Value.Watermark)
		require.Equal(t, event2.OccurredAt, *result.Value.Watermark)
		require.Equal(t, event2.EventId, result.Value.Event.EventId)
	case <-time.After(1 * time.Second):
		t.Fatal("watermark timeout")
	}

	select {
	case result := <-resultChan:
		require.NoError(t, result.Err)
		require.Nil(t, result.Value.Watermark)
		require.Equal(t, event3.EventId, result.Value.Event.EventId)
	case <-time.After(1 * time.Second):
		t.Fatal("watermark timeout")
	}

	for _, expected := range []VmEvent{event1, event2, event3} {
		select {
		case actual := <-appended:
			require.Equal(t, expected.EventId, actual.EventId)
		case <-time.After(time.Second):
			t.Fatalf("event %s was not appended", expected.EventId)
		}
	}
}

func TestIngestContinuesAfterError(t *testing.T) {
	// Setup
	resultChan := make(chan Result[VmEvent], 2)
	appended := make(chan VmEvent, 1)
	ing := NewIngestor(
		&FakeEventClient{resultChan: resultChan},
		&FakeEventDB{appended: appended},
		IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ingestionResultChan := ing.Ingest(ctx)

	event := VmEvent{
		EventId:    "id1",
		OccurredAt: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
	}

	// Execute
	resultChan <- Result[VmEvent]{Err: context.DeadlineExceeded}
	resultChan <- Result[VmEvent]{Value: event}

	// Assert
	select {
	case result := <-ingestionResultChan:
		require.NoError(t, result.Err)
		require.Equal(t, event.EventId, result.Value.Event.EventId)
		require.NotNil(t, result.Value.Watermark)
	case <-time.After(time.Second):
		t.Fatal("event was not processed after error channel closed")
	}

	select {
	case actual := <-appended:
		require.Equal(t, event.EventId, actual.EventId)
	case <-time.After(time.Second):
		t.Fatal("event was not appended after error channel closed")
	}

	close(resultChan)
	select {
	case _, ok := <-ingestionResultChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("ingestion result channel did not close")
	}
}

func TestIngestClosesResultChannelWhenSourceCloses(t *testing.T) {
	// Setup
	clientResultChan := make(chan Result[VmEvent])
	appended := make(chan VmEvent)
	ing := NewIngestor(
		&FakeEventClient{resultChan: clientResultChan},
		&FakeEventDB{appended: appended},
		IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Execute
	resultChan := ing.Ingest(ctx)
	close(clientResultChan)

	// Assert
	select {
	case _, ok := <-resultChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("result channel did not close after its sources closed")
	}
}

func TestIngestPropagatesTerminalClientError(t *testing.T) {
	// Setup
	expectedErr := errors.New("listen failed")
	clientResultChan := make(chan Result[VmEvent])
	close(clientResultChan)
	ing := NewIngestor(
		&FakeEventClient{resultChan: clientResultChan, listenErr: expectedErr},
		&FakeEventDB{appended: make(chan VmEvent)},
		IngestionConfig{},
		slog.Default(),
	)

	// Execute
	resultChan := ing.Ingest(context.Background())

	// Assert
	result, ok := <-resultChan
	require.True(t, ok)
	require.ErrorIs(t, result.Err, expectedErr)
	_, ok = <-resultChan
	require.False(t, ok)
}

func TestIngestStopsOnCancellation(t *testing.T) {
	// Setup
	clientResultChan := make(chan Result[VmEvent], 3)
	appended := make(chan VmEvent, 3)
	ing := newTestIngestor(clientResultChan, appended)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Execute
	resultChan := ing.Ingest(ctx)

	cancel()

	// Assert
	select {
	case _, ok := <-resultChan:
		require.False(t, ok)
	case <-time.After(1 * time.Second):
		t.Fatal("watermark timeout")
	}
}

func newTestIngestor(resultChan chan Result[VmEvent], appended chan VmEvent) *Ingestor {
	return NewIngestor(
		&FakeEventClient{resultChan: resultChan},
		&FakeEventDB{appended: appended},
		IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	)
}
