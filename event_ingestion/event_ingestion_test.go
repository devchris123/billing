package eventingestion

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type FakeEventClient struct {
	vmEventChan chan VmEvent
	errChan     chan error
}

func (ec *FakeEventClient) Close(ctx context.Context) {}

func (ec *FakeEventClient) Listen(ctx context.Context) (<-chan VmEvent, <-chan error) {
	return ec.vmEventChan, ec.errChan
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
	vmEventChan := make(chan VmEvent, 3)
	appended := make(chan VmEvent, 3)
	ing := newTestIngestor(vmEventChan, appended)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Execute
	resultChan := ing.Ingest(ctx)

	event1Time := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	event1 := VmEvent{EventId: "id1", Occurred_at: event1Time}
	event2Time := event1Time.Add(time.Minute)
	event2 := VmEvent{EventId: "id2", Occurred_at: event2Time}
	event3 := VmEvent{EventId: "id3", Occurred_at: event2Time.Add(-5 * time.Minute)}

	vmEventChan <- event1
	vmEventChan <- event2
	vmEventChan <- event3

	// Assert
	select {
	case result := <-resultChan:
		require.NotNil(t, result.Watermark)
		require.Equal(t, event1.Occurred_at, *result.Watermark)
		require.Equal(t, event1.EventId, result.Event.EventId)
	case <-time.After(1 * time.Second):
		t.Fatal("watermark timeout")
	}

	select {
	case result := <-resultChan:
		require.NotNil(t, result.Watermark)
		require.Equal(t, event2.Occurred_at, *result.Watermark)
		require.Equal(t, event2.EventId, result.Event.EventId)
	case <-time.After(1 * time.Second):
		t.Fatal("watermark timeout")
	}

	select {
	case result := <-resultChan:
		require.Nil(t, result.Watermark)
		require.Equal(t, event3.EventId, result.Event.EventId)
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

func TestIngestContinuesAfterErrorChannelCloses(t *testing.T) {
	vmEventChan := make(chan VmEvent, 1)
	errChan := make(chan error)
	appended := make(chan VmEvent, 1)
	ing := NewIngestor(
		&FakeEventClient{vmEventChan: vmEventChan, errChan: errChan},
		&FakeEventDB{appended: appended},
		IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := ing.Ingest(ctx)
	close(errChan)

	event := VmEvent{
		EventId:     "id1",
		Occurred_at: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
	}
	vmEventChan <- event

	select {
	case result := <-resultChan:
		require.Equal(t, event.EventId, result.Event.EventId)
		require.NotNil(t, result.Watermark)
	case <-time.After(time.Second):
		t.Fatal("event was not processed after error channel closed")
	}

	select {
	case actual := <-appended:
		require.Equal(t, event.EventId, actual.EventId)
	case <-time.After(time.Second):
		t.Fatal("event was not appended after error channel closed")
	}

	close(vmEventChan)
	select {
	case _, ok := <-resultChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("result channel did not close after both sources closed")
	}
}

func TestIngestClosesResultChannelWhenSourcesClose(t *testing.T) {
	vmEventChan := make(chan VmEvent)
	errChan := make(chan error)
	appended := make(chan VmEvent)
	ing := NewIngestor(
		&FakeEventClient{vmEventChan: vmEventChan, errChan: errChan},
		&FakeEventDB{appended: appended},
		IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := ing.Ingest(ctx)
	close(vmEventChan)
	close(errChan)

	select {
	case _, ok := <-resultChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("result channel did not close after its sources closed")
	}
}

func TestIngestStopsOnCancellation(t *testing.T) {
	// Setup
	vmEventChan := make(chan VmEvent, 3)
	appended := make(chan VmEvent, 3)
	ing := newTestIngestor(vmEventChan, appended)
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

func newTestIngestor(vmEventChan chan VmEvent, appended chan VmEvent) *Ingestor {
	return NewIngestor(
		&FakeEventClient{vmEventChan: vmEventChan},
		&FakeEventDB{appended: appended},
		IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	)
}
