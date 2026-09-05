package event_client

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type FakeClient struct {
	listenerChan <-chan []byte
	errChan      <-chan error
}

func (kc FakeClient) close() {
	return
}

func (fc FakeClient) listen(ctx context.Context) (<-chan []byte, <-chan error) {
	return fc.listenerChan, fc.errChan
}

func (fc FakeClient) send(ctx context.Context, value []byte) chan error {
	return nil
}

func TestEventClientListen(t *testing.T) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenerChan := make(chan []byte, 2)
	errChan := make(chan error, 2)
	ec := NewEventClient(FakeClient{
		listenerChan: listenerChan,
		errChan:      errChan,
	})

	event1 := VmEvent{Event_id: "id1"}
	event1Json, err := json.Marshal(event1)
	require.NoError(t, err)
	event2 := VmEvent{Event_id: "id2"}
	event2Json, err := json.Marshal(event2)
	require.NoError(t, err)

	listenerChan <- event1Json
	listenerChan <- event2Json

	// listen
	eventChan, errEventChan := ec.Listen(ctx)

	// Assert
	select {
	case value := <-eventChan:
		require.Equal(t, event1.Event_id, value.Event_id)
	case err := <-errEventChan:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("eventChan timeout")
	}

	select {
	case value := <-eventChan:
		require.Equal(t, event2.Event_id, value.Event_id)
	case err := <-errEventChan:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("eventChan timeout")
	}
}

func TestEventClientListenMarshalError(t *testing.T) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenerChan := make(chan []byte, 2)
	errChan := make(chan error, 2)
	ec := NewEventClient(FakeClient{
		listenerChan: listenerChan,
		errChan:      errChan,
	})

	listenerChan <- []byte("malformed")

	// Execute
	eventChan, errEventChan := ec.Listen(ctx)

	// Assert
	select {
	case <-eventChan:
		t.Fatal("expected error")
	case err := <-errEventChan:
		require.Error(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("eventChan timeout")
	}
}

func TestEventClientListenClosedSrcChan(t *testing.T) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenerChan := make(chan []byte, 2)
	errChan := make(chan error, 2)
	ec := NewEventClient(FakeClient{
		listenerChan: listenerChan,
		errChan:      errChan,
	})

	// Execute
	eventChan, errEventChan := ec.Listen(ctx)

	close(listenerChan)
	close(errChan)

	// Assert
	select {
	case _, ok := <-eventChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("event channel did not close")
	}

	select {
	case _, ok := <-errEventChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("error channel did not close")
	}
}

func TestEventClientListenCancels(t *testing.T) {
	// Setup
	listenerChan := make(chan []byte)
	errChan := make(chan error)
	ec := NewEventClient(FakeClient{
		listenerChan: listenerChan,
		errChan:      errChan,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Execute
	eventChan, errEventChan := ec.Listen(ctx)

	cancel()

	// Assert
	select {
	case _, ok := <-eventChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("event channel did not close")
	}

	select {
	case _, ok := <-errEventChan:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("error channel did not close")
	}
}
