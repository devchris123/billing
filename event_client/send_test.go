package event_client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSendSendsValue(t *testing.T) {
	// Setup
	ctx := context.Background()
	myChan := make(chan string, 1)
	value := "hello chan"

	// Execute
	sent := send(ctx, myChan, value)

	// Assert
	require.True(t, sent)
	require.Equal(t, "hello chan", <-myChan)
}

func TestSendCancels(t *testing.T) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	myChan := make(chan string)
	value := "hello chan"
	result := make(chan bool)

	// Execute
	go func() {
		result <- send(ctx, myChan, value)
	}()

	cancel()

	// Assert
	select {
	case sent := <-result:
		require.False(t, sent)
	case <-time.After(time.Second):
		t.Fatal("send did not return after context cancellation")
	}
}
