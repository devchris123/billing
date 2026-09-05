package event_client

import "context"

func send[T any](ctx context.Context, to chan T, value T) bool {
	select {
	case to <- value:
		return true
	case <-ctx.Done():
		return false
	}
}
