package eventingestion

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type VmEvent struct {
	EventId    string
	Flavour    string
	InstanceId string
	OccurredAt time.Time
	ProjectId  string
	EventType  string
}

// Result contains either a successfully decoded value or an error.
type Result[T any] struct {
	Value T
	Err   error
}

// EventClient consumes events serially. Returning nil from the handler allows
// the source record to be acknowledged; returning an error prevents the source
// from advancing until its retry policy succeeds or terminates.
type EventClient interface {
	Close()
	Listen(ctx context.Context, handler func(Result[VmEvent]) error) error
}

type EventAppender interface {
	Append(ctx context.Context, vmEvent VmEvent) error
}

type IngestionConfig struct {
	MaxOutOfOrderness time.Duration
}

type Ingestor struct {
	eventClient     EventClient
	eventAppender   EventAppender
	ingestionConfig IngestionConfig
	logger          *slog.Logger
}

type IngestionResult struct {
	Event     VmEvent
	Watermark *time.Time
}

func NewIngestor(
	eventClient EventClient,
	eventAppender EventAppender,
	ingestionConfig IngestionConfig,
	logger *slog.Logger,
) *Ingestor {
	return &Ingestor{
		eventClient:     eventClient,
		eventAppender:   eventAppender,
		ingestionConfig: ingestionConfig,
		logger:          logger,
	}
}

func (ing *Ingestor) Ingest(ctx context.Context) <-chan Result[IngestionResult] {
	resultChan := make(chan Result[IngestionResult])

	go func() {
		defer close(resultChan)

		var lastSeenEventTimestamp time.Time
		err := ing.eventClient.Listen(ctx, func(vmEvent Result[VmEvent]) error {
			if vmEvent.Err != nil {
				ing.logger.ErrorContext(
					ctx, "Ingest event error",
					slog.Any("error", vmEvent.Err),
				)
				return nil
			}

			if err := ing.eventAppender.Append(ctx, vmEvent.Value); err != nil {
				ing.logger.ErrorContext(
					ctx, "Ingest append error",
					slog.Any("error", err),
				)
				return err
			}

			var watermark *time.Time
			if vmEvent.Value.OccurredAt.After(lastSeenEventTimestamp) {
				lastSeenEventTimestamp = vmEvent.Value.OccurredAt
				nextWatermark := lastSeenEventTimestamp.Add(
					-ing.ingestionConfig.MaxOutOfOrderness,
				)
				watermark = &nextWatermark
			}
			result := Result[IngestionResult]{
				Value: IngestionResult{Event: vmEvent.Value, Watermark: watermark},
			}
			select {
			case resultChan <- result:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			select {
			case resultChan <- Result[IngestionResult]{Err: err}:
			case <-ctx.Done():
			}
		}
	}()

	return resultChan
}
