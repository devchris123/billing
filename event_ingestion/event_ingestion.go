package eventingestion

import (
	"context"
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

type Result[T any] struct {
	Value T
	Err   error
}

type EventClient interface {
	Close(ctx context.Context)
	Listen(ctx context.Context) chan Result[VmEvent]
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

func (ing *Ingestor) Ingest(ctx context.Context) chan IngestionResult {
	resultChan := make(chan IngestionResult)

	vmEventChan := ing.eventClient.Listen(ctx)

	go func() {
		defer close(resultChan)

		var lastSeenEventTimestamp time.Time
		for vmEventChan != nil {
			select {
			case <-ctx.Done():
				ing.logger.DebugContext(
					ctx,
					"Ingest context timeout",
				)
				return
			case vmEvent, ok := <-vmEventChan:
				if !ok {
					ing.logger.InfoContext(
						ctx,
						"Ingest event channel closed",
					)
					vmEventChan = nil
					continue
				}
				if vmEvent.Err != nil {
					ing.logger.ErrorContext(
						ctx, "Ingest event error",
						slog.Any("error", vmEvent.Err),
					)
					continue
				}

				if err := ing.eventAppender.Append(ctx, vmEvent.Value); err != nil {
					ing.logger.ErrorContext(
						ctx, "Ingest append error",
						slog.Any("error", err),
					)
					continue
				}

				var watermark *time.Time
				if vmEvent.Value.OccurredAt.After(lastSeenEventTimestamp) {
					lastSeenEventTimestamp = vmEvent.Value.OccurredAt
					nextWatermark := lastSeenEventTimestamp.Add(
						-ing.ingestionConfig.MaxOutOfOrderness,
					)
					watermark = &nextWatermark
				}
				result := IngestionResult{
					Event:     vmEvent.Value,
					Watermark: watermark,
				}
				select {
				case resultChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return resultChan
}
