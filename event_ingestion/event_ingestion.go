package eventingestion

import (
	"context"
	"log/slog"
	"time"
)

type VmEvent struct {
	Event_id    string
	Flavour     string
	Instance_id string
	Occurred_at time.Time
	Project_id  string
	Event_type  string
}

type EventClient interface {
	Close(ctx context.Context)
	Listen(ctx context.Context) (<-chan VmEvent, <-chan error)
}

type EventIngestionDb interface {
	Append(ctx context.Context, vmEvent VmEvent) error
}

type IngestionConfig struct {
	MaxOutOfOrderness time.Duration
}

type Ingestor struct {
	ec              EventClient
	edb             EventIngestionDb
	ingestionConfig IngestionConfig
	logger          *slog.Logger
}

type IngestionResult struct {
	Event     VmEvent
	Watermark *time.Time
}

func NewIngestor(
	ec EventClient,
	edb EventIngestionDb,
	ingestionConfig IngestionConfig,
	logger *slog.Logger,
) *Ingestor {
	return &Ingestor{
		ec:              ec,
		edb:             edb,
		ingestionConfig: ingestionConfig,
		logger:          logger,
	}
}

func (ing *Ingestor) Ingest(ctx context.Context) chan IngestionResult {
	resultChan := make(chan IngestionResult)

	vmEventChan, errChan := ing.ec.Listen(ctx)

	go func() {
		defer close(resultChan)

		var lastSeenEventTimestamp time.Time
		for vmEventChan != nil || errChan != nil {
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
				ing.logger.DebugContext(
					ctx, "Ingest event",
					slog.Any("event", vmEvent),
				)
				if err := ing.edb.Append(ctx, vmEvent); err != nil {
					ing.logger.ErrorContext(
						ctx, "Ingest append error",
						slog.Any("error", err),
					)
					continue
				}

				var watermark *time.Time
				if vmEvent.Occurred_at.After(lastSeenEventTimestamp) {
					lastSeenEventTimestamp = vmEvent.Occurred_at
					nextWatermark := lastSeenEventTimestamp.Add(
						-ing.ingestionConfig.MaxOutOfOrderness,
					)
					watermark = &nextWatermark
				}
				result := IngestionResult{
					Event:     vmEvent,
					Watermark: watermark,
				}
				select {
				case resultChan <- result:
				case <-ctx.Done():
					return
				}
			case err, ok := <-errChan:
				if !ok {
					ing.logger.InfoContext(
						ctx,
						"Ingest error channel closed",
					)
					errChan = nil
					continue
				}
				ing.logger.ErrorContext(
					ctx, "Ingest error %",
					slog.Any("error", err),
				)
			}
		}
	}()

	return resultChan
}
