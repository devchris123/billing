package session

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/google/uuid"
)

var ErrNotFound = errors.New("not found")
var ErrSessionConflict = errors.New("session conflict")

const (
	InstanceStartEvent       = "instance.start"
	InstanceStopEvent        = "instance.stop"
	sessionizerProcessorName = "sessionizer"
)

type Session struct {
	SessionID    uuid.UUID
	InstanceID   string
	StartedAt    time.Time
	StoppedAt    time.Time
	StartEventID string
	StopEventID  string
}

type SessionStart struct {
	StartEventID string
	InstanceID   string
	StartedAt    time.Time
	Processed    bool
}

type SessionStop struct {
	StopEventID string
	InstanceID  string
	StoppedAt   time.Time
}

type WatermarkCheckpoint struct {
	ProcessorName    string
	ProcessedThrough time.Time
}

type EventReader interface {
	EventsBetween(ctx context.Context, start time.Time, end time.Time) ([]ing.VmEvent, error)
}

type SessionStore interface {
	CreateSessionStart(ctx context.Context, sessStart SessionStart) error
	CreateSessionStop(ctx context.Context, sessStop SessionStop) error
	ReadSessionStart(ctx context.Context, vmID string, stoppedAt time.Time) (SessionStart, error)
	MarkSessionStartProcessed(ctx context.Context, startEventID string) error
	CreateSession(ctx context.Context, session Session) error
}

type CheckpointStore interface {
	UpdateWatermarkCheckpoint(ctx context.Context, checkpoint WatermarkCheckpoint) error
	ReadWatermarkCheckpoint(ctx context.Context, processorName string) (WatermarkCheckpoint, error)
}

type Sessionizer struct {
	ingestor     *ing.Ingestor
	transactor   Transactor
	logger       *slog.Logger
	newSessionID func() (uuid.UUID, error)
}

func NewSessionizer(
	ingestor *ing.Ingestor,
	transactor Transactor,
	logger *slog.Logger,
) *Sessionizer {
	return &Sessionizer{
		ingestor:     ingestor,
		transactor:   transactor,
		logger:       logger,
		newSessionID: uuid.NewV7,
	}
}

func (sess *Sessionizer) loadCheckpoint(ctx context.Context) (WatermarkCheckpoint, error) {
	var processedThrough WatermarkCheckpoint
	err := sess.transactor.Run(ctx, func(stores TxStores) error {
		checkpoint, err := stores.CheckpointStore.ReadWatermarkCheckpoint(
			ctx,
			sessionizerProcessorName,
		)
		if errors.Is(err, ErrNotFound) {
			processedThrough = WatermarkCheckpoint{
				ProcessorName: sessionizerProcessorName,
			}
			return nil
		}
		if err != nil {
			return err
		}
		processedThrough = checkpoint
		return nil
	})
	if err != nil {
		sess.logger.ErrorContext(
			ctx,
			"loading watermark checkpoint",
			slog.Any("error", err),
		)
		return WatermarkCheckpoint{}, err
	}
	return processedThrough, nil
}

func (sess *Sessionizer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	processedThrough, err := sess.loadCheckpoint(ctx)
	if err != nil {
		return err
	}
	resChan := sess.ingestor.Ingest(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case res, ok := <-resChan:
			if !ok {
				return nil
			}
			if res.Err != nil {
				return res.Err
			}
			ingestionResult := res.Value
			if ingestionResult.Watermark == nil {
				continue
			}
			var err error
			processedThrough, err = sess.processWatermark(ctx, processedThrough, ingestionResult)
			if err != nil {
				return err
			}
		}
	}
}

func (sess *Sessionizer) processWatermark(ctx context.Context, processedThrough WatermarkCheckpoint, res ing.IngestionResult) (WatermarkCheckpoint, error) {
	if !res.Watermark.After(processedThrough.ProcessedThrough) {
		return processedThrough, nil
	}

	updatedCheckpoint := processedThrough
	updatedCheckpoint.ProcessedThrough = *res.Watermark

	err := sess.transactor.Run(ctx, func(stores TxStores) error {
		events, err := stores.EventReader.EventsBetween(
			ctx,
			processedThrough.ProcessedThrough,
			updatedCheckpoint.ProcessedThrough,
		)
		if err != nil {
			sess.logger.ErrorContext(
				ctx,
				"fetching events between watermarks",
				slog.Time("start", processedThrough.ProcessedThrough),
				slog.Time("end", updatedCheckpoint.ProcessedThrough),
				slog.Any("error", err),
			)
			return err
		}
		if err := sess.makeSessions(ctx, stores, events); err != nil {
			sess.logger.ErrorContext(
				ctx,
				"make sessions",
				slog.Any("error", err),
			)
			return err
		}
		if err := stores.CheckpointStore.UpdateWatermarkCheckpoint(ctx, updatedCheckpoint); err != nil {
			sess.logger.ErrorContext(
				ctx,
				"updating watermark checkpoint",
				slog.Any("error", err),
			)
			return err
		}
		return nil
	})
	if err != nil {
		return WatermarkCheckpoint{}, err
	}
	return updatedCheckpoint, nil
}

func (sess *Sessionizer) makeSessions(ctx context.Context, stores TxStores, events []ing.VmEvent) error {
	groupedEvents := sess.groupEventsByID(events)
	for vmID, events := range groupedEvents {
		slices.SortFunc(
			events,
			func(a ing.VmEvent, b ing.VmEvent) int {
				if byTime := a.OccurredAt.Compare(b.OccurredAt); byTime != 0 {
					return byTime
				}
				if byType := eventTypeOrder(a.EventType) - eventTypeOrder(b.EventType); byType != 0 {
					return byType
				}
				return strings.Compare(a.EventId, b.EventId)
			},
		)
		sess.logger.DebugContext(
			ctx,
			"making sessions for vm",
			slog.String("instance_id", vmID),
		)
		if err := sess.makeSessionForSingleVM(ctx, stores, events); err != nil {
			return err
		}
	}

	return nil
}

func (sess *Sessionizer) makeSessionForSingleVM(ctx context.Context, stores TxStores, events []ing.VmEvent) error {
	for _, e := range events {
		switch e.EventType {
		case InstanceStartEvent:
			sessionStart := SessionStart{
				StartEventID: e.EventId,
				InstanceID:   e.InstanceId,
				StartedAt:    e.OccurredAt,
			}
			err := stores.SessionStore.CreateSessionStart(ctx, sessionStart)
			if err != nil {
				sess.logger.ErrorContext(
					ctx,
					"create session start",
					slog.String("event_id", e.EventId),
					slog.String("instance_id", e.InstanceId),
					slog.Any("error", err),
				)
				return err
			}
		case InstanceStopEvent:
			sessionStart, err := stores.SessionStore.ReadSessionStart(ctx, e.InstanceId, e.OccurredAt)
			if errors.Is(err, ErrNotFound) {
				sess.logger.InfoContext(
					ctx,
					"out of order event",
					slog.String("event_id", e.EventId),
					slog.String("instance_id", e.InstanceId),
					slog.String("eventtype", e.EventType),
					slog.Time("end", e.OccurredAt),
				)
				sessStop := SessionStop{
					StopEventID: e.EventId,
					InstanceID:  e.InstanceId,
					StoppedAt:   e.OccurredAt,
				}
				err := stores.SessionStore.CreateSessionStop(ctx, sessStop)
				if err != nil {
					sess.logger.ErrorContext(
						ctx,
						"create session stop",
						slog.String("event_id", e.EventId),
						slog.String("instance_id", e.InstanceId),
						slog.Any("error", err),
					)
					return err
				}
				continue
			}
			if err != nil {
				sess.logger.ErrorContext(
					ctx,
					"fetch session start",
					slog.String("event_id", e.EventId),
					slog.String("instance_id", e.InstanceId),
					slog.Any("error", err),
				)
				return err
			}
			sessionID, err := sess.newSessionID()
			if err != nil {
				return err
			}
			session := Session{
				SessionID:    sessionID,
				InstanceID:   e.InstanceId,
				StartedAt:    sessionStart.StartedAt,
				StoppedAt:    e.OccurredAt,
				StartEventID: sessionStart.StartEventID,
				StopEventID:  e.EventId,
			}
			err = stores.SessionStore.CreateSession(ctx, session)
			if err != nil {
				sess.logger.ErrorContext(
					ctx,
					"create session",
					slog.String("event_id", e.EventId),
					slog.String("instance_id", e.InstanceId),
					slog.Any("error", err),
				)
				return err
			}
			err = stores.SessionStore.MarkSessionStartProcessed(ctx, sessionStart.StartEventID)
			if err != nil {
				sess.logger.ErrorContext(
					ctx,
					"mark session start processed",
					slog.String("event_id", e.EventId),
					slog.String("instance_id", e.InstanceId),
					slog.Any("error", err),
				)
				return err
			}
		default:
			sess.logger.DebugContext(
				ctx,
				"ignoring event",
				slog.String("event_id", e.EventId),
				slog.String("instance_id", e.InstanceId),
				slog.String("eventtype", e.EventType),
			)
		}
	}
	return nil
}

func eventTypeOrder(eventType string) int {
	switch eventType {
	case InstanceStartEvent:
		return 0
	case InstanceStopEvent:
		return 1
	default:
		return 2
	}
}

func (sess *Sessionizer) groupEventsByID(events []ing.VmEvent) map[string][]ing.VmEvent {
	groupedEvents := make(map[string][]ing.VmEvent)
	for _, e := range events {
		if _, ok := groupedEvents[e.InstanceId]; !ok {
			groupedEvents[e.InstanceId] = make([]ing.VmEvent, 0)
		}
		groupedEvents[e.InstanceId] = append(groupedEvents[e.InstanceId], e)
	}
	return groupedEvents
}
