package session

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type stubEventReader struct {
	events []ing.VmEvent
	err    error
	starts []time.Time
	ends   []time.Time
}

func (reader *stubEventReader) EventsBetween(
	ctx context.Context,
	start time.Time,
	end time.Time,
) ([]ing.VmEvent, error) {
	reader.starts = append(reader.starts, start)
	reader.ends = append(reader.ends, end)
	return reader.events, reader.err
}

type stubSessionStore struct {
	createStartErr error
	createStopErr  error
	readStart      SessionStart
	readStartErr   error
	createErr      error
	createdStarts  []SessionStart
	createdStops   []SessionStop
	created        []Session
}

func (store *stubSessionStore) CreateSessionStart(
	ctx context.Context,
	start SessionStart,
) error {
	if store.createStartErr != nil {
		return store.createStartErr
	}
	store.createdStarts = append(store.createdStarts, start)
	return nil
}

func (store *stubSessionStore) MarkSessionStartProcessed(
	ctx context.Context,
	startEventID string,
) error {
	if store.readStart.StartEventID == startEventID && !store.readStart.Processed {
		store.readStart.Processed = true
		return nil
	}
	for i, s := range store.createdStarts {
		if s.StartEventID == startEventID && !s.Processed {
			store.createdStarts[i].Processed = true
			return nil
		}
	}
	return ErrNotFound
}

func (store *stubSessionStore) CreateSessionStop(
	ctx context.Context,
	stop SessionStop,
) error {
	if store.createStopErr != nil {
		return store.createStopErr
	}
	store.createdStops = append(store.createdStops, stop)
	return nil
}

func (store *stubSessionStore) ReadSessionStart(
	ctx context.Context,
	vmID string,
	stoppedAt time.Time,
) (SessionStart, error) {
	if store.readStartErr != nil {
		return SessionStart{}, store.readStartErr
	}
	if store.readStart.InstanceID != vmID ||
		store.readStart.Processed ||
		store.readStart.StartedAt.After(stoppedAt) {
		return SessionStart{}, ErrNotFound
	}
	return store.readStart, nil
}

func (store *stubSessionStore) CreateSession(
	ctx context.Context,
	session Session,
) error {
	if store.createErr != nil {
		return store.createErr
	}
	store.created = append(store.created, session)
	return nil
}

type stubCheckpointStore struct {
	checkpoint WatermarkCheckpoint
	fetchErr   error
	updateErr  error
	updates    []WatermarkCheckpoint
}

type stubTransactor struct {
	stores TxStores
	err    error
}

func (transactor *stubTransactor) Run(
	ctx context.Context,
	work func(stores TxStores) error,
) error {
	if transactor.err != nil {
		return transactor.err
	}
	return work(transactor.stores)
}

func (store *stubCheckpointStore) ReadWatermarkCheckpoint(
	ctx context.Context,
	processorName string,
) (WatermarkCheckpoint, error) {
	return store.checkpoint, store.fetchErr
}

func (store *stubCheckpointStore) UpdateWatermarkCheckpoint(
	ctx context.Context,
	checkpoint WatermarkCheckpoint,
) error {
	store.updates = append(store.updates, checkpoint)
	return store.updateErr
}

func newFocusedSessionizer(
	eventReader EventReader,
	sessionStore SessionStore,
	checkpointStore CheckpointStore,
) *Sessionizer {
	return NewSessionizer(
		nil,
		&stubTransactor{stores: TxStores{
			EventReader:     eventReader,
			SessionStore:    sessionStore,
			CheckpointStore: checkpointStore,
		}},
		slog.Default(),
	)
}

type FakeEventClient struct {
	resultChan chan ing.Result[ing.VmEvent]
	stopped    chan struct{}
}

func (ec *FakeEventClient) Close() {}

func (ec *FakeEventClient) Listen(ctx context.Context, handler func(ing.Result[ing.VmEvent]) error) error {
	if ec.stopped != nil {
		defer close(ec.stopped)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-ec.resultChan:
			if !ok {
				return nil
			}
			_ = handler(result)
		}
	}
}

type FakeEventStore struct {
	appended     chan ing.VmEvent
	eventsBefore []ing.VmEvent
}

func (ebd *FakeEventStore) Append(ctx context.Context, vmEvent ing.VmEvent) error {
	ebd.appended <- vmEvent
	return nil
}

func (ebd *FakeEventStore) EventsBetween(ctx context.Context, start time.Time, end time.Time) ([]ing.VmEvent, error) {
	events := make([]ing.VmEvent, 0)
	for _, e := range ebd.eventsBefore {
		if e.OccurredAt.After(start) && !e.OccurredAt.After(end) {
			events = append(events, e)
		}
	}
	return events, nil
}

type FakeSessionStore struct {
	createdSessionStarts      chan SessionStart
	createdSessionStops       chan SessionStop
	createdSessions           chan Session
	createdSessionStartsSlice []SessionStart
}

func (sdb *FakeSessionStore) CreateSessionStart(ctx context.Context, sessStart SessionStart) error {
	sdb.createdSessionStarts <- sessStart
	sdb.createdSessionStartsSlice = append(sdb.createdSessionStartsSlice, sessStart)
	return nil
}

func (sdb *FakeSessionStore) MarkSessionStartProcessed(ctx context.Context, startEventID string) error {
	for i, s := range sdb.createdSessionStartsSlice {
		if s.StartEventID == startEventID && !s.Processed {
			sdb.createdSessionStartsSlice[i].Processed = true
			return nil
		}
	}
	return ErrNotFound
}

func (sdb *FakeSessionStore) CreateSessionStop(ctx context.Context, sessStop SessionStop) error {
	sdb.createdSessionStops <- sessStop
	return nil
}

func (sdb *FakeSessionStore) ReadSessionStart(ctx context.Context, vmID string, stoppedAt time.Time) (SessionStart, error) {
	var latest SessionStart
	found := false
	for _, start := range sdb.createdSessionStartsSlice {
		if start.InstanceID == vmID &&
			!start.Processed &&
			!start.StartedAt.After(stoppedAt) &&
			(!found || start.StartedAt.After(latest.StartedAt)) {
			latest = start
			found = true
		}
	}
	if found {
		return latest, nil
	}
	return SessionStart{}, ErrNotFound
}

func (sdb *FakeSessionStore) CreateSession(ctx context.Context, session Session) error {
	sdb.createdSessions <- session
	return nil
}

type FakeCheckpointStore struct {
	checkpoints      map[string]WatermarkCheckpoint
	checkpointUpdate chan WatermarkCheckpoint
}

func (cdb *FakeCheckpointStore) UpdateWatermarkCheckpoint(ctx context.Context, checkpoint WatermarkCheckpoint) error {
	cdb.checkpoints[checkpoint.ProcessorName] = checkpoint
	cdb.checkpointUpdate <- checkpoint
	return nil
}

func (cdb *FakeCheckpointStore) ReadWatermarkCheckpoint(ctx context.Context, processorName string) (WatermarkCheckpoint, error) {
	if checkpoint, ok := cdb.checkpoints[processorName]; ok {
		return checkpoint, nil
	}
	return WatermarkCheckpoint{}, ErrNotFound
}

func TestSessionizerCreatesSessions(t *testing.T) {
	// Setup
	vmEventChan := make(chan ing.Result[ing.VmEvent], 3)
	appended := make(chan ing.VmEvent, 3)
	date1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	date2 := date1.Add(time.Hour)
	date3 := date1.Add(5 * time.Hour)
	testEvents := []ing.VmEvent{
		ing.VmEvent{EventId: "id-1", InstanceId: "id-1", EventType: InstanceStartEvent, OccurredAt: date1},
		ing.VmEvent{EventId: "id-2", InstanceId: "id-1", EventType: InstanceStopEvent, OccurredAt: date2},
		ing.VmEvent{EventId: "id-3", InstanceId: "id-x", EventType: InstanceStartEvent, OccurredAt: date3},
	}
	eventReader := &FakeEventStore{
		appended:     appended,
		eventsBefore: testEvents,
	}
	ingestionConfig := ing.IngestionConfig{MaxOutOfOrderness: 10 * time.Minute}
	ingestor := ing.NewIngestor(
		&FakeEventClient{resultChan: vmEventChan},
		eventReader,
		ingestionConfig,
		slog.Default(),
	)
	createdSessions := make(chan Session, 3)
	createdSessionStarts := make(chan SessionStart, 3)
	createdSessionStops := make(chan SessionStop, 3)
	sessionStore := &FakeSessionStore{
		createdSessionStarts: createdSessionStarts,
		createdSessionStops:  createdSessionStops,
		createdSessions:      createdSessions,
	}

	checkpointUpdate := make(chan WatermarkCheckpoint, 3)
	sessionizer := NewSessionizer(
		ingestor,
		&stubTransactor{stores: TxStores{
			EventReader:  eventReader,
			SessionStore: sessionStore,
			CheckpointStore: &FakeCheckpointStore{
				checkpoints:      make(map[string]WatermarkCheckpoint),
				checkpointUpdate: checkpointUpdate,
			},
		}},
		slog.Default(),
	)
	expectedSessionID := uuid.MustParse("01994c8e-7c00-7000-8000-000000000001")
	sessionizer.newSessionID = func() (uuid.UUID, error) {
		return expectedSessionID, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, e := range testEvents {
		vmEventChan <- ing.Result[ing.VmEvent]{Value: e}
	}

	// Execute
	errChan := make(chan error, 1)
	go func() {
		errChan <- sessionizer.Run(ctx)
	}()

	// Assert
	select {
	case start := <-createdSessionStarts:
		require.Equal(t, SessionStart{
			StartEventID: "id-1",
			InstanceID:   "id-1",
			StartedAt:    date1,
		}, start)
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("sessionizer did not produce session start")
	}

	select {
	case completed := <-createdSessions:
		require.Equal(t, Session{
			SessionID:    expectedSessionID,
			InstanceID:   "id-1",
			StartedAt:    date1,
			StoppedAt:    date2,
			StartEventID: "id-1",
			StopEventID:  "id-2",
		}, completed)
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("sessionizer did not produce session")
	}

	cancel()

	select {
	case err := <-errChan:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("sessionizer did not stop")
	}

	select {
	case unexpected := <-createdSessionStarts:
		t.Fatalf(
			"unexpected additional session start for %s",
			unexpected.InstanceID,
		)
	default:
		// Correct: exactly one was produced.
	}

	for _, e := range testEvents {
		// test checkpoint update for each event
		select {
		case checkpoint := <-checkpointUpdate:
			require.Equal(t, "sessionizer", checkpoint.ProcessorName)
			require.Equal(t, e.OccurredAt.Add(-ingestionConfig.MaxOutOfOrderness), checkpoint.ProcessedThrough)
		case <-time.After(time.Second):
			t.Fatalf(
				"sessionizer did not update checkpoint for event %s",
				e.EventId,
			)
		}
	}
}

func TestLoadCheckpointReturnsExistingCheckpoint(t *testing.T) {
	// Setup
	expected := WatermarkCheckpoint{
		ProcessorName:    sessionizerProcessorName,
		ProcessedThrough: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
	}
	checkpointStore := &stubCheckpointStore{checkpoint: expected}
	sessionizer := newFocusedSessionizer(nil, nil, checkpointStore)

	// Execute
	actual, err := sessionizer.loadCheckpoint(context.Background())

	// Assert
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestLoadCheckpointInitializesMissingCheckpoint(t *testing.T) {
	// Setup
	checkpointStore := &stubCheckpointStore{fetchErr: ErrNotFound}
	sessionizer := newFocusedSessionizer(nil, nil, checkpointStore)

	// Execute
	checkpoint, err := sessionizer.loadCheckpoint(context.Background())

	// Assert
	require.NoError(t, err)
	require.Equal(t, sessionizerProcessorName, checkpoint.ProcessorName)
	require.True(t, checkpoint.ProcessedThrough.IsZero())
}

func TestLoadCheckpointReturnsStoreError(t *testing.T) {
	// Setup
	expectedErr := errors.New("checkpoint read failed")
	checkpointStore := &stubCheckpointStore{fetchErr: expectedErr}
	sessionizer := newFocusedSessionizer(nil, nil, checkpointStore)

	// Execute
	_, err := sessionizer.loadCheckpoint(context.Background())

	// Assert
	require.ErrorIs(t, err, expectedErr)
}

func TestProcessWatermarkReadsExpectedWindow(t *testing.T) {
	// Setup
	start := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	eventReader := &stubEventReader{}
	checkpointStore := &stubCheckpointStore{}
	sessionizer := newFocusedSessionizer(
		eventReader,
		&stubSessionStore{},
		checkpointStore,
	)
	checkpoint := WatermarkCheckpoint{
		ProcessorName:    sessionizerProcessorName,
		ProcessedThrough: start,
	}

	// Execute
	_, err := sessionizer.processWatermark(
		context.Background(),
		checkpoint,
		ing.IngestionResult{Watermark: &end},
	)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []time.Time{start}, eventReader.starts)
	require.Equal(t, []time.Time{end}, eventReader.ends)
}

func TestProcessWatermarkAdvancesCheckpointAfterSuccess(t *testing.T) {
	// Setup
	start := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	checkpointStore := &stubCheckpointStore{}
	sessionizer := newFocusedSessionizer(
		&stubEventReader{},
		&stubSessionStore{},
		checkpointStore,
	)
	checkpoint := WatermarkCheckpoint{
		ProcessorName:    sessionizerProcessorName,
		ProcessedThrough: start,
	}

	// Execute
	updated, err := sessionizer.processWatermark(
		context.Background(),
		checkpoint,
		ing.IngestionResult{Watermark: &end},
	)

	// Assert
	require.NoError(t, err)
	require.Equal(t, end, updated.ProcessedThrough)
	require.Equal(t, []WatermarkCheckpoint{updated}, checkpointStore.updates)
}

func TestProcessWatermarkIgnoresNonAdvancingWatermark(t *testing.T) {
	for name, watermarkOffset := range map[string]time.Duration{
		"equal":    0,
		"backward": -time.Minute,
	} {
		t.Run(name, func(t *testing.T) {
			// Setup
			processedThrough := WatermarkCheckpoint{
				ProcessorName:    sessionizerProcessorName,
				ProcessedThrough: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
			}
			watermark := processedThrough.ProcessedThrough.Add(watermarkOffset)
			eventReader := &stubEventReader{}
			checkpointStore := &stubCheckpointStore{}
			sessionizer := newFocusedSessionizer(
				eventReader,
				&stubSessionStore{},
				checkpointStore,
			)

			// Execute
			actual, err := sessionizer.processWatermark(
				context.Background(),
				processedThrough,
				ing.IngestionResult{Watermark: &watermark},
			)

			// Assert
			require.NoError(t, err)
			require.Equal(t, processedThrough, actual)
			require.Empty(t, eventReader.starts)
			require.Empty(t, eventReader.ends)
			require.Empty(t, checkpointStore.updates)
		})
	}
}

func TestProcessWatermarkDoesNotAdvanceWhenEventReadFails(t *testing.T) {
	// Setup
	expectedErr := errors.New("event read failed")
	watermark := time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC)
	checkpointStore := &stubCheckpointStore{}
	sessionizer := newFocusedSessionizer(
		&stubEventReader{err: expectedErr},
		&stubSessionStore{},
		checkpointStore,
	)

	// Execute
	_, err := sessionizer.processWatermark(
		context.Background(),
		WatermarkCheckpoint{ProcessorName: sessionizerProcessorName},
		ing.IngestionResult{Watermark: &watermark},
	)

	// Assert
	require.ErrorIs(t, err, expectedErr)
	require.Empty(t, checkpointStore.updates)
}

func TestProcessWatermarkDoesNotAdvanceWhenSessionWriteFails(t *testing.T) {
	// Setup
	expectedErr := errors.New("session start write failed")
	watermark := time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC)
	eventReader := &stubEventReader{events: []ing.VmEvent{
		{
			EventId:    "event-1",
			InstanceId: "instance-1",
			EventType:  InstanceStartEvent,
			OccurredAt: watermark.Add(-time.Minute),
		},
	}}
	checkpointStore := &stubCheckpointStore{}
	sessionizer := newFocusedSessionizer(
		eventReader,
		&stubSessionStore{createStartErr: expectedErr},
		checkpointStore,
	)

	// Execute
	_, err := sessionizer.processWatermark(
		context.Background(),
		WatermarkCheckpoint{ProcessorName: sessionizerProcessorName},
		ing.IngestionResult{Watermark: &watermark},
	)

	// Assert
	require.ErrorIs(t, err, expectedErr)
	require.Empty(t, checkpointStore.updates)
}

func TestProcessWatermarkReturnsCheckpointUpdateError(t *testing.T) {
	// Setup
	expectedErr := errors.New("checkpoint update failed")
	watermark := time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC)
	checkpointStore := &stubCheckpointStore{updateErr: expectedErr}
	sessionizer := newFocusedSessionizer(
		&stubEventReader{},
		&stubSessionStore{},
		checkpointStore,
	)

	// Execute
	_, err := sessionizer.processWatermark(
		context.Background(),
		WatermarkCheckpoint{ProcessorName: sessionizerProcessorName},
		ing.IngestionResult{Watermark: &watermark},
	)

	// Assert
	require.ErrorIs(t, err, expectedErr)
	require.Len(t, checkpointStore.updates, 1)
	require.Equal(t, watermark, checkpointStore.updates[0].ProcessedThrough)
}

func TestProcessWatermarkStoresStopWithoutStart(t *testing.T) {
	// Setup
	stopTime := time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC)
	watermark := stopTime.Add(time.Minute)
	eventReader := &stubEventReader{events: []ing.VmEvent{
		{
			EventId:    "event-1",
			InstanceId: "instance-1",
			EventType:  InstanceStopEvent,
			OccurredAt: stopTime,
		},
	}}
	sessionStore := &stubSessionStore{readStartErr: ErrNotFound}
	checkpointStore := &stubCheckpointStore{}
	sessionizer := newFocusedSessionizer(
		eventReader,
		sessionStore,
		checkpointStore,
	)

	// Execute
	updated, err := sessionizer.processWatermark(
		context.Background(),
		WatermarkCheckpoint{ProcessorName: sessionizerProcessorName},
		ing.IngestionResult{Watermark: &watermark},
	)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []SessionStop{
		{StopEventID: "event-1", InstanceID: "instance-1", StoppedAt: stopTime},
	}, sessionStore.createdStops)
	require.Empty(t, sessionStore.created)
	require.Equal(t, watermark, updated.ProcessedThrough)
	require.Len(t, checkpointStore.updates, 1)
}

func TestProcessWatermarkCreatesZeroDurationSession(t *testing.T) {
	// Setup
	eventTime := time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC)
	expectedSessionID := uuid.MustParse("01994c8e-7c00-7000-8000-000000000002")
	sessionStore := &stubSessionStore{
		readStart: SessionStart{
			StartEventID: "start-event",
			InstanceID:   "instance-1",
			StartedAt:    eventTime,
		},
	}
	watermark := eventTime.Add(time.Minute)
	checkpointStore := &stubCheckpointStore{}
	sessionizer := newFocusedSessionizer(
		&stubEventReader{events: []ing.VmEvent{
			{
				EventId:    "stop-event",
				InstanceId: "instance-1",
				EventType:  InstanceStopEvent,
				OccurredAt: eventTime,
			},
		}},
		sessionStore,
		checkpointStore,
	)
	sessionizer.newSessionID = func() (uuid.UUID, error) {
		return expectedSessionID, nil
	}

	// Execute
	_, err := sessionizer.processWatermark(
		context.Background(),
		WatermarkCheckpoint{ProcessorName: sessionizerProcessorName},
		ing.IngestionResult{Watermark: &watermark},
	)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []Session{
		{
			SessionID:    expectedSessionID,
			InstanceID:   "instance-1",
			StartedAt:    eventTime,
			StoppedAt:    eventTime,
			StartEventID: "start-event",
			StopEventID:  "stop-event",
		},
	}, sessionStore.created)
	require.Equal(t, []WatermarkCheckpoint{
		{
			ProcessorName:    sessionizerProcessorName,
			ProcessedThrough: watermark,
		},
	}, checkpointStore.updates)
}

func TestSessionizerConsumesPendingStartAfterCompletingSession(t *testing.T) {
	// Setup
	startedAt := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	createdStarts := make(chan SessionStart, 1)
	createdStops := make(chan SessionStop, 1)
	createdSessions := make(chan Session, 2)
	sessionStore := &FakeSessionStore{
		createdSessionStarts: createdStarts,
		createdSessionStops:  createdStops,
		createdSessions:      createdSessions,
	}
	sessionizer := newFocusedSessionizer(nil, sessionStore, nil)

	// Execute
	err := sessionizer.makeSessionForSingleVM(context.Background(), TxStores{SessionStore: sessionStore}, []ing.VmEvent{
		{
			EventId:    "start-event",
			InstanceId: "instance-1",
			EventType:  InstanceStartEvent,
			OccurredAt: startedAt,
		},
		{
			EventId:    "first-stop-event",
			InstanceId: "instance-1",
			EventType:  InstanceStopEvent,
			OccurredAt: startedAt.Add(time.Hour),
		},
		{
			EventId:    "second-stop-event",
			InstanceId: "instance-1",
			EventType:  InstanceStopEvent,
			OccurredAt: startedAt.Add(2 * time.Hour),
		},
	})

	// Assert
	require.NoError(t, err)
	require.Len(t, createdSessions, 1)
	require.Len(t, createdStops, 1)
	storedStop := <-createdStops
	require.Equal(t, "second-stop-event", storedStop.StopEventID)
}

func TestSessionizerCreatesZeroDurationSessionWhenStopArrivesBeforeStart(t *testing.T) {
	// Setup
	eventTime := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	createdStarts := make(chan SessionStart, 1)
	createdStops := make(chan SessionStop, 1)
	createdSessions := make(chan Session, 1)
	sessionStore := &FakeSessionStore{
		createdSessionStarts: createdStarts,
		createdSessionStops:  createdStops,
		createdSessions:      createdSessions,
	}
	sessionizer := newFocusedSessionizer(nil, sessionStore, nil)

	// Execute
	err := sessionizer.makeSessions(context.Background(), TxStores{SessionStore: sessionStore}, []ing.VmEvent{
		{
			EventId:    "stop-event",
			InstanceId: "instance-1",
			EventType:  InstanceStopEvent,
			OccurredAt: eventTime,
		},
		{
			EventId:    "start-event",
			InstanceId: "instance-1",
			EventType:  InstanceStartEvent,
			OccurredAt: eventTime,
		},
	})

	// Assert
	require.NoError(t, err)
	require.Len(t, createdSessions, 1)
	require.Empty(t, createdStops)
	created := <-createdSessions
	require.Equal(t, eventTime, created.StartedAt)
	require.Equal(t, eventTime, created.StoppedAt)
}

func TestSessionizerStopsIngestionWhenWatermarkProcessingFails(t *testing.T) {
	// Setup
	expectedErr := errors.New("read events")
	clientResults := make(chan ing.Result[ing.VmEvent], 1)
	listenerStopped := make(chan struct{})
	eventClient := &FakeEventClient{
		resultChan: clientResults,
		stopped:    listenerStopped,
	}
	eventStore := &FakeEventStore{appended: make(chan ing.VmEvent, 1)}
	ingestor := ing.NewIngestor(
		eventClient,
		eventStore,
		ing.IngestionConfig{},
		slog.Default(),
	)
	sessionizer := NewSessionizer(
		ingestor,
		&stubTransactor{stores: TxStores{
			EventReader:     &stubEventReader{err: expectedErr},
			SessionStore:    &stubSessionStore{},
			CheckpointStore: &stubCheckpointStore{fetchErr: ErrNotFound},
		}},
		slog.Default(),
	)
	clientResults <- ing.Result[ing.VmEvent]{Value: ing.VmEvent{
		EventId:    "event-1",
		OccurredAt: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
	}}

	// Execute
	err := sessionizer.Run(context.Background())

	// Assert
	require.ErrorIs(t, err, expectedErr)
	select {
	case <-listenerStopped:
	case <-time.After(time.Second):
		t.Fatal("ingestion listener did not stop")
	}
}
