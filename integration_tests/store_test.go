//go:build integration

package integration_tests

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"testing"
	"time"

	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/ghaering/core-api-task/session"
	"github.com/ghaering/core-api-task/store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresStores(t *testing.T) {
	// Setup
	ctx := context.Background()
	postgresC, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("../migrations", "001_init.up.sql")),
		postgres.WithDatabase("events"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(postgresC); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	connectionString, err := postgresC.ConnectionString(
		ctx,
		"sslmode=disable",
		"application_name=test",
	)
	require.NoError(t, err)

	db, err := store.Open(connectionString)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	eventStore := store.NewEventDB(db)
	sessionStore := store.NewSessionDB(db)
	checkpointStore := store.NewCheckpointDB(db)
	transactor := store.NewPostgresTransactor(db)

	t.Run("transactor commits work", func(t *testing.T) {
		// Setup
		start := session.SessionStart{
			StartEventID: "committed-start",
			InstanceID:   "committed-instance",
			StartedAt:    time.Date(2025, 1, 1, 8, 0, 0, 0, time.UTC),
		}

		// Execute
		err := transactor.Run(ctx, func(stores session.TxStores) error {
			return stores.SessionStore.CreateSessionStart(ctx, start)
		})

		// Assert
		require.NoError(t, err)
		actual, err := sessionStore.ReadSessionStart(
			ctx,
			start.InstanceID,
			start.StartedAt.Add(time.Hour),
		)
		require.NoError(t, err)
		require.Equal(t, start.StartEventID, actual.StartEventID)
	})

	t.Run("transactor rolls back all stores", func(t *testing.T) {
		// Setup
		expectedErr := errors.New("work failed")
		start := session.SessionStart{
			StartEventID: "rolled-back-start",
			InstanceID:   "rolled-back-instance",
			StartedAt:    time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC),
		}
		checkpoint := session.WatermarkCheckpoint{
			ProcessorName:    "rolled-back-processor",
			ProcessedThrough: start.StartedAt,
		}

		// Execute
		err := transactor.Run(ctx, func(stores session.TxStores) error {
			if err := stores.SessionStore.CreateSessionStart(ctx, start); err != nil {
				return err
			}
			if err := stores.CheckpointStore.UpdateWatermarkCheckpoint(ctx, checkpoint); err != nil {
				return err
			}
			return expectedErr
		})

		// Assert
		require.ErrorIs(t, err, expectedErr)
		_, err = sessionStore.ReadSessionStart(
			ctx,
			start.InstanceID,
			start.StartedAt.Add(time.Hour),
		)
		require.ErrorIs(t, err, session.ErrNotFound)
		_, err = checkpointStore.ReadWatermarkCheckpoint(ctx, checkpoint.ProcessorName)
		require.ErrorIs(t, err, session.ErrNotFound)
	})

	t.Run("event range excludes previous checkpoint", func(t *testing.T) {
		// Setup
		start := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
		end := start.Add(time.Hour)
		for _, event := range []ing.VmEvent{
			{EventId: "range-start", OccurredAt: start},
			{EventId: "range-middle", OccurredAt: start.Add(30 * time.Minute)},
			{EventId: "range-end", OccurredAt: end},
		} {
			require.NoError(t, eventStore.Append(ctx, event))
		}

		// Execute
		events, err := eventStore.EventsBetween(ctx, start, end)

		// Assert
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"range-middle", "range-end"}, eventIDs(events))
	})

	t.Run("start selection excludes starts after stop", func(t *testing.T) {
		// Setup
		instanceID := "time-bounded-instance"
		stopTime := time.Date(2025, 1, 3, 11, 0, 0, 0, time.UTC)
		expected := session.SessionStart{
			StartEventID: "compatible-start",
			InstanceID:   instanceID,
			StartedAt:    stopTime.Add(-time.Hour),
		}
		future := session.SessionStart{
			StartEventID: "future-start",
			InstanceID:   instanceID,
			StartedAt:    stopTime.Add(time.Hour),
		}
		require.NoError(t, sessionStore.CreateSessionStart(ctx, expected))
		require.NoError(t, sessionStore.CreateSessionStart(ctx, future))

		// Execute
		actual, err := sessionStore.ReadSessionStart(ctx, instanceID, stopTime)

		// Assert
		require.NoError(t, err)
		require.Equal(t, expected.StartEventID, actual.StartEventID)
		require.Equal(t, expected.InstanceID, actual.InstanceID)
		require.True(t, expected.StartedAt.Equal(actual.StartedAt))
	})

	t.Run("processed start is excluded from matching", func(t *testing.T) {
		// Setup
		startedAt := time.Date(2025, 1, 3, 14, 0, 0, 0, time.UTC)
		start := session.SessionStart{
			StartEventID: "processed-start",
			InstanceID:   "processed-instance",
			StartedAt:    startedAt,
		}
		require.NoError(t, sessionStore.CreateSessionStart(ctx, start))

		// Execute
		err := sessionStore.MarkSessionStartProcessed(ctx, start.StartEventID)

		// Assert
		require.NoError(t, err)
		_, err = sessionStore.ReadSessionStart(ctx, start.InstanceID, startedAt.Add(time.Hour))
		require.ErrorIs(t, err, session.ErrNotFound)
	})

	t.Run("marking one start leaves other pending starts", func(t *testing.T) {
		// Setup
		instanceID := "multiple-pending-instance"
		older := session.SessionStart{
			StartEventID: "older-pending-start",
			InstanceID:   instanceID,
			StartedAt:    time.Date(2025, 1, 3, 15, 0, 0, 0, time.UTC),
		}
		newer := session.SessionStart{
			StartEventID: "newer-pending-start",
			InstanceID:   instanceID,
			StartedAt:    older.StartedAt.Add(time.Hour),
		}
		require.NoError(t, sessionStore.CreateSessionStart(ctx, older))
		require.NoError(t, sessionStore.CreateSessionStart(ctx, newer))
		require.NoError(t, sessionStore.MarkSessionStartProcessed(ctx, newer.StartEventID))

		// Execute
		actual, err := sessionStore.ReadSessionStart(
			ctx,
			instanceID,
			newer.StartedAt.Add(time.Hour),
		)

		// Assert
		require.NoError(t, err)
		require.Equal(t, older.StartEventID, actual.StartEventID)
	})

	t.Run("conflicting session is reported", func(t *testing.T) {
		// Setup
		startedAt := time.Date(2025, 1, 4, 10, 0, 0, 0, time.UTC)
		first := session.Session{
			SessionID:    uuid.New(),
			InstanceID:   "conflict-instance",
			StartedAt:    startedAt,
			StoppedAt:    startedAt.Add(time.Hour),
			StartEventID: "conflict-start",
			StopEventID:  "conflict-stop-1",
		}
		require.NoError(t, sessionStore.CreateSession(ctx, first))
		conflicting := first
		conflicting.SessionID = uuid.New()
		conflicting.StopEventID = "conflict-stop-2"

		// Execute
		err := sessionStore.CreateSession(ctx, conflicting)

		// Assert
		require.ErrorIs(t, err, session.ErrSessionConflict)
	})

	t.Run("exact session retry is accepted", func(t *testing.T) {
		// Setup
		startedAt := time.Date(2025, 1, 4, 11, 0, 0, 0, time.UTC)
		original := session.Session{
			SessionID:    uuid.New(),
			InstanceID:   "retry-instance",
			StartedAt:    startedAt,
			StoppedAt:    startedAt.Add(time.Hour),
			StartEventID: "retry-start",
			StopEventID:  "retry-stop",
		}
		require.NoError(t, sessionStore.CreateSession(ctx, original))

		// Execute
		err := sessionStore.CreateSession(ctx, original)

		// Assert
		require.NoError(t, err)
	})

	t.Run("zero-duration session is accepted", func(t *testing.T) {
		// Setup
		eventTime := time.Date(2025, 1, 4, 12, 0, 0, 0, time.UTC)
		zeroDurationSession := session.Session{
			SessionID:    uuid.New(),
			InstanceID:   "zero-duration-instance",
			StartedAt:    eventTime,
			StoppedAt:    eventTime,
			StartEventID: "zero-duration-start",
			StopEventID:  "zero-duration-stop",
		}

		// Execute
		err := sessionStore.CreateSession(ctx, zeroDurationSession)

		// Assert
		require.NoError(t, err)
	})

	t.Run("checkpoint never moves backwards", func(t *testing.T) {
		// Setup
		latest := session.WatermarkCheckpoint{
			ProcessorName:    "monotonic-test",
			ProcessedThrough: time.Date(2025, 1, 5, 12, 0, 0, 0, time.UTC),
		}
		require.NoError(t, checkpointStore.UpdateWatermarkCheckpoint(ctx, latest))

		// Execute
		err := checkpointStore.UpdateWatermarkCheckpoint(ctx, session.WatermarkCheckpoint{
			ProcessorName:    latest.ProcessorName,
			ProcessedThrough: latest.ProcessedThrough.Add(-time.Hour),
		})

		// Assert
		require.NoError(t, err)
		actual, err := checkpointStore.ReadWatermarkCheckpoint(ctx, latest.ProcessorName)
		require.NoError(t, err)
		require.Equal(t, latest.ProcessorName, actual.ProcessorName)
		require.True(t, latest.ProcessedThrough.Equal(actual.ProcessedThrough))
	})
}

func eventIDs(events []ing.VmEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.EventId)
	}
	return ids
}
