package integration_tests

import (
	"context"
	"log"
	"path/filepath"
	"testing"

	event_db "github.com/ghaering/core-api-task/event_db"
	event "github.com/ghaering/core-api-task/event_ingestion"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestEventDb(t *testing.T) {
	ctx := context.Background()
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"
	postgresC, errC := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithInitScripts(filepath.Join("../migrations", "001_init.up.sql")),
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.BasicWaitStrategies(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(postgresC); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	require.NoError(t, errC)

	connStr, err := postgresC.ConnectionString(ctx, "sslmode=disable", "application_name=test")
	require.NoError(t, err)

	eventDb, err := event_db.NewEventDB(connStr)
	require.NoError(t, err)

	event := event.VmEvent{EventId: "id1"}
	err = eventDb.Append(ctx, event)
	require.NoError(t, err)
}
