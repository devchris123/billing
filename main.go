package main

import (
	"context"
	"log/slog"

	ec "github.com/ghaering/core-api-task/event_client"
	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/ghaering/core-api-task/session"
	"github.com/ghaering/core-api-task/store"
)

func main() {
	// Streaming setup
	ctx := context.Background()
	kc, err := ec.NewKafkaClient(
		[]string{},
		"",
		"",
	)
	if err != nil {
		slog.ErrorContext(ctx, "create kafka client", slog.Any("error", err))
		return
	}
	ec := ec.NewEventClient(kc)
	eventDB, err := store.NewEventDB("")
	if err != nil {
		slog.ErrorContext(ctx, "create event db", slog.Any("error", err))
		return
	}
	sessionDB, err := store.NewSessionDB("")
	if err != nil {
		slog.ErrorContext(ctx, "create session db", slog.Any("error", err))
		return
	}
	checkpointDB, err := store.NewCheckpointDB("")
	if err != nil {
		slog.ErrorContext(ctx, "create checkpoint db", slog.Any("error", err))
		return
	}
	sessionizer := session.NewSessionizer(ing.NewIngestor(
		ec,
		eventDB,
		ing.IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	),
		eventDB,
		sessionDB,
		checkpointDB,
		slog.Default(),
	)
	slog.InfoContext(ctx, "built sessionizer", slog.Any("sessionizer", sessionizer))
	if err := sessionizer.Run(ctx); err != nil {
		slog.ErrorContext(ctx, "run sessionizer", slog.Any("error", err))
		return
	}
}
