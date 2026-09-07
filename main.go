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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventClient, err := ec.NewKafkaClient(
		[]string{},
		"",
		"",
		&ec.VmEventJsonEncoder{},
		ec.RetryWithExponentialBackoff,
	)
	if err != nil {
		slog.ErrorContext(ctx, "create kafka client", slog.Any("error", err))
		return
	}
	defer eventClient.Close()
	db, err := store.Open("")
	if err != nil {
		slog.ErrorContext(ctx, "open database", slog.Any("error", err))
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.ErrorContext(ctx, "close database", slog.Any("error", err))
		}
	}()
	eventDB := store.NewEventDB(db)
	transactor := store.NewPostgresTransactor(db)
	sessionizer := session.NewSessionizer(ing.NewIngestor(
		eventClient,
		eventDB,
		ing.IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	),
		transactor,
		slog.Default(),
	)
	slog.InfoContext(ctx, "built sessionizer", slog.Any("sessionizer", sessionizer))
	if err := sessionizer.Run(ctx); err != nil {
		slog.ErrorContext(ctx, "run sessionizer", slog.Any("error", err))
		return
	}
}
