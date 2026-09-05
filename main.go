package main

import (
	"context"
	"log/slog"

	ec "github.com/ghaering/core-api-task/event_client"
	edb "github.com/ghaering/core-api-task/event_db"
	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/ghaering/core-api-task/session"
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
	edb := edb.NewEventDB()
	sessionizer := session.NewSessionizer(ing.NewIngestor(
		ec,
		edb,
		ing.IngestionConfig{MaxOutOfOrderness: 0},
		slog.Default(),
	))
	slog.InfoContext(ctx, "built sessionizer", slog.Any("sessionizer", sessionizer))
}
