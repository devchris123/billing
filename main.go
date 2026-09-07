package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	ec "github.com/ghaering/core-api-task/event_client"
	ing "github.com/ghaering/core-api-task/event_ingestion"
	"github.com/ghaering/core-api-task/session"
	"github.com/ghaering/core-api-task/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log := slog.With(slog.String("component", "main"))
		log.Error("run", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(args []string) error {
	conf, err := parseAndValidate(args)
	if err != nil {
		return fmt.Errorf("parse and validate config: %w", err)
	}

	mainLogger := slog.With(slog.String("component", "main"))

	// Setup signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Kafka setup
	eventClient, err := ec.NewKafkaClient(
		conf.kafkaBrokers,
		conf.kafkaGroup,
		conf.kafkaTopic,
		&ec.VmEventJsonEncoder{},
		ec.RetryWithExponentialBackoff,
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer eventClient.Close()

	// Database setup
	db, err := store.Open(conf.databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			mainLogger.Error("close database", slog.Any("error", err))
		}
	}()
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	err = db.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	// Build Ingestor
	eventDB := store.NewEventDB(db)
	ingestionLogger := slog.With(slog.String("component", "ingestor"))
	ingestionConfig := ing.IngestionConfig{MaxOutOfOrderness: conf.maxOutOfOrderness}
	ingestor := ing.NewIngestor(
		eventClient,
		eventDB,
		ingestionConfig,
		ingestionLogger,
	)

	// Build Sessionizer
	transactor := store.NewPostgresTransactor(db)
	sessionLogger := slog.With(slog.String("component", "sessionizer"))
	sessionizer := session.NewSessionizer(
		ingestor,
		transactor,
		sessionLogger,
	)

	// Run Sessionizer
	mainLogger.Info(
		"starting sessionizer",
		slog.String("kafka_topic", conf.kafkaTopic),
		slog.String("kafka_group", conf.kafkaGroup),
		slog.Duration("max_out_of_orderness", conf.maxOutOfOrderness),
	)
	if err := sessionizer.Run(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		return fmt.Errorf("run sessionizer: %w", err)
	}
	return nil
}
