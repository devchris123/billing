package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

type AppConfig struct {
	kafkaBrokers      []string
	kafkaGroup        string
	kafkaTopic        string
	databaseURL       string
	maxOutOfOrderness time.Duration
}

func parseAndValidate(args []string) (AppConfig, error) {
	maxOutOfOrderness, err := envOrDefaultDur("MAX_OUT_OF_ORDERNESS", 10*time.Minute)
	if err != nil {
		return AppConfig{}, fmt.Errorf("invalid MAX_OUT_OF_ORDERNESS value: %w", err)
	}
	appConf := AppConfig{
		kafkaGroup:        envOrDefault("KAFKA_GROUP", "billing.sessionizer"),
		kafkaTopic:        envOrDefault("KAFKA_TOPIC", "billing.vm.events"),
		databaseURL:       envOrDefault("DB_URL", ""),
		maxOutOfOrderness: maxOutOfOrderness,
	}

	brokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	flagSet := createFlagSet(&appConf, &brokers)

	if err := flagSet.Parse(args); err != nil {
		return AppConfig{}, fmt.Errorf("parse flags: %w", err)
	}

	appConf.kafkaBrokers = strings.Split(brokers, ",")

	normalizeConfig(&appConf)
	if err := validateConfig(appConf); err != nil {
		return AppConfig{}, fmt.Errorf("validate config: %w", err)
	}

	return appConf, nil
}

func createFlagSet(conf *AppConfig, brokers *string) *flag.FlagSet {
	flags := flag.NewFlagSet("core-api-task", flag.ContinueOnError)

	flags.StringVar(brokers, "kafka-brokers", *brokers, "comma separated list of kafka brokers in host:port format")
	flags.StringVar(&conf.kafkaGroup, "kafka-group", conf.kafkaGroup, "logical subscriber group for kafka events. Partitions will be distributed among subscribers in the same group")
	flags.StringVar(&conf.kafkaTopic, "kafka-topic", conf.kafkaTopic, "kafka topic for events")
	flags.StringVar(&conf.databaseURL, "db-url", conf.databaseURL, "database url")
	flags.DurationVar(&conf.maxOutOfOrderness, "max-out-of-orderness", conf.maxOutOfOrderness, "maximum out of orderness for event ingestion")

	return flags
}

func normalizeConfig(conf *AppConfig) {
	conf.databaseURL = strings.TrimSpace(conf.databaseURL)
	conf.kafkaGroup = strings.TrimSpace(conf.kafkaGroup)
	conf.kafkaTopic = strings.TrimSpace(conf.kafkaTopic)

	for i := range conf.kafkaBrokers {
		conf.kafkaBrokers[i] = strings.TrimSpace(conf.kafkaBrokers[i])
	}
}

func validateConfig(conf AppConfig) error {
	var validationErrors []error

	if conf.databaseURL == "" {
		validationErrors = append(validationErrors, errors.New("database url must be provided. Provide a value via --db-url flag or DB_URL environment variable"))
	}

	if len(conf.kafkaBrokers) == 0 {
		validationErrors = append(validationErrors, errors.New("kafka brokers must be provided. Provide a value via --kafka-brokers flag or KAFKA_BROKERS environment variable"))
	}

	for i := range conf.kafkaBrokers {
		if conf.kafkaBrokers[i] == "" {
			validationErrors = append(
				validationErrors,
				fmt.Errorf("kafka broker at position %d must not be empty", i+1),
			)
		}
	}

	if conf.kafkaGroup == "" {
		validationErrors = append(validationErrors, errors.New("kafka group must be provided. Provide a value via --kafka-group flag or KAFKA_GROUP environment variable"))
	}

	if conf.kafkaTopic == "" {
		validationErrors = append(validationErrors, errors.New("kafka topic must be provided. Provide a value via --kafka-topic flag or KAFKA_TOPIC environment variable"))
	}

	if conf.maxOutOfOrderness < 0 {
		validationErrors = append(validationErrors, errors.New("max out of orderness must be non-negative. Provide a value via --max-out-of-orderness flag"))
	}

	return errors.Join(validationErrors...)
}

func envOrDefaultDur(envVar string, defaultDur time.Duration) (time.Duration, error) {
	envDur := os.Getenv(envVar)
	if envDur == "" {
		return defaultDur, nil
	}
	parsedDur, err := time.ParseDuration(envDur)
	if err != nil {
		return 0, fmt.Errorf("invalid duration for %s: %w", envVar, err)
	}
	return parsedDur, nil
}

func envOrDefault(envVar string, defaultVal string) string {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		return defaultVal
	}
	return envVal
}
