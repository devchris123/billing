package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseAndValidateUsesDefaults(t *testing.T) {
	// Setup
	setConfigEnvironment(t, "", "", "", "", "")
	t.Setenv("DB_URL", "postgres://localhost/test")

	// Execute
	conf, err := parseAndValidate(nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:9092"}, conf.kafkaBrokers)
	require.Equal(t, "billing.sessionizer", conf.kafkaGroup)
	require.Equal(t, "billing.vm.events", conf.kafkaTopic)
	require.Equal(t, "postgres://localhost/test", conf.databaseURL)
	require.Equal(t, 10*time.Minute, conf.maxOutOfOrderness)
}

func TestParseAndValidateUsesEnvironment(t *testing.T) {
	// Setup
	setConfigEnvironment(
		t,
		" kafka-1:9092, kafka-2:9092 ",
		"environment-group",
		"environment-topic",
		"postgres://environment/database",
		"45s",
	)

	// Execute
	conf, err := parseAndValidate(nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, conf.kafkaBrokers)
	require.Equal(t, "environment-group", conf.kafkaGroup)
	require.Equal(t, "environment-topic", conf.kafkaTopic)
	require.Equal(t, "postgres://environment/database", conf.databaseURL)
	require.Equal(t, 45*time.Second, conf.maxOutOfOrderness)
}

func TestParseAndValidateFlagsOverrideEnvironment(t *testing.T) {
	// Setup
	setConfigEnvironment(
		t,
		"environment-kafka:9092",
		"environment-group",
		"environment-topic",
		"postgres://environment/database",
		"1m",
	)
	args := []string{
		"--kafka-brokers", " flag-kafka-1:9092, flag-kafka-2:9092 ",
		"--kafka-group", "flag-group",
		"--kafka-topic", "flag-topic",
		"--db-url", "postgres://flag/database",
		"--max-out-of-orderness", "2m30s",
	}

	// Execute
	conf, err := parseAndValidate(args)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"flag-kafka-1:9092", "flag-kafka-2:9092"}, conf.kafkaBrokers)
	require.Equal(t, "flag-group", conf.kafkaGroup)
	require.Equal(t, "flag-topic", conf.kafkaTopic)
	require.Equal(t, "postgres://flag/database", conf.databaseURL)
	require.Equal(t, 2*time.Minute+30*time.Second, conf.maxOutOfOrderness)
}

func TestParseAndValidateRejectsInvalidEnvironmentDuration(t *testing.T) {
	// Setup
	setConfigEnvironment(t, "", "", "", "postgres://localhost/test", "not-a-duration")

	// Execute
	_, err := parseAndValidate(nil)

	// Assert
	require.ErrorContains(t, err, "invalid MAX_OUT_OF_ORDERNESS value")
}

func TestParseAndValidateRejectsInvalidFlagDuration(t *testing.T) {
	// Setup
	setConfigEnvironment(t, "", "", "", "postgres://localhost/test", "")

	// Execute
	_, err := parseAndValidate([]string{"--max-out-of-orderness", "not-a-duration"})

	// Assert
	require.ErrorContains(t, err, "parse flags")
}

func TestParseAndValidateReportsAllValidationErrors(t *testing.T) {
	// Setup
	setConfigEnvironment(t, "", "", "", "", "")
	args := []string{
		"--db-url", " ",
		"--kafka-brokers", ",,",
		"--kafka-group", " ",
		"--kafka-topic", " ",
		"--max-out-of-orderness", "-1m",
	}

	// Execute
	_, err := parseAndValidate(args)

	// Assert
	require.ErrorContains(t, err, "database url must be provided")
	require.ErrorContains(t, err, "kafka broker at position 1 must not be empty")
	require.ErrorContains(t, err, "kafka broker at position 2 must not be empty")
	require.ErrorContains(t, err, "kafka broker at position 3 must not be empty")
	require.ErrorContains(t, err, "kafka group must be provided")
	require.ErrorContains(t, err, "kafka topic must be provided")
	require.ErrorContains(t, err, "max out of orderness must be non-negative")
}

func setConfigEnvironment(
	t *testing.T,
	brokers string,
	group string,
	topic string,
	databaseURL string,
	maxOutOfOrderness string,
) {
	t.Helper()
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("KAFKA_GROUP", group)
	t.Setenv("KAFKA_TOPIC", topic)
	t.Setenv("DB_URL", databaseURL)
	t.Setenv("MAX_OUT_OF_ORDERNESS", maxOutOfOrderness)
}
