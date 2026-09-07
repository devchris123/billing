package event_client

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/twmb/franz-go/pkg/kgo"

	ing "github.com/ghaering/core-api-task/event_ingestion"
)

const defaultMaxHandlerRetryElapsedTime = 2 * time.Minute

type Encoder[T any] interface {
	Decode(value []byte) (T, error)
	Encode(value T) ([]byte, error)
}

// RetryFunc retries an operation until it succeeds or returns a terminal error.
type RetryFunc func(ctx context.Context, operation func() error) error

type KafkaClient[T any] struct {
	client  *kgo.Client
	topic   string
	encoder Encoder[T]
	retry   RetryFunc
}

func NewKafkaClient[T any](
	brokers []string,
	group string,
	topic string,
	encoder Encoder[T],
	retry RetryFunc,
) (*KafkaClient[T], error) {
	if encoder == nil {
		return nil, fmt.Errorf("encoder must not be nil")
	}
	if retry == nil {
		return nil, fmt.Errorf("retry function must not be nil")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return &KafkaClient[T]{
		client:  cl,
		topic:   topic,
		encoder: encoder,
		retry:   retry,
	}, nil
}

func (kc *KafkaClient[T]) Close() {
	kc.client.Close()
}

// Listen consumes records serially until the context is cancelled or a
// terminal Kafka or retry error occurs. A nil handler result acknowledges the
// record; a non-nil result is retried according to the configured policy.
func (kc *KafkaClient[T]) Listen(ctx context.Context, handler func(ing.Result[T]) error) error {
	if handler == nil {
		return fmt.Errorf("handler must not be nil")
	}
	for {
		fetches := kc.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch Kafka records: %w", errs[0].Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			value, err := kc.encoder.Decode(record.Value)
			if err != nil {
				_ = handler(ing.Result[T]{Err: err})
				// Skip poison records until dead-letter handling is available.
				// Committing here deliberately prevents endless redelivery.
				if err := kc.client.CommitRecords(ctx, record); err != nil {
					return fmt.Errorf("commit Kafka record after decode error: %w", err)
				}
				continue
			}
			if err := kc.retry(ctx, func() error {
				return handler(ing.Result[T]{Value: value})
			}); err != nil {
				return fmt.Errorf(
					"process Kafka record %s[%d] at offset %d: %w",
					record.Topic,
					record.Partition,
					record.Offset,
					err,
				)
			}
			if err := kc.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf(
					"commit Kafka record %s[%d] at offset %d: %w",
					record.Topic,
					record.Partition,
					record.Offset,
					err,
				)
			}
		}
	}
}

// RetryWithExponentialBackoff retries transient processing failures for a
// bounded period so a consumer does not hold fetched records indefinitely.
func RetryWithExponentialBackoff(
	ctx context.Context,
	operation func() error,
) error {
	_, err := backoff.Retry(
		ctx,
		func() (struct{}, error) {
			return struct{}{}, operation()
		},
		backoff.WithBackOff(backoff.NewExponentialBackOff()),
		backoff.WithMaxElapsedTime(defaultMaxHandlerRetryElapsedTime),
	)
	return err
}

func (kc *KafkaClient[T]) Send(ctx context.Context, value T) <-chan error {
	errChan := make(chan error, 1)
	encodedValue, err := kc.encoder.Encode(value)
	if err != nil {
		errChan <- err
		close(errChan)
		return errChan
	}
	record := &kgo.Record{Topic: kc.topic, Value: encodedValue}
	kc.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		defer close(errChan)
		if err != nil {
			errChan <- err
		}
	})
	return errChan
}
