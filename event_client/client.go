package event_client

import (
	"context"
	"fmt"

	"github.com/cenkalti/backoff/v7"
	"github.com/twmb/franz-go/pkg/kgo"

	ing "github.com/ghaering/core-api-task/event_ingestion"
)

type Encoder[T any] interface {
	Decode(value []byte) (T, error)
	Encode(vmEvent T) ([]byte, error)
}

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
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaClient[T]{
		client:  cl,
		topic:   topic,
		encoder: encoder,
		retry:   retry,
	}, nil
}

func (kc *KafkaClient[T]) Close(ctx context.Context) {
	kc.client.Close()
}

func (kc *KafkaClient[T]) Listen(ctx context.Context, handler func(ing.Result[T]) error) error {
	for {
		fetches := kc.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				_ = handler(ing.Result[T]{Err: e.Err})
			}
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			value, err := kc.encoder.Decode(record.Value)
			if err != nil {
				_ = handler(ing.Result[T]{Err: err})
				if err := kc.client.CommitRecords(ctx, record); err != nil {
					return wrapError(err, "commit kafka record after decode error")
				}
				continue
			}
			if err := kc.retry(ctx, func() error {
				return handler(ing.Result[T]{Value: value})
			}); err != nil {
				return wrapError(err, "process kafka record")
			}
			if err := kc.client.CommitRecords(ctx, record); err != nil {
				return wrapError(err, "commit kafka record")
			}
		}
	}
}

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
		backoff.WithMaxElapsedTime(0),
	)
	return err
}

func (kc *KafkaClient[T]) Send(ctx context.Context, value T) chan error {
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

func wrapError(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}
