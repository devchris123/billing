package event_client

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	conc "github.com/ghaering/core-api-task/concurrency"
	ing "github.com/ghaering/core-api-task/event_ingestion"
)

type Encoder[T any] interface {
	Decode(value []byte) (T, error)
	Encode(vmEvent T) ([]byte, error)
}

type KafkaClient[T any] struct {
	client  *kgo.Client
	topic   string
	encoder Encoder[T]
}

func NewKafkaClient[T any](
	brokers []string,
	group string,
	topic string,
	encoder Encoder[T],
) (*KafkaClient[T], error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaClient[T]{client: cl, topic: topic, encoder: encoder}, nil
}

func (kc *KafkaClient[T]) Close(ctx context.Context) {
	kc.client.Close()
}

func (kc *KafkaClient[T]) Listen(ctx context.Context) chan ing.Result[T] {
	listenerChan := make(chan ing.Result[T])

	go func() {
		defer close(listenerChan)

		for {
			fetches := kc.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					if !conc.Send(ctx, listenerChan, ing.Result[T]{Err: err.Err}) {
						return
					}
				}
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				value, err := kc.encoder.Decode(record.Value)
				if err != nil {
					if !conc.Send(ctx, listenerChan, ing.Result[T]{Err: err}) {
						return
					}
					continue
				}
				if !conc.Send(ctx, listenerChan, ing.Result[T]{Value: value}) {
					return
				}
			}
		}
	}()
	return listenerChan
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
