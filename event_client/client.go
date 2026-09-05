package event_client

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient struct {
	client *kgo.Client
	topic  string
}

func NewKafkaClient(
	brokers []string,
	group string,
	topic string,
) (*KafkaClient, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaClient{client: cl, topic: topic}, nil
}

func (kc KafkaClient) close() {
	kc.client.Close()
}

func (kc KafkaClient) listen(ctx context.Context) (<-chan []byte, <-chan error) {
	listenerChan := make(chan []byte)
	errChan := make(chan error)
	go func() {
		defer close(listenerChan)
		defer close(errChan)

		for {
			fetches := kc.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					if !send(ctx, errChan, err.Err) {
						return
					}
				}
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if !send(ctx, listenerChan, record.Value) {
					return
				}
			}
		}
	}()
	return listenerChan, errChan
}

func (kc KafkaClient) send(ctx context.Context, value []byte) chan error {
	errChan := make(chan error)
	record := &kgo.Record{Topic: kc.topic, Value: value}
	kc.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		defer close(errChan)
		if err != nil {
			errChan <- err
		}
	})
	return errChan
}
