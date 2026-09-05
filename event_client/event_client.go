package event_client

import (
	"context"
	"encoding/json"

	conc "github.com/ghaering/core-api-task/concurrency"
	event "github.com/ghaering/core-api-task/event_ingestion"
)

type Client interface {
	close()
	listen(ctx context.Context) (<-chan []byte, <-chan error)
	send(ctx context.Context, value []byte) chan error
}

type EventClient struct {
	kc Client
}

func NewEventClient(kc Client) *EventClient {
	return &EventClient{kc}
}

func (ec *EventClient) Close(ctx context.Context) {
	ec.kc.close()
}

func (ec *EventClient) Listen(ctx context.Context) (<-chan event.VmEvent, <-chan error) {
	vmListenChan := make(chan event.VmEvent)
	vmErrChan := make(chan error)

	listenChan, errChan := ec.kc.listen(ctx)

	go func() {
		defer close(vmListenChan)
		defer close(vmErrChan)

		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-listenChan:
				if !ok {
					return
				}
				vmEvent := &event.VmEvent{}
				err := json.Unmarshal(value, vmEvent)
				if err != nil {
					if !conc.Send(ctx, vmErrChan, err) {
						return
					}
				} else {
					if !conc.Send(ctx, vmListenChan, *vmEvent) {
						return
					}
				}
			case err, ok := <-errChan:
				if !ok {
					return
				}
				if !conc.Send(ctx, vmErrChan, err) {
					return
				}
			}
		}
	}()

	return vmListenChan, vmErrChan
}

func (ec *EventClient) Send(ctx context.Context, vmEvent event.VmEvent) (chan error, error) {
	bytes, err := json.Marshal(vmEvent)
	if err != nil {
		return nil, err
	}

	return ec.kc.send(ctx, bytes), nil
}
