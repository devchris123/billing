package event_client

import (
	"context"
	"encoding/json"
	"time"
)

type Client interface {
	close()
	listen(ctx context.Context) (<-chan []byte, <-chan error)
	send(ctx context.Context, value []byte) chan error
}

type EventClient struct {
	kc Client
}

type VmEvent struct {
	Event_id    string
	Flavour     string
	Instance_id string
	Occurred_at time.Time
	Project_id  string
	Event_type  string
}

func NewEventClient(kc Client) *EventClient {
	return &EventClient{kc}
}

func (ec *EventClient) Listen(ctx context.Context) (<-chan VmEvent, <-chan error) {
	vmListenChan := make(chan VmEvent)
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
				vmEvent := &VmEvent{}
				err := json.Unmarshal(value, vmEvent)
				if err != nil {
					if !send(ctx, vmErrChan, err) {
						return
					}
				} else {
					if !send(ctx, vmListenChan, *vmEvent) {
						return
					}
				}
			case err, ok := <-errChan:
				if !ok {
					return
				}
				if !send(ctx, vmErrChan, err) {
					return
				}
			}
		}
	}()

	return vmListenChan, vmErrChan
}

func (ec *EventClient) Send(ctx context.Context, vmEvent VmEvent) (chan error, error) {
	bytes, err := json.Marshal(vmEvent)
	if err != nil {
		return nil, err
	}

	return ec.kc.send(ctx, bytes), nil
}
