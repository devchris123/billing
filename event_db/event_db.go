package eventdb

import (
	"context"

	event "github.com/ghaering/core-api-task/event_ingestion"
)

type EventDb struct {
}

func NewEventDB() *EventDb {
	return &EventDb{}
}

func (edb *EventDb) Append(ctx context.Context, vmEvent event.VmEvent) error {
	return nil
}
