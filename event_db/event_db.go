package eventdb

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"

	event "github.com/ghaering/core-api-task/event_ingestion"
)

type EventDb struct {
	db *sql.DB
}

func NewEventDB(databaseURL string) (*EventDb, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	return &EventDb{db: db}, nil
}

const insertEventQuery = `
		INSERT INTO vm_events (
			event_id, 
			occurred_at, 
			project_id, 
			instance_id, 
			event_type, 
			flavour
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING
`

func (edb *EventDb) Append(ctx context.Context, vmEvent event.VmEvent) error {
	_, err := edb.db.ExecContext(ctx, insertEventQuery,
		vmEvent.EventId,
		vmEvent.Occurred_at,
		vmEvent.ProjectId,
		vmEvent.InstanceId,
		vmEvent.EventType,
		vmEvent.Flavour,
	)
	if err != nil {
		return err
	}
	return nil
}
