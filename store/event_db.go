package store

import (
	"context"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	event "github.com/ghaering/core-api-task/event_ingestion"
)

type EventDB struct {
	db DBTX
}

func NewEventDB(db DBTX) *EventDB {
	return &EventDB{db: db}
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

func (edb *EventDB) Append(ctx context.Context, vmEvent event.VmEvent) error {
	_, err := edb.db.ExecContext(ctx, insertEventQuery,
		vmEvent.EventId,
		vmEvent.OccurredAt,
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

const eventsBetweenQuery = `
		SELECT event_id, occurred_at, project_id, instance_id, event_type, flavour
		FROM vm_events
		WHERE occurred_at > $1 AND occurred_at <= $2
`

func (edb *EventDB) EventsBetween(ctx context.Context, start time.Time, end time.Time) (events []event.VmEvent, err error) {
	rows, err := edb.db.QueryContext(ctx, eventsBetweenQuery, start, end)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()

	for rows.Next() {
		var vmEvent event.VmEvent
		if err := rows.Scan(
			&vmEvent.EventId,
			&vmEvent.OccurredAt,
			&vmEvent.ProjectId,
			&vmEvent.InstanceId,
			&vmEvent.EventType,
			&vmEvent.Flavour,
		); err != nil {
			return nil, err
		}
		events = append(events, vmEvent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
