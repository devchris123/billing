package store

import (
	"context"
	"database/sql"

	session "github.com/ghaering/core-api-task/session"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type CheckpointDB struct {
	db *sql.DB
}

func NewCheckpointDB(databaseURL string) (*CheckpointDB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	return &CheckpointDB{db: db}, nil
}

const updateWatermarkCheckpointQuery = `
	INSERT INTO watermark_checkpoints (
		processor_name, 
		processed_through
	)
	VALUES ($1, $2)
	ON CONFLICT (processor_name) 
	DO UPDATE SET 
		processed_through = EXCLUDED.processed_through
	WHERE EXCLUDED.processed_through > 
		watermark_checkpoints.processed_through
`

func (c *CheckpointDB) UpdateWatermarkCheckpoint(ctx context.Context, checkpoint session.WatermarkCheckpoint) error {
	_, err := c.db.ExecContext(ctx, updateWatermarkCheckpointQuery, checkpoint.ProcessorName, checkpoint.ProcessedThrough)
	return err
}

const readWatermarkCheckpointQuery = `
	SELECT processor_name, processed_through
		FROM watermark_checkpoints
		WHERE processor_name = $1;
`

func (c *CheckpointDB) ReadWatermarkCheckpoint(ctx context.Context, processorName string) (session.WatermarkCheckpoint, error) {
	var checkpoint session.WatermarkCheckpoint
	err := c.db.QueryRowContext(ctx, readWatermarkCheckpointQuery, processorName).
		Scan(&checkpoint.ProcessorName, &checkpoint.ProcessedThrough)
	if err != nil {
		if err == sql.ErrNoRows {
			return session.WatermarkCheckpoint{}, session.ErrNotFound
		}
		return session.WatermarkCheckpoint{}, err
	}
	return checkpoint, nil
}
