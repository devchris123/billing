package store

import (
	"context"
	"database/sql"
	"errors"

	session "github.com/ghaering/core-api-task/session"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DBTX interface {
	ExecContext(
		context.Context,
		string,
		...any,
	) (sql.Result, error)

	QueryContext(
		context.Context,
		string,
		...any,
	) (*sql.Rows, error)

	QueryRowContext(
		context.Context,
		string,
		...any,
	) *sql.Row
}

type PostgresTransactor struct {
	db *sql.DB
}

func Open(databaseURL string) (*sql.DB, error) {
	return sql.Open("pgx", databaseURL)
}

func NewPostgresTransactor(db *sql.DB) *PostgresTransactor {
	return &PostgresTransactor{db: db}
}

func (transactor *PostgresTransactor) Run(
	ctx context.Context,
	work func(stores session.TxStores) error,
) (err error) {
	tx, err := transactor.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = errors.Join(err, rollbackErr)
			}
			return
		}
		err = tx.Commit()
	}()

	stores := session.TxStores{
		EventReader:     NewEventDB(tx),
		SessionStore:    NewSessionDB(tx),
		CheckpointStore: NewCheckpointDB(tx),
	}

	return work(stores)
}
