package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	session "github.com/ghaering/core-api-task/session"
)

type SessionDB struct {
	db DBTX
}

func NewSessionDB(db DBTX) *SessionDB {
	return &SessionDB{db: db}
}

const insertSessionStartQuery = `
		INSERT INTO session_starts (
			start_event_id, 
			instance_id, 
			started_at
		)
		VALUES ($1, $2, $3)
		ON CONFLICT (start_event_id) DO NOTHING
`

func (sdb *SessionDB) CreateSessionStart(ctx context.Context, sessStart session.SessionStart) error {
	_, err := sdb.db.ExecContext(ctx, insertSessionStartQuery,
		sessStart.StartEventID,
		sessStart.InstanceID,
		sessStart.StartedAt,
	)
	if err != nil {
		return err
	}
	return nil
}

const markSessionStartProcessedQuery = `
		UPDATE session_starts
		SET processed = TRUE
		WHERE start_event_id = $1
`

func (sdb *SessionDB) MarkSessionStartProcessed(ctx context.Context, startEventID string) error {
	result, err := sdb.db.ExecContext(ctx, markSessionStartProcessedQuery, startEventID)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return session.ErrNotFound
	}
	return nil
}

const insertSessionStopQuery = `
		INSERT INTO session_stops (
			stop_event_id, 
			instance_id, 
			stopped_at
		)
		VALUES ($1, $2, $3)
		ON CONFLICT (stop_event_id) DO NOTHING
`

func (sdb *SessionDB) CreateSessionStop(ctx context.Context, sessStop session.SessionStop) error {
	_, err := sdb.db.ExecContext(ctx, insertSessionStopQuery,
		sessStop.StopEventID,
		sessStop.InstanceID,
		sessStop.StoppedAt,
	)
	if err != nil {
		return err
	}
	return nil
}

const readSessionStartQuery = `
		SELECT start_event_id, instance_id, started_at
		FROM session_starts
		WHERE instance_id = $1
			AND started_at <= $2
			AND processed = FALSE
		ORDER BY started_at DESC
		LIMIT 1
`

func (sdb *SessionDB) ReadSessionStart(ctx context.Context, vmID string, stoppedAt time.Time) (session.SessionStart, error) {
	var sessStart session.SessionStart
	err := sdb.db.QueryRowContext(ctx, readSessionStartQuery, vmID, stoppedAt).Scan(
		&sessStart.StartEventID,
		&sessStart.InstanceID,
		&sessStart.StartedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return session.SessionStart{}, session.ErrNotFound
	}
	if err != nil {
		return session.SessionStart{}, err
	}
	return sessStart, nil
}

const insertSessionQuery = `
		INSERT INTO sessions (
			session_id,
			instance_id,
			started_at,
			stopped_at,
			start_event_id,
			stop_event_id
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
`

const readConflictingSessionQuery = `
		SELECT session_id, instance_id, started_at, stopped_at, start_event_id, stop_event_id
		FROM sessions
		WHERE session_id = $1
			OR start_event_id = $2
			OR stop_event_id = $3
		LIMIT 1
`

func (sdb *SessionDB) CreateSession(ctx context.Context, sess session.Session) error {
	result, err := sdb.db.ExecContext(ctx, insertSessionQuery,
		sess.SessionID,
		sess.InstanceID,
		sess.StartedAt,
		sess.StoppedAt,
		sess.StartEventID,
		sess.StopEventID,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("%w: %s", session.ErrSessionConflict, pgErr.ConstraintName)
		}
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 1 {
		return nil
	}

	existing, err := sdb.readConflictingSession(ctx, sess)
	if err != nil {
		return err
	}
	if sessionsEqual(existing, sess) {
		return nil
	}
	return session.ErrSessionConflict
}

func (sdb *SessionDB) readConflictingSession(ctx context.Context, sess session.Session) (session.Session, error) {
	var existing session.Session
	err := sdb.db.QueryRowContext(
		ctx,
		readConflictingSessionQuery,
		sess.SessionID,
		sess.StartEventID,
		sess.StopEventID,
	).Scan(
		&existing.SessionID,
		&existing.InstanceID,
		&existing.StartedAt,
		&existing.StoppedAt,
		&existing.StartEventID,
		&existing.StopEventID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return session.Session{}, session.ErrSessionConflict
	}
	return existing, err
}

func sessionsEqual(a session.Session, b session.Session) bool {
	return a.SessionID == b.SessionID &&
		a.InstanceID == b.InstanceID &&
		a.StartedAt.Equal(b.StartedAt) &&
		a.StoppedAt.Equal(b.StoppedAt) &&
		a.StartEventID == b.StartEventID &&
		a.StopEventID == b.StopEventID
}
