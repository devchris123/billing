
CREATE TABLE IF NOT EXISTS vm_events (
    event_id text PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    project_id text NOT NULL,
    instance_id text NOT NULL, 
    event_type text NOT NULL,
    flavour text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_vm_events_occurred_at ON vm_events (occurred_at);

CREATE TABLE IF NOT EXISTS session_starts (
    start_event_id text PRIMARY KEY,
    instance_id text NOT NULL,
    started_at timestamptz NOT NULL,
    processed boolean NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_session_starts_instance_id ON session_starts (instance_id);

CREATE TABLE IF NOT EXISTS session_stops (
    stop_event_id text PRIMARY KEY,
    instance_id text NOT NULL,
    stopped_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_stops_instance_id ON session_stops (instance_id);

CREATE TABLE IF NOT EXISTS sessions (
    session_id uuid PRIMARY KEY,
    instance_id text NOT NULL,
    started_at timestamptz NOT NULL,
    stopped_at timestamptz NOT NULL,
    start_event_id text NOT NULL UNIQUE,
    stop_event_id text NOT NULL UNIQUE,

    CHECK (stopped_at >= started_at)
);

CREATE TABLE IF NOT EXISTS watermark_checkpoints (
    processor_name text PRIMARY KEY,
    processed_through timestamptz NOT NULL
);
