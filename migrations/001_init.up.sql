
CREATE TABLE IF NOT EXISTS vm_events (
    event_id text PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    project_id text NOT NULL,
    instance_id text NOT NULL, 
    event_type text NOT NULL,
    flavour text NOT NULL
)
