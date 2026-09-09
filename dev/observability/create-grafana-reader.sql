DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'grafana_reader'
    ) THEN
        CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana';
    ELSE
        ALTER ROLE grafana_reader WITH LOGIN PASSWORD 'grafana';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE billing TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE billing IN SCHEMA public
    GRANT SELECT ON TABLES TO grafana_reader;
