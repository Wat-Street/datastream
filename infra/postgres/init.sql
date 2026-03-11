CREATE TABLE datasets (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP(6) NOT NULL DEFAULT now(),
    dataset_name TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    timestamp TIMESTAMP(6) NOT NULL,
    data JSONB NOT NULL
);

-- non-unique index for range queries on
-- (dataset_name, dataset_version, timestamp)
CREATE INDEX idx_datasets_name_version_ts
ON datasets (dataset_name, dataset_version, timestamp);

-- read-only role for the pgweb UI
CREATE USER datastream_readonly WITH PASSWORD 'readonly';
GRANT CONNECT ON DATABASE datastream TO datastream_readonly;
GRANT USAGE ON SCHEMA public TO datastream_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datastream_readonly;
-- apply to future tables too
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO datastream_readonly;
