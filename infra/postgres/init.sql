CREATE TABLE datasets (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP(6) NOT NULL DEFAULT now(),
    dataset_name TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    timestamp TIMESTAMP(6) NOT NULL,
    data JSONB NOT NULL,
    UNIQUE (dataset_name, dataset_version, timestamp)
);
