-- read-only role for the pgweb UI
CREATE USER datastream_readonly WITH PASSWORD 'readonly';
GRANT CONNECT ON DATABASE datastream TO datastream_readonly;
GRANT USAGE ON SCHEMA public TO datastream_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datastream_readonly;
-- apply to future tables too
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO datastream_readonly;
