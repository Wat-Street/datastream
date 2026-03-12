"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-12
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE datasets (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMP(6) NOT NULL DEFAULT now(),
            dataset_name TEXT NOT NULL,
            dataset_version TEXT NOT NULL,
            timestamp TIMESTAMP(6) NOT NULL,
            data JSONB NOT NULL
        )
    """)
    op.execute("""
        CREATE INDEX idx_datasets_name_version_ts
        ON datasets (dataset_name, dataset_version, timestamp)
    """)
    # read-only role for pgweb UI
    op.execute("CREATE USER datastream_readonly WITH PASSWORD 'readonly'")
    op.execute("GRANT CONNECT ON DATABASE datastream TO datastream_readonly")
    op.execute("GRANT USAGE ON SCHEMA public TO datastream_readonly")
    op.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO datastream_readonly")
    op.execute("""
        ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO datastream_readonly
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_datasets_name_version_ts")
    op.execute("DROP TABLE IF EXISTS datasets")
    op.execute(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM datastream_readonly"
    )
    op.execute("REVOKE USAGE ON SCHEMA public FROM datastream_readonly")
    op.execute("REVOKE CONNECT ON DATABASE datastream FROM datastream_readonly")
    op.execute("DROP USER IF EXISTS datastream_readonly")
