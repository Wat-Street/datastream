from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

# real builder scripts live two levels up from the server package
REAL_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"

# DDL matching 001_initial_schema.py (without the readonly user)
CREATE_TABLE = """
    CREATE TABLE datasets (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMP(6) NOT NULL DEFAULT now(),
        dataset_name TEXT NOT NULL,
        dataset_version TEXT NOT NULL,
        timestamp TIMESTAMP(6) NOT NULL,
        data JSONB NOT NULL
    )
"""
CREATE_INDEX = """
    CREATE INDEX idx_datasets_name_version_ts
    ON datasets (dataset_name, dataset_version, timestamp)
"""


def _conninfo(container: PostgresContainer) -> str:
    """Build a libpq conninfo string from the testcontainer."""
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return (
        f"host={host} port={port} "
        f"dbname={container.dbname} "
        f"user={container.username} "
        f"password={container.password}"
    )


@pytest.fixture(scope="session")
def postgres_container():
    """Start a fresh postgres container for the benchmark session."""
    with PostgresContainer("postgres:16") as pg:
        conn = psycopg.connect(_conninfo(pg), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE)
            cur.execute(CREATE_INDEX)
        conn.close()
        yield pg


@pytest.fixture(scope="session")
def db_conn(postgres_container):
    """Persistent psycopg connection to the test postgres."""
    conn = psycopg.connect(_conninfo(postgres_container), autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def clean_db(db_conn):
    """Truncate datasets table before each benchmark round."""
    db_conn.execute("TRUNCATE datasets RESTART IDENTITY")


@pytest.fixture(autouse=True)
def patch_db_conn(db_conn, monkeypatch):
    """Redirect all production DB calls to the test database."""
    import db.connection
    import db.datasets

    @contextmanager
    def _test_conn():
        yield db_conn

    monkeypatch.setattr(db.connection, "get_conn", _test_conn)
    monkeypatch.setattr(db.datasets, "get_conn", _test_conn)


@pytest.fixture(autouse=True)
def real_scripts_dir(monkeypatch):
    """Point SCRIPTS_DIR to the real builders/scripts/ directory."""
    from runtime import config, loader

    monkeypatch.setattr(config, "SCRIPTS_DIR", REAL_SCRIPTS_DIR)
    monkeypatch.setattr(loader, "SCRIPTS_DIR", REAL_SCRIPTS_DIR)


@pytest.fixture()
def client():
    """FastAPI test client wrapping the real app (no lifespan)."""
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)
