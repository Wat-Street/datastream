from pathlib import Path

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

# DDL matching the main integration conftest
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

# real builder scripts
REAL_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "scripts"


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
    """Start a fresh postgres container for the concurrency test session."""
    with PostgresContainer("postgres:16") as pg:
        conn = psycopg.connect(_conninfo(pg), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE)
            cur.execute(CREATE_INDEX)
        conn.close()
        yield pg


@pytest.fixture(scope="session")
def conninfo(postgres_container):
    """Connection info string for the test container."""
    return _conninfo(postgres_container)


@pytest.fixture(autouse=True)
def patch_build_lock():
    """Override parent conftest no-op; use real advisory locks."""
    yield


@pytest.fixture(autouse=True)
def patch_db_conn():
    """Override parent conftest; use real pool connections."""
    yield


@pytest.fixture(autouse=True)
def pool_lifecycle(conninfo):
    """Open a real connection pool for each test, close after."""
    import db.connection

    db.connection.open_pool(conninfo, min_size=4, max_size=10)
    yield
    db.connection.close_pool()


@pytest.fixture(autouse=True)
def clean_db(conninfo):
    """Truncate datasets table before each test."""
    conn = psycopg.connect(conninfo, autocommit=True)
    conn.execute("TRUNCATE datasets RESTART IDENTITY")
    conn.close()


@pytest.fixture(autouse=True)
def real_scripts_dir(monkeypatch):
    """Point SCRIPTS_DIR to real builders/scripts/."""
    from runtime import config, loader

    monkeypatch.setattr(config, "SCRIPTS_DIR", REAL_SCRIPTS_DIR)
    monkeypatch.setattr(loader, "SCRIPTS_DIR", REAL_SCRIPTS_DIR)


@pytest.fixture()
def client():
    """FastAPI test client (no lifespan)."""
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


@pytest.fixture()
def write_temp_builder(tmp_path, monkeypatch):
    """Factory for creating temp builder scripts + configs.

    Monkeypatches SCRIPTS_DIR to the temp dir. Returns a callable:
        (dataset_name, version, config_toml, builder_py) -> (name, version)
    """
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()

    from runtime import config, loader

    monkeypatch.setattr(config, "SCRIPTS_DIR", scripts_dir)
    monkeypatch.setattr(loader, "SCRIPTS_DIR", scripts_dir)

    def _write(dataset_name, version, config_toml, builder_py):
        d = scripts_dir / dataset_name / version
        d.mkdir(parents=True)
        (d / "config.toml").write_text(config_toml)
        (d / "builder.py").write_text(builder_py)
        return dataset_name, version

    return _write
