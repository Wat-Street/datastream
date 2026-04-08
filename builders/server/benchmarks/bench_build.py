"""End-to-end build benchmark for the builder server.

Two entry points:
  1. pytest-benchmark:
     uv run pytest builders/server/benchmarks/ --benchmark-only
  2. standalone (for py-spy):
     cd builders/server && uv run python -m benchmarks.bench_build
"""

import argparse
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
from fastapi.testclient import TestClient

if TYPE_CHECKING:
    from pytest_benchmark.fixture import BenchmarkFixture
else:
    type BenchmarkFixture = Any

log = logging.getLogger(__name__)

# -- pytest-benchmark tests --------------------------------------------------


@pytest.mark.benchmark(group="build")
def test_bench_build_mock_ohlc_90d(
    client: TestClient,
    db_conn: psycopg.Connection,
    benchmark: BenchmarkFixture,
) -> None:
    """Benchmark building mock-ohlc/0.1.0 over a 90-day range."""

    def setup():
        db_conn.execute("TRUNCATE datasets RESTART IDENTITY")

    def do_build():
        resp = client.post(
            "/api/v1/build/mock-ohlc/0.1.0",
            params={
                "start": "2024-01-01T00:00:00",
                "end": "2024-03-31T00:00:00",
            },
        )
        assert resp.status_code == 200

    benchmark.pedantic(do_build, setup=setup, rounds=3, warmup_rounds=0)


# -- standalone entry point for py-spy profiling ------------------------------


def _run_standalone(days: int) -> None:
    """Run a single build against a testcontainer postgres, for profiling."""
    import psycopg
    from testcontainers.postgres import PostgresContainer

    scripts_dir = Path(__file__).resolve().parent.parent.parent / "scripts"

    create_table = """
        CREATE TABLE datasets (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMP(6) NOT NULL DEFAULT now(),
            dataset_name TEXT NOT NULL,
            dataset_version TEXT NOT NULL,
            timestamp TIMESTAMP(6) NOT NULL,
            data JSONB NOT NULL
        )
    """
    create_index = """
        CREATE INDEX idx_datasets_name_version_ts
        ON datasets (dataset_name, dataset_version, timestamp)
    """

    log.info("starting postgres container...")
    with PostgresContainer("postgres:16") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        conninfo = (
            f"host={host} port={port} "
            f"dbname={pg.dbname} "
            f"user={pg.username} "
            f"password={pg.password}"
        )

        # create schema
        conn = psycopg.connect(conninfo, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(create_table)
            cur.execute(create_index)

        # patch modules to use test db
        import db.connection
        import db.datasets
        from runtime import config, loader

        @contextmanager
        def _test_conn():
            yield conn

        db.connection.get_conn = _test_conn  # type: ignore[assignment]  # benchmark patching
        db.datasets.get_conn = _test_conn  # type: ignore[assignment]  # benchmark patching
        config.SCRIPTS_DIR = scripts_dir
        loader.SCRIPTS_DIR = scripts_dir

        # build
        from fastapi.testclient import TestClient
        from main import app

        client = TestClient(app)
        start_date = datetime(2024, 1, 1)
        end_date = start_date + timedelta(days=days)

        log.info("building mock-ohlc/0.1.0 for %d days...", days)
        t0 = time.perf_counter()
        resp = client.post(
            "/api/v1/build/mock-ohlc/0.1.0",
            params={
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
        )
        elapsed = time.perf_counter() - t0

        assert resp.status_code == 200, f"build failed: {resp.status_code} {resp.text}"
        log.info("done in %.2fs (status %d)", elapsed, resp.status_code)

        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="standalone build benchmark",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="number of days to build",
    )
    args = parser.parse_args()
    _run_standalone(args.days)
