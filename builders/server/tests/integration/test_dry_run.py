import random
from datetime import datetime
from unittest.mock import patch

import pytest
from core.service.store import MemoryStore, PostgresStore
from core.utils.semver import SemVer

pytestmark = pytest.mark.integration

V010 = SemVer.parse("0.1.0")


def _db_row_count(db_conn) -> int:
    """Total number of rows in the datasets table."""
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM datasets")
        return cur.fetchone()[0]


def _expected_ohlc(timestamp: datetime) -> dict:
    """Reproduce the deterministic mock-ohlc output for a given timestamp."""
    random.seed(str(timestamp))
    base = round(random.uniform(100, 300), 2)
    return {
        "ticker": "AAPL",
        "open": base,
        "high": round(base + random.uniform(0, 50), 2),
        "low": round(base - random.uniform(0, 30), 2),
        "close": round(base + random.uniform(-10, 20), 2),
    }


# --- DB is never touched ---


def test_dry_run_leaves_db_empty(client, db_conn):
    """A dry-run build inserts nothing into the datasets table."""
    assert _db_row_count(db_conn) == 0

    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-04", "dry-run": "true"},
    )
    assert resp.status_code == 200

    # builders ran (rows returned) but nothing was written
    assert len(resp.json()["rows"]) == 3
    assert _db_row_count(db_conn) == 0


def test_dry_run_does_not_disturb_existing_data(client, db_conn):
    """A dry run over a range with committed data neither reads it nor writes more."""
    # commit one real row first
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    before = _db_row_count(db_conn)
    assert before == 1

    # dry run over a wider range: rebuilds the whole window in isolation
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-04", "dry-run": "true"},
    )
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 3
    # row count unchanged: dry run wrote nothing
    assert _db_row_count(db_conn) == before


# --- builders actually run, output is correct ---


def test_dry_run_returns_correct_builder_output(client, db_conn):
    """Returned rows match the deterministic builder output."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02", "dry-run": "true"},
    )
    assert resp.status_code == 200

    rows = resp.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["timestamp"] == "2024-01-02T00:00:00"
    assert rows[0]["data"] == [_expected_ohlc(datetime(2024, 1, 2))]
    assert _db_row_count(db_conn) == 0


def test_dry_run_spies_on_runner(client, db_conn):
    """The builder subprocess is actually invoked during a dry run."""
    from core.runtime import runner

    real_run_builder = runner.run_builder
    with patch.object(runner, "run_builder", side_effect=real_run_builder) as spy:
        resp = client.post(
            "/api/v1/build/mock-ohlc/0.1.0",
            params={"start": "2024-01-02", "end": "2024-01-03", "dry-run": "true"},
        )
    assert resp.status_code == 200
    # one builder invocation per timestamp
    assert spy.call_count == 2


# --- dependency graphs build in isolation ---


def test_dry_run_dependency_chain(client, db_conn):
    """Dry run of a dependent dataset rebuilds the whole chain in memory."""
    resp = client.post(
        "/api/v1/build/mock-daily-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02", "dry-run": "true"},
    )
    assert resp.status_code == 200

    rows = resp.json()["rows"]
    assert len(rows) == 1
    close = rows[0]["data"][0]
    # the close should equal the ohlc close the dependency produced in-memory
    assert close["close"] == _expected_ohlc(datetime(2024, 1, 2))["close"]

    # neither the dataset nor its dependency was written
    assert _db_row_count(db_conn) == 0


def test_dry_run_lookback_chain(client, db_conn):
    """Dry run of a lookback dataset builds the expanded dependency window in memory."""
    resp = client.post(
        "/api/v1/build/mock-moving-avg/0.1.0",
        params={"start": "2024-01-08", "end": "2024-01-08", "dry-run": "true"},
    )
    assert resp.status_code == 200

    rows = resp.json()["rows"]
    assert len(rows) == 1
    assert "average" in rows[0]["data"][0]
    assert _db_row_count(db_conn) == 0


# --- store contract: PostgresStore vs MemoryStore parity ---


def _seed(store, name, version):
    """Insert a fixed multi-timestamp, multi-row scenario into a store."""
    store.insert_rows(
        name,
        version,
        [
            (datetime(2024, 1, 1), [{"t": "AAPL", "v": 1}, {"t": "MSFT", "v": 2}]),
            (datetime(2024, 1, 2), [{"t": "AAPL", "v": 3}]),
            (datetime(2024, 1, 4), [{"t": "AAPL", "v": 4}]),
        ],
    )


def test_store_parity_read_methods(db_conn):
    """PostgresStore and MemoryStore return identical results for the same data."""
    pg = PostgresStore()
    mem = MemoryStore()
    _seed(pg, "parity", V010)
    _seed(mem, "parity", V010)

    jan1, jan2, jan3, jan4 = (datetime(2024, 1, d) for d in (1, 2, 3, 4))

    # get_existing_timestamps over several windows
    for start, end in [(jan1, jan4), (jan2, jan3), (jan3, jan3)]:
        assert pg.get_existing_timestamps(
            "parity", V010, start, end
        ) == mem.get_existing_timestamps("parity", V010, start, end)

    # get_rows_range over several windows
    for start, end in [(jan1, jan4), (jan1, jan2), (jan3, jan4)]:
        assert pg.get_rows_range("parity", V010, start, end) == mem.get_rows_range(
            "parity", V010, start, end
        )

    # get_rows_timestamps for specific selections
    for sel in [[jan1], [jan1, jan4], [jan3], []]:
        assert pg.get_rows_timestamps("parity", V010, sel) == mem.get_rows_timestamps(
            "parity", V010, sel
        )
