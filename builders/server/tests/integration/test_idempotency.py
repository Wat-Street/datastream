from datetime import datetime

import pytest

pytestmark = pytest.mark.integration


def _query_rows(db_conn, dataset_name, version="0.1.0"):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT dataset_name, dataset_version, timestamp, data, created_at "
            "FROM datasets "
            "WHERE dataset_name = %s AND dataset_version = %s "
            "ORDER BY timestamp, data->>'ticker'",
            (dataset_name, version),
        )
        return cur.fetchall()


def _row_count(db_conn, dataset_name, version="0.1.0"):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM datasets "
            "WHERE dataset_name = %s AND dataset_version = %s",
            (dataset_name, version),
        )
        return cur.fetchone()[0]


def test_rebuild_same_range_noop(client, db_conn):
    """build then rebuild -> row count unchanged."""
    params = {"start": "2024-01-02", "end": "2024-01-04"}

    client.post("/api/v1/build/mock-ohlc/0.1.0", params=params)
    count_first = _row_count(db_conn, "mock-ohlc")
    rows_first = _query_rows(db_conn, "mock-ohlc")

    client.post("/api/v1/build/mock-ohlc/0.1.0", params=params)
    count_second = _row_count(db_conn, "mock-ohlc")
    rows_second = _query_rows(db_conn, "mock-ohlc")

    assert count_first == count_second == 3
    # data and created_at should be identical
    for r1, r2 in zip(rows_first, rows_second, strict=True):
        assert r1[3] == r2[3]
        assert r1[4] == r2[4]


def test_rebuild_dep_chain_noop(client, db_conn):
    """build mock-daily-close twice -> all row counts unchanged."""
    params = {"start": "2024-01-02", "end": "2024-01-02"}

    client.post("/api/v1/build/mock-daily-close/0.1.0", params=params)
    ohlc_count_1 = _row_count(db_conn, "mock-ohlc")
    close_count_1 = _row_count(db_conn, "mock-daily-close")

    client.post("/api/v1/build/mock-daily-close/0.1.0", params=params)
    ohlc_count_2 = _row_count(db_conn, "mock-ohlc")
    close_count_2 = _row_count(db_conn, "mock-daily-close")

    assert ohlc_count_1 == ohlc_count_2 == 1
    assert close_count_1 == close_count_2 == 1


def test_extend_range_adds_only_new(client, db_conn):
    """build days 1-2, then build days 1-4 -> 4 rows, originals untouched."""
    # build first two days
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )
    rows_initial = _query_rows(db_conn, "mock-ohlc")
    assert len(rows_initial) == 2

    # extend to 4 days
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-05"},
    )
    rows_extended = _query_rows(db_conn, "mock-ohlc")
    assert len(rows_extended) == 4

    # original rows should be untouched (same data + created_at)
    originals = {r[2]: r for r in rows_initial}
    for row in rows_extended:
        ts = row[2]
        if ts in originals:
            assert row[3] == originals[ts][3]  # data unchanged
            assert row[4] == originals[ts][4]  # created_at unchanged

    # new rows should be Jan 4 and Jan 5
    new_timestamps = [r[2] for r in rows_extended if r[2] not in originals]
    assert sorted(new_timestamps) == [
        datetime(2024, 1, 4),
        datetime(2024, 1, 5),
    ]
