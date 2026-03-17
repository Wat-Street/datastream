from datetime import datetime

import pytest

pytestmark = pytest.mark.integration


def _row_count(db_conn, dataset_name, version="0.1.0"):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM datasets "
            "WHERE dataset_name = %s AND dataset_version = %s",
            (dataset_name, version),
        )
        return cur.fetchone()[0]


def _query_rows(db_conn, dataset_name, version="0.1.0"):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT dataset_name, dataset_version, timestamp, data, created_at "
            "FROM datasets "
            "WHERE dataset_name = %s AND dataset_version = %s "
            "ORDER BY timestamp",
            (dataset_name, version),
        )
        return cur.fetchall()


def test_start_date_clamping(client, db_conn):
    """start before dataset start-date -> clamped, rows only from start-date onward."""
    # mock-ohlc start-date is 2020-01-01
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2019-12-30", "end": "2020-01-02"},
    )
    assert resp.status_code == 200

    rows = _query_rows(db_conn, "mock-ohlc")
    timestamps = [r[2] for r in rows]
    # should only have rows from 2020-01-01 onward
    assert all(ts >= datetime(2020, 1, 1) for ts in timestamps)
    assert len(rows) == 2  # Jan 1, Jan 2


def test_end_before_start_date(client):
    """end before start-date -> 500."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2019-01-01", "end": "2019-06-01"},
    )
    assert resp.status_code == 500


def test_weekend_only_range(client, write_temp_builder):
    """weekday calendar + Sat-Sun range -> 422."""
    name, version = write_temp_builder(
        "weekday-test",
        "0.1.0",
        """\
name = "weekday-test"
version = "0.1.0"
builder = "builder.py"
calendar = "weekday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    return [{"value": 1}]
""",
    )
    # 2024-01-06 is Saturday, 2024-01-07 is Sunday
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-06", "end": "2024-01-07"},
    )
    assert resp.status_code == 422


def test_weekday_skips_weekends(client, db_conn, write_temp_builder):
    """weekday calendar over Mon-Sun -> only 5 rows (weekdays)."""
    name, version = write_temp_builder(
        "weekday-range",
        "0.1.0",
        """\
name = "weekday-range"
version = "0.1.0"
builder = "builder.py"
calendar = "weekday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    return [{"value": 1}]
""",
    )
    # 2024-01-01 (Mon) to 2024-01-07 (Sun) = 5 weekdays
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-01", "end": "2024-01-07"},
    )
    assert resp.status_code == 200
    assert _row_count(db_conn, name) == 5


def test_already_built_skipped(client, db_conn):
    """build same range twice -> same row count, data unchanged."""
    params = {"start": "2024-01-02", "end": "2024-01-04"}

    resp1 = client.post("/api/v1/build/mock-ohlc/0.1.0", params=params)
    assert resp1.status_code == 200
    rows_after_first = _query_rows(db_conn, "mock-ohlc")

    resp2 = client.post("/api/v1/build/mock-ohlc/0.1.0", params=params)
    assert resp2.status_code == 200
    rows_after_second = _query_rows(db_conn, "mock-ohlc")

    assert len(rows_after_first) == len(rows_after_second) == 3
    # data should be identical
    for r1, r2 in zip(rows_after_first, rows_after_second, strict=True):
        assert r1[3] == r2[3]  # data column
        assert r1[4] == r2[4]  # created_at unchanged


def test_partial_range_fills_gaps(client, db_conn):
    """build day 1, then build days 1-3 -> 3 rows, day 1 has original created_at."""
    # build day 1
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    rows_day1 = _query_rows(db_conn, "mock-ohlc")
    assert len(rows_day1) == 1
    original_created_at = rows_day1[0][4]

    # build days 1-3
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-04"},
    )
    rows_all = _query_rows(db_conn, "mock-ohlc")
    assert len(rows_all) == 3

    # day 1 should have the original created_at (not rebuilt)
    day1_row = [r for r in rows_all if r[2] == datetime(2024, 1, 2)][0]
    assert day1_row[4] == original_created_at


def test_dep_not_rebuilt(client, db_conn):
    """build mock-ohlc, then build mock-daily-close -> mock-ohlc rows unchanged."""
    # build mock-ohlc first
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    ohlc_before = _query_rows(db_conn, "mock-ohlc")

    # building mock-daily-close triggers mock-ohlc as a dep
    client.post(
        "/api/v1/build/mock-daily-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    ohlc_after = _query_rows(db_conn, "mock-ohlc")

    assert len(ohlc_before) == len(ohlc_after) == 1
    assert ohlc_before[0][3] == ohlc_after[0][3]  # data unchanged
    assert ohlc_before[0][4] == ohlc_after[0][4]  # created_at unchanged


def test_lookback_near_start_date(client, db_conn):
    """lookback extends before start-date -> fewer data points, still ok."""
    # mock-moving-avg start-date is 2020-01-01, lookback is 5d
    # building for Jan 3 means lookback window is Dec 30 - Jan 3
    # but dep start-date is Jan 1, so only Jan 1-3 are built
    resp = client.post(
        "/api/v1/build/mock-moving-avg/0.1.0",
        params={"start": "2020-01-03", "end": "2020-01-03"},
    )
    assert resp.status_code == 200

    avg_rows = _query_rows(db_conn, "mock-moving-avg")
    assert len(avg_rows) == 1

    # the average should still be computed (with fewer points)
    close_rows = _query_rows(db_conn, "mock-daily-close")
    prices = [r[3]["close"] for r in close_rows if r[2] <= datetime(2020, 1, 3)]
    expected_avg = round(sum(prices) / len(prices), 2)
    assert avg_rows[0][3]["average"] == expected_avg
