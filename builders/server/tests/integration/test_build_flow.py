import random
from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.integration


def _query_rows(db_conn, dataset_name, version="0.1.0"):
    """Helper to fetch all rows for a dataset, ordered by timestamp."""
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT dataset_name, dataset_version, timestamp, data "
            "FROM datasets "
            "WHERE dataset_name = %s AND dataset_version = %s "
            "ORDER BY timestamp, data->>'ticker'",
            (dataset_name, version),
        )
        return cur.fetchall()


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


def test_build_root_single_day(client, db_conn):
    """POST mock-ohlc for 1 day -> 200, 1 row in DB with correct schema."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    rows = _query_rows(db_conn, "mock-ohlc")
    assert len(rows) == 1

    name, version, ts, data = rows[0]
    assert name == "mock-ohlc"
    assert version == "0.1.0"
    assert set(data.keys()) == {"ticker", "open", "high", "low", "close"}
    assert data["ticker"] == "AAPL"


def test_build_root_date_range(client, db_conn):
    """POST mock-ohlc for 3 days -> 3 rows, one per day."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-04"},
    )
    assert resp.status_code == 200

    rows = _query_rows(db_conn, "mock-ohlc")
    assert len(rows) == 3

    timestamps = [row[2] for row in rows]
    expected = [datetime(2024, 1, d) for d in (2, 3, 4)]
    assert timestamps == expected


def test_build_single_dep_chain(client, db_conn):
    """POST mock-daily-close -> auto-builds mock-ohlc, close values match."""
    resp = client.post(
        "/api/v1/build/mock-daily-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    # mock-ohlc should have been built as a dependency
    ohlc_rows = _query_rows(db_conn, "mock-ohlc")
    assert len(ohlc_rows) == 1

    close_rows = _query_rows(db_conn, "mock-daily-close")
    assert len(close_rows) == 1

    # close value should match the ohlc close
    ohlc_data = ohlc_rows[0][3]
    close_data = close_rows[0][3]
    assert close_data["close"] == ohlc_data["close"]
    assert close_data["ticker"] == "AAPL"


def test_build_multi_row(client, db_conn):
    """POST mock-multi-ohlc -> 3 rows (AAPL, MSFT, GOOG) for 1 timestamp."""
    resp = client.post(
        "/api/v1/build/mock-multi-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    rows = _query_rows(db_conn, "mock-multi-ohlc")
    assert len(rows) == 3

    tickers = {row[3]["ticker"] for row in rows}
    assert tickers == {"AAPL", "MSFT", "GOOG"}


def test_build_multi_row_dep_chain(client, db_conn):
    """POST mock-multi-close -> builds mock-multi-ohlc, 3+3 rows."""
    resp = client.post(
        "/api/v1/build/mock-multi-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    ohlc_rows = _query_rows(db_conn, "mock-multi-ohlc")
    assert len(ohlc_rows) == 3

    close_rows = _query_rows(db_conn, "mock-multi-close")
    assert len(close_rows) == 3

    # close values should match
    ohlc_by_ticker = {r[3]["ticker"]: r[3] for r in ohlc_rows}
    for row in close_rows:
        ticker = row[3]["ticker"]
        assert row[3]["close"] == ohlc_by_ticker[ticker]["close"]


def test_build_lookback(client, db_conn):
    """POST mock-moving-avg for 1 day -> builds full chain, average is correct."""
    # build for Jan 8 so lookback window (5d) covers Jan 3-8
    resp = client.post(
        "/api/v1/build/mock-moving-avg/0.1.0",
        params={"start": "2024-01-08", "end": "2024-01-08"},
    )
    assert resp.status_code == 200

    avg_rows = _query_rows(db_conn, "mock-moving-avg")
    assert len(avg_rows) == 1

    # verify the dependency chain was built
    ohlc_rows = _query_rows(db_conn, "mock-ohlc")
    close_rows = _query_rows(db_conn, "mock-daily-close")
    assert len(ohlc_rows) > 0
    assert len(close_rows) > 0

    # compute expected average from the close prices in [Jan 3, Jan 8]
    ts_jan8 = datetime(2024, 1, 8)
    close_prices = [
        r[3]["close"]
        for r in close_rows
        if r[2] >= datetime(2024, 1, 3) and r[2] <= ts_jan8
    ]
    expected_avg = round(sum(close_prices) / len(close_prices), 2)
    assert avg_rows[0][3]["average"] == expected_avg


def test_build_lookback_range(client, db_conn):
    """POST mock-moving-avg for 3 days -> each day gets correct lookback window."""
    resp = client.post(
        "/api/v1/build/mock-moving-avg/0.1.0",
        params={"start": "2024-01-08", "end": "2024-01-10"},
    )
    assert resp.status_code == 200

    avg_rows = _query_rows(db_conn, "mock-moving-avg")
    assert len(avg_rows) == 3

    close_rows = _query_rows(db_conn, "mock-daily-close")

    # verify each day's average against the lookback window
    for avg_row in avg_rows:
        avg_ts = avg_row[2]
        window_start = datetime(avg_ts.year, avg_ts.month, avg_ts.day) - timedelta(
            days=5
        )
        prices = [
            r[3]["close"] for r in close_rows if r[2] >= window_start and r[2] <= avg_ts
        ]
        expected = round(sum(prices) / len(prices), 2)
        assert avg_row[3]["average"] == expected


def test_data_correctness_through_chain(client, db_conn):
    """Build full chain, verify numeric values are deterministic and consistent."""
    resp = client.post(
        "/api/v1/build/mock-daily-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-04"},
    )
    assert resp.status_code == 200

    ohlc_rows = _query_rows(db_conn, "mock-ohlc")
    close_rows = _query_rows(db_conn, "mock-daily-close")

    assert len(ohlc_rows) == 3
    assert len(close_rows) == 3

    for ohlc_row, close_row in zip(ohlc_rows, close_rows, strict=True):
        ohlc_data = ohlc_row[3]
        close_data = close_row[3]

        # verify deterministic output matches expected
        expected = _expected_ohlc(ohlc_row[2])
        assert ohlc_data == expected

        # close should match ohlc close
        assert close_data["close"] == ohlc_data["close"]
        assert close_data["ticker"] == "AAPL"
