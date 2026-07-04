import pytest

pytestmark = pytest.mark.integration


def _row_count(db_conn, name: str, version: str) -> int:
    """Count rows in the datasets table for a (name, version) pair."""
    cur = db_conn.execute(
        "SELECT count(*) FROM datasets WHERE dataset_name = %s "
        "AND dataset_version = %s",
        (name, version),
    )
    return cur.fetchone()[0]


def test_delete_removes_only_in_range_rows(client, db_conn):
    """DELETE removes rows in [start, end] and leaves the rest untouched."""
    # build 4 days: jan 2 through jan 5
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-05"},
    )
    assert resp.status_code == 200
    assert _row_count(db_conn, "mock-ohlc", "0.1.0") == 4

    # delete the middle two days
    resp = client.delete(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "2024-01-03", "end": "2024-01-04"},
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["dataset_name"] == "mock-ohlc"
    assert body["dataset_version"] == "0.1.0"
    assert body["rows_deleted"] == 2
    assert body["start"] == "2024-01-03T00:00:00"
    assert body["end"] == "2024-01-04T00:00:00"

    # the two out-of-range days remain
    cur = db_conn.execute(
        "SELECT timestamp FROM datasets WHERE dataset_name = 'mock-ohlc' "
        "ORDER BY timestamp"
    )
    remaining = [row[0].isoformat() for row in cur.fetchall()]
    assert remaining == ["2024-01-02T00:00:00", "2024-01-05T00:00:00"]


def test_delete_reports_actual_range_narrower_than_requested(client, db_conn):
    """The response range reflects the rows actually deleted, not the request."""
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )

    # request a much wider range than the data spans
    resp = client.delete(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "2023-06-01", "end": "2024-06-01"},
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["rows_deleted"] == 2
    assert body["start"] == "2024-01-02T00:00:00"
    assert body["end"] == "2024-01-03T00:00:00"


def test_delete_dependency_with_dependent_data_allowed(client, db_conn):
    """Deleting from a dataset succeeds even when dependents have derived data."""
    # builds mock-daily-close and its dependency mock-ohlc
    resp = client.post(
        "/api/v1/build/mock-daily-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )
    assert resp.status_code == 200
    assert _row_count(db_conn, "mock-ohlc", "0.1.0") == 2
    assert _row_count(db_conn, "mock-daily-close", "0.1.0") == 2

    # deleting the dependency is allowed; the dependent's rows stay
    resp = client.delete(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )
    assert resp.status_code == 200
    assert resp.json()["rows_deleted"] == 2
    assert _row_count(db_conn, "mock-ohlc", "0.1.0") == 0
    assert _row_count(db_conn, "mock-daily-close", "0.1.0") == 2


def test_delete_multi_row_timestamps_counts_all_rows(client, db_conn):
    """Multi-row timestamps delete all rows and count each one."""
    client.post(
        "/api/v1/build/mock-multi-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    # 3 tickers share the single timestamp
    assert _row_count(db_conn, "mock-multi-ohlc", "0.1.0") == 3

    resp = client.delete(
        "/api/v1/data/mock-multi-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200
    assert resp.json()["rows_deleted"] == 3
    assert _row_count(db_conn, "mock-multi-ohlc", "0.1.0") == 0


def test_delete_unknown_dataset_404(client, db_conn):
    """Unknown dataset returns 404."""
    resp = client.delete(
        "/api/v1/data/no-such-dataset/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )
    assert resp.status_code == 404


def test_delete_no_data_in_range_404(client, db_conn):
    """Known dataset with no rows in range returns 404."""
    resp = client.delete(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )
    assert resp.status_code == 404


def test_delete_invalid_version_400(client):
    """Bad version returns 400."""
    resp = client.delete(
        "/api/v1/data/mock-ohlc/bad-version",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )
    assert resp.status_code == 400


def test_delete_invalid_timestamp_400(client):
    """Bad timestamp returns 400."""
    resp = client.delete(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "not-a-date", "end": "2024-01-03"},
    )
    assert resp.status_code == 400
