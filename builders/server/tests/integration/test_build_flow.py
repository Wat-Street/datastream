import pytest

pytestmark = pytest.mark.integration


def test_build_root_single_day(client, db_conn):
    """POST mock-ohlc for 1 day -> 200, 1 row in DB with correct schema."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT dataset_name, dataset_version, timestamp, data "
            "FROM datasets WHERE dataset_name = 'mock-ohlc'"
        )
        rows = cur.fetchall()

    assert len(rows) == 1
    name, version, ts, data = rows[0]
    assert name == "mock-ohlc"
    assert version == "0.1.0"
    # verify schema keys
    assert set(data.keys()) == {"ticker", "open", "high", "low", "close"}
    assert data["ticker"] == "AAPL"
