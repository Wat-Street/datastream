import pytest

pytestmark = pytest.mark.integration


def test_default_build_data_returns_complete(client, db_conn):
    """GET with default build-data=true builds and returns complete data."""
    resp = client.get(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-04"},
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["dataset_name"] == "mock-ohlc"
    assert body["dataset_version"] == "0.1.0"
    assert body["total_timestamps"] == 3
    assert body["returned_timestamps"] == 3
    assert len(body["rows"]) == 3

    row = body["rows"][0]
    assert row["timestamp"] == "2024-01-02T00:00:00"
    assert len(row["data"]) == 1
    assert set(row["data"][0].keys()) == {"ticker", "open", "high", "low", "close"}
    assert row["data"][0]["ticker"] == "AAPL"


def test_default_build_multi_row(client, db_conn):
    """GET with default build-data=true on multi-row dataset."""
    resp = client.get(
        "/api/v1/data/mock-multi-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["total_timestamps"] == 1
    assert body["returned_timestamps"] == 1
    assert len(body["rows"][0]["data"]) == 3

    tickers = {d["ticker"] for d in body["rows"][0]["data"]}
    assert tickers == {"AAPL", "MSFT", "GOOG"}


def test_default_build_with_dependencies(client, db_conn):
    """GET with build-data=true builds dependency chain."""
    resp = client.get(
        "/api/v1/data/mock-daily-close/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["returned_timestamps"] == 1
    assert body["rows"][0]["data"][0]["ticker"] == "AAPL"
    assert "close" in body["rows"][0]["data"][0]

    # verify dependency was built too
    ohlc_resp = client.get(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={
            "start": "2024-01-02",
            "end": "2024-01-02",
            "build-data": "false",
        },
    )
    assert ohlc_resp.status_code == 200
    assert ohlc_resp.json()["returned_timestamps"] == 1


def test_no_build_with_prebuilt_data_200(client, db_conn):
    """GET with build-data=false returns 200 when data is complete."""
    # pre-build
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-03"},
    )

    resp = client.get(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={
            "start": "2024-01-02",
            "end": "2024-01-03",
            "build-data": "false",
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["total_timestamps"] == 2
    assert body["returned_timestamps"] == 2


def test_no_build_empty_returns_206(client, db_conn):
    """GET with build-data=false and no data returns 206."""
    resp = client.get(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={
            "start": "2024-01-02",
            "end": "2024-01-02",
            "build-data": "false",
        },
    )
    assert resp.status_code == 206

    body = resp.json()
    assert body["total_timestamps"] == 1
    assert body["returned_timestamps"] == 0
    assert body["rows"] == []


def test_no_build_partial_returns_206(client, db_conn):
    """GET with build-data=false and partial data returns 206."""
    # build only 1 of 3 days
    client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )

    resp = client.get(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={
            "start": "2024-01-02",
            "end": "2024-01-04",
            "build-data": "false",
        },
    )
    assert resp.status_code == 206

    body = resp.json()
    assert body["total_timestamps"] == 3
    assert body["returned_timestamps"] == 1
    assert len(body["rows"]) == 1


def test_fetch_invalid_version(client):
    """Bad version returns 400."""
    resp = client.get(
        "/api/v1/data/mock-ohlc/bad-version",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 400


def test_fetch_invalid_timestamp(client):
    """Bad timestamp returns 400."""
    resp = client.get(
        "/api/v1/data/mock-ohlc/0.1.0",
        params={"start": "not-a-date", "end": "2024-01-02"},
    )
    assert resp.status_code == 400
