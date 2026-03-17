from datetime import datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from main import app
from service.builder import DataResult, NoValidTimestampsError

client: TestClient = TestClient(app)


@patch("api.routes.build_dataset")
def test_build_endpoint_success(mock_build: MagicMock) -> None:
    """POST returns 200 with status ok."""
    mock_build.return_value = None
    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_build_endpoint_invalid_version() -> None:
    """Bad version returns 400."""
    resp = client.post("/api/v1/build/ds/bad-version?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 400


def test_build_endpoint_invalid_timestamp() -> None:
    """Bad timestamp returns 400."""
    resp = client.post("/api/v1/build/ds/0.1.0?start=not-a-date&end=2024-01-31")
    assert resp.status_code == 400


@patch("api.routes.build_dataset", side_effect=FileNotFoundError("config not found"))
def test_build_endpoint_internal_error(mock_build: MagicMock) -> None:
    """Config not found returns 500."""
    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 500


@patch(
    "api.routes.build_dataset",
    side_effect=NoValidTimestampsError("no valid timestamps in range"),
)
def test_build_endpoint_no_valid_timestamps(mock_build: MagicMock) -> None:
    """No valid calendar timestamps returns 422."""
    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-06&end=2024-01-07")
    assert resp.status_code == 422
    assert "no valid timestamps in range" in resp.json()["detail"]


# --- GET /data tests ---


@patch("api.routes.get_data")
def test_data_endpoint_default_build_200(mock_get_data: MagicMock) -> None:
    """GET with default build-data=true returns 200 with metadata."""
    ts = datetime(2024, 1, 2)
    mock_get_data.return_value = DataResult(
        data={ts: [{"ticker": "AAPL", "close": 150}]},
        total_timestamps=1,
        returned_timestamps=1,
    )

    resp = client.get("/api/v1/data/ds/0.1.0?start=2024-01-02&end=2024-01-02")

    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset_name"] == "ds"
    assert body["dataset_version"] == "0.1.0"
    assert body["total_timestamps"] == 1
    assert body["returned_timestamps"] == 1
    assert len(body["rows"]) == 1
    assert body["rows"][0]["timestamp"] == "2024-01-02T00:00:00"
    assert body["rows"][0]["data"] == [{"ticker": "AAPL", "close": 150}]
    # verify build_data=True was passed through
    mock_get_data.assert_called_once()
    assert mock_get_data.call_args.kwargs["build_data"] is True


@patch("api.routes.get_data")
def test_data_endpoint_no_build_complete_200(mock_get_data: MagicMock) -> None:
    """GET with build-data=false and complete data returns 200."""
    ts = datetime(2024, 1, 2)
    mock_get_data.return_value = DataResult(
        data={ts: [{"val": 1}]},
        total_timestamps=1,
        returned_timestamps=1,
    )

    resp = client.get(
        "/api/v1/data/ds/0.1.0?start=2024-01-02&end=2024-01-02&build-data=false"
    )

    assert resp.status_code == 200
    assert resp.json()["total_timestamps"] == 1
    assert resp.json()["returned_timestamps"] == 1


@patch("api.routes.get_data")
def test_data_endpoint_no_build_incomplete_206(mock_get_data: MagicMock) -> None:
    """GET with build-data=false and missing data returns 206."""
    mock_get_data.return_value = DataResult(
        data={},
        total_timestamps=3,
        returned_timestamps=0,
    )

    resp = client.get(
        "/api/v1/data/ds/0.1.0?start=2024-01-01&end=2024-01-03&build-data=false"
    )

    assert resp.status_code == 206
    body = resp.json()
    assert body["total_timestamps"] == 3
    assert body["returned_timestamps"] == 0
    assert body["rows"] == []


@patch("api.routes.get_data")
def test_data_endpoint_no_build_partial_206(mock_get_data: MagicMock) -> None:
    """GET with build-data=false and partial data returns 206."""
    ts = datetime(2024, 1, 1)
    mock_get_data.return_value = DataResult(
        data={ts: [{"val": 1}]},
        total_timestamps=3,
        returned_timestamps=1,
    )

    resp = client.get(
        "/api/v1/data/ds/0.1.0?start=2024-01-01&end=2024-01-03&build-data=false"
    )

    assert resp.status_code == 206
    assert resp.json()["returned_timestamps"] == 1


def test_data_endpoint_invalid_version() -> None:
    """Bad version returns 400."""
    resp = client.get("/api/v1/data/ds/bad-version?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 400


def test_data_endpoint_invalid_timestamp() -> None:
    """Bad timestamp returns 400."""
    resp = client.get("/api/v1/data/ds/0.1.0?start=not-a-date&end=2024-01-31")
    assert resp.status_code == 400


@patch("api.routes.get_data", side_effect=FileNotFoundError("config not found"))
def test_data_endpoint_internal_error(mock_get_data: MagicMock) -> None:
    """Config not found returns 500."""
    resp = client.get("/api/v1/data/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 500


@patch(
    "api.routes.get_data",
    side_effect=NoValidTimestampsError("no valid timestamps in range"),
)
def test_data_endpoint_no_valid_timestamps_422(mock_get_data: MagicMock) -> None:
    """No valid calendar timestamps returns 422."""
    resp = client.get("/api/v1/data/ds/0.1.0?start=2024-01-06&end=2024-01-07")
    assert resp.status_code == 422
    assert "no valid timestamps in range" in resp.json()["detail"]
