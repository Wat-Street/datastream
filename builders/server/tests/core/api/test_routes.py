from datetime import datetime
from unittest.mock import MagicMock, patch

from core.service.builder import DataResult, NoValidTimestampsError
from core.service.catalog import DatasetInfo
from fastapi.testclient import TestClient
from main import app

client: TestClient = TestClient(app)


@patch("core.api.routes.build_dataset")
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


@patch(
    "core.api.routes.build_dataset",
    side_effect=FileNotFoundError("config not found"),
)
def test_build_endpoint_internal_error(mock_build: MagicMock) -> None:
    """Config not found returns 500."""
    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 500


@patch(
    "core.api.routes.build_dataset",
    side_effect=NoValidTimestampsError("no valid timestamps in range"),
)
def test_build_endpoint_no_valid_timestamps(mock_build: MagicMock) -> None:
    """No valid calendar timestamps returns 422."""
    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-06&end=2024-01-07")
    assert resp.status_code == 422
    assert "no valid timestamps in range" in resp.json()["detail"]


@patch("core.api.routes.build_dataset")
def test_build_endpoint_dry_run_returns_rows(mock_build: MagicMock) -> None:
    """POST with dry-run=true returns the produced rows, not just status ok."""
    ts = datetime(2024, 1, 2)
    mock_build.return_value = {ts: [{"ticker": "AAPL", "close": 150}]}

    resp = client.post(
        "/api/v1/build/ds/0.1.0?start=2024-01-02&end=2024-01-02&dry-run=true"
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset_name"] == "ds"
    assert body["dataset_version"] == "0.1.0"
    assert body["dry_run"] is True
    assert body["rows"] == [
        {"timestamp": "2024-01-02T00:00:00", "data": [{"ticker": "AAPL", "close": 150}]}
    ]
    # dry_run=True threaded through to build_dataset
    assert mock_build.call_args.kwargs["dry_run"] is True


@patch("core.api.routes.build_dataset")
def test_build_endpoint_default_is_not_dry_run(mock_build: MagicMock) -> None:
    """POST without dry-run defaults to a real build (status ok, dry_run=False)."""
    mock_build.return_value = None

    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-02&end=2024-01-02")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    assert mock_build.call_args.kwargs["dry_run"] is False


@patch("core.api.routes.build_dataset")
def test_build_endpoint_dry_run_empty_rows(mock_build: MagicMock) -> None:
    """dry-run with no produced rows returns an empty rows list."""
    mock_build.return_value = {}

    resp = client.post(
        "/api/v1/build/ds/0.1.0?start=2024-01-02&end=2024-01-02&dry-run=true"
    )

    assert resp.status_code == 200
    assert resp.json()["rows"] == []


@patch(
    "core.api.routes.build_dataset",
    side_effect=NoValidTimestampsError("no valid timestamps in range"),
)
def test_build_endpoint_dry_run_no_valid_timestamps(mock_build: MagicMock) -> None:
    """dry-run surfaces the same 422 as a real build on no valid timestamps."""
    resp = client.post(
        "/api/v1/build/ds/0.1.0?start=2024-01-06&end=2024-01-07&dry-run=true"
    )
    assert resp.status_code == 422


# --- GET /data tests ---


@patch("core.api.routes.get_data")
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


@patch("core.api.routes.get_data")
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


@patch("core.api.routes.get_data")
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


@patch("core.api.routes.get_data")
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


@patch("core.api.routes.get_data", side_effect=FileNotFoundError("config not found"))
def test_data_endpoint_internal_error(mock_get_data: MagicMock) -> None:
    """Config not found returns 500."""
    resp = client.get("/api/v1/data/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 500


@patch(
    "core.api.routes.get_data",
    side_effect=NoValidTimestampsError("no valid timestamps in range"),
)
def test_data_endpoint_no_valid_timestamps_422(mock_get_data: MagicMock) -> None:
    """No valid calendar timestamps returns 422."""
    resp = client.get("/api/v1/data/ds/0.1.0?start=2024-01-06&end=2024-01-07")
    assert resp.status_code == 422
    assert "no valid timestamps in range" in resp.json()["detail"]


# --- GET /datasets tests ---


@patch("core.api.routes.list_datasets")
def test_datasets_endpoint_returns_list(mock_list: MagicMock) -> None:
    """Returns 200 with datasets array."""
    mock_list.return_value = [
        DatasetInfo(name="mock-ohlc", version="0.1.0", has_data=True),
        DatasetInfo(name="faang-daily-close", version="0.1.0", has_data=False),
    ]
    resp = client.get("/api/v1/datasets")
    assert resp.status_code == 200
    body = resp.json()
    assert "datasets" in body
    assert len(body["datasets"]) == 2
    assert body["datasets"][0] == {
        "name": "mock-ohlc",
        "version": "0.1.0",
        "has_data": True,
    }
    assert body["datasets"][1] == {
        "name": "faang-daily-close",
        "version": "0.1.0",
        "has_data": False,
    }


@patch("core.api.routes.list_datasets")
def test_datasets_endpoint_empty(mock_list: MagicMock) -> None:
    """Returns 200 with empty list when no datasets discovered."""
    mock_list.return_value = []
    resp = client.get("/api/v1/datasets")
    assert resp.status_code == 200
    assert resp.json() == {"datasets": []}


@patch("core.api.routes.list_datasets", side_effect=OSError("disk error"))
def test_datasets_endpoint_internal_error(mock_list: MagicMock) -> None:
    """Unexpected failure returns 500."""
    resp = client.get("/api/v1/datasets")
    assert resp.status_code == 500
