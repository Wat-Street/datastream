from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from main import app
from service.builder import NoValidTimestampsError

client: TestClient = TestClient(app)


@patch("api.routes.build_dataset")
def test_build_endpoint_success(mock_build: MagicMock) -> None:
    """POST returns 200 with status ok."""
    mock_build.return_value = None
    resp = client.post("/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_build_endpoint_invalid_version() -> None:
    """Bad version returns 400."""
    resp = client.post("/build/ds/bad-version?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 400


def test_build_endpoint_invalid_timestamp() -> None:
    """Bad timestamp returns 400."""
    resp = client.post("/build/ds/0.1.0?start=not-a-date&end=2024-01-31")
    assert resp.status_code == 400


@patch("api.routes.build_dataset", side_effect=FileNotFoundError("config not found"))
def test_build_endpoint_internal_error(mock_build: MagicMock) -> None:
    """Config not found returns 500."""
    resp = client.post("/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 500


@patch(
    "api.routes.build_dataset",
    side_effect=NoValidTimestampsError("no valid timestamps in range"),
)
def test_build_endpoint_no_valid_timestamps(mock_build: MagicMock) -> None:
    """No valid calendar timestamps returns 422."""
    resp = client.post("/build/ds/0.1.0?start=2024-01-06&end=2024-01-07")
    assert resp.status_code == 422
    assert "no valid timestamps in range" in resp.json()["detail"]
