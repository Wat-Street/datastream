import json
from datetime import datetime

import httpx
import pytest
from datastream.client import DatastreamClient, get_data
from datastream.exceptions import DatastreamAPIError
from datastream.types import DatasetVersion


def _mock_transport(status_code: int, body: dict) -> httpx.MockTransport:
    """Create a mock transport that returns a fixed response."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
            content=json.dumps(body).encode(),
            headers={"content-type": "application/json"},
        )

    return httpx.MockTransport(handler)


SAMPLE_RESPONSE = {
    "dataset_name": "mock-ohlc",
    "dataset_version": "0.1.0",
    "total_timestamps": 2,
    "returned_timestamps": 2,
    "rows": [
        {
            "timestamp": "2024-01-02T00:00:00",
            "data": [{"ticker": "AAPL", "open": 100, "close": 130}],
        },
        {
            "timestamp": "2024-01-03T00:00:00",
            "data": [{"ticker": "AAPL", "open": 131, "close": 135}],
        },
    ],
}


def test_get_data_success():
    transport = _mock_transport(200, SAMPLE_RESPONSE)
    client = DatastreamClient(base_url="http://test:3000/api/v1", transport=transport)
    resp = client.get_data(
        "mock-ohlc", "0.1.0", datetime(2024, 1, 2), datetime(2024, 1, 3)
    )

    assert resp.dataset_name == "mock-ohlc"
    assert str(resp.dataset_version) == "0.1.0"
    assert resp.total_timestamps == 2
    assert resp.returned_timestamps == 2
    assert len(resp.rows) == 2
    assert resp.rows[0].timestamp == datetime(2024, 1, 2)
    assert resp.rows[0].data == [{"ticker": "AAPL", "open": 100, "close": 130}]


def test_get_data_partial_206():
    partial = {**SAMPLE_RESPONSE, "returned_timestamps": 1}
    partial["rows"] = [SAMPLE_RESPONSE["rows"][0]]
    transport = _mock_transport(206, partial)
    client = DatastreamClient(base_url="http://test:3000/api/v1", transport=transport)
    resp = client.get_data(
        "mock-ohlc",
        "0.1.0",
        datetime(2024, 1, 2),
        datetime(2024, 1, 3),
        build_data=False,
    )

    assert resp.returned_timestamps == 1
    assert resp.total_timestamps == 2
    assert len(resp.rows) == 1


def test_get_data_error_400():
    transport = _mock_transport(400, {"detail": "bad request"})
    client = DatastreamClient(base_url="http://test:3000/api/v1", transport=transport)
    with pytest.raises(DatastreamAPIError) as exc_info:
        client.get_data("bad", "0.0.1", datetime(2024, 1, 1), datetime(2024, 1, 2))

    assert exc_info.value.status_code == 400


def test_get_data_error_500():
    transport = _mock_transport(500, {"detail": "internal error"})
    client = DatastreamClient(base_url="http://test:3000/api/v1", transport=transport)
    with pytest.raises(DatastreamAPIError) as exc_info:
        client.get_data("bad", "0.0.1", datetime(2024, 1, 1), datetime(2024, 1, 2))

    assert exc_info.value.status_code == 500


def test_get_data_accepts_version_object():
    transport = _mock_transport(200, SAMPLE_RESPONSE)
    client = DatastreamClient(base_url="http://test:3000/api/v1", transport=transport)
    resp = client.get_data(
        "mock-ohlc",
        DatasetVersion(0, 1, 0),
        datetime(2024, 1, 2),
        datetime(2024, 1, 3),
    )
    assert resp.dataset_name == "mock-ohlc"


def test_module_level_get_data(monkeypatch):
    transport = _mock_transport(200, SAMPLE_RESPONSE)
    # patch DatastreamClient to inject transport
    original_init = DatastreamClient.__init__

    def patched_init(self, base_url=None, transport=None):
        original_init(self, base_url=base_url, transport=transport or _transport)

    _transport = transport
    monkeypatch.setattr(DatastreamClient, "__init__", patched_init)

    resp = get_data("mock-ohlc", "0.1.0", datetime(2024, 1, 2), datetime(2024, 1, 3))
    assert resp.dataset_name == "mock-ohlc"
