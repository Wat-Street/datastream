# Datastream Python SDK

A lightweight Python client for the datastream API, packaged as `datastream-sdk` under `builders/sdk/`.

## Installation

The SDK is a uv workspace member. `uv sync` from the repo root installs it.

## Usage

```python
from datetime import datetime
from datastream import get_data, configure

# optional: override default base URL (defaults to http://localhost:3000/api/v1)
configure("https://datastream.example.com/api/v1")

resp = get_data("mock-ohlc", "0.1.0", datetime(2024, 1, 2), datetime(2024, 1, 3))
print(resp.total_timestamps, resp.returned_timestamps)
for row in resp.rows:
    print(row.timestamp, row.data)
```

## Package structure

```
builders/sdk/
  pyproject.toml
  datastream/
    __init__.py        # re-exports public API
    client.py          # DatastreamClient class + module-level get_data()
    config.py          # default endpoint config, configure()
    types.py           # DatasetVersion, DatasetName, DatasetRow, DatasetResponse
    exceptions.py      # DatastreamError, DatastreamAPIError
  tests/
    test_types.py
    test_exceptions.py
    test_config.py
    test_client.py
```

## Public API

- `get_data(name, version, start, end, *, build_data=True)` - fetch dataset data (convenience function)
- `DatastreamClient(base_url=None, transport=None)` - reusable client instance
- `configure(base_url)` - set the default base URL
- `DatasetVersion` - frozen dataclass with `parse(str)` classmethod and `__str__`
- `DatasetName` - NewType wrapping str
- `DatasetRow` - dataclass with `timestamp` and `data` fields
- `DatasetResponse` - dataclass with dataset metadata and `rows`
- `DatastreamError` - base exception
- `DatastreamAPIError` - API error with `status_code` and `detail`

## Behavior

- `get_data` calls `GET /api/v1/data/{name}/{version}` with query params
- Returns `DatasetResponse` for both 200 (complete) and 206 (partial) responses
- Raises `DatastreamAPIError` for non-2xx status codes (400, 422, 500, etc.)
- Callers detect partial data by checking `returned_timestamps < total_timestamps`
- Version can be passed as a string (`"0.1.0"`) or `DatasetVersion` object
- Uses `httpx` for HTTP (sync only, async support is a future addition)
