# API layer (`core/api`)

Endpoint handlers using `APIRouter`. `routes.py` defines two routers, both mounted in
`main.py` under the `/api/v1` prefix:

- `public_router`: carries the unauthenticated `GET /status` (the docker healthcheck hits
  `/api/v1/status`, so it must stay open).
- `router`: everything else, mounted with `dependencies=[Depends(verify_api_key)]`, so every
  other endpoint requires a valid api key. See `core/auth/AGENTS.md`.

Handlers are thin: parse and validate input, delegate to `core/service`, shape the response.
Business logic lives in the service layer, not here.

## Endpoints

```
GET  /status
GET  /datasets
POST /build/{dataset_name}/{dataset_version}?start=<ts>&end=<ts>&dry-run=<bool>
GET  /data/{dataset_name}/{dataset_version}?start=<ts>&end=<ts>&build-data=<bool>
```

### `GET /datasets`

Returns all datasets pre-loaded into the runtime config registry at startup, annotated with
whether each has any data in the DB. Delegates to `core.service.catalog.list_datasets`.

```json
{
  "datasets": [
    {"name": "mock-ohlc", "version": "0.1.0", "has_data": true},
    {"name": "faang-daily-close", "version": "0.1.0", "has_data": false}
  ]
}
```

- `has_data`: `true` when at least one row exists in the DB for that `(name, version)` pair.
- sorted alphabetically by name, then by version.

| Status | Meaning |
|--------|---------|
| `200` | success |
| `500` | unexpected failure (filesystem error, DB error) |

### `GET /data`

Fetches dataset data for a time range. By default it builds missing data before returning.
`build-data=false` opts out for fast read-only access. Delegates to `core.service.builder.get_data`.

Query params: `start`, `end` (required timestamps); `build-data` (optional, default `true`).

```json
{
  "dataset_name": "mock-ohlc",
  "dataset_version": "0.1.0",
  "total_timestamps": 3,
  "returned_timestamps": 3,
  "rows": [
    {"timestamp": "2024-01-02T00:00:00", "data": [
      {"ticker": "AAPL", "open": 100, "high": 150, "low": 90, "close": 130}
    ]}
  ]
}
```

Each entry in `rows` holds all data dicts for that timestamp (multiple rows can share a
timestamp). Rows are sorted by timestamp; empty list when nothing matches.
- `total_timestamps`: valid calendar timestamps in the requested range (via `generate_timestamps()`).
- `returned_timestamps`: distinct timestamps actually returned from the DB.

| Status | Meaning |
|--------|---------|
| `200` | Data complete, or `build-data=true` (default) was used |
| `206` | `build-data=false` and `returned_timestamps < total_timestamps` (incomplete) |
| `400` | Malformed input (invalid version or timestamp) |
| `401` | Missing or invalid API key |
| `422` | `build-data=true` but no valid calendar timestamps in range |
| `500` | Unexpected failure (config not found, DB error) |

### `POST /build`

Builds missing data for the range and writes it to the DB. Delegates to
`core.service.builder.build_dataset`. See `core/service/AGENTS.md` for the build pipeline.

`dry-run` (optional, default `false`): when `true`, the whole build runs against a
request-private in-memory store and nothing is written to the DB. A real build returns
`{"status": "ok"}`; a dry run returns the produced rows:

```json
{
  "dataset_name": "mock-ohlc",
  "dataset_version": "0.1.0",
  "dry_run": true,
  "rows": [
    {"timestamp": "2024-01-02T00:00:00", "data": [
      {"ticker": "AAPL", "open": 100, "high": 150, "low": 90, "close": 130}
    ]}
  ]
}
```

`rows` uses the same shape as `GET /data`. Builder and validation failures surface the same
400/422/500 semantics as a real build.

| Status | Meaning |
|--------|---------|
| `400` | Malformed input (invalid version format or unparseable timestamp) |
| `401` | Missing or invalid API key |
| `422` | Valid input but no valid calendar timestamps in range (e.g. weekday-only dataset over a weekend) |
| `500` | Unexpected failure (config not found, builder crash, DB error) |

The service is the only public-facing security boundary (auth, rate limiting, input validation).
