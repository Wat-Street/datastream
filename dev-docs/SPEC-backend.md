# Datastream Backend

A service to serve and build financial time-series data.

## Service

A single Python FastAPI service handles both public API traffic and builds. It listens on port 3000.

### Public endpoints

```
GET /status
```

```
GET /datasets
```

```
POST /build/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>
```

```
GET /data/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>&build-data=<bool>
```

### Datasets endpoint

`GET /datasets` returns all datasets discovered on the filesystem, annotated with whether each has any data in the database.

**Response format:**

```json
{
  "datasets": [
    {"name": "mock-ohlc", "version": "0.1.0", "has_data": true},
    {"name": "faang-daily-close", "version": "0.1.0", "has_data": false}
  ]
}
```

- `has_data`: `true` when at least one row exists in the DB for that `(name, version)` pair, `false` otherwise
- datasets are sorted alphabetically by name, then by version
- datasets are discovered by scanning `SCRIPTS_DIR` for directories containing a `config.toml`

**Status codes:**

| Status | Meaning |
|--------|---------|
| `200` | success |
| `500` | unexpected failure (filesystem error, DB error) |

### Data endpoint

`GET /data` fetches dataset data for a time range. By default it builds missing data before returning, ensuring complete results. Callers can opt out of building with `build-data=false` for fast read-only access.

**Query parameters:**
- `start`, `end` (required): timestamp range
- `build-data` (optional, default `true`): when `true`, builds missing data before fetching; when `false`, returns only existing data

**Response format:**

```json
{
  "dataset_name": "mock-ohlc",
  "dataset_version": "0.1.0",
  "total_timestamps": 3,
  "returned_timestamps": 3,
  "rows": [
    {
      "timestamp": "2024-01-02T00:00:00",
      "data": [
        {"ticker": "AAPL", "open": 100, "high": 150, "low": 90, "close": 130}
      ]
    }
  ]
}
```

Each entry in `rows` contains all data dicts for that timestamp (matching the DB model where multiple rows can share a timestamp). Rows are sorted by timestamp. If no data exists for the range, `rows` is an empty list.

**Metadata fields:**
- `total_timestamps`: number of valid calendar timestamps in the requested range (computed via `generate_timestamps()`)
- `returned_timestamps`: number of distinct timestamps actually returned from the DB

**Status codes:**

| Status | Meaning |
|--------|---------|
| `200` | Data is complete, or `build-data=true` (default) was used |
| `206` | `build-data=false` and `returned_timestamps < total_timestamps` (incomplete data) |
| `400` | Malformed input (invalid version or timestamp) |
| `422` | `build-data=true` but no valid calendar timestamps in range |
| `500` | Unexpected failure (config not found, DB error) |

### Build behavior

- On a `POST /build` request, it builds missing data for the requested range and writes it to the database.
- It dynamically imports builder scripts to run them (see below).
    - All builder scripts will be implemented by internal users, so we may trust that all code is safe.
    - But we must not trust that builder scripts will not crash, so each builder invocation runs in an isolated subprocess.
    - Each builder runs in its own `subprocess.Popen` process, communicating via JSON over stdin/stdout. A standalone worker script (`isolated_worker.py`) handles deserialization, builder import, and result serialization. It uses only stdlib so it works in any venv.
    - If the subprocess crashes, the main process catches the failure cleanly without going down.
- Builders **never** have direct access to the database — all reads and writes are handled by the server. For now, this is enforced by convention. TODO: add a runtime guard to enforce this.
- After builder scripts are run, we upload the data to the Postgres database (see below).
- Builds are recursive: before building a dataset, the server will automatically build any dependencies that are missing data for the requested range.
- The service is the only public-facing security boundary (auth, rate limiting, input validation).

### Build error responses

| Status | Meaning |
|--------|---------|
| `400` | Malformed input (invalid version format or unparseable timestamp) |
| `422` | Valid input but no valid calendar timestamps exist in the requested range (e.g. weekday-only dataset requested over a weekend) |
| `500` | Unexpected failure (config not found, builder crash, DB error) |

### Server architecture

The server code lives under `builders/server/` and is organized into four layers:

```
builders/server/
├── main.py         # entrypoint: creates FastAPI app, mounts routers
├── log_config.py   # central structlog configuration
├── api/            # endpoint handlers using APIRouter
│   └── routes.py
├── service/      # build orchestration (dependency resolution, timestamp generation)
│   └── builder.py
├── db/           # database connection management and queries
│   ├── connection.py
│   └── datasets.py
├── runtime/      # config loading, subprocess isolation, schema validation, venv management
│   ├── config.py
│   ├── isolated_worker.py  # standalone worker script (stdlib-only, runs in builder subprocesses)
│   ├── loader.py
│   ├── runner.py
│   ├── serialization.py    # JSON serialization for subprocess IPC
│   ├── validator.py
│   └── venv_management.py  # per-builder venv creation and caching
├── utils/        # shared utilities
│   ├── retry.py            # generic retry with exponential backoff
│   └── semver.py
└── tests/        # mirrors the layer structure
    ├── api/
    ├── service/
    ├── db/
    ├── runtime/
    └── utils/
```

`main.py` is the uvicorn entrypoint (`main:app`). It creates the `FastAPI` app, mounts routers from `api/`, and runs a `lifespan` handler that calls `setup_builder_venvs()` on startup to create per-builder virtual environments. Dependencies flow strictly downward: `api -> service -> db/runtime`. No layer imports upward.

**Scripts directory resolution**: `SCRIPTS_DIR` in `runtime/config.py` defaults to a path relative to the source file (`../scripts` from the server package root). This works in Docker where scripts are volume-mounted at `/app/scripts`. For local dev, the scripts live at `builders/scripts` (a sibling of `builders/server`), so the `SCRIPTS_DIR` env var overrides the default. `just backend-dev` sets this automatically.

### Logging

The server uses `structlog` for structured logging. Configuration lives in `log_config.py` and is called once at import time in `main.py`.

**Processor pipeline**: `merge_contextvars` -> `add_log_level` -> `TimeStamper(iso)` -> renderer. The renderer is `ConsoleRenderer` by default (human-readable) or `JSONRenderer` when `LOG_FORMAT=json` is set.

**stdlib integration**: stdlib `logging` is routed through structlog via `ProcessorFormatter`, so uvicorn logs flow through the same pipeline.

**Request context**: a FastAPI middleware in `main.py` clears contextvars per request and binds a unique `request_id`. The build endpoint also binds `dataset_name` and `version` to context.

**What is logged**:
- `api/routes.py`: build failures (exception)
- `service/builder.py`: start-date clamping (warning), skipped builds (info), build progress (info), insert counts (info)
- `db/connection.py`: new connections (debug)
- `db/datasets.py`: query execution (debug), rows inserted (info)
- `runtime/runner.py`: subprocess start/complete (info), stderr output (warning), timeouts and crashes (error)
- `runtime/config.py`: config loaded (debug)
- `runtime/loader.py`: builder script imported (debug)
- `runtime/venv_management.py`: venv creation progress (info), failures (exception)

### CI workflow

Backend checks run in `.github/workflows/backend-ci.yml`.

The workflow triggers on:
- `builders/**`
- `pyproject.toml`
- `uv.lock`
- `.github/workflows/backend-ci.yml`

This keeps backend CI scoped to builder and shared Python dependency changes.

### Benchmarking

End-to-end build benchmarks live under `builders/server/benchmarks/`. They measure wall-clock time for a full build request through the server (HTTP handler → dependency resolution → subprocess spawns → DB insert) using a testcontainer postgres so as not to pollute the real database.

Two profiling modes:

- **`just bench`** — runs `pytest-benchmark` over a 90-day `mock-ohlc` build, 3 rounds, outputs a timing table (mean/stddev/min/max).
- **`just bench-profile [DAYS]`** — wraps the standalone `benchmarks/bench_build.py` script with `py-spy record --subprocesses`, producing `bench-flamegraph.svg`. The `--subprocesses` flag captures the builder subprocess spawns, which dominate build time for simple datasets.

The benchmark test uses `benchmark.pedantic(rounds=3, warmup_rounds=0)` — explicit rounds because each round is expensive, no warmup because every round must do real work (the DB is truncated between rounds by the `clean_db` fixture).

The standalone `__main__` script does the same testcontainer setup without pytest, so it can be wrapped directly by `py-spy`. Module patching is done via direct attribute assignment rather than `monkeypatch`.

The flame graph should reveal the breakdown between: subprocess spawn overhead (`subprocess.Popen` + interpreter startup), JSON serialization (stdin/stdout IPC), calendar/timestamp generation, and DB operations (`get_existing_timestamps` + `insert_rows`).

## Containers

- The service, Postgres database, and Caddy reverse proxy each run in their own Docker containers.
- Containers communicate over a Docker-managed internal network. Only Caddy exposes host ports (80/443). Internal services (postgres, builder, pgweb) have no host port mappings in production.
- `builders/scripts/` is mounted as a volume into the builder container at runtime, so scripts can be updated without rebuilding the image.

### Network topology

```
Internet --> [Caddy :80/:443] --internal--> [builder:3000]
                                            [postgres:5432]  (no host port)
                                            [pgweb:8080]     (no host port)
```

Caddy terminates TLS and reverse proxies to the builder over the Docker bridge network. Internal traffic stays plain HTTP.

### Caddy reverse proxy

Caddy provides automatic HTTPS via Let's Encrypt with minimal configuration. The domain is set via the `DOMAIN` env var in `infra/.env`.

- **Production**: set `DOMAIN=datastream.yourdomain.com`, Caddy auto-provisions a Let's Encrypt certificate
- **Local Docker**: `DOMAIN=localhost` uses Caddy's internal CA (self-signed), or `DOMAIN=:80` for plain HTTP
- **Local dev** (`just backend-dev`): unaffected, hits uvicorn on localhost:3000 directly via the dev overlay

### Dev overlay

`infra/docker-compose.dev.yml` re-exposes internal service ports for local development:
- postgres:5432, builder:3000, pgweb:8080

Use `just docker-up-dev` to start with the overlay, or `just docker-up` for production mode.

### Infra directory layout

```
infra/
  docker-compose.yml      # production compose (only Caddy ports exposed)
  docker-compose.dev.yml  # dev overlay (re-exposes internal ports)
  .env                    # environment variables (DB credentials, DOMAIN, etc.)
  builder/
    Dockerfile
  caddy/
    Caddyfile             # Caddy reverse proxy config
  postgres/
    Dockerfile
    init.sql
```

## Dataset and builder model

The two main components of the service are datasets and builders.
- Datasets define the data, they are typically series of (timestamp, data) pairs.
    - Datasets are uniquely identified by a (name, version) pair, where name is a string and version is a SemVer (e.g. 1.1.12).
    - Datasets may have dependencies on other datasets. For example, a 5 day moving average may depend on daily pricing data.
    - Datasets with no dependencies are called "root datasets".
- Builders are the logic to create more data. These will be Python scripts that can be called by the main process.

### Start date

Each dataset declares a `start-date` in `config.toml` (format: `YYYY-MM-DD`), which defines the earliest date data can be built for. At build time:
- If the request's `end` is before `start-date`, a `ValueError` is raised.
- If the request's `start` is before `start-date`, `start` is clamped to `start-date` with a warning log.

The `start-date` field is required and validated when loading the config.

### Overwrite policy

- When a build is triggered for a time range, the builder server first queries the DB for all distinct timestamps in that range that already have any rows for the given dataset.
- If any rows exist for a timestamp, it is considered fully built and skipped.
- Only missing timestamps are built and inserted — existing rows are never overwritten.
- This avoids re-running builders unnecessarily; DB writes are plain `INSERT` (no upsert needed).

### Atomicity

Builds are atomic at the request level. If any single timestamp in the requested range fails to build, no data from that request is committed to the database — not even the timestamps that succeeded. This is enforced by batching all inserts for the range into a single database transaction and rolling back on any failure.

### Build concurrency

The service runs a single uvicorn worker. FastAPI dispatches sync endpoint handlers to a thread pool, so concurrent requests for the same dataset can race between the "check what's missing" DB read and the "insert rows" DB write, producing duplicate rows.

**Per-dataset locking**: a `threading.Lock` per `(dataset_name, dataset_version)` pair serializes the critical section of `_build_recursive` (steps: check existing timestamps, compute missing, build, validate, insert). Different datasets build concurrently; the same dataset serializes. Locks are created lazily and stored in a module-level registry (`service/locks.py`).

**Lock scope in `_build_recursive`**:
1. Load config, clamp start date, build dependencies recursively (all **outside** the lock)
2. Generate valid timestamps (outside the lock, deterministic)
3. **Acquire lock**
4. Check existing timestamps in DB, compute missing, build each missing timestamp, validate + insert atomically
5. **Release lock**

Dependencies are built before the parent's lock is acquired, so each dataset only holds its own lock during its own build phase. This minimizes lock hold time and avoids unnecessary serialization of the dependency tree.

**Deadlock safety**: requires the dependency graph to be acyclic. A cycle (X -> Y -> X) would deadlock because a thread building X acquires `lock(X)`, recurses into Y, acquires `lock(Y)`, recurses back into X, and blocks on `lock(X)`. Currently `validate_dependency_graph` does not detect cycles. Cycle detection is a follow-up.

**Scaling assumption**: this design assumes a single uvicorn worker (single process). Multi-worker deployments would require Postgres advisory locks or a similar distributed locking mechanism.

### Datasets postgres schema

Datasets will be stored in a Postgres database. The table used to store datasets is `datasets`. Each row contains some timeseries data along with some metadata. It has the following columns:

Metadata columns:
- `id`: primary key, automatically increasing,
- `created_at`: a timestamp, accurate to the microsecond, for when the current row was created at,
- `dataset_name`: a string, the dataset name to which the current row's data belongs to,
- `dataset_version`: a string, the stringified semver of the current row's dataset (validated at the application level, no DB constraint),

The combination of `(dataset_name, dataset_version, timestamp)` is **not unique** — multiple rows can share the same triple (e.g. multiple tickers at the same timestamp). A non-unique index on `(dataset_name, dataset_version, timestamp)` keeps range queries fast. TODO: partition the table by `dataset_name` or time range for performance at scale.

Data columns:
- `timestamp`: a timestamp, what timestamp this row represents,
- `data`: a JSONB, key-value pairs for the current row's data,
    - the JSONB **must not be nested for more than one level**

Here is an example of what the decoded JSON in the `data` column is:

```json
{
    "ticker": "AAPL",
    "open": 123,
    "high": 456,
    "low": 100,
    "close": 200
}
```

### Builders layout

Builders are stateless Python scripts. To each dataset there is a builder script, and that script will build only that dataset. Builders are stored under the `builders/` directory. Each builder is minimally a `builder.py` and a `config.toml`.

The `[schema]` section in `config.toml` is used for runtime validation:
- After a builder returns its output list, the builder server validates each dict in the list against the schema before inserting into the DB.
- Validation checks that all declared keys are present and that values match the declared types.
- Validation correctness is the priority over performance.
- The builder script for dataset `(dataset_name, dataset_version)` is under `builders/scripts/dataset_name/dataset_version/builder.py`. The config is stored under `builders/scripts/dataset_name/dataset_version/config.toml`.

Here is an example `builder.py` (subject to change):

```python
from datetime import datetime
from typing import Any

def build(dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime) -> list[dict[str, Any]]:
    return [{"ticker": "AAPL", "price": 123}]
```

Type notes:
- `timestamp`: a `datetime.datetime` with microsecond precision.
- `dependencies`: maps each dependency's **name** (not name+version) to a **dict keyed by timestamp**, where each value is a list of data dicts. For deps without lookback, the dict contains only the current timestamp. For deps with lookback, the dict contains all timestamps in the lookback window `[T - lookback + step, T]` (N points inclusive, where step is one unit of the lookback duration). Versions are resolved by the builder server using `config.toml`, so builder scripts never need to reference them directly.
- Return value: a **list of dicts**, where each dict is one row to insert. Single-row datasets return a list of length 1.

And here is an example `config.toml` (subject to change):

```toml
name = "dataset name"
version = "0.0.1"
builder = "builder.py"  # not strictly necessary, here just in case
calendar = "NYSE"  # defines the valid timestamps for this dataset
start-date = "2020-01-01"  # earliest date data can be built for (YYYY-MM-DD)
env-vars = true  # optional, default false; loads .env file into builder subprocess

[schema]
ticker = "str"
price = "int"

[dependencies]
dependency-a = "0.0.2"  # simple: version only (no lookback)
dependency-b = { version = "0.0.1", lookback = "5d" }  # with lookback window
```

There may be other Python files in the same directory or relative sub-directories, and they will be imported using the Python module system.

### Builder environment variables

Builder scripts may need secrets or config (API keys, credentials) passed via environment variables. This is supported through the `env-vars` field in `config.toml`.

**Config field**: `env-vars` is an optional boolean (default `false`). When `true`, the builder server loads a `.env` file and injects its variables into the builder subprocess.

**`.env` location**: The `.env` file must be in the same directory as the builder script: `builders/scripts/<dataset_name>/<version>/.env`. This file is **not** committed to git (see `builders/scripts/.gitignore`).

**`.env.template` convention**: By convention, committed `.env.template` files document which variables a builder needs. The server does not read these; they exist purely as documentation for humans.

**Runtime behavior**:
- The `.env` file is validated at build time, not config load time. This means CI can load configs for datasets with `env-vars = true` without needing the actual `.env` file present.
- If `env-vars` is `true` and the `.env` file is missing at build time, a `FileNotFoundError` is raised.
- The main server process never reads the `.env` values. The `.env` file is parsed inside the subprocess only (using a minimal stdlib-only parser in `isolated_worker.py`), so secrets never enter the parent process memory.
- Environment variables are scoped to the subprocess and do not leak to the parent.

### Per-builder virtual environments

Builder scripts may need external libraries (pandas, numpy, API clients, etc.) that differ between builders. Each builder can declare its own dependencies via a `requirements.txt` file in its directory.

**Dependency specification**: a standard `requirements.txt` in the builder directory (`builders/scripts/<name>/<version>/requirements.txt`).

**Venv location**: `.venv/` inside each builder's directory, gitignored via `builders/scripts/.gitignore`.

**Install timing**: eager on server startup. The FastAPI `lifespan` handler calls `setup_builder_venvs()` which scans all builder directories and creates venvs for any that have a `requirements.txt`.

**Caching**: venv creation is skipped if `.venv/.requirements_hash` (a crc32 hash of `requirements.txt`) matches the current file. Changing `requirements.txt` triggers a rebuild on next startup.

**Tooling**: `uv` is used for venv creation (`uv venv`) and package installation (`uv pip install`).

**Builders without requirements.txt**: no venv is created, no overhead. The runner uses the system Python (`sys.executable`) for these builders.

**Venv detection**: at build time, `runner.py` checks if `script_dir/.venv/bin/python` exists. If so, it uses that interpreter for the subprocess; otherwise it falls back to `sys.executable`.

**Error handling**: if one builder's venv creation fails, it logs a warning but does not block other builders from being set up.

**Updated builder directory layout**:

```
builders/scripts/<dataset_name>/<version>/
  builder.py            # builder script (required)
  config.toml           # dataset config (required)
  requirements.txt      # python dependencies (optional)
  .env                  # environment variables (optional, gitignored)
  .env.template         # documents required env vars (optional)
  .venv/                # per-builder venv (auto-created, gitignored)
```

### Subprocess execution model

Builder subprocesses use `subprocess.Popen` with JSON over stdin/stdout for IPC:

1. The runner serializes builder inputs (dependencies, timestamp, paths, env file) to JSON via `serialization.py`
2. The subprocess runs `isolated_worker.py` using the builder's venv python (or system python)
3. `isolated_worker.py` is stdlib-only: it deserializes input, imports and runs the builder, serializes output
4. The runner deserializes the JSON response from stdout

This model supports per-builder venvs since each subprocess uses its own Python interpreter. The worker script has no dependencies on server code.

### Retry behavior

Builder subprocess execution is wrapped in automatic retry with exponential backoff via a generic `retry_with_backoff()` utility in `utils/retry.py`.

**Constants** (defined in `runtime/runner.py`):
- `RETRY_MAX_RETRIES = 5`
- `RETRY_INITIAL_DELAY = 2.0` seconds
- `RETRY_BACKOFF_FACTOR = 2.0`

**Delay progression**: 2s, 4s, 8s, 16s, 32s (total worst-case wait: ~62s + subprocess execution time).

**What is retried**: all subprocess failures, including timeouts (`TimeoutExpired`), crashes (non-zero exit with no stdout), and worker errors (`WorkerError` from the isolated worker). The entire subprocess execution is retried from scratch on each attempt.

**What is NOT retried**: payload serialization (deterministic, runs once before the retry loop), schema validation (runs after `run_builder()` returns, in the caller), and dependency resolution (runs before `run_builder()` is called).

**Logging**: each retry attempt logs a structlog warning with the attempt number, delay, and error message.

**Integration test override**: integration tests monkeypatch retry constants in `tests/integration/conftest.py` to keep runtime fast while preserving retry semantics:
- `RETRY_MAX_RETRIES = 3`
- `RETRY_INITIAL_DELAY = 0.01`
- `RETRY_BACKOFF_FACTOR = 2.0`

This keeps integration coverage for transient-recovery and retry-exhaustion behavior without incurring production-scale backoff delays.

### Mock builders

The following mock builders exist for testing and development:

- `mock-ohlc/0.1.0`: root dataset, generates single-row OHLC data for AAPL per timestamp
- `mock-daily-close/0.1.0`: depends on mock-ohlc, extracts the close price (single row)
- `mock-multi-ohlc/0.1.0`: root dataset, generates multi-row OHLC data for AAPL, MSFT, GOOG per timestamp
- `mock-multi-close/0.1.0`: depends on mock-multi-ohlc, extracts close prices for each ticker (multi-row)
- `mock-moving-avg/0.1.0`: depends on mock-daily-close with `lookback = "5d"`, computes 5-day moving average of close prices

### Lookback dependencies

Certain datasets depend on a time window of historical data from their dependencies. For example, a moving average needs the last 5 days of close prices.

**Config format**: Dependencies support two formats:
- Simple: `dep = "0.1.0"` (no lookback, builder receives only the current timestamp's data)
- Table with lookback: `dep = {version = "0.1.0", lookback = "5d"}`

Lookback is a duration string using these units: `"5d"` (days), `"24h"` (hours), `"30m"` (minutes), `"60s"` (seconds). Must be a positive value. After parsing, all dependencies are normalized to `{"version": str, "lookback_subtract": timedelta | None}`. `lookback_subtract` is pre-computed as `amount - 1` units (e.g. `timedelta(days=4)` for "5d").

**Semantics**: Lookback defines an inclusive window of N points. `lookback = "5d"` means "fetch 5 days of dependency data in `[T - 4d, T]`" (5 days inclusive). The window start is computed as `T - lookback_subtract`.

**Data format**: Builders always receive `dict[str, dict[datetime, list[dict]]]` — dependency data keyed by name, then by timestamp, then a list of rows. This applies regardless of whether lookback is set.

**Build range expansion**: When building dependencies recursively, the builder server expands the dependency's build start date using `dep_start = start - lookback_subtract` so historical data covers the correct inclusive window.

**Edge case**: Near a dependency's `start-date`, the lookback window may return fewer data points than usual. Builder scripts are responsible for handling short windows gracefully.

Example config:

```toml
name = "dataset name"
version = "0.0.1"
builder = "builder.py"
granularity = "1d"
start-date = "2020-01-01"

[schema]
ticker = "str"
average = "float"

[dependencies]
mock-daily-close = { version = "0.0.1", lookback = "5d" }
```

## Timestamp granularity

Timestamps are stored with microsecond precision (using `pandas.Timestamp`). In practice, the finest granularity used will be per-second.

Each dataset has a declared granularity (e.g. `"1s"`, `"1m"`, `"1d"`), which will be a field in `config.toml`. A dataset may only depend on another dataset whose granularity is **finer or equal** to its own. For example, a daily dataset may depend on per-second data or another daily dataset, but an hourly dataset cannot depend on a daily one. This constraint is enforced at build time by `validate_dependency_graph()` before any data is built.

## Calendars

Calendars determine which timestamps are valid for a dataset. Each dataset must declare a `calendar` in `config.toml`. At build time, `generate_timestamps()` filters candidate timestamps through the calendar's `is_open()` method, so only valid dates are built and stored.

### Calendar interface

The `Calendar` ABC lives in `builders/server/calendars/interface.py`:
- `name: str` — unique identifier (abstract property)
- `granularity: timedelta` — smallest time step (abstract property)
- `is_open(timestamp: datetime) -> bool` — whether a timestamp is valid (abstract method)
- `next_open(timestamp: datetime) -> datetime | None` — returns the next valid datetime >= timestamp, or None if never open again (abstract method)

Calendars are lightweight structures that are allowed to maintain state but should be minimal.

### Available calendars

All calendars are registered in `CALENDARS_MAP` (a `dict[str, Calendar]`) in `builders/server/calendars/registry.py`:

- **`everyday`** — every day is valid (`is_open` always returns `True`). This is the default calendar.
- **`weekday`** — Monday through Friday are valid, Saturday and Sunday are not.
- **`nyse-daily`** — NYSE trading days only (excludes weekends and all NYSE holidays). Uses `exchange_calendars` library with the XNYS exchange calendar.

### Integration with timestamp generation

`generate_timestamps()` in `service/builder.py` accepts an optional `Calendar`. When provided, each candidate timestamp is checked via `calendar.is_open()` and excluded if not open. `_build_recursive()` passes `cfg.calendar` automatically.

### Config integration

- `DatasetConfig.calendar` is of type `Calendar` (not a string).
- During config loading, the calendar string from `config.toml` is validated against `CALENDARS_MAP` and resolved to a `Calendar` instance.
- The `calendar` field is required. Missing it raises a `ValueError` during config validation.
- Unknown calendar names raise a `ValueError` during config validation.

### Layout

```
builders/server/calendars/
├── __init__.py
├── interface.py      # Calendar ABC
├── utils.py          # utility functions
├── definitions/      # concrete calendar class implementations
└── registry.py       # CALENDARS_MAP registry
```

### Future work

- Cross-dataset dependency lookups may use as-of semantics (nearest prior valid timestamp) to handle calendar mismatches.
- Additional calendars (e.g. NYSE trading days) can be added by subclassing `Calendar` and registering in `CALENDARS_MAP`.
