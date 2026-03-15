# Datastream Backend

A service to serve and build financial time-series data.

## Service

A single Python FastAPI service handles both public API traffic and builds. It listens on port 3000.

### Public endpoints

```
GET /ping
```

```
POST /build/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>
```

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
├── main.py       # entrypoint: creates FastAPI app, mounts routers
├── api/          # endpoint handlers using APIRouter
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
└── tests/        # mirrors the layer structure
    ├── api/
    ├── service/
    ├── db/
    └── runtime/
```

`main.py` is the uvicorn entrypoint (`main:app`). It creates the `FastAPI` app, mounts routers from `api/`, and runs a `lifespan` handler that calls `setup_builder_venvs()` on startup to create per-builder virtual environments. Dependencies flow strictly downward: `api -> service -> db/runtime`. No layer imports upward.

## MVP trigger

- For MVP, builds are triggered via a standalone Python CLI script.
- The script is entirely separate from the server — it communicates only via HTTP using the `requests` library.
- It calls `POST /build/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>` on the server directly.

## Containers

- The service and the Postgres database run in their own Docker containers.
- Containers communicate over a Docker-managed internal network — no public ports are exposed except where necessary.
- `builders/scripts/` is mounted as a volume into the builder container at runtime, so scripts can be updated without rebuilding the image.

The `infra/` directory is laid out as follows:

```
infra/
  docker-compose.yml
  .env              # global environment variables (DB credentials, connection strings, etc.)
  builder/
    Dockerfile
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
- For MVP, validation correctness is the priority over performance.
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
- `dependencies`: maps each dependency's **name** (not name+version) to a **dict keyed by timestamp**, where each value is a list of data dicts. For deps without lookback, the dict contains only the current timestamp. For deps with lookback, the dict contains all timestamps in the lookback window `[T - lookback, T]`. Versions are resolved by the builder server using `config.toml`, so builder scripts never need to reference them directly.
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

Lookback is a duration string using these units: `"5d"` (days), `"24h"` (hours), `"30m"` (minutes), `"60s"` (seconds). Must be a positive value. After parsing, all dependencies are normalized to `{"version": str, "lookback": timedelta | None}`.

**Semantics**: Lookback defines a time window, not a point count. `lookback = "5d"` means "fetch all dependency data in `[T - 5d, T]`". The number of data points depends on the dependency's granularity (e.g., 5 daily points or 121 hourly points for a 5-day window).

**Data format**: Builders always receive `dict[str, dict[datetime, list[dict]]]` — dependency data keyed by name, then by timestamp, then a list of rows. This applies regardless of whether lookback is set.

**Build range expansion**: When building dependencies recursively, the builder server expands the dependency's build start date by the lookback duration (`dep_start = start - lookback`) so historical data is available for the full window.

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
├── definitions.py    # concrete calendar classes
└── registry.py       # CALENDARS_MAP registry
```

### Future work

- Cross-dataset dependency lookups may use as-of semantics (nearest prior valid timestamp) to handle calendar mismatches.
- Additional calendars (e.g. NYSE trading days) can be added by subclassing `Calendar` and registering in `CALENDARS_MAP`.
