# Datastream

A service to serve and build financial time-series data.

## Main services

The first service is the main, network exposed service that is the API. It will serve endpoints such as the following.
- It will be implemented in Rust using tokio.
- It treats the database as a cache: on a `GET` request, it reads directly from the DB.
- On a `POST /build` request, if data is missing for the requested range, it triggers the builder server to fill the gaps, then reads from the DB.
- The builder server **never** passes data back to the main service over the network — all communication goes through the database.
- The main service is the only public-facing security boundary (auth, rate limiting, input validation).

```
GET /api/v1/data/dataset_name/dataset_version?start=timestamp,end=timestamp
```

```
POST /api/v1/build/dataset_name/dataset_version?start=timestamp,end=timestamp
```

The second service is the builder server, which hosts and serves all builder scripts.
- It is not accessible by a public API.
- It can only be called from the main API service.
- Its sole purpose is building data and writing it to the database — it never returns data over the network.
- Internal API: `POST /build/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>`
- It will be implemented in Python, and dynamically import builder scripts to run them (see below).
    - All builder scripts will be implemented by internal users, so we may trust that all code is safe.
    - But we must not trust that builder scripts will not crash, so each builder invocation runs in an isolated subprocess.
    - The subprocess passes its result back to the main builder server process via IPC (e.g. a `multiprocessing.Queue` or pipe).
    - If the subprocess crashes, the main process catches the failure cleanly without going down.
- Builders **never** have direct access to the database — all reads and writes are handled by the builder server. For now, this is enforced by convention. TODO: add a runtime guard to enforce this.
- After builder scripts are run, we upload the data to the Postgres database (see below).
- Builds are recursive: before building a dataset, the builder server will automatically build any dependencies that are missing data for the requested range.

### Builder server architecture

The builder server code lives under `builders/server/` and is organized into four layers:

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
├── runtime/      # config loading, dynamic builder import, subprocess isolation, schema validation
│   ├── config.py
│   ├── loader.py
│   ├── runner.py
│   └── validator.py
└── tests/        # mirrors the layer structure
    ├── api/
    ├── service/
    ├── db/
    └── runtime/
```

`main.py` is the uvicorn entrypoint (`main:app`). It creates the `FastAPI` app and mounts routers from `api/`. Dependencies flow strictly downward: `api → service → db/runtime`. No layer imports upward.

## MVP trigger

- For MVP (no main API server), builds are triggered via a standalone Python CLI script.
- The script is entirely separate from the builder server — it communicates only via HTTP using the `requests` library.
- It calls `POST /build/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>` on the builder server directly.

## Containers

- All services and the Postgres database run in their own Docker containers.
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

### Overwrite policy

- When a build is triggered for a time range, the builder server first queries the DB for all timestamps in that range that already exist for the given dataset.
- Only missing timestamps are built and inserted — existing rows are never overwritten.
- This avoids re-running builders unnecessarily; DB writes are plain `INSERT` (no upsert needed).

### Datasets postgres schema

Datasets will be stored in a Postgres database. The table used to store datasets is `datasets`. Each row contains some timeseries data along with some metadata. It has the following columns:

Metadata columns:
- `id`: primary key, automatically increasing,
- `created_at`: a timestamp, accurate to the microsecond, for when the current row was created at,
- `dataset_name`: a string, the dataset name to which the current row's data belongs to,
- `dataset_version`: a string, the stringified semver of the current row's dataset (validated at the application level, no DB constraint),

The combination of `(dataset_name, dataset_version, timestamp)` is unique and serves as the primary index for range queries. TODO: partition the table by `dataset_name` or time range for performance at scale.

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
- After a builder returns its output dict, the builder server validates it against the schema before inserting into the DB.
- Validation checks that all declared keys are present and that values match the declared types.
- For MVP, validation correctness is the priority over performance.
- The builder script for dataset `(dataset_name, dataset_version)` is under `builders/scripts/dataset_name/dataset_version/builder.py`. The config is stored under `builders/scripts/dataset_name/dataset_version/config.toml`.

Here is an example `builder.py` (subject to change):

```python
from datetime import datetime

def build(dependencies: dict[str, dict], timestamp: datetime) -> dict[str, Any]:
    return {"ticker": "AAPL", "price": 123}
```

Type notes:
- `timestamp`: a `datetime.datetime` with microsecond precision.
- `dependencies`: maps each dependency's **name** (not name+version) to a dict of its data for the given timestamp. Versions are resolved by the builder server using `config.toml`, so builder scripts never need to reference them directly.

And here is an example `config.toml` (subject to change):

```toml
name = "dataset name"
version = "0.0.1"
builder = "builder.py"  # not strictly necessary, here just in case
calendar = "NYSE"  # defines the valid timestamps for this dataset

[schema]
ticker = "str"
price = "int"

[dependencies]
dependency-a = "0.0.2"  # map dependencies to their versions
```

There may be other Python files in the same directory or relative sub-directories, and they will be imported using the Python module system.

### Lookback dependencies

TODO (don't implement yet)

Certain datasets may depend on multiple days of a dependent dataset. For example, a moving average looks at the closing prices of the past 5 days of data. The config looks like:

```toml
name = "dataset name"
version = "0.0.1"
builder = "builder.py"

[schema]
ticker = "str"
average = "float"

[dependencies]
mock-daily-close = { version = "0.0.1", lookback = 5 }  # last 5 days of data, including todays
```

In dependencies if lookback is not specified, it defaults to 1.

## Timestamp granularity

Timestamps are stored with microsecond precision (using `pandas.Timestamp`). In practice, the finest granularity used will be per-second.

Each dataset has a declared granularity (e.g. `"1s"`, `"1m"`, `"1d"`), which will be a field in `config.toml`. A dataset may only depend on another dataset whose granularity is **strictly finer** than its own. For example, a daily dataset may depend on per-second data, but not the reverse. This constraint ensures the dependency graph flows from fine-grained to coarse-grained data and prevents circular dependencies by construction.

## Calendars

TODO

- Each dataset declares a `calendar` in `config.toml` (e.g. `"NYSE"` for NYSE trading days).
- The calendar defines the valid timestamps for that dataset.
- The builder server only schedules builds for timestamps that fall on calendar dates.
- Timestamps are stored sparsely — no rows exist for dates outside the calendar.
- Cross-dataset dependency lookups use as-of semantics (nearest prior valid timestamp) to handle calendar mismatches.
