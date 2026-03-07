# MVP Implementation Plan

Implements the builder server, Postgres container, example builder scripts, and CLI trigger. The main API server (Rust) is out of scope for MVP.

See @dev-docs/SPEC.md for the full technical spec.

Checked items below are already implemented.

## Repository layout

```
datastream-rs/
  infra/
    docker-compose.yml
    .env
    builder/
      Dockerfile
    postgres/
      Dockerfile
      init.sql
  builders/
    server/
      main.py          # builder server entrypoint
      db.py            # DB connection and queries
      loader.py        # dynamic builder script importer
      runner.py        # subprocess isolation + IPC
      validator.py     # schema validation
      config.py        # config.toml parsing
    scripts/
      mock-ohlc/
        0.1.0/
          builder.py
          config.toml
      mock-daily-close/
        0.1.0/
          builder.py
          config.toml
  trigger.py           # MVP CLI script
```

---

## Step 1: Infra directory scaffold

- [x] Create the `infra/` directory structure:

```
infra/
  docker-compose.yml
  .env
  builder/
    Dockerfile
  postgres/
    Dockerfile
    init.sql
```

- [x]Create the `builders/` directory structure (scripts will be populated in a later step):

```
builders/
  server/
  scripts/
```

---

## Step 2: Postgres container

### `infra/postgres/init.sql`
- [x]Create the `datasets` table:
  - `id` — `BIGSERIAL PRIMARY KEY`
  - `created_at` — `TIMESTAMP(6) NOT NULL DEFAULT now()` (microsecond precision)
  - `dataset_name` — `TEXT NOT NULL`
  - `dataset_version` — `TEXT NOT NULL` (semver string, validated at application level only)
  - `timestamp` — `TIMESTAMP(6) NOT NULL`
  - `data` — `JSONB NOT NULL`
  - Unique constraint on `(dataset_name, dataset_version, timestamp)` — this also serves as the primary range query index.

### `infra/postgres/Dockerfile`
- [x]Base image: `postgres:16`
- [x]Copy `init.sql` into `/docker-entrypoint-initdb.d/`

---

## Step 3: Builder server

Implemented in Python using **FastAPI**. Exposes one internal endpoint:

```
POST /build/{dataset_name}/{dataset_version}?start=<timestamp>&end=<timestamp>
```

- `start` and `end` are ISO 8601 timestamps (microsecond precision).
- Returns `200 OK` on success, `500` with an error message on failure.
- Never returns dataset rows over the network.

### `builders/server/config.py`
- [x]`load_config(dataset_name, dataset_version)` — parses `config.toml` for a given dataset.
  - Loads: `name`, `version`, `calendar`, `granularity`, `[schema]`, `[dependencies]`.
  - Validates that `name` and `version` match the directory path they were loaded from.

### `builders/server/db.py`
- [x]Manages a connection pool to Postgres (use `psycopg2` or `asyncpg`).
- [x]`get_existing_timestamps(dataset_name, dataset_version, start, end) -> list[pd.Timestamp]`
  - Query all timestamps in range that already have rows.
- [x]`insert_rows(dataset_name, dataset_version, rows: list[tuple[pd.Timestamp, dict]])`
  - Plain `INSERT` (no upsert). Inserts `(dataset_name, dataset_version, timestamp, data)`.
- [x]`get_rows(dataset_name, dataset_version, timestamps: list[pd.Timestamp]) -> dict[pd.Timestamp, dict]`
  - Fetch dependency data for a list of specific timestamps.

### `builders/server/loader.py`
- [x]`load_builder(dataset_name, dataset_version) -> Callable`
  - Dynamically imports `builders/scripts/{dataset_name}/{dataset_version}/builder.py`.
  - Returns the `build` function from that module.
  - Adds the script's directory to `sys.path` so relative imports work.

### `builders/server/runner.py`
- [x]`run_builder(build_fn, dependencies, timestamp) -> dict`
  - Spawns a `multiprocessing.Process`.
  - The subprocess calls `build_fn(dependencies, timestamp)` and puts the result into a `multiprocessing.Queue`.
  - The main process waits for the result with a timeout.
  - If the subprocess crashes or times out, raises a clean exception — the main server process is unaffected.

### `builders/server/validator.py`
- [x]`validate(data: dict, schema: dict[str, str]) -> None`
  - Checks all declared keys are present in `data`.
  - Checks values match declared types (`"str"`, `"int"`, `"float"`, `"bool"`).
  - Raises a descriptive `ValidationError` on failure.
  - Correctness over performance for MVP.

### `builders/server/main.py`
- [x]Implements the `POST /build/{dataset_name}/{dataset_version}` endpoint. Logic:
  1. Parse `start` and `end` into `pd.Timestamp`.
  2. Load `config.toml` for the requested dataset.
  3. **Recursive dependency resolution**: for each dependency declared in `config.toml`, recursively call this same endpoint for the dependency's `(name, version)` with the same `start`/`end` range.
  4. **Bulk pre-check**: query DB for all timestamps in `[start, end]` that already exist for this dataset. Compute the missing timestamps based on the dataset's declared granularity.
  5. For each missing timestamp:
     a. Fetch dependency data from DB for that timestamp (one `get_rows` call per dependency).
     b. Run the builder in a subprocess via `runner.py`.
     c. Validate the output against the schema via `validator.py`.
     d. Collect validated rows.
  6. Bulk insert all collected rows via `db.py`.

---

## Step 4: Example builder scripts

### `mock-ohlc/0.1.0` (root dataset)
- [x]`build(dependencies, timestamp)` returns mock OHLC data for a single ticker (e.g. `AAPL`) using a deterministic function of the timestamp.
- [x]`config.toml`: `calendar = "NYSE"`, `granularity = "1d"`, schema has `ticker` (`str`), `open`, `high`, `low`, `close` (all `float`).

### `mock-daily-close/0.1.0` (derived dataset)
- [x]`build(dependencies, timestamp)` extracts the `close` value from `dependencies["mock-ohlc"]`.
- [x]`config.toml`: `calendar = "NYSE"`, `granularity = "1d"`, depends on `mock-ohlc = "0.1.0"`, schema has `ticker` (`str`) and `close` (`float`).

---

## Step 5: Infra wiring

### `infra/.env`
- [x]Create with DB credentials and `DATABASE_URL`.

### `infra/docker-compose.yml`
- [x]`postgres` service: built from `infra/postgres/Dockerfile`, reads credentials from `.env`.
- [x]`builder` service: built from `infra/builder/Dockerfile`, reads `DATABASE_URL` from `.env`, mounts `./builders/scripts` as a volume to `/app/scripts` inside the container. Exposes its port only on the internal Docker network.
- [x]Both services on the same Docker-managed internal network.

### `infra/builder/Dockerfile`
- [x]Base image: `python:3.12-slim`
- [x]Install dependencies: `fastapi`, `uvicorn`, `psycopg2-binary`, `pandas`, `requests`.
- [x]Copy `builders/server/` into `/app/`.
- [x]Entrypoint: `uvicorn main:app --host 0.0.0.0 --port 8000`.

---

## Step 6: MVP CLI trigger

### `trigger.py`
- [x]Standalone script, no shared code with the builder server.
- [x]Accepts `dataset_name`, `dataset_version`, `start`, `end` as CLI arguments.
- [x]Makes a single `POST /build/{dataset_name}/{dataset_version}?start=...&end=...` request to the builder server using `requests`.
- [x]Prints success or error to stdout.

Example usage:
```
python trigger.py mock-ohlc 0.1.0 2024-01-01 2024-01-31
```

---

## Out of scope for MVP
- Main API server (Rust/tokio)
- Calendar implementation (NYSE schedule enforcement) — for MVP, assume all timestamps in the given range are valid
- Lookback dependencies
- Auth, rate limiting
- Granularity constraint enforcement between dependent datasets
- Runtime guard preventing builders from accessing the DB directly
