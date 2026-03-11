# Datastream

A service to serve and build financial time-series data. See [SPEC.md](dev-docs/SPEC.md) for the full technical spec.

The current MVP implements the **builder server** (Python/FastAPI), a **Postgres database**, two example builder scripts, and a **CLI trigger** to kick off builds. The main API server (Rust) is not yet implemented.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Python 3.12+ with [uv](https://docs.astral.sh/uv/) and the `requests` package (for the CLI trigger only)

## Quick start

### 1. Start the services

From the `infra/` directory, build and start the containers in the background:

```bash
cd infra
docker compose up --build -d
```

This starts:
- **Postgres** on the internal Docker network (also exposed on `localhost:5432`)
- **Builder server** on the internal Docker network (also exposed on `localhost:8000`)
- **Postgres viewer** (pgweb) at [http://localhost:8080](http://localhost:8080)

To view logs:

```bash
docker compose -f infra/docker-compose.yml logs -f
```

### 2. Trigger a build

From the project root:

```bash
uv pip install requests
python3 trigger.py <dataset_name> <dataset_version> <start> <end>
```

**Example — build mock OHLC data (root dataset):**

```bash
python3 trigger.py mock-ohlc 0.1.0 2024-01-01 2024-01-31
```

**Example — build mock daily close (derived dataset, depends on mock-ohlc):**

```bash
python trigger.py mock-daily-close 0.1.0 2024-01-01 2024-01-31
```

The builder server handles dependency resolution automatically — building `mock-daily-close` will first build `mock-ohlc` if data is missing.

### 3. Verify data in Postgres

```bash
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U datastream -d datastream -c "SELECT * FROM datasets LIMIT 10;"
```

## Stopping the services

```bash
cd infra
docker compose down
```

To also remove the database volume (wipes all data):

```bash
docker compose down -v
```

## Project structure

```
datastream-rs/
  infra/                      # Docker infrastructure (compose, Dockerfiles, DB init)
  builders/
    server/                   # Builder server (FastAPI)
      main.py                 # entrypoint
      api/                    # endpoint handlers
      service/                # build orchestration and dependency resolution
      db/                     # DB connection and queries
      runtime/                # config loading, dynamic import, subprocess isolation, validation
      tests/
    scripts/                  # builder scripts, volume-mounted into the container
      <dataset>/<version>/    # one directory per dataset version
        config.toml
        builder.py
  trigger.py                  # MVP CLI trigger script
```

## Adding a new dataset

1. Create a directory under `builders/scripts/<dataset_name>/<version>/`.
2. Add a `config.toml` with `name`, `version`, `calendar`, `granularity`, `[schema]`, and optionally `[dependencies]`.
3. Add a `builder.py` with a `build(dependencies, timestamp)` function that returns a dict matching the schema.
4. Since scripts are volume-mounted, no container rebuild is needed — just trigger a build.
