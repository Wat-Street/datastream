# Datastream

A service to serve and build financial time-series data. Datastream implements a **builder server** (Python/FastAPI), a **Postgres database**, a **Caddy reverse proxy** with automatic HTTPS, example builder scripts, and a **Svelte frontend**.

See the full technical specs: [backend](dev-docs/SPEC-backend.md) | [frontend](dev-docs/SPEC-frontend.md) | [sdk](dev-docs/SPEC-sdk.md)

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Python 3.12+ with [uv](https://docs.astral.sh/uv/)
- [just](https://github.com/casey/just) (task runner)
- [Bun](https://bun.sh/) (frontend package manager, only needed for frontend dev)
- [Cargo](https://doc.rust-lang.org/cargo/) with [`inferno`](https://github.com/jonhoo/inferno) installed (`cargo install inferno`) if you want to run benchmarks

## Quick start

### 1. Start the services (production mode)

```bash
just docker-up
```

This starts four containers on an internal Docker network:
- **Caddy** reverse proxy on `localhost:80` / `localhost:443` (the only externally exposed ports)
- **Builder server** (FastAPI on port 3000, internal only)
- **Postgres** (port 5432, internal only)
- **pgweb** (read-only DB viewer on port 8080, internal only)

Caddy terminates TLS and reverse proxies all traffic to the builder server. The domain is controlled by `DOMAIN` in `infra/.env` (defaults to `localhost`).

For **local development** with all internal ports exposed on the host:

```bash
just docker-up-dev
```

This additionally exposes `localhost:3000` (builder), `localhost:5432` (Postgres), and `localhost:8080` (pgweb).

### 2. Trigger a build

Build mock OHLC data (root dataset):

```bash
curl -X POST "http://localhost/api/v1/build/mock-ohlc/0.1.0?start=2024-01-01&end=2024-01-31"
```

Build a derived dataset (automatically builds `mock-ohlc` first if data is missing):

```bash
curl -X POST "http://localhost/api/v1/build/mock-daily-close/0.1.0?start=2024-01-01&end=2024-01-31"
```

Fetch data for a dataset:

```bash
curl "http://localhost/api/v1/data/mock-ohlc/0.1.0?start=2024-01-01&end=2024-01-31"
```

These examples go through Caddy on port 80. In dev mode (`just docker-up-dev` or `just backend-dev`), replace `localhost` with `localhost:3000` to hit the builder server directly.

### 3. Verify data

Via pgweb (dev mode): open [http://localhost:8080](http://localhost:8080)

Via psql:

```bash
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U datastream -d datastream -c "SELECT * FROM datasets LIMIT 10;"
```

### 4. Stop the services

```bash
just docker-down      # production
just docker-down-dev  # dev overlay
```

To also remove the database volume (wipes all data):

```bash
docker compose -f infra/docker-compose.yml down -v
```

## Local development (without full Docker stack)

Run the backend server locally against just the Postgres container:

```bash
just backend-dev
```

This starts Postgres via Docker, runs Alembic migrations, and launches uvicorn on `localhost:3000` with hot reload.

Run the frontend dev server (proxies `/api` to `localhost:3000`):

```bash
just frontend-dev
```

This starts the Vite dev server on `localhost:5173`.

## API endpoints

All endpoints are prefixed with `/api/v1`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/status` | Health check |
| `POST` | `/api/v1/build/{name}/{version}?start=<ts>&end=<ts>` | Trigger a build for the given time range |
| `GET` | `/api/v1/data/{name}/{version}?start=<ts>&end=<ts>&build-data=<bool>` | Fetch data (optionally building missing data first) |

## Project structure

```
datastream-rs/
  .benchmarks/                  # benchmark artifacts/output (e.g. profiling flames)
  infra/                        # Docker infrastructure
    docker-compose.yml            # production (only Caddy ports exposed)
    docker-compose.dev.yml        # dev overlay (exposes internal ports)
    .env                          # environment variables
    builder/
      Dockerfile
      entrypoint.sh               # runs migrations, then starts uvicorn
    caddy/
      Caddyfile                   # reverse proxy config
    postgres/
      Dockerfile
  builders/
    server/                     # builder server (FastAPI)
      main.py                     # entrypoint (creates app, mounts routers)
      api/                        # endpoint handlers
      service/                    # build orchestration, dependency resolution
      db/                         # database connection and queries
        migrations/               # Alembic migration scripts
      runtime/                    # config loading, subprocess isolation, validation
      calendars/                  # calendar definitions (everyday, weekday, nyse-daily)
      tests/                      # unit and integration tests
    sdk/                        # Python SDK (datastream-sdk)
      datastream/                 # importable package
      tests/
    scripts/                    # builder scripts (volume-mounted into container)
      <dataset>/<version>/
        config.toml
        builder.py
        requirements.txt          # optional per-builder Python dependencies
        .env                      # optional env vars (gitignored)
  frontend/                     # Svelte + Vite frontend
  justfile                      # task runner commands
  alembic.ini                   # Alembic migration config
  pyproject.toml                # Python project config (uv)
```

## Mock datasets

| Dataset | Version | Type | Calendar | Dependencies |
|---------|---------|------|----------|-------------|
| `mock-ohlc` | `0.1.0` | Root | everyday | None |
| `mock-daily-close` | `0.1.0` | Derived | everyday | `mock-ohlc` |
| `mock-multi-ohlc` | `0.1.0` | Root | everyday | None |
| `mock-multi-close` | `0.1.0` | Derived | everyday | `mock-multi-ohlc` |
| `mock-moving-avg` | `0.1.0` | Derived | everyday | `mock-daily-close` (5d lookback) |

## Adding a new dataset

1. Create a directory at `builders/scripts/<dataset_name>/<version>/`.
2. Add a `config.toml` with `name`, `version`, `calendar`, `granularity`, `start-date`, `[schema]`, and optionally `[dependencies]`.
3. Add a `builder.py` with a `build(dependencies, timestamp)` function that returns a `list[dict]` matching the schema.
4. Optionally add a `requirements.txt` for external Python dependencies (a per-builder venv is created automatically on server startup).
5. Optionally add a `.env` file for secrets and set `env-vars = true` in `config.toml`.
6. Scripts are volume-mounted into the container, so no rebuild is needed.

Example `config.toml`:

```toml
name = "my-dataset"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
ticker = "str"
price = "float"

[dependencies]
some-dependency = "0.1.0"
other-dependency = { version = "0.1.0", lookback = "5d" }
```

Example `builder.py`:

```python
from datetime import datetime
from typing import Any


def build(
    dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime
) -> list[dict[str, Any]]:
    return [{"ticker": "AAPL", "price": 123.45}]
```
