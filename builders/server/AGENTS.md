# Backend map (`builders/server`)

A single Python FastAPI service serves public API traffic and runs builds. It listens on
port 3000. `main.py` is the uvicorn entrypoint (`main:app`): it creates the app, mounts the
routers from `core/api`, and runs a `lifespan` handler that loads api keys, calls
`load_all_configs(SCRIPTS_DIR)` + `setup_builder_venvs(SCRIPTS_DIR)`, and opens the DB pool.

Detailed docs live co-located with the code they describe (this file plus the per-package
`AGENTS.md` files below). Read the one nearest to what you are changing. Per-function
mechanics live in module and function docstrings, which are the source of truth for "what a
function does". These files carry the cross-cutting "why".

## Layer layout

Dependencies flow strictly downward: `api -> service -> db/runtime`. No layer imports upward.

```
builders/server/
  main.py            entrypoint: FastAPI app, router mounts, lifespan, request-id middleware
  log_config.py      central structlog configuration
  core/
    api/             endpoint handlers (APIRouter). See core/api/AGENTS.md
    auth/            api-key hashing + verify_api_key dependency + mint CLI. See core/auth/AGENTS.md
    service/         build orchestration, scheduling, execution, stores. See core/service/AGENTS.md
    runtime/         config loading, subprocess isolation, schema validation, venvs. See core/runtime/AGENTS.md
    db/              connection pool, dataset queries, migrations. See core/db/AGENTS.md
    calendars/       calendar ABC + registry + definitions. See core/calendars/AGENTS.md
    utils/           shared helpers (retry.py, semver.py)
  workers/           subprocess_worker.py: stdlib-only builder worker (see core/runtime/AGENTS.md)
  benchmarks/        end-to-end build benchmarks (see "Benchmarking" below)
  tests/             mirrors the layer structure
```

Authoring datasets and builder scripts is documented in `builders/scripts/AGENTS.md`.
Containers, Caddy, and infra live in `infra/AGENTS.md`.

**Scripts directory resolution**: `SCRIPTS_DIR` in `core/runtime/config.py` defaults to a path
relative to the source file (`../scripts` from the server package root). This works in Docker
where scripts are volume-mounted at `/app/scripts`. For local dev the scripts live at
`builders/scripts` (a sibling of `builders/server`), so the `SCRIPTS_DIR` env var overrides the
default. `just backend-dev` sets this automatically.

## Logging

The server uses `structlog`. Configuration lives in `log_config.py`, called once at import
time in `main.py`.

**Processor pipeline**: `merge_contextvars` -> `add_log_level` -> `TimeStamper(iso)` -> renderer.
The renderer is `ConsoleRenderer` by default, or `JSONRenderer` when `LOG_FORMAT=json` is set.

**stdlib integration**: stdlib `logging` is routed through structlog via `ProcessorFormatter`,
so uvicorn logs flow through the same pipeline.

**Request context**: a FastAPI middleware in `main.py` clears contextvars per request and binds
a unique `request_id`. The build and data endpoints also bind `dataset_name` and `version`.

**What is logged**:
- `main.py`: api key count at startup (info)
- `core/auth`: matched `team` label bound to the request context on each authenticated request
- `core/api/routes.py`: build and data-fetch failures (exception)
- `core/service/scheduler.py`: start-date clamping (warning)
- `core/service/orchestrator.py`: build plan summary (info), level start/complete (info)
- `core/service/worker.py`: skipped builds (info), build progress (info), insert counts (info)
- `core/db/connection.py`: new connections (debug)
- `core/db/datasets.py`: query execution (debug), rows inserted (info)
- `core/runtime/runner.py`: subprocess start/complete (info), stderr (warning), timeouts/crashes (error)
- `core/runtime/registry.py`: config loaded during startup preload (debug)
- `core/runtime/loader.py`: builder script imported (debug)
- `core/runtime/venv_management.py`: venv creation progress (info), failures (exception)

## CI

Backend checks run in `.github/workflows/backend-ci.yml`. The workflow triggers on
`builders/**`, `pyproject.toml`, `uv.lock`, and `.github/workflows/backend-ci.yml`, keeping
backend CI scoped to builder and shared Python dependency changes.

## Benchmarking

End-to-end build benchmarks live under `builders/server/benchmarks/`. They measure wall-clock
time for a full build request through the server (HTTP handler -> dependency resolution ->
subprocess spawns -> DB insert) using a testcontainer postgres so the real database is not
touched.

- `just bench`: `pytest-benchmark` over a 90-day `mock-ohlc` build, 3 rounds, timing table.
- `just bench-profile [DAYS]`: wraps the standalone `benchmarks/bench_build.py` with
  `py-spy record --subprocesses`, producing `bench-flamegraph.svg`. The `--subprocesses` flag
  captures the builder subprocess spawns, which dominate build time for simple datasets.

The benchmark test uses `benchmark.pedantic(rounds=3, warmup_rounds=0)`: explicit rounds because
each round is expensive, no warmup because every round must do real work (the DB is truncated
between rounds by the `clean_db` fixture). The standalone `__main__` script does the same
testcontainer setup without pytest so it can be wrapped directly by `py-spy`; module patching is
done via direct attribute assignment rather than `monkeypatch`.
