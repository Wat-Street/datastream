# Build pipeline (`core/service`)

Build execution is split into three layers. Each module's docstring is the source of truth for
its mechanics; read them before changing behavior. This file carries only the cross-file
invariants that no single module owns.

- `scheduler.py`: computes a topological build plan (graph collection via DFS, level ordering
  via Kahn's algorithm). Its docstring documents the Node/Edge/Range model and diamond-dependency
  handling.
- `worker.py`: `execute_job()` builds one dataset over one time range.
- `orchestrator.py`: `run_build()` executes the plan level by level (barrier model).
- `store.py`: the `Store` ABC and its two backends (`PostgresStore`, `MemoryStore`).
- `models.py`: `JobDescriptor`, `JobResult`, `BuildPlan` data types.
- `timestamps.py`: `generate_timestamps()`, `NoValidTimestampsError`.
- `locks.py`: per-dataset `threading.Lock` registry.
- `catalog.py`: `list_datasets()` backing `GET /datasets`.
- `builder.py`: the public entry points `build_dataset()` and `get_data()`.

`build_dataset()` is the single boundary that reads the `dry_run` flag and constructs the store,
then delegates to `run_build()`. Everything below takes a ready-made `store` and never sees the
flag, so build logic is identical for real and dry runs.

## Store abstraction and dry-run

All data access during a build (reads and writes) goes through the `Store` interface, so the
same build logic runs against Postgres for real builds and an in-memory store for dry runs.

- `PostgresStore` (real builds): forwards each data method to `core.db.datasets`. `build_lock`
  returns the shared per-dataset lock from `locks.py`.
- `MemoryStore` (dry runs): a request-private dict `{(name, version): {timestamp: [rows]}}`.
  `insert_rows` round-trips each row through `json.dumps`/`loads` to mirror Postgres `Jsonb`
  serialization, so non-serializable builder output still fails. It never opens a DB connection,
  and `build_lock` returns a `nullcontext`.

A dry run rebuilds the whole dependency graph in isolation (the store starts empty, so it never
reads real committed data), takes no shared lock (it cannot corrupt real data), and needs no
cleanup (the `MemoryStore` is garbage-collected when the request ends, even on crash).

## Commit model and atomicity

- **Per-level commit**: all jobs in level N insert to the store before level N+1 starts.
  Workers read dependency data back from the same store, so the hand-off `A -> store -> B` works
  identically for Postgres and the dry-run `MemoryStore`. If level N fails, levels 0..N-1 remain
  committed (real builds) or are simply retained in the discarded `MemoryStore` (dry runs).
- **Per-job atomicity**: each job accumulates rows in memory and bulk-inserts only if all its
  timestamps succeed. If any timestamp fails, no rows are inserted for that job.

## Overwrite policy

Before building a range, the worker queries the DB for distinct timestamps that already have any
rows for the dataset. If a timestamp has any rows it is considered fully built and skipped. Only
missing timestamps are built and inserted; existing rows are never overwritten. Writes are plain
`INSERT` (no upsert).

## Concurrency and locking

The service runs a single uvicorn worker; FastAPI dispatches sync handlers to a thread pool, so
concurrent requests for the same dataset can race between the "check missing" read and the
"insert" write. A `threading.Lock` per `(dataset_name, dataset_version)` (in `locks.py`)
serializes the critical section of `execute_job()`: check existing timestamps, compute missing,
build, validate, insert. Different datasets build concurrently; the same dataset serializes.
Timestamp generation runs outside the lock.

**Deadlock safety**: the dependency graph is validated acyclic at startup by
`registry.load_all_configs()`. The orchestrator executes levels in topological order
(dependencies before dependents), so lock ordering follows the sort and cannot deadlock.

**Scaling assumption**: single uvicorn worker (single process). Multi-worker deployments would
require Postgres advisory locks or similar.

## Start date and granularity rules

- Each dataset declares `start-date` in `config.toml`. At build time, if the request `end` is
  before `start-date` a `ValueError` is raised; if `start` is before `start-date`, `start` is
  clamped to `start-date` with a warning log (in the scheduler).
- A dataset may only depend on another whose granularity is finer or equal to its own. Enforced
  at build time by `validate_dependency_graph()` before any data is built.

Lookback dependency semantics (build-range expansion by `lookback_subtract`) are documented in
`builders/scripts/AGENTS.md` alongside the `config.toml` format.
