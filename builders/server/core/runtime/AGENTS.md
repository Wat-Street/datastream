# Runtime (`core/runtime`)

Config loading, subprocess isolation, schema validation, and per-builder venv management.

- `config.py`: parses and validates `config.toml` into `DatasetConfig`; owns `SCRIPTS_DIR`.
- `registry.py`: startup preload + in-memory config registry; validates the dependency graph
  acyclic and granularity-consistent at load time.
- `loader.py`: dynamically imports a builder module.
- `runner.py`: spawns the builder subprocess with retry.
- `serialization.py`: JSON serialization for subprocess IPC (docstring covers the format).
- `validator.py`: validates builder output against the `[schema]` section.
- `venv_management.py`: per-builder venv creation and caching (docstring covers the details).

## Subprocess execution model

Builders never touch the DB (enforced by convention for now; a runtime guard is a TODO). Each
builder runs in its own `subprocess.Popen` process, communicating via JSON over stdin/stdout, so
a crashing builder cannot bring down the server. The standalone worker script is
`builders/server/workers/subprocess_worker.py`: it is stdlib-only (so it runs in any venv),
deserializes input, imports and runs the builder, and serializes output. It has no dependency on
server code.

1. `runner.py` serializes builder inputs (dependencies, timestamp, paths, env file) to JSON via
   `serialization.py`.
2. The subprocess runs `subprocess_worker.py` using the builder's venv python (or system python).
3. `subprocess_worker.py` deserializes, runs the builder, serializes the result.
4. `runner.py` deserializes the JSON response from stdout.

**Venv detection**: at build time `runner.py` checks if `script_dir/.venv/bin/python` exists and
uses it; otherwise it falls back to `sys.executable`.

## Schema validation

After a builder returns its output list, `validator.py` validates each dict against the
`[schema]` section of `config.toml` before insert: all declared keys present, values match
declared types. Correctness is prioritized over performance.

## Per-builder venvs

Each builder may declare dependencies via a `requirements.txt` in its directory. Venvs are
created eagerly on startup: the `lifespan` handler calls `setup_builder_venvs()`, which scans
builder directories and creates a `.venv/` for any with a `requirements.txt`. Creation is skipped
if `.venv/.requirements_hash` (a crc32 of `requirements.txt`) matches the current file, so
changing `requirements.txt` triggers a rebuild on next startup. `uv` is used (`uv venv`,
`uv pip install`). Builders without a `requirements.txt` get no venv and use system Python. A
single builder's venv failure logs a warning and does not block others.

## Environment variables

`env-vars` in `config.toml` is optional (default `false`). When `true`, the server loads a
`.env` file from the builder directory and injects it into the subprocess. Validation happens at
build time, not config load, so CI can load configs for `env-vars = true` datasets without the
`.env` present; a missing `.env` at build time raises `FileNotFoundError`. The parent process
never reads the values: the `.env` is parsed inside the subprocess only (a minimal stdlib parser
in `subprocess_worker.py`), so secrets never enter parent memory.

## Retry

Builder subprocess execution is wrapped in `retry_with_backoff()` (`core/utils/retry.py`).
Constants in `runner.py`: `RETRY_MAX_RETRIES = 5`, `RETRY_INITIAL_DELAY = 2.0`,
`RETRY_BACKOFF_FACTOR = 2.0` (delays 2s, 4s, 8s, 16s, 32s). Retried: all subprocess failures
(`TimeoutExpired`, crashes, `WorkerError`). Not retried: payload serialization (deterministic,
runs once), schema validation (runs after the builder returns), dependency resolution (runs
before). Each attempt logs a structlog warning. Integration tests monkeypatch the constants in
`tests/integration/conftest.py` (`MAX_RETRIES=3`, `INITIAL_DELAY=0.01`) to stay fast while
preserving retry semantics.
