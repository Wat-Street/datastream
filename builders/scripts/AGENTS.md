# Datasets and builders (`builders/scripts`)

Datasets are timeseries of `(timestamp, data)` pairs, uniquely identified by a `(name, version)`
pair where version is a SemVer (e.g. `1.1.12`). A dataset may depend on other datasets (e.g. a
5-day moving average depends on daily pricing). Datasets with no dependencies are root datasets.

Builders are stateless Python scripts. Each dataset has exactly one builder that builds only that
dataset. The server dynamically imports and runs builders in isolated subprocesses (see
`builders/server/core/runtime/AGENTS.md`). Builder scripts are written by internal users; their
code is trusted, but they are not trusted to never crash, so each runs in its own subprocess.

## Directory layout

```
builders/scripts/<dataset_name>/<version>/
  builder.py            builder script (required)
  config.toml           dataset config (required)
  requirements.txt      python dependencies (optional)
  .env                  environment variables (optional, gitignored)
  .env.template         documents required env vars (optional)
  .venv/                per-builder venv (auto-created, gitignored)
```

Other Python files may live alongside and are imported via the module system.

## `builder.py`

```python
from datetime import datetime
from typing import Any

def build(dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime) -> list[dict[str, Any]]:
    return [{"ticker": "AAPL", "price": 123}]
```

- `timestamp`: a `datetime.datetime` with microsecond precision.
- `dependencies`: maps each dependency's **name** (not name+version) to a dict keyed by
  timestamp, each value a list of data dicts. Without lookback the dict holds only the current
  timestamp. With lookback it holds all timestamps in the window `[T - lookback + step, T]`
  (N points inclusive). Versions are resolved by the server, so builders never reference them.
- Return value: a list of dicts, one row per dict. Single-row datasets return a length-1 list.

## `config.toml`

```toml
name = "dataset name"
version = "0.0.1"
builder = "builder.py"        # optional
calendar = "NYSE"             # valid timestamps for this dataset (required)
granularity = "1d"            # smallest time step, e.g. "1s", "1m", "1d"
start-date = "2020-01-01"     # earliest date data can be built for, YYYY-MM-DD (required)
env-vars = true               # optional, default false; load .env into the subprocess

[schema]
ticker = "str"
price = "int"

[dependencies]
dependency-a = "0.0.2"                                    # simple: version only, no lookback
dependency-b = { version = "0.0.1", lookback = "5d" }    # with lookback window
```

The `[schema]` section drives runtime validation: after a builder returns, each dict is checked
for all declared keys with matching types before insert.

## Start date

`start-date` defines the earliest buildable date. At build time, if `end` is before it a
`ValueError` is raised; if `start` is before it, `start` is clamped with a warning. The field is
required and validated at config load.

## Granularity

Each dataset declares a `granularity`. A dataset may only depend on another whose granularity is
finer or equal to its own (e.g. a daily dataset may depend on per-second or daily data, but an
hourly dataset cannot depend on a daily one). Enforced at build time.

## Lookback dependencies

Some datasets need a window of historical data from a dependency (e.g. a moving average needs the
last 5 days of closes).

- Format: simple `dep = "0.1.0"` (no lookback), or `dep = {version = "0.1.0", lookback = "5d"}`.
- Duration units: `d` (days), `h` (hours), `m` (minutes), `s` (seconds); must be positive.
- Semantics: `lookback = "5d"` means an inclusive window of 5 points, `[T - 4d, T]`. After
  parsing, deps are normalized to `{"version": str, "lookback_subtract": timedelta | None}`,
  where `lookback_subtract` is `amount - 1` units (e.g. `timedelta(days=4)` for "5d").
- Build-range expansion: when building a dependency recursively, the server expands its start via
  `dep_start = start - lookback_subtract` so history covers the inclusive window.
- Edge case: near a dependency's `start-date` the window may return fewer points; builders must
  handle short windows gracefully.

## Environment variables

Set `env-vars = true` to have the server load a `.env` from the builder directory into the
subprocess. The `.env` is gitignored and parsed only inside the subprocess, so secrets never
reach the parent process. Commit a `.env.template` to document required variables (the server does
not read it). See `builders/server/core/runtime/AGENTS.md` for the runtime behavior.

## Mock builders

For testing and development:
- `mock-ohlc/0.1.0`: root, single-row OHLC for AAPL per timestamp.
- `mock-daily-close/0.1.0`: depends on mock-ohlc, extracts the close (single row).
- `mock-multi-ohlc/0.1.0`: root, multi-row OHLC for AAPL, MSFT, GOOG per timestamp.
- `mock-multi-close/0.1.0`: depends on mock-multi-ohlc, extracts close per ticker (multi-row).
- `mock-moving-avg/0.1.0`: depends on mock-daily-close with `lookback = "5d"`, 5-day moving avg.
