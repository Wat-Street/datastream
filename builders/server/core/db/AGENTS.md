# Database (`core/db`)

- `connection.py`: connection pool management (`open_pool`, `close_pool`).
- `datasets.py`: dataset queries (existing timestamps, range/timestamp reads, bulk insert).
  `PostgresStore` in `core/service/store.py` forwards to these functions.
- `migrations/`: alembic migrations.

Only the server touches the DB; builders never do (see `core/runtime/AGENTS.md`).

## `datasets` table

Datasets are stored in Postgres in the `datasets` table. Each row is one timeseries data point
plus metadata.

Metadata columns:
- `id`: auto-increment primary key.
- `created_at`: timestamp (microsecond precision) of row creation.
- `dataset_name`: string, the dataset the row belongs to.
- `dataset_version`: string, the stringified semver (validated at the application level, no DB
  constraint).

Data columns:
- `timestamp`: the timestamp this row represents.
- `data`: JSONB key-value pairs for the row. The JSONB must not be nested more than one level.

Example decoded `data`:

```json
{"ticker": "AAPL", "open": 123, "high": 456, "low": 100, "close": 200}
```

## Uniqueness and indexing

`(dataset_name, dataset_version, timestamp)` is **not unique**: multiple rows can share the same
triple (e.g. multiple tickers at the same timestamp). A non-unique index on
`(dataset_name, dataset_version, timestamp)` keeps range queries fast.

Timestamps are stored with microsecond precision (via `pandas.Timestamp`); in practice the finest
granularity used is per-second.

TODO: partition the table by `dataset_name` or time range for performance at scale.
