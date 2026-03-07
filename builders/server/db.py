import os

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

_pool = None


def get_conn():
    """Get a database connection (simple single-connection approach for MVP)."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.connect(os.environ["DATABASE_URL"])
    return _pool


def get_existing_timestamps(
    dataset_name: str,
    dataset_version: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[pd.Timestamp]:
    """Query all timestamps in [start, end] that already have rows for this dataset."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp FROM datasets
            WHERE dataset_name = %s
              AND dataset_version = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp
            """,
            (dataset_name, dataset_version, start.to_pydatetime(), end.to_pydatetime()),
        )
        return [pd.Timestamp(row[0]) for row in cur.fetchall()]


def insert_rows(
    dataset_name: str,
    dataset_version: str,
    rows: list[tuple[pd.Timestamp, dict]],
) -> None:
    """Bulk insert rows into the datasets table."""
    if not rows:
        return
    conn = get_conn()
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO datasets (dataset_name, dataset_version, timestamp, data)
            VALUES %s
            """,
            [
                (
                    dataset_name,
                    dataset_version,
                    ts.to_pydatetime(),
                    psycopg2.extras.Json(data),
                )
                for ts, data in rows
            ],
        )
    conn.commit()


def get_rows(
    dataset_name: str,
    dataset_version: str,
    timestamps: list[pd.Timestamp],
) -> dict[pd.Timestamp, dict]:
    """Fetch data for specific timestamps."""
    if not timestamps:
        return {}
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT timestamp, data FROM datasets
            WHERE dataset_name = %s
              AND dataset_version = %s
              AND timestamp = ANY(%s)
            """,
            (
                dataset_name,
                dataset_version,
                [ts.to_pydatetime() for ts in timestamps],
            ),
        )
        return {pd.Timestamp(row["timestamp"]): row["data"] for row in cur.fetchall()}
