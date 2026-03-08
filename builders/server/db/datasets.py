from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from db.connection import get_conn


def get_existing_timestamps(
    dataset_name: str,
    dataset_version: str,
    start: datetime,
    end: datetime,
) -> list[datetime]:
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
            (dataset_name, dataset_version, start, end),
        )
        return [row[0] for row in cur.fetchall()]


def insert_rows(
    dataset_name: str,
    dataset_version: str,
    rows: list[tuple[datetime, dict]],
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
                    ts,
                    psycopg2.extras.Json(data),
                )
                for ts, data in rows
            ],
        )
    conn.commit()


def get_rows(
    dataset_name: str,
    dataset_version: str,
    timestamps: list[datetime],
) -> dict[datetime, dict]:
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
                timestamps,
            ),
        )
        return {row["timestamp"]: row["data"] for row in cur.fetchall()}
