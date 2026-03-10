from collections import defaultdict
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from utils.semver import SemVer

from db.connection import get_conn


def get_existing_timestamps(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> list[datetime]:
    """Query all timestamps in [start, end] that already have rows for this dataset."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT timestamp FROM datasets
            WHERE dataset_name = %s
              AND dataset_version = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp
            """,
            (dataset_name, str(dataset_version), start, end),
        )
        return [row[0] for row in cur.fetchall()]


def insert_rows(
    dataset_name: str,
    dataset_version: SemVer,
    rows: list[tuple[datetime, list[dict]]],
) -> None:
    """Bulk insert rows into the datasets table.

    Each entry is a (timestamp, list[dict]) pair where the list contains one or more
    data dicts to insert for that timestamp.
    """
    if not rows:
        return

    dataset_version_str = str(dataset_version)
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
                    dataset_version_str,
                    ts,
                    psycopg2.extras.Json(data),
                )
                for ts, data_list in rows
                for data in data_list
            ],
        )
    conn.commit()


def get_rows(
    dataset_name: str,
    dataset_version: SemVer,
    timestamps: list[datetime],
) -> dict[datetime, list[dict]]:
    """Fetch data for specific timestamps, returning a list of dicts per timestamp."""
    if not timestamps:
        return {}

    dataset_version_str = str(dataset_version)
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
                dataset_version_str,
                timestamps,
            ),
        )
        result: dict[datetime, list[dict]] = defaultdict(list)
        for row in cur.fetchall():
            result[row["timestamp"]].append(row["data"])
        return dict(result)
