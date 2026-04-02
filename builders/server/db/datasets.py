from collections import defaultdict
from datetime import datetime

import structlog
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from utils.semver import SemVer

from db.connection import get_conn

logger = structlog.get_logger()


def get_existing_timestamps(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> list[datetime]:
    """Query all timestamps in [start, end] that already have rows for this dataset."""
    logger.debug(
        "querying existing timestamps",
        dataset=dataset_name,
        version=str(dataset_version),
    )
    with get_conn() as conn, conn.cursor() as cur:
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
    values = [
        (dataset_name, dataset_version_str, ts, Jsonb(data))
        for ts, data_list in rows
        for data in data_list
    ]
    with get_conn() as conn, conn.transaction():
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO datasets (dataset_name, dataset_version, timestamp, data)
            VALUES (%s, %s, %s, %s)
            """,
            values,
        )

    total = sum(len(data_list) for _, data_list in rows)
    logger.info(
        "rows inserted",
        dataset=dataset_name,
        version=dataset_version_str,
        count=total,
    )


def get_rows_range(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> dict[datetime, list[dict]]:
    """Fetch data for a time range [start, end], keyed by timestamp."""
    logger.debug(
        "querying rows by range",
        dataset=dataset_name,
        version=str(dataset_version),
    )
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT timestamp, data FROM datasets
            WHERE dataset_name = %s
              AND dataset_version = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp
            """,
            (dataset_name, str(dataset_version), start, end),
        )
        result: dict[datetime, list[dict]] = defaultdict(list)
        for row in cur.fetchall():
            result[row["timestamp"]].append(row["data"])
        return dict(result)


def get_datasets_with_data() -> set[tuple[str, str]]:
    """Return (name, version) pairs for all datasets that have at least one row."""
    logger.debug("querying datasets with data")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT dataset_name, dataset_version FROM datasets")
        return {(row[0], row[1]) for row in cur.fetchall()}


def get_rows_timestamps(
    dataset_name: str,
    dataset_version: SemVer,
    timestamps: list[datetime],
) -> dict[datetime, list[dict]]:
    """Fetch data for specific timestamps, returning a list of dicts per timestamp."""
    if not timestamps:
        return {}

    dataset_version_str = str(dataset_version)
    logger.debug(
        "querying rows by timestamps",
        dataset=dataset_name,
        version=dataset_version_str,
    )
    with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
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
