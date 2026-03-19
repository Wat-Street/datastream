import zlib
from contextlib import contextmanager

import psycopg
import structlog

from db.connection import get_pool

logger = structlog.get_logger()

# namespace for build advisory locks, arbitrary fixed value
_LOCK_NAMESPACE = 1


def _lock_key(dataset_name: str, dataset_version: str) -> int:
    """Compute a stable int32 key from dataset name and version."""
    raw = zlib.crc32(f"{dataset_name}/{dataset_version}".encode())
    # pg_advisory_lock takes int4, crc32 returns unsigned 32-bit;
    # convert to signed range to avoid overflow
    if raw >= 0x80000000:
        raw -= 0x100000000
    return raw


@contextmanager
def build_lock(dataset_name: str, dataset_version: str):
    """Context manager that holds a session-level advisory lock for a dataset build.

    Opens a dedicated connection (not from the pool) so the lock connection
    never interferes with pool-managed query connections.
    """
    key = _lock_key(dataset_name, dataset_version)

    # get conninfo from pool to open a dedicated connection
    pool = get_pool()
    conn = psycopg.connect(pool.conninfo, autocommit=True)
    try:
        conn.execute("SELECT pg_advisory_lock(%s, %s)", (_LOCK_NAMESPACE, key))
        logger.debug(
            "advisory lock acquired",
            dataset=dataset_name,
            version=dataset_version,
            key=key,
        )
        yield conn
    finally:
        try:
            conn.execute(
                "SELECT pg_advisory_unlock(%s, %s)",
                (_LOCK_NAMESPACE, key),
            )
            logger.debug(
                "advisory lock released",
                dataset=dataset_name,
                version=dataset_version,
                key=key,
            )
        except Exception:
            logger.warning(
                "failed to release advisory lock",
                dataset=dataset_name,
                version=dataset_version,
            )
        conn.close()
