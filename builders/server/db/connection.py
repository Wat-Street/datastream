import os
from contextlib import contextmanager

import psycopg2
import structlog

logger = structlog.get_logger()

_pool = None


@contextmanager
def get_conn():
    """Get a database connection (simple single-connection approach)."""
    global _pool
    if _pool is None or _pool.closed:
        logger.debug("creating new database connection")
        _pool = psycopg2.connect(os.environ["DATABASE_URL"])
    yield _pool
