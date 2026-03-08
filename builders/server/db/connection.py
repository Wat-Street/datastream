import os

import psycopg2

_pool = None


def get_conn():
    """Get a database connection (simple single-connection approach for MVP)."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.connect(os.environ["DATABASE_URL"])
    return _pool
