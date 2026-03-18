from contextlib import contextmanager

import structlog
from psycopg2.pool import ThreadedConnectionPool

logger = structlog.get_logger()

_pool: ThreadedConnectionPool | None = None


def open_pool(dsn: str, minconn: int = 2, maxconn: int = 10) -> None:
    """Initialize the connection pool."""
    global _pool
    logger.info("opening connection pool", minconn=minconn, maxconn=maxconn)
    _pool = ThreadedConnectionPool(minconn, maxconn, dsn)


def close_pool() -> None:
    """Close the connection pool and release all connections."""
    global _pool
    if _pool is not None:
        logger.info("closing connection pool")
        _pool.closeall()
        _pool = None


@contextmanager
def get_conn():
    """Check out a connection from the pool, return it when done."""
    if _pool is None:
        raise RuntimeError("connection pool not initialized, call open_pool() first")
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)
