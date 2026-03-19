import structlog
from psycopg_pool import ConnectionPool

logger = structlog.get_logger()

_pool: ConnectionPool | None = None


def open_pool(conninfo: str, min_size: int = 2, max_size: int = 10) -> None:
    """Initialize the connection pool."""
    global _pool
    logger.info("opening connection pool", min_size=min_size, max_size=max_size)
    _pool = ConnectionPool(conninfo, min_size=min_size, max_size=max_size, open=True)


def close_pool() -> None:
    """Close the connection pool and release all connections."""
    global _pool
    if _pool is not None:
        logger.info("closing connection pool")
        _pool.close()
        _pool = None


def get_conn():
    """Check out a connection from the pool, return it when done."""
    if _pool is None:
        raise RuntimeError("connection pool not initialized, call open_pool() first")
    return _pool.connection()


def get_pool() -> ConnectionPool:
    """Return the connection pool, raising if not initialized."""
    if _pool is None:
        raise RuntimeError("connection pool not initialized, call open_pool() first")
    return _pool
