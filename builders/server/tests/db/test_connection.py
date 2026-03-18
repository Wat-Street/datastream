from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from db import connection


@pytest.fixture(autouse=True)
def reset_pool() -> Generator[None, None, None]:
    """Reset connection._pool between tests."""
    connection._pool = None
    yield
    connection._pool = None


@patch("db.connection.ConnectionPool")
def test_open_pool_creates_pool(mock_pool_cls: MagicMock) -> None:
    """open_pool initializes ConnectionPool."""
    connection.open_pool("postgresql://test@localhost/test", min_size=1, max_size=5)
    mock_pool_cls.assert_called_once_with(
        "postgresql://test@localhost/test",
        min_size=1,
        max_size=5,
        open=True,
    )
    assert connection._pool is mock_pool_cls.return_value


@patch("db.connection.ConnectionPool")
def test_close_pool_closes_and_clears(mock_pool_cls: MagicMock) -> None:
    """close_pool calls close and sets _pool to None."""
    connection.open_pool("postgresql://test@localhost/test")
    mock_pool = mock_pool_cls.return_value

    connection.close_pool()
    mock_pool.close.assert_called_once()
    assert connection._pool is None


def test_close_pool_noop_when_not_initialized() -> None:
    """close_pool does nothing if pool was never opened."""
    connection.close_pool()
    assert connection._pool is None


def test_get_conn_raises_without_pool() -> None:
    """get_conn raises RuntimeError if pool not initialized."""
    with pytest.raises(RuntimeError, match="connection pool not initialized"):
        connection.get_conn()


@patch("db.connection.ConnectionPool")
def test_get_conn_delegates_to_pool(mock_pool_cls: MagicMock) -> None:
    """get_conn returns pool.connection() context manager."""
    mock_pool = mock_pool_cls.return_value

    connection.open_pool("postgresql://test@localhost/test")
    result = connection.get_conn()

    mock_pool.connection.assert_called_once()
    assert result is mock_pool.connection.return_value
