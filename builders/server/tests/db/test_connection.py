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


@patch("db.connection.ThreadedConnectionPool")
def test_open_pool_creates_pool(mock_pool_cls: MagicMock) -> None:
    """open_pool initializes ThreadedConnectionPool."""
    connection.open_pool("postgresql://test@localhost/test", minconn=1, maxconn=5)
    mock_pool_cls.assert_called_once_with(1, 5, "postgresql://test@localhost/test")
    assert connection._pool is mock_pool_cls.return_value


@patch("db.connection.ThreadedConnectionPool")
def test_close_pool_closes_and_clears(mock_pool_cls: MagicMock) -> None:
    """close_pool calls closeall and sets _pool to None."""
    connection.open_pool("postgresql://test@localhost/test")
    mock_pool = mock_pool_cls.return_value

    connection.close_pool()
    mock_pool.closeall.assert_called_once()
    assert connection._pool is None


def test_close_pool_noop_when_not_initialized() -> None:
    """close_pool does nothing if pool was never opened."""
    connection.close_pool()
    assert connection._pool is None


def test_get_conn_raises_without_pool() -> None:
    """get_conn raises RuntimeError if pool not initialized."""
    with (
        pytest.raises(RuntimeError, match="connection pool not initialized"),
        connection.get_conn(),
    ):
        pass


@patch("db.connection.ThreadedConnectionPool")
def test_get_conn_checks_out_and_returns(mock_pool_cls: MagicMock) -> None:
    """get_conn gets a connection from pool and puts it back."""
    mock_pool = mock_pool_cls.return_value
    mock_conn = MagicMock()
    mock_pool.getconn.return_value = mock_conn

    connection.open_pool("postgresql://test@localhost/test")

    with connection.get_conn() as conn:
        assert conn is mock_conn
        mock_pool.getconn.assert_called_once()
        # not returned yet
        mock_pool.putconn.assert_not_called()

    # returned after exiting context
    mock_pool.putconn.assert_called_once_with(mock_conn)


@patch("db.connection.ThreadedConnectionPool")
def test_get_conn_returns_on_exception(mock_pool_cls: MagicMock) -> None:
    """Connection is returned to pool even if an exception occurs."""
    mock_pool = mock_pool_cls.return_value
    mock_conn = MagicMock()
    mock_pool.getconn.return_value = mock_conn

    connection.open_pool("postgresql://test@localhost/test")

    with (
        pytest.raises(ValueError, match="test error"),
        connection.get_conn(),
    ):
        raise ValueError("test error")

    mock_pool.putconn.assert_called_once_with(mock_conn)
