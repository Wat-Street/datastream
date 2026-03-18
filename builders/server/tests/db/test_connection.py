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


@patch("db.connection.psycopg2.connect")
def test_get_conn_reads_database_url(
    mock_connect: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Connects using DATABASE_URL env var."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    with connection.get_conn() as conn:
        mock_connect.assert_called_once_with("postgresql://test:test@localhost/test")
        assert conn is mock_conn


@patch("db.connection.psycopg2.connect")
def test_get_conn_reuses_connection(
    mock_connect: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Second call reuses existing connection."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_connect.return_value = mock_conn

    with connection.get_conn():
        pass
    with connection.get_conn():
        pass
    mock_connect.assert_called_once()


@patch("db.connection.psycopg2.connect")
def test_get_conn_reconnects_when_closed(
    mock_connect: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconnects if connection is closed."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    mock_conn1 = MagicMock()
    mock_conn1.closed = False
    mock_conn2 = MagicMock()
    mock_conn2.closed = False
    mock_connect.side_effect = [mock_conn1, mock_conn2]

    with connection.get_conn():
        pass
    # simulate closed connection
    mock_conn1.closed = True
    with connection.get_conn() as conn2:
        assert mock_connect.call_count == 2
        assert conn2 is mock_conn2
