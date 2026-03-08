from unittest.mock import MagicMock, patch

import pandas as pd

from db import datasets


@patch("db.datasets.get_conn")
def test_get_existing_timestamps(mock_get_conn: MagicMock) -> None:
    """Returns list[pd.Timestamp] from cursor rows."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (pd.Timestamp("2024-01-01").to_pydatetime(),),
        (pd.Timestamp("2024-01-02").to_pydatetime(),),
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.return_value = mock_conn

    result = datasets.get_existing_timestamps(
        "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31")
    )
    assert len(result) == 2
    assert result[0] == pd.Timestamp("2024-01-01")
    assert result[1] == pd.Timestamp("2024-01-02")


@patch("db.datasets.get_conn")
def test_get_existing_timestamps_empty(mock_get_conn: MagicMock) -> None:
    """No rows returns empty list."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.return_value = mock_conn

    result = datasets.get_existing_timestamps(
        "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31")
    )
    assert result == []


@patch("db.datasets.get_conn")
def test_insert_rows_empty_returns_early(mock_get_conn: MagicMock) -> None:
    """Empty rows list skips DB call."""
    datasets.insert_rows("ds", "0.1.0", [])
    mock_get_conn.assert_not_called()


@patch("db.datasets.execute_values")
@patch("db.datasets.get_conn")
def test_insert_rows_calls_execute_values(
    mock_get_conn: MagicMock, mock_exec_values: MagicMock
) -> None:
    """Verify execute_values is called with correct SQL and args."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.return_value = mock_conn

    rows = [(pd.Timestamp("2024-01-01"), {"ticker": "AAPL"})]
    datasets.insert_rows("ds", "0.1.0", rows)

    mock_exec_values.assert_called_once()
    args = mock_exec_values.call_args
    assert "INSERT INTO datasets" in args[0][1]


@patch("db.datasets.get_conn")
def test_get_rows_empty_timestamps_returns_empty_dict(mock_get_conn: MagicMock) -> None:
    """Empty input returns empty dict."""
    result = datasets.get_rows("ds", "0.1.0", [])
    assert result == {}
    mock_get_conn.assert_not_called()


@patch("db.datasets.get_conn")
def test_get_rows_returns_mapped_data(mock_get_conn: MagicMock) -> None:
    """Returns dict[pd.Timestamp, dict] correctly."""
    ts = pd.Timestamp("2024-01-01")
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {"timestamp": ts.to_pydatetime(), "data": {"ticker": "AAPL", "price": 100}},
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.return_value = mock_conn

    result = datasets.get_rows("ds", "0.1.0", [ts])
    assert ts in result
    assert result[ts] == {"ticker": "AAPL", "price": 100}
