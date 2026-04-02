from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

from db import datasets
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")


def _mock_get_conn(mock_conn):
    """Create a context-manager mock for get_conn that yields mock_conn."""

    @contextmanager
    def _get_conn():
        yield mock_conn

    return _get_conn


@patch("db.datasets.get_conn")
def test_get_existing_timestamps(mock_get_conn: MagicMock) -> None:
    """Returns list[datetime] from cursor rows."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (datetime(2024, 1, 1),),
        (datetime(2024, 1, 2),),
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    result = datasets.get_existing_timestamps(
        "ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 31)
    )
    assert len(result) == 2
    assert result[0] == datetime(2024, 1, 1)
    assert result[1] == datetime(2024, 1, 2)

    # verify DISTINCT is in the query
    executed_sql = mock_cursor.execute.call_args[0][0]
    assert "DISTINCT" in executed_sql


@patch("db.datasets.get_conn")
def test_get_existing_timestamps_empty(mock_get_conn: MagicMock) -> None:
    """No rows returns empty list."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    result = datasets.get_existing_timestamps(
        "ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 31)
    )
    assert result == []


@patch("db.datasets.get_conn")
def test_insert_rows_empty_returns_early(mock_get_conn: MagicMock) -> None:
    """Empty rows list skips DB call."""
    datasets.insert_rows("ds", V010, [])
    mock_get_conn.assert_not_called()


@patch("db.datasets.get_conn")
def test_insert_rows_calls_executemany(mock_get_conn: MagicMock) -> None:
    """Verify executemany is called with correct SQL and args."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.transaction.return_value.__enter__ = MagicMock()
    mock_conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    rows: list[tuple[datetime, list[dict]]] = [
        (datetime(2024, 1, 1), [{"ticker": "AAPL"}, {"ticker": "MSFT"}])
    ]
    datasets.insert_rows("ds", V010, rows)

    mock_cursor.executemany.assert_called_once()
    args = mock_cursor.executemany.call_args
    assert "INSERT INTO datasets" in args[0][0]
    # two data dicts should be flattened into two insert tuples
    insert_tuples = args[0][1]
    assert len(insert_tuples) == 2


@patch("db.datasets.get_conn")
def test_get_rows_timestamps_empty_timestamps_returns_empty_dict(
    mock_get_conn: MagicMock,
) -> None:
    """Empty input returns empty dict."""
    result = datasets.get_rows_timestamps("ds", V010, [])
    assert result == {}
    mock_get_conn.assert_not_called()


@patch("db.datasets.get_conn")
def test_get_rows_timestamps_returns_list_per_timestamp(
    mock_get_conn: MagicMock,
) -> None:
    """Returns dict[datetime, list[dict]] with multiple rows aggregated."""
    ts = datetime(2024, 1, 1)
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {"timestamp": ts, "data": {"ticker": "AAPL", "price": 100}},
        {"timestamp": ts, "data": {"ticker": "MSFT", "price": 200}},
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    result = datasets.get_rows_timestamps("ds", V010, [ts])
    assert ts in result
    assert len(result[ts]) == 2
    assert result[ts][0] == {"ticker": "AAPL", "price": 100}
    assert result[ts][1] == {"ticker": "MSFT", "price": 200}


@patch("db.datasets.get_conn")
def test_get_rows_range_returns_dict_by_timestamp(mock_get_conn: MagicMock) -> None:
    """Returns dict[datetime, list[dict]] for a time range."""
    ts1 = datetime(2024, 1, 1)
    ts2 = datetime(2024, 1, 2)
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {"timestamp": ts1, "data": {"val": 10}},
        {"timestamp": ts1, "data": {"val": 11}},
        {"timestamp": ts2, "data": {"val": 20}},
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    result = datasets.get_rows_range("ds", V010, ts1, ts2)
    assert len(result) == 2
    assert len(result[ts1]) == 2
    assert len(result[ts2]) == 1
    assert result[ts1][0] == {"val": 10}
    assert result[ts2][0] == {"val": 20}


@patch("db.datasets.get_conn")
def test_get_datasets_with_data_returns_set(mock_get_conn: MagicMock) -> None:
    """Returns set of (name, version) tuples from cursor rows."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("mock-ohlc", "0.1.0"),
        ("mock-daily-close", "0.1.0"),
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    result = datasets.get_datasets_with_data()
    assert result == {("mock-ohlc", "0.1.0"), ("mock-daily-close", "0.1.0")}
    executed_sql = mock_cursor.execute.call_args[0][0]
    assert "DISTINCT" in executed_sql
    assert "dataset_name" in executed_sql
    assert "dataset_version" in executed_sql


@patch("db.datasets.get_conn")
def test_get_datasets_with_data_empty_table(mock_get_conn: MagicMock) -> None:
    """Empty table returns empty set."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_get_conn.side_effect = _mock_get_conn(mock_conn)

    result = datasets.get_datasets_with_data()
    assert result == set()
