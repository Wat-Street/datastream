import threading
from contextlib import nullcontext
from datetime import datetime
from unittest.mock import patch

import pytest
from core.service.store import MemoryStore, PostgresStore

from .conftest import V010

JAN1 = datetime(2024, 1, 1)
JAN2 = datetime(2024, 1, 2)
JAN3 = datetime(2024, 1, 3)
JAN4 = datetime(2024, 1, 4)


# --- MemoryStore: insert + read round-trip ---


def test_memory_insert_and_get_rows_range() -> None:
    """Inserted rows are read back by range, keyed by timestamp."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN1, [{"v": 1}]), (JAN2, [{"v": 2}])])

    result = store.get_rows_range("ds", V010, JAN1, JAN2)

    assert result == {JAN1: [{"v": 1}], JAN2: [{"v": 2}]}


def test_memory_get_rows_range_filters_outside_window() -> None:
    """get_rows_range excludes timestamps outside [start, end]."""
    store = MemoryStore()
    store.insert_rows(
        "ds", V010, [(JAN1, [{"v": 1}]), (JAN2, [{"v": 2}]), (JAN3, [{"v": 3}])]
    )

    result = store.get_rows_range("ds", V010, JAN2, JAN3)

    assert result == {JAN2: [{"v": 2}], JAN3: [{"v": 3}]}


def test_memory_get_rows_range_is_sorted() -> None:
    """get_rows_range returns timestamps ascending regardless of insert order."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN3, [{"v": 3}])])
    store.insert_rows("ds", V010, [(JAN1, [{"v": 1}])])
    store.insert_rows("ds", V010, [(JAN2, [{"v": 2}])])

    result = store.get_rows_range("ds", V010, JAN1, JAN3)

    assert list(result.keys()) == [JAN1, JAN2, JAN3]


def test_memory_multi_row_timestamp() -> None:
    """Multiple rows sharing a timestamp accumulate into a list."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN1, [{"t": "AAPL"}, {"t": "MSFT"}])])
    store.insert_rows("ds", V010, [(JAN1, [{"t": "GOOG"}])])

    result = store.get_rows_range("ds", V010, JAN1, JAN1)

    assert result == {JAN1: [{"t": "AAPL"}, {"t": "MSFT"}, {"t": "GOOG"}]}


def test_memory_get_existing_timestamps() -> None:
    """get_existing_timestamps returns distinct sorted timestamps in range with data."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN1, [{"v": 1}]), (JAN3, [{"v": 3}])])

    assert store.get_existing_timestamps("ds", V010, JAN1, JAN4) == [JAN1, JAN3]
    # out-of-range start clips JAN1
    assert store.get_existing_timestamps("ds", V010, JAN2, JAN4) == [JAN3]


def test_memory_get_rows_timestamps_selects_specific() -> None:
    """get_rows_timestamps returns only the requested timestamps that have data."""
    store = MemoryStore()
    store.insert_rows(
        "ds", V010, [(JAN1, [{"v": 1}]), (JAN2, [{"v": 2}]), (JAN3, [{"v": 3}])]
    )

    result = store.get_rows_timestamps("ds", V010, [JAN1, JAN3])

    assert result == {JAN1: [{"v": 1}], JAN3: [{"v": 3}]}


def test_memory_get_rows_timestamps_empty_input() -> None:
    """Empty timestamp list returns empty dict (matches Postgres behavior)."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN1, [{"v": 1}])])

    assert store.get_rows_timestamps("ds", V010, []) == {}


def test_memory_reads_unknown_dataset_return_empty() -> None:
    """Reads for a dataset never inserted return empty results, not errors."""
    store = MemoryStore()

    assert store.get_existing_timestamps("ghost", V010, JAN1, JAN4) == []
    assert store.get_rows_range("ghost", V010, JAN1, JAN4) == {}
    assert store.get_rows_timestamps("ghost", V010, [JAN1]) == {}


def test_memory_isolated_by_name_and_version() -> None:
    """Different (name, version) pairs do not bleed into each other."""
    from core.utils.semver import SemVer

    v020 = SemVer.parse("0.2.0")
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN1, [{"v": 1}])])
    store.insert_rows("ds", v020, [(JAN1, [{"v": 99}])])
    store.insert_rows("other", V010, [(JAN1, [{"v": 7}])])

    assert store.get_rows_range("ds", V010, JAN1, JAN1) == {JAN1: [{"v": 1}]}
    assert store.get_rows_range("ds", v020, JAN1, JAN1) == {JAN1: [{"v": 99}]}
    assert store.get_rows_range("other", V010, JAN1, JAN1) == {JAN1: [{"v": 7}]}


def test_memory_two_stores_are_independent() -> None:
    """Each MemoryStore holds its own data (per-request isolation)."""
    a = MemoryStore()
    b = MemoryStore()
    a.insert_rows("ds", V010, [(JAN1, [{"v": 1}])])

    assert a.get_rows_range("ds", V010, JAN1, JAN1) == {JAN1: [{"v": 1}]}
    assert b.get_rows_range("ds", V010, JAN1, JAN1) == {}


def test_memory_insert_empty_is_noop() -> None:
    """Inserting an empty row list does nothing."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [])

    assert store.get_rows_range("ds", V010, JAN1, JAN4) == {}


def test_memory_insert_rejects_non_serializable() -> None:
    """json round-trip rejects non-serializable builder output (Jsonb parity)."""
    store = MemoryStore()

    with pytest.raises(TypeError):
        store.insert_rows("ds", V010, [(JAN1, [{"bad": {1, 2, 3}}])])


def test_memory_reads_return_copies() -> None:
    """Mutating a returned list does not corrupt the store's internal state."""
    store = MemoryStore()
    store.insert_rows("ds", V010, [(JAN1, [{"v": 1}])])

    result = store.get_rows_range("ds", V010, JAN1, JAN1)
    result[JAN1].append({"v": 999})

    assert store.get_rows_range("ds", V010, JAN1, JAN1) == {JAN1: [{"v": 1}]}


# --- build_lock behavior ---


def test_memory_build_lock_is_nullcontext() -> None:
    """MemoryStore.build_lock never blocks -- a dry run takes no real lock."""
    store = MemoryStore()
    lock = store.build_lock("ds", V010)
    assert isinstance(lock, type(nullcontext()))


@patch("core.service.store.get_build_lock")
def test_postgres_build_lock_uses_shared_registry(mock_get_lock) -> None:
    """PostgresStore.build_lock delegates to the shared per-dataset lock registry."""
    sentinel = threading.Lock()
    mock_get_lock.return_value = sentinel

    result = PostgresStore().build_lock("ds", V010)

    mock_get_lock.assert_called_once_with("ds", "0.1.0")
    assert result is sentinel


# --- PostgresStore forwarding ---


@patch("core.db.datasets")
def test_postgres_store_forwards_all_methods(mock_db) -> None:
    """Every PostgresStore method forwards verbatim to core.db.datasets."""
    store = PostgresStore()

    store.get_existing_timestamps("ds", V010, JAN1, JAN2)
    mock_db.get_existing_timestamps.assert_called_once_with("ds", V010, JAN1, JAN2)

    store.get_rows_range("ds", V010, JAN1, JAN2)
    mock_db.get_rows_range.assert_called_once_with("ds", V010, JAN1, JAN2)

    store.get_rows_timestamps("ds", V010, [JAN1])
    mock_db.get_rows_timestamps.assert_called_once_with("ds", V010, [JAN1])

    rows = [(JAN1, [{"v": 1}])]
    store.insert_rows("ds", V010, rows)
    mock_db.insert_rows.assert_called_once_with("ds", V010, rows)
