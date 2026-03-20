import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from runtime.config import DependencyInfo
from service.builder import build_dataset

from .conftest import V010, _cfg


@pytest.mark.parametrize("n_threads", [3, 5, 10, 15])
@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_concurrent_builds_same_dataset_no_duplicates(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
    n_threads: int,
) -> None:
    """N threads build same (name, version, range); rows inserted exactly once."""
    mock_config.load_config.return_value = _cfg()
    mock_runner.run_builder.return_value = [{"val": 1}]

    # track how many times insert_rows is called
    insert_call_count = 0
    insert_lock = threading.Lock()

    def fake_get_existing(name, version, start, end):
        # first caller sees nothing, subsequent callers see data (after lock release)
        with insert_lock:
            if insert_call_count > 0:
                return [datetime(2024, 1, 1), datetime(2024, 1, 2)]
            return []

    def fake_insert(name, version, rows):
        nonlocal insert_call_count
        with insert_lock:
            insert_call_count += 1

    mock_db.get_existing_timestamps.side_effect = fake_get_existing
    mock_db.insert_rows.side_effect = fake_insert

    errors: list[Exception] = []
    barrier = threading.Barrier(n_threads)

    def build_thread():
        try:
            barrier.wait()
            build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 2))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=build_thread) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # exactly one thread should have inserted
    assert insert_call_count == 1


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_concurrent_builds_overlapping_ranges(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Two threads build overlapping ranges; each timestamp built exactly once."""
    mock_config.load_config.return_value = _cfg()
    mock_runner.run_builder.return_value = [{"val": 1}]

    # track which timestamps get inserted
    inserted_timestamps: list[datetime] = []
    insert_lock = threading.Lock()

    def fake_get_existing(name, version, start, end):
        with insert_lock:
            return [ts for ts in inserted_timestamps if start <= ts <= end]

    def fake_insert(name, version, rows):
        with insert_lock:
            for ts, _ in rows:
                inserted_timestamps.append(ts)

    mock_db.get_existing_timestamps.side_effect = fake_get_existing
    mock_db.insert_rows.side_effect = fake_insert

    errors: list[Exception] = []
    barrier = threading.Barrier(2)

    def build_range(start, end):
        try:
            barrier.wait()
            build_dataset("ds", V010, start, end)
        except Exception as e:
            errors.append(e)

    # Jan 1-3 vs Jan 2-4 (overlap on Jan 2-3)
    t1 = threading.Thread(
        target=build_range, args=(datetime(2024, 1, 1), datetime(2024, 1, 3))
    )
    t2 = threading.Thread(
        target=build_range, args=(datetime(2024, 1, 2), datetime(2024, 1, 4))
    )
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors
    # each date should appear exactly once
    for d in range(1, 5):
        count = inserted_timestamps.count(datetime(2024, 1, d))
        assert count == 1, f"Jan {d} inserted {count} times, expected 1"


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_concurrent_builds_shared_dependency(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Two datasets sharing a dependency; dependency built exactly once."""
    configs = {
        "ds-a": _cfg(
            name="ds-a",
            dependencies={"shared-dep": DependencyInfo(version=V010)},
        ),
        "ds-b": _cfg(
            name="ds-b",
            dependencies={"shared-dep": DependencyInfo(version=V010)},
        ),
        "shared-dep": _cfg(name="shared-dep"),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    mock_runner.run_builder.return_value = [{"val": 1}]

    # track inserts per dataset
    insert_counts: dict[str, int] = {}
    insert_lock = threading.Lock()

    def fake_get_existing(name, version, start, end):
        with insert_lock:
            if insert_counts.get(name, 0) > 0:
                return [datetime(2024, 1, 1)]
            return []

    def fake_insert(name, version, rows):
        with insert_lock:
            insert_counts[name] = insert_counts.get(name, 0) + 1

    mock_db.get_existing_timestamps.side_effect = fake_get_existing
    mock_db.insert_rows.side_effect = fake_insert
    mock_db.get_rows_timestamps.return_value = {datetime(2024, 1, 1): [{"val": 1}]}

    errors: list[Exception] = []
    barrier = threading.Barrier(2)

    def build_thread(name):
        try:
            barrier.wait()
            build_dataset(name, V010, datetime(2024, 1, 1), datetime(2024, 1, 1))
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=build_thread, args=("ds-a",))
    t2 = threading.Thread(target=build_thread, args=("ds-b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors
    # shared-dep should be inserted exactly once
    assert insert_counts.get("shared-dep", 0) == 1


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_concurrent_builds_deep_dependency_chain(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Concurrent builds on chain A -> B -> C; no duplicate builds at any level."""
    configs = {
        "ds-a": _cfg(
            name="ds-a",
            dependencies={"ds-b": DependencyInfo(version=V010)},
        ),
        "ds-b": _cfg(
            name="ds-b",
            dependencies={"ds-c": DependencyInfo(version=V010)},
        ),
        "ds-c": _cfg(name="ds-c"),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    mock_runner.run_builder.return_value = [{"val": 1}]

    insert_counts: dict[str, int] = {}
    insert_lock = threading.Lock()

    def fake_get_existing(name, version, start, end):
        with insert_lock:
            if insert_counts.get(name, 0) > 0:
                return [datetime(2024, 1, 1)]
            return []

    def fake_insert(name, version, rows):
        with insert_lock:
            insert_counts[name] = insert_counts.get(name, 0) + 1

    mock_db.get_existing_timestamps.side_effect = fake_get_existing
    mock_db.insert_rows.side_effect = fake_insert
    mock_db.get_rows_timestamps.return_value = {datetime(2024, 1, 1): [{"val": 1}]}

    errors: list[Exception] = []
    barrier = threading.Barrier(3)

    def build_thread(name):
        try:
            barrier.wait()
            build_dataset(name, V010, datetime(2024, 1, 1), datetime(2024, 1, 1))
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=build_thread, args=(name,))
        for name in ["ds-a", "ds-b", "ds-c"]
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    for name in ["ds-a", "ds-b", "ds-c"]:
        assert insert_counts.get(name, 0) == 1, (
            f"{name} inserted {insert_counts.get(name, 0)} times"
        )


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_lock_released_on_builder_failure(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Builder crash releases the lock so a subsequent build can proceed."""
    mock_config.load_config.return_value = _cfg()
    mock_db.get_existing_timestamps.return_value = []

    call_count = 0

    def fake_run_builder(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("builder crashed")
        return [{"val": 1}]

    mock_runner.run_builder.side_effect = fake_run_builder

    # first build should fail
    with pytest.raises(RuntimeError, match="builder crashed"):
        build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))

    # second build should succeed (lock was released)
    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))
    mock_db.insert_rows.assert_called_once()
    assert mock_runner.run_builder.call_count == 2


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_second_request_skips_after_first_completes(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Second request re-checks missing after lock, finds nothing, skips building."""
    mock_config.load_config.return_value = _cfg()
    mock_runner.run_builder.return_value = [{"val": 1}]

    # first call sees empty, second sees data (simulating first thread's insert)
    call_count = 0

    def fake_get_existing(name, version, start, end):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return []
        return [datetime(2024, 1, 1)]

    mock_db.get_existing_timestamps.side_effect = fake_get_existing

    # first build inserts
    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))
    assert mock_db.insert_rows.call_count == 1

    # second build sees data already present, skips
    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))
    # insert_rows should still have been called only once
    assert mock_db.insert_rows.call_count == 1
    # runner called only once (first build)
    assert mock_runner.run_builder.call_count == 1
