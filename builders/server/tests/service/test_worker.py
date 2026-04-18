import threading
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from runtime.config import DependencyInfo, SchemaType
from service.models import JobDescriptor
from service.worker import execute_job

from .conftest import V010, _cfg

JAN1 = datetime(2024, 1, 1)
JAN5 = datetime(2024, 1, 5)


def _job(
    name: str = "ds", start: datetime = JAN1, end: datetime = JAN5
) -> JobDescriptor:
    return JobDescriptor(dataset_name=name, dataset_version=V010, start=start, end=end)


def _never_cancelled() -> threading.Event:
    return threading.Event()


# --- all timestamps exist -> success, no insert ---


@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_all_timestamps_exist_skips_build(mock_registry, mock_db) -> None:
    """When all timestamps already exist, no builder runs and no insert."""
    mock_registry.get_config.return_value = _cfg(name="ds")
    # 5 days = 5 timestamps for everyday calendar
    mock_db.get_existing_timestamps.return_value = [
        JAN1 + timedelta(days=i) for i in range(5)
    ]

    result = execute_job(_job(), _never_cancelled())

    assert result.success is True
    assert result.error is None
    mock_db.insert_rows.assert_not_called()


# --- missing timestamps built, validated, and inserted ---


@patch("service.worker.validator")
@patch("service.worker.runner")
@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_missing_timestamps_built_and_inserted(
    mock_registry, mock_db, mock_runner, mock_validator
) -> None:
    """Missing timestamps trigger builder + validate + bulk insert."""
    mock_registry.get_config.return_value = _cfg(
        name="ds", schema={"price": SchemaType.INT}
    )
    mock_db.get_existing_timestamps.return_value = [JAN1]  # only first exists
    mock_runner.run_builder.return_value = [{"price": 100}]

    result = execute_job(_job(), _never_cancelled())

    assert result.success is True
    # builder called for each missing timestamp (Jan 2-5 = 4 calls)
    assert mock_runner.run_builder.call_count == 4
    # validator called for each missing timestamp
    assert mock_validator.validate_rows.call_count == 4
    # single bulk insert
    mock_db.insert_rows.assert_called_once()
    args = mock_db.insert_rows.call_args
    assert args[0][0] == "ds"
    assert len(args[0][2]) == 4  # 4 rows inserted


# --- builder failure mid-range -> no rows inserted ---


@patch("service.worker.runner")
@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_builder_failure_no_partial_insert(mock_registry, mock_db, mock_runner) -> None:
    """If builder fails on timestamp 3 of 5, no rows are inserted."""
    mock_registry.get_config.return_value = _cfg(name="ds")
    mock_db.get_existing_timestamps.return_value = []  # all missing

    call_count = 0

    def fail_on_third(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 3:
            raise RuntimeError("builder crashed")
        return [{"val": 1}]

    mock_runner.run_builder.side_effect = fail_on_third

    result = execute_job(_job(), _never_cancelled())

    assert result.success is False
    assert result.error is not None
    assert "builder crashed" in result.error
    mock_db.insert_rows.assert_not_called()


# --- cancelled event stops early ---


@patch("service.worker.runner")
@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_cancelled_event_stops_early(mock_registry, mock_db, mock_runner) -> None:
    """When cancelled is set, worker stops before building remaining timestamps."""
    mock_registry.get_config.return_value = _cfg(name="ds")
    mock_db.get_existing_timestamps.return_value = []
    mock_runner.run_builder.return_value = [{"val": 1}]

    cancelled = threading.Event()

    # set cancelled after first builder call
    def cancel_after_first(*args, **kwargs):
        result = [{"val": 1}]
        cancelled.set()
        return result

    mock_runner.run_builder.side_effect = cancel_after_first

    result = execute_job(_job(), cancelled)

    assert result.success is False
    assert result.error is not None
    assert "cancelled" in result.error
    # builder ran once, then cancelled before second
    assert mock_runner.run_builder.call_count == 1
    mock_db.insert_rows.assert_not_called()


# --- lookback dep data uses get_rows_range ---


@patch("service.worker.runner")
@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_lookback_dep_uses_get_rows_range(mock_registry, mock_db, mock_runner) -> None:
    """Dependency with lookback fetches data via get_rows_range."""
    mock_registry.get_config.return_value = _cfg(
        name="ds",
        dependencies={
            "dep": DependencyInfo(version=V010, lookback_subtract=timedelta(days=4)),
        },
    )
    mock_db.get_existing_timestamps.return_value = []
    mock_db.get_rows_range.return_value = {JAN1: [{"val": 1}]}
    mock_runner.run_builder.return_value = [{"val": 2}]

    # single day range so only one timestamp
    result = execute_job(_job(start=JAN1, end=JAN1), _never_cancelled())

    assert result.success is True
    mock_db.get_rows_range.assert_called_once()
    mock_db.get_rows_timestamps.assert_not_called()


# --- no-lookback dep data uses get_rows_timestamps ---


@patch("service.worker.runner")
@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_no_lookback_dep_uses_get_rows_timestamps(
    mock_registry, mock_db, mock_runner
) -> None:
    """Dependency without lookback fetches data via get_rows_timestamps."""
    mock_registry.get_config.return_value = _cfg(
        name="ds",
        dependencies={
            "dep": DependencyInfo(version=V010),
        },
    )
    mock_db.get_existing_timestamps.return_value = []
    mock_db.get_rows_timestamps.return_value = {JAN1: [{"val": 1}]}
    mock_runner.run_builder.return_value = [{"val": 2}]

    result = execute_job(_job(start=JAN1, end=JAN1), _never_cancelled())

    assert result.success is True
    mock_db.get_rows_timestamps.assert_called_once()
    mock_db.get_rows_range.assert_not_called()


# --- missing dep data raises RuntimeError ---


@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_missing_dep_data_returns_failure(mock_registry, mock_db) -> None:
    """When dependency data is missing, worker returns failure."""
    mock_registry.get_config.return_value = _cfg(
        name="ds",
        dependencies={
            "dep": DependencyInfo(version=V010),
        },
    )
    mock_db.get_existing_timestamps.return_value = []
    mock_db.get_rows_timestamps.return_value = {}  # no data

    result = execute_job(_job(start=JAN1, end=JAN1), _never_cancelled())

    assert result.success is False
    assert result.error is not None
    assert "missing data" in result.error.lower()
    mock_db.insert_rows.assert_not_called()


# --- no valid timestamps raises NoValidTimestampsError ---


@patch("service.worker.db.datasets")
@patch("service.worker.registry")
def test_no_valid_timestamps_raises(mock_registry, mock_db) -> None:
    """When no valid calendar timestamps exist, NoValidTimestampsError propagates."""
    from calendars.registry import CALENDARS_MAP
    from service.timestamps import NoValidTimestampsError

    mock_registry.get_config.return_value = _cfg(
        name="ds",
        calendar=CALENDARS_MAP["weekday"],
    )

    # Saturday to Sunday
    with pytest.raises(NoValidTimestampsError, match="no valid calendar timestamps"):
        execute_job(
            _job(start=datetime(2024, 1, 6), end=datetime(2024, 1, 7)),
            _never_cancelled(),
        )
