from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from calendars.definitions.always_open import AlwaysOpenCalendar
from calendars.definitions.everyday import EverydayCalendar
from calendars.definitions.weekday import WeekdayCalendar
from service.builder import (
    NoValidTimestampsError,
    build_dataset,
    generate_timestamps,
    get_data,
)

from .conftest import _1D, V010, _cfg

_1H = timedelta(hours=1)
_1M = timedelta(minutes=1)
_1S = timedelta(seconds=1)
_EVERYDAY = EverydayCalendar()
_ALWAYS_OPEN = AlwaysOpenCalendar()


# --- generate_timestamps tests ---


def test_generate_timestamps_1d() -> None:
    """Daily over 3 days returns 3 timestamps."""
    result = generate_timestamps(
        datetime(2024, 1, 1), datetime(2024, 1, 3), _1D, _EVERYDAY
    )
    assert len(result) == 3
    assert result[0] == datetime(2024, 1, 1)
    assert result[-1] == datetime(2024, 1, 3)


def test_generate_timestamps_1h() -> None:
    """Hourly frequency works."""
    result = generate_timestamps(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 1, 2, 0),
        _1H,
        _ALWAYS_OPEN,
    )
    assert len(result) == 3


def test_generate_timestamps_1m() -> None:
    """Minute frequency works."""
    result = generate_timestamps(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 1, 0, 5),
        _1M,
        _ALWAYS_OPEN,
    )
    assert len(result) == 6


def test_generate_timestamps_1s() -> None:
    """Second frequency works."""
    result = generate_timestamps(
        datetime(2024, 1, 1, 0, 0, 0),
        datetime(2024, 1, 1, 0, 0, 3),
        _1S,
        _ALWAYS_OPEN,
    )
    assert len(result) == 4


def test_generate_timestamps_same_start_end() -> None:
    """Returns single timestamp when start equals end."""
    result = generate_timestamps(
        datetime(2024, 1, 1), datetime(2024, 1, 1), _1D, _EVERYDAY
    )
    assert len(result) == 1


def test_generate_timestamps_end_before_start() -> None:
    """Returns empty list when end is before start."""
    result = generate_timestamps(
        datetime(2024, 1, 3), datetime(2024, 1, 1), _1D, _EVERYDAY
    )
    assert len(result) == 0


def test_generate_timestamps_weekday_calendar_filters_weekends() -> None:
    """Weekday calendar excludes Saturday and Sunday."""
    # 2024-01-01 (Mon) through 2024-01-07 (Sun) = 7 days, 5 weekdays
    cal = WeekdayCalendar()
    result = generate_timestamps(
        datetime(2024, 1, 1), datetime(2024, 1, 7), _1D, calendar=cal
    )
    assert len(result) == 5
    for ts in result:
        assert ts.weekday() < 5


def test_generate_timestamps_everyday_includes_all_days() -> None:
    """Everyday calendar includes every day."""
    result = generate_timestamps(
        datetime(2024, 1, 1), datetime(2024, 1, 7), _1D, calendar=_EVERYDAY
    )
    assert len(result) == 7


def test_generate_timestamps_start_on_closed_day_advances_to_next_open() -> None:
    """Start on weekend advances to next weekday via next_open."""
    # 2024-01-06 (Sat) -> next open is 2024-01-08 (Mon)
    cal = WeekdayCalendar()
    result = generate_timestamps(
        datetime(2024, 1, 6), datetime(2024, 1, 10), _1D, calendar=cal
    )
    assert result[0] == datetime(2024, 1, 8)
    assert len(result) == 3  # Mon, Tue, Wed


def test_generate_timestamps_start_on_closed_day_no_valid_range_returns_empty() -> None:
    """Start on weekend with end before next open returns empty."""
    cal = WeekdayCalendar()
    result = generate_timestamps(
        datetime(2024, 1, 6), datetime(2024, 1, 7), _1D, calendar=cal
    )
    assert result == []


# --- build_dataset delegation tests ---
# detailed build behavior is tested in test_scheduler, test_worker, test_orchestrator


@patch("service.builder.run_build")
def test_build_dataset_delegates_to_orchestrator(mock_run_build: MagicMock) -> None:
    """build_dataset delegates to run_build with the same args."""
    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 5))

    mock_run_build.assert_called_once_with(
        "ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 5)
    )


@patch("service.builder.run_build")
def test_build_dataset_propagates_value_error(mock_run_build: MagicMock) -> None:
    """ValueError from scheduler (end before start-date) propagates."""
    mock_run_build.side_effect = ValueError("before start-date")

    with pytest.raises(ValueError, match="before start-date"):
        build_dataset("ds", V010, datetime(2024, 5, 1), datetime(2024, 5, 15))


@patch("service.builder.run_build")
def test_build_dataset_propagates_runtime_error(mock_run_build: MagicMock) -> None:
    """RuntimeError from worker failure propagates."""
    mock_run_build.side_effect = RuntimeError("build failed")

    with pytest.raises(RuntimeError, match="build failed"):
        build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 5))


# --- get_data tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.registry")
def test_get_data_no_build_returns_data(
    mock_registry: MagicMock, mock_db: MagicMock
) -> None:
    """get_data with build_data=False returns data and metadata."""
    mock_registry.get_config.return_value = _cfg()
    ts = datetime(2024, 1, 1)
    db_data = {ts: [{"ticker": "AAPL", "close": 150}]}
    mock_db.get_rows_range.return_value = db_data

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 2)
    result = get_data("ds", V010, start, end, build_data=False)

    assert result.data == db_data
    assert result.returned_timestamps == 1
    assert result.total_timestamps == 2
    mock_registry.get_config.assert_called_once_with("ds", V010)
    mock_db.get_rows_range.assert_called_once_with("ds", V010, start, end)


@patch("service.builder.db.datasets")
@patch("service.builder.registry")
def test_get_data_no_build_empty_result(
    mock_registry: MagicMock, mock_db: MagicMock
) -> None:
    """get_data with build_data=False and no data returns empty with metadata."""
    mock_registry.get_config.return_value = _cfg()
    mock_db.get_rows_range.return_value = {}

    result = get_data(
        "ds",
        V010,
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
        build_data=False,
    )

    assert result.data == {}
    assert result.returned_timestamps == 0
    assert result.total_timestamps == 2


@patch("service.builder.build_dataset")
@patch("service.builder.db.datasets")
@patch("service.builder.registry")
def test_get_data_with_build_calls_build_dataset(
    mock_registry: MagicMock,
    mock_db: MagicMock,
    mock_build: MagicMock,
) -> None:
    """get_data with build_data=True calls build_dataset before fetching."""
    mock_registry.get_config.return_value = _cfg()
    ts = datetime(2024, 1, 1)
    mock_db.get_rows_range.return_value = {ts: [{"val": 1}]}

    result = get_data(
        "ds",
        V010,
        ts,
        ts,
        build_data=True,
    )

    mock_build.assert_called_once_with("ds", V010, ts, ts)
    assert result.data == {ts: [{"val": 1}]}
    assert result.total_timestamps == 1
    assert result.returned_timestamps == 1


@patch("service.builder.build_dataset")
@patch("service.builder.registry")
def test_get_data_with_build_no_valid_timestamps_raises(
    mock_registry: MagicMock,
    mock_build: MagicMock,
) -> None:
    """get_data with build_data=True propagates NoValidTimestampsError."""
    mock_registry.get_config.return_value = _cfg()
    mock_build.side_effect = NoValidTimestampsError("no valid timestamps")

    with pytest.raises(NoValidTimestampsError, match="no valid timestamps"):
        get_data(
            "ds",
            V010,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            build_data=True,
        )


@patch("service.builder.registry")
def test_get_data_config_not_found_raises(mock_registry: MagicMock) -> None:
    """get_data raises when dataset config doesn't exist in registry."""
    mock_registry.get_config.side_effect = ValueError("not found in config registry")

    with pytest.raises(ValueError, match="not found in config registry"):
        get_data(
            "nonexistent",
            V010,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            build_data=True,
        )
