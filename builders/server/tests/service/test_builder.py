from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from calendars.definitions.always_open import AlwaysOpenCalendar
from calendars.definitions.everyday import EverydayCalendar
from calendars.definitions.weekday import WeekdayCalendar
from runtime.config import DependencyInfo
from service.builder import (
    NoValidTimestampsError,
    build_dataset,
    generate_timestamps,
    get_data,
    validate_dependency_graph,
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


# --- build_dataset tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_skips_existing(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """All timestamps exist, runner never called."""
    mock_config.load_config.return_value = _cfg()
    # All timestamps already exist
    mock_db.get_existing_timestamps.return_value = [
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
    ]

    with patch("service.builder.runner") as mock_runner:
        build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 2))
        mock_runner.run_builder.assert_not_called()
    mock_db.insert_rows.assert_not_called()


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_builds_missing(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Missing timestamps trigger runner + insert."""
    mock_config.load_config.return_value = _cfg()
    mock_db.get_existing_timestamps.return_value = [datetime(2024, 1, 1)]
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 2))

    # Only 2024-01-02 is missing, so runner called once
    assert mock_runner.run_builder.call_count == 1
    mock_db.insert_rows.assert_called_once()
    inserted_rows = mock_db.insert_rows.call_args[0][2]
    assert len(inserted_rows) == 1
    assert inserted_rows[0][0] == datetime(2024, 1, 2)
    assert inserted_rows[0][1] == [{"val": 1}]


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_recursive_dependencies(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Dependencies built before parent."""

    def fake_load_config(name, version):
        if name == "parent":
            return _cfg(
                name="parent",
                dependencies={"child": DependencyInfo(version=V010)},
            )
        return _cfg(name="child")

    mock_config.load_config.side_effect = fake_load_config
    # All timestamps exist so no building needed, but we track config load order
    mock_db.get_existing_timestamps.return_value = [datetime(2024, 1, 1)]

    build_dataset("parent", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))

    # config.load_config called for child first (recursive), then parent
    calls = mock_config.load_config.call_args_list
    # First call is parent, second is child (recursive call)
    assert calls[0][0][0] == "parent"
    assert calls[1][0][0] == "child"


@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_missing_dependency_data_raises(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
) -> None:
    """Missing dep data raises RuntimeError."""

    def fake_load_config(name, version):
        if name == "ds":
            return _cfg(
                dependencies={"dep": DependencyInfo(version=V010)},
            )
        # dep has no dependencies, so recursion stops
        return _cfg(name="dep")

    mock_config.load_config.side_effect = fake_load_config
    # No existing timestamps for either dataset
    mock_db.get_existing_timestamps.return_value = []
    # dep has no data for the timestamp (after dep build completes with no inserts)
    mock_db.get_rows_timestamps.return_value = {}
    mock_runner.run_builder.return_value = []

    with pytest.raises(RuntimeError, match="missing data for timestamp"):
        build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_passes_dep_data_as_dict_of_timestamps(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Dependency data is passed as dict[datetime, list[dict]] to the builder."""

    def fake_load_config(name, version):
        if name == "ds":
            return _cfg(
                dependencies={"dep": DependencyInfo(version=V010)},
            )
        return _cfg(name="dep")

    mock_config.load_config.side_effect = fake_load_config
    # dep recurses first, then ds
    mock_db.get_existing_timestamps.side_effect = [
        [datetime(2024, 1, 1)],  # dep already built (recursive call happens first)
        [],  # ds has no data
    ]
    # dep returns multi-row data keyed by timestamp
    ts = datetime(2024, 1, 1)
    dep_rows = [{"ticker": "AAPL", "close": 150}, {"ticker": "MSFT", "close": 200}]
    mock_db.get_rows_timestamps.return_value = {ts: dep_rows}
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))

    # verify dep_data passed to runner is dict[datetime, list[dict]]
    runner_call = mock_runner.run_builder.call_args
    passed_deps = runner_call[0][2]
    assert passed_deps["dep"] == {ts: dep_rows}


# --- start-date enforcement tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_end_before_start_date_raises(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """End date before dataset start-date raises ValueError."""
    mock_config.load_config.return_value = _cfg(start_date=datetime(2024, 6, 1))

    with pytest.raises(ValueError, match="before dataset start-date"):
        build_dataset("ds", V010, datetime(2024, 5, 1), datetime(2024, 5, 15))


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_start_before_start_date_clamps(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Start date before dataset start-date gets clamped."""
    mock_config.load_config.return_value = _cfg(start_date=datetime(2024, 1, 3))
    mock_db.get_existing_timestamps.return_value = []
    mock_runner.run_builder.return_value = [{"val": 1}]

    # request starts on Jan 1 but start-date is Jan 3
    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 4))

    # timestamps should start from Jan 3 (clamped), not Jan 1
    inserted_rows = mock_db.insert_rows.call_args[0][2]
    timestamps = [row[0] for row in inserted_rows]
    assert timestamps[0] == datetime(2024, 1, 3)
    assert datetime(2024, 1, 1) not in timestamps
    assert datetime(2024, 1, 2) not in timestamps


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_after_start_date_proceeds_normally(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Both dates after start-date proceeds without clamping."""
    mock_config.load_config.return_value = _cfg()
    mock_db.get_existing_timestamps.return_value = []
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 3))

    inserted_rows = mock_db.insert_rows.call_args[0][2]
    timestamps = [row[0] for row in inserted_rows]
    assert timestamps[0] == datetime(2024, 1, 1)
    assert len(timestamps) == 3


# --- validate_dependency_graph tests ---


@patch("service.builder.config")
def test_validate_graph_coarser_parent_passes(
    mock_config: MagicMock,
) -> None:
    """1d parent depending on 1h dep is valid."""

    def fake_load_config(name, version):
        if name == "parent":
            return _cfg(
                name="parent",
                dependencies={"child": DependencyInfo(version=V010)},
            )
        return _cfg(name="child", granularity=_1H)

    mock_config.load_config.side_effect = fake_load_config
    # should not raise
    validate_dependency_graph("parent", V010)


@patch("service.builder.config")
def test_validate_graph_equal_granularity_passes(
    mock_config: MagicMock,
) -> None:
    """1d parent depending on 1d dep is valid."""

    def fake_load_config(name, version):
        if name == "parent":
            return _cfg(
                name="parent",
                dependencies={"child": DependencyInfo(version=V010)},
            )
        return _cfg(name="child")

    mock_config.load_config.side_effect = fake_load_config
    # should not raise
    validate_dependency_graph("parent", V010)


@patch("service.builder.config")
def test_validate_graph_finer_parent_raises(
    mock_config: MagicMock,
) -> None:
    """1h parent depending on 1d dep raises ValueError."""

    def fake_load_config(name, version):
        if name == "parent":
            return _cfg(
                name="parent",
                granularity=_1H,
                dependencies={"child": DependencyInfo(version=V010)},
            )
        return _cfg(name="child")

    mock_config.load_config.side_effect = fake_load_config
    with pytest.raises(ValueError, match="finer than dependency"):
        validate_dependency_graph("parent", V010)


@patch("service.builder.config")
def test_validate_graph_two_deps_one_coarser_raises(
    mock_config: MagicMock,
) -> None:
    """1h parent with 1m and 1d deps raises on the coarser dep."""
    configs = {
        "parent": _cfg(
            name="parent",
            granularity=_1H,
            dependencies={
                "fine": DependencyInfo(version=V010),
                "coarse": DependencyInfo(version=V010),
            },
        ),
        "fine": _cfg(name="fine", granularity=_1M),
        "coarse": _cfg(name="coarse"),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    with pytest.raises(ValueError, match="finer than dependency"):
        validate_dependency_graph("parent", V010)


@pytest.mark.parametrize(
    ["parent_start", "child1_start", "child2_start"],
    [
        (datetime(2020, 1, 1), datetime(2020, 1, 1), datetime(2020, 1, 1)),
        (datetime(2020, 1, 3), datetime(2020, 1, 1), datetime(2020, 1, 2)),
        (datetime(2022, 10, 15), datetime(2021, 5, 22), datetime(2022, 9, 9)),
    ],
)
@patch("service.builder.config")
def test_validate_graph_start_dates_ok(
    mock_config: MagicMock,
    parent_start: datetime,
    child1_start: datetime,
    child2_start: datetime,
) -> None:
    """Parent has start date after children."""
    configs = {
        "parent": _cfg(
            name="parent",
            start_date=parent_start,
            dependencies={
                "child1": DependencyInfo(version=V010),
                "child2": DependencyInfo(version=V010),
            },
        ),
        "child1": _cfg(name="child1", start_date=child1_start),
        "child2": _cfg(name="child2", start_date=child2_start),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    validate_dependency_graph("parent", V010)


@patch("service.builder.config")
def test_validate_graph_start_dates_parent_before_dep_raises(
    mock_config: MagicMock,
) -> None:
    """Parent with start date before dep raises ValueError."""

    def fake_load_config(name, version):
        if name == "parent":
            return _cfg(
                name="parent",
                dependencies={"child": DependencyInfo(version=V010)},
            )
        return _cfg(name="child", start_date=datetime(2021, 6, 1))

    mock_config.load_config.side_effect = fake_load_config
    with pytest.raises(ValueError, match="comes before dependency"):
        validate_dependency_graph("parent", V010)


@patch("service.builder.config")
def test_validate_graph_start_dates_no_deps_ok(
    mock_config: MagicMock,
) -> None:
    """Root dataset with no dependencies always passes start date check."""
    mock_config.load_config.return_value = _cfg(name="root")
    validate_dependency_graph("root", V010)


@patch("service.builder.config")
def test_validate_graph_start_dates_deep_chain_ok(
    mock_config: MagicMock,
) -> None:
    """Three-level chain where each ancestor starts after its descendant passes."""
    configs = {
        "grandparent": _cfg(
            name="grandparent",
            start_date=datetime(2022, 1, 1),
            dependencies={"parent": DependencyInfo(version=V010)},
        ),
        "parent": _cfg(
            name="parent",
            start_date=datetime(2021, 1, 1),
            dependencies={"child": DependencyInfo(version=V010)},
        ),
        "child": _cfg(name="child"),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    validate_dependency_graph("grandparent", V010)


@patch("service.builder.config")
def test_validate_graph_start_dates_deep_chain_violation_raises(
    mock_config: MagicMock,
) -> None:
    """Grandparent violating grandchild's start date raises ValueError."""
    configs = {
        "grandparent": _cfg(
            name="grandparent",
            start_date=datetime(2019, 1, 1),
            dependencies={"parent": DependencyInfo(version=V010)},
        ),
        "parent": _cfg(
            name="parent",
            dependencies={"child": DependencyInfo(version=V010)},
        ),
        "child": _cfg(name="child"),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    with pytest.raises(ValueError, match="comes before dependency"):
        validate_dependency_graph("grandparent", V010)


@pytest.mark.parametrize(
    ["parent_start", "child1_start", "child2_start"],
    [
        (datetime(2020, 1, 1), datetime(2021, 1, 1), datetime(2020, 1, 1)),
        (datetime(2020, 1, 1), datetime(2020, 1, 1), datetime(2021, 1, 1)),
        (datetime(2020, 1, 1), datetime(2021, 6, 15), datetime(2020, 12, 31)),
    ],
)
@patch("service.builder.config")
def test_validate_graph_start_dates_parent_before_any_dep_raises(
    mock_config: MagicMock,
    parent_start: datetime,
    child1_start: datetime,
    child2_start: datetime,
) -> None:
    """Parent has start date before at least one child — raises ValueError."""
    configs = {
        "parent": _cfg(
            name="parent",
            start_date=parent_start,
            dependencies={
                "child1": DependencyInfo(version=V010),
                "child2": DependencyInfo(version=V010),
            },
        ),
        "child1": _cfg(name="child1", start_date=child1_start),
        "child2": _cfg(name="child2", start_date=child2_start),
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    with pytest.raises(ValueError, match="comes before dependency"):
        validate_dependency_graph("parent", V010)


# --- NoValidTimestampsError tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_no_valid_timestamps_raises(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """Weekend-only range on weekday calendar raises NoValidTimestampsError."""
    mock_config.load_config.return_value = _cfg(calendar=WeekdayCalendar())

    # 2024-01-06 (Sat) and 2024-01-07 (Sun) — no weekday timestamps
    with pytest.raises(NoValidTimestampsError, match="no valid calendar timestamps"):
        build_dataset("ds", V010, datetime(2024, 1, 6), datetime(2024, 1, 7))

    mock_db.get_existing_timestamps.assert_not_called()


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_valid_range_all_built_does_not_raise(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """Valid weekday range with all timestamps already built does not raise."""
    mock_config.load_config.return_value = _cfg(calendar=WeekdayCalendar())
    # 2024-01-08 (Mon) and 2024-01-09 (Tue) — both weekdays, both already built
    mock_db.get_existing_timestamps.return_value = [
        datetime(2024, 1, 8),
        datetime(2024, 1, 9),
    ]

    # should not raise
    build_dataset("ds", V010, datetime(2024, 1, 8), datetime(2024, 1, 9))


# --- lookback tests ---


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_lookback_expands_dep_build_range(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Lookback dep build range is expanded by the lookback duration."""

    def fake_load_config(name, version):
        if name == "ds":
            return _cfg(
                dependencies={
                    "dep": DependencyInfo(
                        version=V010,
                        lookback_subtract=timedelta(days=4),
                    ),
                },
            )
        return _cfg(name="dep")

    mock_config.load_config.side_effect = fake_load_config
    # everything already exists so we just check build range
    mock_db.get_existing_timestamps.return_value = [
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
        datetime(2024, 1, 3),
    ]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 3))

    # dep's get_existing_timestamps should be called with expanded range
    dep_call = mock_db.get_existing_timestamps.call_args_list[0]
    # dep start should be 2024-01-01 - 5d + 1d = 2023-12-28
    assert dep_call[0][2] == datetime(2023, 12, 28)
    assert dep_call[0][3] == datetime(2024, 1, 3)


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_lookback_fetches_range(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """With lookback, get_rows_range is used and passes dict to builder."""

    def fake_load_config(name, version):
        if name == "ds":
            return _cfg(
                dependencies={
                    "dep": DependencyInfo(
                        version=V010,
                        lookback_subtract=timedelta(days=1),
                    ),
                },
            )
        return _cfg(name="dep")

    mock_config.load_config.side_effect = fake_load_config
    mock_db.get_existing_timestamps.side_effect = [
        # dep: all timestamps exist (expanded range)
        [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
        # ds: needs to build Jan 3
        [datetime(2024, 1, 1), datetime(2024, 1, 2)],
    ]
    # lookback range query returns multiple timestamps
    range_data = {
        datetime(2024, 1, 2): [{"val": 20}],
        datetime(2024, 1, 3): [{"val": 30}],
    }
    mock_db.get_rows_range.return_value = range_data
    mock_runner.run_builder.return_value = [{"avg": 25}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 3))

    # get_rows_range should be called (not get_rows_timestamps)
    mock_db.get_rows_range.assert_called_once()
    call_args = mock_db.get_rows_range.call_args[0]
    assert call_args[0] == "dep"
    # range should be [Jan 3 - 2d + 1d, Jan 3] = [Jan 2, Jan 3]
    assert call_args[2] == datetime(2024, 1, 2)
    assert call_args[3] == datetime(2024, 1, 3)

    # verify the dict was passed to runner
    runner_call = mock_runner.run_builder.call_args
    passed_deps = runner_call[0][2]
    assert passed_deps["dep"] == range_data


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_no_lookback_uses_get_rows_timestamps(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Without lookback, get_rows_timestamps is used (not get_rows_range)."""

    def fake_load_config(name, version):
        if name == "ds":
            return _cfg(
                dependencies={"dep": DependencyInfo(version=V010)},
            )
        return _cfg(name="dep")

    mock_config.load_config.side_effect = fake_load_config
    mock_db.get_existing_timestamps.side_effect = [
        [datetime(2024, 1, 1)],  # dep
        [],  # ds
    ]
    ts = datetime(2024, 1, 1)
    mock_db.get_rows_timestamps.return_value = {ts: [{"val": 1}]}
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))

    mock_db.get_rows_timestamps.assert_called_once()
    mock_db.get_rows_range.assert_not_called()


# --- get_data tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_get_data_no_build_returns_data(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """get_data with build_data=False returns data and metadata."""
    mock_config.load_config.return_value = _cfg()
    ts = datetime(2024, 1, 1)
    db_data = {ts: [{"ticker": "AAPL", "close": 150}]}
    mock_db.get_rows_range.return_value = db_data

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 2)
    result = get_data("ds", V010, start, end, build_data=False)

    assert result.data == db_data
    assert result.returned_timestamps == 1
    assert result.total_timestamps == 2
    mock_config.load_config.assert_called_once_with("ds", V010)
    mock_db.get_rows_range.assert_called_once_with("ds", V010, start, end)


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_get_data_no_build_empty_result(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """get_data with build_data=False and no data returns empty with metadata."""
    mock_config.load_config.return_value = _cfg()
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
@patch("service.builder.config")
def test_get_data_with_build_calls_build_dataset(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_build: MagicMock,
) -> None:
    """get_data with build_data=True calls build_dataset before fetching."""
    mock_config.load_config.return_value = _cfg()
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
@patch("service.builder.config")
def test_get_data_with_build_no_valid_timestamps_raises(
    mock_config: MagicMock,
    mock_build: MagicMock,
) -> None:
    """get_data with build_data=True propagates NoValidTimestampsError."""
    mock_config.load_config.return_value = _cfg()
    mock_build.side_effect = NoValidTimestampsError("no valid timestamps")

    with pytest.raises(NoValidTimestampsError, match="no valid timestamps"):
        get_data(
            "ds",
            V010,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            build_data=True,
        )


@patch("service.builder.config")
def test_get_data_config_not_found_raises(mock_config: MagicMock) -> None:
    """get_data raises when dataset config doesn't exist."""
    mock_config.load_config.side_effect = FileNotFoundError("config not found")

    with pytest.raises(FileNotFoundError, match="config not found"):
        get_data(
            "nonexistent",
            V010,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            build_data=True,
        )
