from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from service.builder import (
    build_dataset,
    generate_timestamps,
    validate_dependency_graph,
)
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")

# --- generate_timestamps tests ---


def test_generate_timestamps_1d() -> None:
    """Daily over 3 days returns 3 timestamps."""
    result = generate_timestamps(datetime(2024, 1, 1), datetime(2024, 1, 3), "1d")
    assert len(result) == 3
    assert result[0] == datetime(2024, 1, 1)
    assert result[-1] == datetime(2024, 1, 3)


def test_generate_timestamps_1h() -> None:
    """Hourly frequency works."""
    result = generate_timestamps(
        datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0), "1h"
    )
    assert len(result) == 3


def test_generate_timestamps_1m() -> None:
    """Minute frequency works."""
    result = generate_timestamps(
        datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 5), "1m"
    )
    assert len(result) == 6


def test_generate_timestamps_1s() -> None:
    """Second frequency works."""
    result = generate_timestamps(
        datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 0, 3), "1s"
    )
    assert len(result) == 4


def test_generate_timestamps_unsupported_granularity() -> None:
    """Raises ValueError for unsupported granularity."""
    with pytest.raises(ValueError, match="Unsupported granularity"):
        generate_timestamps(datetime(2024, 1, 1), datetime(2024, 1, 2), "1w")


def test_generate_timestamps_same_start_end() -> None:
    """Returns single timestamp when start equals end."""
    result = generate_timestamps(datetime(2024, 1, 1), datetime(2024, 1, 1), "1d")
    assert len(result) == 1


def test_generate_timestamps_end_before_start() -> None:
    """Returns empty list when end is before start."""
    result = generate_timestamps(datetime(2024, 1, 3), datetime(2024, 1, 1), "1d")
    assert len(result) == 0


# --- build_dataset tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_skips_existing(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """All timestamps exist, runner never called."""
    mock_config.load_config.return_value = {
        "name": "ds",
        "version": "0.1.0",
        "granularity": "1d",
        "start-date": "2020-01-01",
    }
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
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_builds_missing(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Missing timestamps trigger runner + insert."""
    mock_config.load_config.return_value = {
        "name": "ds",
        "version": "0.1.0",
        "granularity": "1d",
        "start-date": "2020-01-01",
    }
    mock_db.get_existing_timestamps.return_value = [datetime(2024, 1, 1)]
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
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
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_recursive_dependencies(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Dependencies built before parent."""

    def fake_load_config(name, version):
        if name == "parent":
            return {
                "name": "parent",
                "version": "0.1.0",
                "granularity": "1d",
                "start-date": "2020-01-01",
                "dependencies": {
                    "child": {"version": "0.1.0", "lookback": None},
                },
            }
        return {
            "name": "child",
            "version": "0.1.0",
            "granularity": "1d",
            "start-date": "2020-01-01",
        }

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
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_missing_dependency_data_raises(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
) -> None:
    """Missing dep data raises RuntimeError."""

    def fake_load_config(name, version):
        if name == "ds":
            return {
                "name": "ds",
                "version": "0.1.0",
                "granularity": "1d",
                "start-date": "2020-01-01",
                "dependencies": {
                    "dep": {"version": "0.1.0", "lookback": None},
                },
            }
        # dep has no dependencies, so recursion stops
        return {
            "name": "dep",
            "version": "0.1.0",
            "granularity": "1d",
            "start-date": "2020-01-01",
        }

    mock_config.load_config.side_effect = fake_load_config
    # No existing timestamps for either dataset
    mock_db.get_existing_timestamps.return_value = []
    # dep has no data for the timestamp (after dep build completes with no inserts)
    mock_db.get_rows.return_value = {}
    mock_loader.load_builder.return_value = lambda d, t: []
    mock_runner.run_builder.return_value = []

    with pytest.raises(RuntimeError, match="missing data for timestamp"):
        build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_passes_dep_data_as_dict_of_timestamps(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Dependency data is passed as dict[datetime, list[dict]] to the builder."""

    def fake_load_config(name, version):
        if name == "ds":
            return {
                "name": "ds",
                "version": "0.1.0",
                "granularity": "1d",
                "start-date": "2020-01-01",
                "dependencies": {
                    "dep": {"version": "0.1.0", "lookback": None},
                },
            }
        return {
            "name": "dep",
            "version": "0.1.0",
            "granularity": "1d",
            "start-date": "2020-01-01",
        }

    mock_config.load_config.side_effect = fake_load_config
    # dep recurses first, then ds
    mock_db.get_existing_timestamps.side_effect = [
        [datetime(2024, 1, 1)],  # dep already built (recursive call happens first)
        [],  # ds has no data
    ]
    # dep returns multi-row data keyed by timestamp
    ts = datetime(2024, 1, 1)
    dep_rows = [{"ticker": "AAPL", "close": 150}, {"ticker": "MSFT", "close": 200}]
    mock_db.get_rows.return_value = {ts: dep_rows}
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))

    # verify dep_data passed to runner is dict[datetime, list[dict]]
    runner_call = mock_runner.run_builder.call_args
    passed_deps = runner_call[0][1]
    assert passed_deps["dep"] == {ts: dep_rows}


# --- start-date enforcement tests ---


@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_end_before_start_date_raises(
    mock_config: MagicMock, mock_db: MagicMock
) -> None:
    """End date before dataset start-date raises ValueError."""
    mock_config.load_config.return_value = {
        "name": "ds",
        "version": "0.1.0",
        "granularity": "1d",
        "start-date": "2024-06-01",
    }

    with pytest.raises(ValueError, match="before dataset start-date"):
        build_dataset("ds", V010, datetime(2024, 5, 1), datetime(2024, 5, 15))


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_start_before_start_date_clamps(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Start date before dataset start-date gets clamped."""
    mock_config.load_config.return_value = {
        "name": "ds",
        "version": "0.1.0",
        "granularity": "1d",
        "start-date": "2024-01-03",
    }
    mock_db.get_existing_timestamps.return_value = []
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
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
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_after_start_date_proceeds_normally(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Both dates after start-date proceeds without clamping."""
    mock_config.load_config.return_value = {
        "name": "ds",
        "version": "0.1.0",
        "granularity": "1d",
        "start-date": "2020-01-01",
    }
    mock_db.get_existing_timestamps.return_value = []
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
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
            return {
                "name": "parent",
                "version": "0.1.0",
                "granularity": "1d",
                "schema": {"val": "int"},
                "start-date": "2020-01-01",
                "dependencies": {
                    "child": {"version": "0.1.0", "lookback": None},
                },
            }
        return {
            "name": "child",
            "version": "0.1.0",
            "granularity": "1h",
            "schema": {"val": "int"},
            "start-date": "2020-01-01",
        }

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
            return {
                "name": "parent",
                "version": "0.1.0",
                "granularity": "1d",
                "schema": {"val": "int"},
                "start-date": "2020-01-01",
                "dependencies": {
                    "child": {"version": "0.1.0", "lookback": None},
                },
            }
        return {
            "name": "child",
            "version": "0.1.0",
            "granularity": "1d",
            "schema": {"val": "int"},
            "start-date": "2020-01-01",
        }

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
            return {
                "name": "parent",
                "version": "0.1.0",
                "granularity": "1h",
                "schema": {"val": "int"},
                "start-date": "2020-01-01",
                "dependencies": {
                    "child": {"version": "0.1.0", "lookback": None},
                },
            }
        return {
            "name": "child",
            "version": "0.1.0",
            "granularity": "1d",
            "schema": {"val": "int"},
            "start-date": "2020-01-01",
        }

    mock_config.load_config.side_effect = fake_load_config
    with pytest.raises(ValueError, match="finer than dependency"):
        validate_dependency_graph("parent", V010)


@patch("service.builder.config")
def test_validate_graph_two_deps_one_coarser_raises(
    mock_config: MagicMock,
) -> None:
    """1h parent with 1m and 1d deps raises on the coarser dep."""
    configs = {
        "parent": {
            "name": "parent",
            "version": "0.1.0",
            "granularity": "1h",
            "schema": {"val": "int"},
            "start-date": "2020-01-01",
            "dependencies": {
                "fine": {"version": "0.1.0", "lookback": None},
                "coarse": {"version": "0.1.0", "lookback": None},
            },
        },
        "fine": {
            "name": "fine",
            "version": "0.1.0",
            "granularity": "1m",
            "schema": {"val": "int"},
            "start-date": "2020-01-01",
        },
        "coarse": {
            "name": "coarse",
            "version": "0.1.0",
            "granularity": "1d",
            "schema": {"val": "int"},
            "start-date": "2020-01-01",
        },
    }
    mock_config.load_config.side_effect = lambda name, version: configs[name]
    with pytest.raises(ValueError, match="finer than dependency"):
        validate_dependency_graph("parent", V010)


# --- lookback tests ---


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_lookback_expands_dep_build_range(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Lookback dep build range is expanded by the lookback duration."""

    def fake_load_config(name, version):
        if name == "ds":
            return {
                "name": "ds",
                "version": "0.1.0",
                "granularity": "1d",
                "start-date": "2020-01-01",
                "dependencies": {
                    "dep": {
                        "version": "0.1.0",
                        "lookback": timedelta(days=5),
                    },
                },
            }
        return {
            "name": "dep",
            "version": "0.1.0",
            "granularity": "1d",
            "start-date": "2020-01-01",
        }

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
    # dep start should be 2024-01-01 - 5d = 2023-12-27
    assert dep_call[0][2] == datetime(2023, 12, 27)
    assert dep_call[0][3] == datetime(2024, 1, 3)


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_lookback_fetches_range(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """With lookback, get_rows_range is used and passes dict to builder."""

    def fake_load_config(name, version):
        if name == "ds":
            return {
                "name": "ds",
                "version": "0.1.0",
                "granularity": "1d",
                "start-date": "2020-01-01",
                "dependencies": {
                    "dep": {
                        "version": "0.1.0",
                        "lookback": timedelta(days=2),
                    },
                },
            }
        return {
            "name": "dep",
            "version": "0.1.0",
            "granularity": "1d",
            "start-date": "2020-01-01",
        }

    mock_config.load_config.side_effect = fake_load_config
    mock_db.get_existing_timestamps.side_effect = [
        # dep: all timestamps exist (expanded range)
        [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
        # ds: needs to build Jan 3
        [datetime(2024, 1, 1), datetime(2024, 1, 2)],
    ]
    # lookback range query returns multiple timestamps
    range_data = {
        datetime(2024, 1, 1): [{"val": 10}],
        datetime(2024, 1, 2): [{"val": 20}],
        datetime(2024, 1, 3): [{"val": 30}],
    }
    mock_db.get_rows_range.return_value = range_data
    mock_loader.load_builder.return_value = lambda d, t: [{"avg": 20}]
    mock_runner.run_builder.return_value = [{"avg": 20}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 3))

    # get_rows_range should be called (not get_rows)
    mock_db.get_rows_range.assert_called_once()
    call_args = mock_db.get_rows_range.call_args[0]
    assert call_args[0] == "dep"
    # range should be [Jan 3 - 2d, Jan 3] = [Jan 1, Jan 3]
    assert call_args[2] == datetime(2024, 1, 1)
    assert call_args[3] == datetime(2024, 1, 3)

    # verify the dict was passed to runner
    runner_call = mock_runner.run_builder.call_args
    passed_deps = runner_call[0][1]
    assert passed_deps["dep"] == range_data


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_no_lookback_uses_get_rows(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Without lookback, get_rows is used (not get_rows_range)."""

    def fake_load_config(name, version):
        if name == "ds":
            return {
                "name": "ds",
                "version": "0.1.0",
                "granularity": "1d",
                "start-date": "2020-01-01",
                "dependencies": {
                    "dep": {"version": "0.1.0", "lookback": None},
                },
            }
        return {
            "name": "dep",
            "version": "0.1.0",
            "granularity": "1d",
            "start-date": "2020-01-01",
        }

    mock_config.load_config.side_effect = fake_load_config
    mock_db.get_existing_timestamps.side_effect = [
        [datetime(2024, 1, 1)],  # dep
        [],  # ds
    ]
    ts = datetime(2024, 1, 1)
    mock_db.get_rows.return_value = {ts: [{"val": 1}]}
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 1))

    mock_db.get_rows.assert_called_once()
    mock_db.get_rows_range.assert_not_called()
