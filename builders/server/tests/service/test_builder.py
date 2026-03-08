from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from service.builder import build_dataset, generate_timestamps

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
    }
    # All timestamps already exist
    mock_db.get_existing_timestamps.return_value = [
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
    ]

    with patch("service.builder.runner") as mock_runner:
        build_dataset("ds", "0.1.0", datetime(2024, 1, 1), datetime(2024, 1, 2))
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
    }
    mock_db.get_existing_timestamps.return_value = [datetime(2024, 1, 1)]
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", "0.1.0", datetime(2024, 1, 1), datetime(2024, 1, 2))

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
                "dependencies": {"child": "0.1.0"},
            }
        return {"name": "child", "version": "0.1.0", "granularity": "1d"}

    mock_config.load_config.side_effect = fake_load_config
    # All timestamps exist so no building needed, but we track config load order
    mock_db.get_existing_timestamps.return_value = [datetime(2024, 1, 1)]

    build_dataset("parent", "0.1.0", datetime(2024, 1, 1), datetime(2024, 1, 1))

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
                "dependencies": {"dep": "0.1.0"},
            }
        # dep has no dependencies, so recursion stops
        return {"name": "dep", "version": "0.1.0", "granularity": "1d"}

    mock_config.load_config.side_effect = fake_load_config
    # No existing timestamps for either dataset
    mock_db.get_existing_timestamps.return_value = []
    # dep has no data for the timestamp (after dep build completes with no inserts)
    mock_db.get_rows.return_value = {}
    mock_loader.load_builder.return_value = lambda d, t: []
    mock_runner.run_builder.return_value = []

    with pytest.raises(RuntimeError, match="missing data for timestamp"):
        build_dataset("ds", "0.1.0", datetime(2024, 1, 1), datetime(2024, 1, 1))


@patch("service.builder.validator")
@patch("service.builder.runner")
@patch("service.builder.loader")
@patch("service.builder.db.datasets")
@patch("service.builder.config")
def test_build_dataset_passes_dep_data_as_list(
    mock_config: MagicMock,
    mock_db: MagicMock,
    mock_loader: MagicMock,
    mock_runner: MagicMock,
    mock_validator: MagicMock,
) -> None:
    """Dependency data is passed as list[dict] to the builder."""

    def fake_load_config(name, version):
        if name == "ds":
            return {
                "name": "ds",
                "version": "0.1.0",
                "granularity": "1d",
                "dependencies": {"dep": "0.1.0"},
            }
        return {"name": "dep", "version": "0.1.0", "granularity": "1d"}

    mock_config.load_config.side_effect = fake_load_config
    # dep recurses first, then ds
    mock_db.get_existing_timestamps.side_effect = [
        [datetime(2024, 1, 1)],  # dep already built (recursive call happens first)
        [],  # ds has no data
    ]
    # dep returns multi-row data
    dep_data = [{"ticker": "AAPL", "close": 150}, {"ticker": "MSFT", "close": 200}]
    mock_db.get_rows.return_value = {datetime(2024, 1, 1): dep_data}
    mock_loader.load_builder.return_value = lambda d, t: [{"val": 1}]
    mock_runner.run_builder.return_value = [{"val": 1}]

    build_dataset("ds", "0.1.0", datetime(2024, 1, 1), datetime(2024, 1, 1))

    # verify dep_data passed to runner is the list from get_rows
    runner_call = mock_runner.run_builder.call_args
    passed_deps = runner_call[0][1]
    assert passed_deps["dep"] == dep_data
