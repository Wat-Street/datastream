from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from service.builder import build_dataset, generate_timestamps

# --- generate_timestamps tests ---


def test_generate_timestamps_1d() -> None:
    """Daily over 3 days returns 3 timestamps."""
    result = generate_timestamps(
        pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"), "1d"
    )
    assert len(result) == 3
    assert result[0] == pd.Timestamp("2024-01-01")
    assert result[-1] == pd.Timestamp("2024-01-03")


def test_generate_timestamps_1h() -> None:
    """Hourly frequency works."""
    result = generate_timestamps(
        pd.Timestamp("2024-01-01 00:00"), pd.Timestamp("2024-01-01 02:00"), "1h"
    )
    assert len(result) == 3


def test_generate_timestamps_1m() -> None:
    """Minute frequency works."""
    result = generate_timestamps(
        pd.Timestamp("2024-01-01 00:00"), pd.Timestamp("2024-01-01 00:05"), "1m"
    )
    assert len(result) == 6


def test_generate_timestamps_1s() -> None:
    """Second frequency works."""
    result = generate_timestamps(
        pd.Timestamp("2024-01-01 00:00:00"), pd.Timestamp("2024-01-01 00:00:03"), "1s"
    )
    assert len(result) == 4


def test_generate_timestamps_unsupported_granularity() -> None:
    """Raises ValueError for unsupported granularity."""
    with pytest.raises(ValueError, match="Unsupported granularity"):
        generate_timestamps(
            pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), "1w"
        )


def test_generate_timestamps_same_start_end() -> None:
    """Returns single timestamp when start equals end."""
    result = generate_timestamps(
        pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01"), "1d"
    )
    assert len(result) == 1


def test_generate_timestamps_end_before_start() -> None:
    """Returns empty list when end is before start."""
    result = generate_timestamps(
        pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-01"), "1d"
    )
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
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-01-02"),
    ]

    with patch("service.builder.runner") as mock_runner:
        build_dataset(
            "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")
        )
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
    mock_db.get_existing_timestamps.return_value = [pd.Timestamp("2024-01-01")]
    mock_loader.load_builder.return_value = lambda d, t: {"val": 1}
    mock_runner.run_builder.return_value = {"val": 1}

    build_dataset("ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"))

    # Only 2024-01-02 is missing, so runner called once
    assert mock_runner.run_builder.call_count == 1
    mock_db.insert_rows.assert_called_once()
    inserted_rows = mock_db.insert_rows.call_args[0][2]
    assert len(inserted_rows) == 1
    assert inserted_rows[0][0] == pd.Timestamp("2024-01-02")


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
    mock_db.get_existing_timestamps.return_value = [pd.Timestamp("2024-01-01")]

    build_dataset(
        "parent", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")
    )

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
    mock_loader.load_builder.return_value = lambda d, t: {}
    mock_runner.run_builder.return_value = {}

    with pytest.raises(RuntimeError, match="missing data for timestamp"):
        build_dataset(
            "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")
        )
