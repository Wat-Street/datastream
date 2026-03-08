from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from main import app, generate_timestamps

client: TestClient = TestClient(app)


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


# --- endpoint tests ---


@patch("main._build_dataset")
def test_build_endpoint_success(mock_build: MagicMock) -> None:
    """POST returns 200 with status ok."""
    mock_build.return_value = None
    resp = client.post("/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_build_endpoint_invalid_timestamp() -> None:
    """Bad timestamp returns 400."""
    resp = client.post("/build/ds/0.1.0?start=not-a-date&end=2024-01-31")
    assert resp.status_code == 400


@patch("main._build_dataset", side_effect=FileNotFoundError("config not found"))
def test_build_endpoint_internal_error(mock_build: MagicMock) -> None:
    """Config not found returns 500."""
    resp = client.post("/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 500


# --- _build_dataset tests ---


@patch("main.db.datasets")
@patch("main.config")
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

    from main import _build_dataset

    with patch("main.runner") as mock_runner:
        _build_dataset(
            "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")
        )
        mock_runner.run_builder.assert_not_called()
    mock_db.insert_rows.assert_not_called()


@patch("main.validator")
@patch("main.runner")
@patch("main.loader")
@patch("main.db.datasets")
@patch("main.config")
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

    from main import _build_dataset

    _build_dataset(
        "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")
    )

    # Only 2024-01-02 is missing, so runner called once
    assert mock_runner.run_builder.call_count == 1
    mock_db.insert_rows.assert_called_once()
    inserted_rows = mock_db.insert_rows.call_args[0][2]
    assert len(inserted_rows) == 1
    assert inserted_rows[0][0] == pd.Timestamp("2024-01-02")


@patch("main.validator")
@patch("main.runner")
@patch("main.loader")
@patch("main.db.datasets")
@patch("main.config")
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

    from main import _build_dataset

    _build_dataset(
        "parent", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")
    )

    # config.load_config called for child first (recursive), then parent
    calls = mock_config.load_config.call_args_list
    # First call is parent, second is child (recursive call)
    assert calls[0][0][0] == "parent"
    assert calls[1][0][0] == "child"


@patch("main.runner")
@patch("main.loader")
@patch("main.db.datasets")
@patch("main.config")
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

    from main import _build_dataset

    with pytest.raises(RuntimeError, match="missing data for timestamp"):
        _build_dataset(
            "ds", "0.1.0", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")
        )
