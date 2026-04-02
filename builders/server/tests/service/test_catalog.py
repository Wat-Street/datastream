from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock, patch

from service.catalog import DatasetInfo, discover_datasets, list_datasets

# --- discover_datasets tests ---


def test_discover_datasets_finds_configs(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Returns (name, version) for each dir with a config.toml."""
    write_config(mock_scripts_dir, "ds-a", "0.1.0", "")
    write_config(mock_scripts_dir, "ds-b", "1.0.0", "")
    result = discover_datasets()
    assert ("ds-a", "0.1.0") in result
    assert ("ds-b", "1.0.0") in result


def test_discover_datasets_skips_no_config(mock_scripts_dir: Path) -> None:
    """Dirs without config.toml are skipped."""
    (mock_scripts_dir / "orphan" / "0.1.0").mkdir(parents=True)
    result = discover_datasets()
    assert ("orphan", "0.1.0") not in result


def test_discover_datasets_empty_dir(mock_scripts_dir: Path) -> None:
    """Empty scripts dir returns empty list."""
    result = discover_datasets()
    assert result == []


def test_discover_datasets_sorted(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Results are sorted by name then version."""
    write_config(mock_scripts_dir, "z-ds", "0.1.0", "")
    write_config(mock_scripts_dir, "a-ds", "0.1.0", "")
    result = discover_datasets()
    names = [r[0] for r in result]
    assert names == sorted(names)


# --- list_datasets tests ---


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.discover_datasets")
def test_list_datasets_marks_has_data(
    mock_discover: MagicMock, mock_has_data: MagicMock
) -> None:
    """has_data=True when (name, version) is in the DB set."""
    mock_discover.return_value = [("mock-ohlc", "0.1.0"), ("mock-daily-close", "0.1.0")]
    mock_has_data.return_value = {("mock-ohlc", "0.1.0")}

    result = list_datasets()
    assert len(result) == 2
    assert result[0] == DatasetInfo(name="mock-ohlc", version="0.1.0", has_data=True)
    assert result[1] == DatasetInfo(
        name="mock-daily-close", version="0.1.0", has_data=False
    )


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.discover_datasets")
def test_list_datasets_all_no_data(
    mock_discover: MagicMock, mock_has_data: MagicMock
) -> None:
    """All datasets have has_data=False when DB set is empty."""
    mock_discover.return_value = [("ds", "0.1.0")]
    mock_has_data.return_value = set()

    result = list_datasets()
    assert result == [DatasetInfo(name="ds", version="0.1.0", has_data=False)]


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.discover_datasets")
def test_list_datasets_empty_scripts_dir(
    mock_discover: MagicMock, mock_has_data: MagicMock
) -> None:
    """Empty discovery returns empty list regardless of DB."""
    mock_discover.return_value = []
    mock_has_data.return_value = {("ds", "0.1.0")}

    result = list_datasets()
    assert result == []


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.discover_datasets")
def test_list_datasets_single_db_call(
    mock_discover: MagicMock, mock_has_data: MagicMock
) -> None:
    """DB is queried exactly once regardless of how many datasets exist."""
    mock_discover.return_value = [("a", "0.1.0"), ("b", "0.1.0"), ("c", "0.1.0")]
    mock_has_data.return_value = set()

    list_datasets()
    mock_has_data.assert_called_once()
