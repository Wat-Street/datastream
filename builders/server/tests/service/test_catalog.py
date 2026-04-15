from unittest.mock import MagicMock, patch

from service.catalog import DatasetInfo, list_datasets
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")
V020 = SemVer.parse("0.2.0")

# minimal registry entries: catalog only iterates keys, values are unused
_MOCK_REGISTRY = {
    ("mock-ohlc", V010): MagicMock(),
    ("mock-daily-close", V010): MagicMock(),
}


# --- list_datasets tests ---


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.CONFIG_REGISTRY", _MOCK_REGISTRY)
def test_list_datasets_marks_has_data(mock_has_data: MagicMock) -> None:
    """has_data=True when (name, str(version)) is in the DB set."""
    mock_has_data.return_value = {("mock-ohlc", "0.1.0")}

    result = list_datasets()

    assert len(result) == 2
    # results are sorted by name, so daily-close comes before ohlc
    assert result[0] == DatasetInfo(
        name="mock-daily-close", version="0.1.0", has_data=False
    )
    assert result[1] == DatasetInfo(name="mock-ohlc", version="0.1.0", has_data=True)


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.CONFIG_REGISTRY", _MOCK_REGISTRY)
def test_list_datasets_all_no_data(mock_has_data: MagicMock) -> None:
    """All datasets have has_data=False when DB set is empty."""
    mock_has_data.return_value = set()

    result = list_datasets()

    assert all(not d.has_data for d in result)
    assert len(result) == 2


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch("service.catalog.CONFIG_REGISTRY", {})
def test_list_datasets_empty_registry(mock_has_data: MagicMock) -> None:
    """Empty registry returns empty list regardless of DB."""
    mock_has_data.return_value = {("ds", "0.1.0")}

    result = list_datasets()

    assert result == []


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch(
    "service.catalog.CONFIG_REGISTRY",
    {
        ("a", V010): MagicMock(),
        ("b", V010): MagicMock(),
        ("c", V010): MagicMock(),
    },
)
def test_list_datasets_single_db_call(mock_has_data: MagicMock) -> None:
    """DB is queried exactly once regardless of how many datasets exist."""
    mock_has_data.return_value = set()

    list_datasets()

    mock_has_data.assert_called_once()


@patch("service.catalog.db.datasets.get_datasets_with_data")
@patch(
    "service.catalog.CONFIG_REGISTRY",
    {
        ("z-ds", V010): MagicMock(),
        ("a-ds", V010): MagicMock(),
        ("m-ds", V020): MagicMock(),
    },
)
def test_list_datasets_sorted_by_name_then_version(mock_has_data: MagicMock) -> None:
    """Results are sorted by name then version."""
    mock_has_data.return_value = set()

    result = list_datasets()

    names = [d.name for d in result]
    assert names == sorted(names)
