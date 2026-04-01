from datetime import datetime
from unittest.mock import patch

import pytest
from datastream.types import DatasetResponse, DatasetRow, DatasetVersion


def test_parse_valid_semver():
    v = DatasetVersion.parse("1.2.3")
    assert v.major == 1
    assert v.minor == 2
    assert v.patch == 3


def test_parse_zero_version():
    v = DatasetVersion.parse("0.0.0")
    assert v == DatasetVersion(0, 0, 0)


def test_str_roundtrip():
    assert str(DatasetVersion.parse("0.1.0")) == "0.1.0"
    assert str(DatasetVersion(10, 20, 30)) == "10.20.30"


def test_parse_invalid_raises():
    with pytest.raises(ValueError, match="invalid semver"):
        DatasetVersion.parse("not-a-version")


def test_parse_incomplete_raises():
    with pytest.raises(ValueError, match="invalid semver"):
        DatasetVersion.parse("1.2")


def test_parse_extra_parts_raises():
    with pytest.raises(ValueError, match="invalid semver"):
        DatasetVersion.parse("1.2.3.4")


def test_frozen():
    v = DatasetVersion.parse("1.0.0")
    with pytest.raises(AttributeError):
        v.major = 2  # type: ignore[misc]


def test_dataset_response_construction():
    row = DatasetRow(
        timestamp=datetime(2024, 1, 2),
        data=[{"ticker": "AAPL", "close": 150}],
    )
    resp = DatasetResponse(
        dataset_name="mock-ohlc",
        dataset_version=DatasetVersion.parse("0.1.0"),
        total_timestamps=3,
        returned_timestamps=1,
        rows=[row],
    )
    assert resp.dataset_name == "mock-ohlc"
    assert str(resp.dataset_version) == "0.1.0"
    assert resp.total_timestamps == 3
    assert resp.returned_timestamps == 1
    assert len(resp.rows) == 1
    assert resp.rows[0].data == [{"ticker": "AAPL", "close": 150}]


def _make_response(rows: list[DatasetRow]) -> DatasetResponse:
    return DatasetResponse(
        dataset_name="mock-ohlc",
        dataset_version=DatasetVersion.parse("0.1.0"),
        total_timestamps=len(rows),
        returned_timestamps=len(rows),
        rows=rows,
    )


def test_to_pandas_flat_rows():
    pytest.importorskip("pandas")
    rows = [
        DatasetRow(
            timestamp=datetime(2024, 1, 2),
            data=[
                {"ticker": "AAPL", "close": 130},
                {"ticker": "MSFT", "close": 220},
            ],
        ),
    ]
    df = _make_response(rows).to_pandas()
    assert list(df.columns) == ["timestamp", "ticker", "close"]
    assert len(df) == 2
    assert df["ticker"].tolist() == ["AAPL", "MSFT"]
    assert df["timestamp"].tolist() == [datetime(2024, 1, 2), datetime(2024, 1, 2)]


def test_to_pandas_empty_rows():
    pandas = pytest.importorskip("pandas")
    df = _make_response([]).to_pandas()
    assert isinstance(df, pandas.DataFrame)
    assert len(df) == 0


def test_to_pandas_import_error():
    with (
        patch.dict("sys.modules", {"pandas": None}),
        pytest.raises(ImportError, match="pip install datastream-sdk\\[pandas\\]"),
    ):
        _make_response([]).to_pandas()


def test_to_polars_flat_rows():
    pytest.importorskip("polars")
    rows = [
        DatasetRow(
            timestamp=datetime(2024, 1, 2),
            data=[
                {"ticker": "AAPL", "close": 130},
                {"ticker": "MSFT", "close": 220},
            ],
        ),
    ]
    df = _make_response(rows).to_polars()
    assert set(df.columns) == {"timestamp", "ticker", "close"}
    assert len(df) == 2
    assert df["ticker"].to_list() == ["AAPL", "MSFT"]


def test_to_polars_empty_rows():
    polars = pytest.importorskip("polars")
    df = _make_response([]).to_polars()
    assert isinstance(df, polars.DataFrame)
    assert len(df) == 0


def test_to_polars_import_error():
    with (
        patch.dict("sys.modules", {"polars": None}),
        pytest.raises(ImportError, match="pip install datastream-sdk\\[polars\\]"),
    ):
        _make_response([]).to_polars()
