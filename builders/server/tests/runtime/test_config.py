from datetime import timedelta

import pytest
from runtime.config import (
    SchemaType,
    normalize_config,
    parse_lookback,
)

# --- SchemaType tests ---


@pytest.mark.parametrize(
    ["schema_type", "py_type"],
    [
        (SchemaType.INT, int),
        (SchemaType.FLOAT, (int, float)),
        (SchemaType.BOOL, bool),
        (SchemaType.STR, str),
    ],
)
def test_schema_type_to_type(
    schema_type: SchemaType, py_type: type | tuple[type, ...]
) -> None:
    assert schema_type.to_type() == py_type


# --- normalize_config tests ---


def test_normalize_config() -> None:
    mock_config = {
        "name": "my-dataset",
        "version": "2.0.0",
        "builder": "builder.py",
        "granularity": "1m",
        "start-date": "2020-01-01",
        "schema": {
            "val1": "int",
            "val2": "float",
            "val3": "str",
            "val4": "bool",
            "val5": "float",
        },
    }
    expected = {
        "name": "my-dataset",
        "version": "2.0.0",
        "builder": "builder.py",
        "granularity": "1m",
        "start-date": "2020-01-01",
        "schema": {
            "val1": SchemaType.INT,
            "val2": SchemaType.FLOAT,
            "val3": SchemaType.STR,
            "val4": SchemaType.BOOL,
            "val5": SchemaType.FLOAT,
        },
    }

    normalize_config(mock_config)
    assert mock_config == expected


# --- parse_lookback tests ---


def test_parse_lookback_days() -> None:
    """Parses '5d' to 4-day subtract (5 days inclusive)."""
    assert parse_lookback("5d") == timedelta(days=4)


def test_parse_lookback_hours() -> None:
    """Parses '24h' to 23-hour subtract (24 hours inclusive)."""
    assert parse_lookback("24h") == timedelta(hours=23)


def test_parse_lookback_minutes() -> None:
    """Parses '30m' to 29-minute subtract (30 minutes inclusive)."""
    assert parse_lookback("30m") == timedelta(minutes=29)


def test_parse_lookback_seconds() -> None:
    """Parses '60s' to 59-second subtract (60 seconds inclusive)."""
    assert parse_lookback("60s") == timedelta(seconds=59)


def test_parse_lookback_invalid_format_raises() -> None:
    """Invalid format raises ValueError."""
    with pytest.raises(ValueError, match="invalid lookback"):
        parse_lookback("5x")


def test_parse_lookback_no_number_raises() -> None:
    """Missing number raises ValueError."""
    with pytest.raises(ValueError, match="invalid lookback"):
        parse_lookback("d")


def test_parse_lookback_zero_raises() -> None:
    """Zero lookback raises ValueError."""
    with pytest.raises(ValueError, match="must be positive"):
        parse_lookback("0d")


def test_parse_lookback_empty_raises() -> None:
    """Empty string raises ValueError."""
    with pytest.raises(ValueError, match="invalid lookback"):
        parse_lookback("")
