from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest
from runtime import config
from runtime.config import DependencyInfo, parse_lookback
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")


def test_load_valid_config(mock_scripts_dir: Path, write_config: Callable) -> None:
    """Valid config.toml returns correct dict."""
    write_config(
        mock_scripts_dir,
        "my-dataset",
        "0.1.0",
        """
name = "my-dataset"
version = "0.1.0"
builder = "builder.py"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"
""",
    )
    cfg = config.load_config("my-dataset", V010)
    assert cfg["name"] == "my-dataset"
    assert cfg["version"] == "0.1.0"
    assert cfg["granularity"] == "1d"


def test_load_config_missing_file_raises(mock_scripts_dir: Path) -> None:
    """Nonexistent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        config.load_config("nonexistent", SemVer.parse("0.0.1"))


def test_load_config_missing_name_field(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without name raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
version = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="missing 'name' field"):
        config.load_config("ds", V010)


def test_load_config_missing_version_field(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without version raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
""",
    )
    with pytest.raises(ValueError, match="missing 'version' field"):
        config.load_config("ds", V010)


def test_load_config_name_mismatch(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Name doesn't match dir raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "wrong-name"
version = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="does not match"):
        config.load_config("ds", V010)


def test_load_config_version_mismatch(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Version doesn't match dir raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "9.9.9"
""",
    )
    with pytest.raises(ValueError, match="does not match"):
        config.load_config("ds", V010)


def test_load_config_with_dependencies(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Dependencies section parsed correctly."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"

[dependencies]
dep-a = "0.0.2"
dep-b = "1.0.0"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["dependencies"] == {
        "dep-a": DependencyInfo(version=SemVer.parse("0.0.2")),
        "dep-b": DependencyInfo(version=SemVer.parse("1.0.0")),
    }


def test_load_config_missing_schema_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without schema raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="missing 'schema' field"):
        config.load_config("ds", V010)


def test_load_config_empty_schema_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with empty schema raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"

[schema]
""",
    )
    with pytest.raises(ValueError, match="'schema' must not be empty"):
        config.load_config("ds", V010)


def test_load_config_unknown_schema_type_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Schema with unknown type raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"

[schema]
ts = "datetime"
""",
    )
    with pytest.raises(ValueError, match="unknown type 'datetime'"):
        config.load_config("ds", V010)


def test_load_config_with_schema(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Schema section parsed correctly."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
ticker = "str"
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["schema"] == {"ticker": "str", "price": "int"}


def test_load_config_missing_granularity_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without granularity raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="missing 'granularity' field"):
        config.load_config("ds", V010)


def test_load_config_invalid_granularity_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with unknown granularity raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1w"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="unknown granularity '1w'"):
        config.load_config("ds", V010)


def test_load_config_missing_start_date_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without start-date raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="missing 'start-date' field"):
        config.load_config("ds", V010)


def test_load_config_invalid_start_date_format_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with wrong date format raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "01-01-2024"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="invalid start-date"):
        config.load_config("ds", V010)


def test_load_config_invalid_start_date_not_a_date_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with impossible date raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2024-13-01"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="not a real date"):
        config.load_config("ds", V010)


def test_load_config_valid_start_date(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with valid start-date passes validation."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2024-06-15"

[schema]
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["start-date"] == "2024-06-15"


# --- parse_lookback tests ---


def test_parse_lookback_days() -> None:
    """Parses '5d' to 5-day timedelta."""
    assert parse_lookback("5d") == timedelta(days=5)


def test_parse_lookback_hours() -> None:
    """Parses '24h' to 24-hour timedelta."""
    assert parse_lookback("24h") == timedelta(hours=24)


def test_parse_lookback_minutes() -> None:
    """Parses '30m' to 30-minute timedelta."""
    assert parse_lookback("30m") == timedelta(minutes=30)


def test_parse_lookback_seconds() -> None:
    """Parses '60s' to 60-second timedelta."""
    assert parse_lookback("60s") == timedelta(seconds=60)


def test_parse_lookback_invalid_format() -> None:
    """Invalid format raises ValueError."""
    with pytest.raises(ValueError, match="invalid lookback"):
        parse_lookback("5x")


def test_parse_lookback_no_number() -> None:
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


# --- dependency normalization tests ---


def test_load_config_dep_table_with_lookback(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Table dep with lookback normalizes correctly."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"

[dependencies]
dep-a = {version = "0.0.2", lookback = "5d"}
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["dependencies"]["dep-a"] == DependencyInfo(
        version=SemVer.parse("0.0.2"), lookback=timedelta(days=5)
    )


def test_load_config_dep_table_without_lookback(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Table dep without lookback gets None."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"

[dependencies]
dep-a = {version = "0.0.2"}
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["dependencies"]["dep-a"] == DependencyInfo(
        version=SemVer.parse("0.0.2"),
    )


def test_load_config_dep_table_missing_version_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Table dep without version raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"

[dependencies]
dep-a = {lookback = "5d"}
""",
    )
    with pytest.raises(ValueError, match="missing 'version'"):
        config.load_config("ds", V010)


def test_load_config_dep_invalid_lookback_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Table dep with invalid lookback raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"

[dependencies]
dep-a = {version = "0.0.2", lookback = "bad"}
""",
    )
    with pytest.raises(ValueError, match="invalid lookback"):
        config.load_config("ds", V010)


def test_load_config_dep_invalid_type_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Dependency with invalid type (not str or table) raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"

[schema]
price = "int"

[dependencies]
dep-a = 123
""",
    )
    with pytest.raises(ValueError, match="must be a version string or a table"):
        config.load_config("ds", V010)
