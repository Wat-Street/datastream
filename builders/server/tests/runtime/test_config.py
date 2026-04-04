from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from runtime import config
from runtime.config import (
    DependencyInfo,
    SchemaType,
    clear_config_caches,
    parse_lookback,
)
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")


@pytest.fixture(autouse=True)
def clear_config_cache() -> None:
    """clear lru_cache between tests to prevent cross-test contamination."""
    clear_config_caches()


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

    config.normalize_config(mock_config)
    assert mock_config == expected


# --- load_config tests ---


def test_load_config_valid(mock_scripts_dir: Path, write_config: Callable) -> None:
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
calendar = "everyday"

[schema]
price = "int"
""",
    )
    cfg = config.load_config("my-dataset", V010)
    assert cfg.name == "my-dataset"
    assert cfg.version == V010
    assert cfg.granularity == timedelta(days=1)


def test_load_config_uses_cached_result(
    mock_scripts_dir: Path, write_config: Callable, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeated load_config calls for the same dataset use cached result."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
builder = "builder.py"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )

    call_count = 0
    original = config.check_dependency_graph_cycles

    def wrapped(dataset_name: str, dataset_version: SemVer) -> None:
        nonlocal call_count
        call_count += 1
        original(dataset_name, dataset_version)

    monkeypatch.setattr(config, "check_dependency_graph_cycles", wrapped)
    clear_config_caches()

    first = config.load_config("ds", V010)
    second = config.load_config("ds", V010)

    assert first == second
    assert call_count == 1


def test_load_config_missing_file_raises(mock_scripts_dir: Path) -> None:
    """Nonexistent file raises FileNotFoundError."""
    _ = mock_scripts_dir  # fixture patches SCRIPTS_DIR; value not needed
    with pytest.raises(FileNotFoundError):
        config.load_config("nonexistent", SemVer.parse("0.0.1"))


def test_load_config_missing_name_field_raises(
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


def test_load_config_missing_version_field_raises(
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


def test_load_config_name_mismatch_raises(
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


def test_load_config_version_mismatch_raises(
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
    for dep_name, dep_ver in [("dep-a", "0.0.2"), ("dep-b", "1.0.0")]:
        write_config(
            mock_scripts_dir,
            dep_name,
            dep_ver,
            f"""
name = "{dep_name}"
version = "{dep_ver}"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
        )
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep-a = "0.0.2"
dep-b = "1.0.0"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.dependencies == {
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
calendar = "everyday"

[schema]
ticker = "str"
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.schema == {"ticker": SchemaType.STR, "price": SchemaType.INT}


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
calendar = "everyday"

[schema]
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.start_date == datetime(2024, 6, 15)


# --- calendar validation tests ---


def test_load_config_valid_calendar(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with a known calendar resolves to a Calendar instance."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.calendar.name == "everyday"


def test_load_config_unknown_calendar_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config with unknown calendar raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "DOES_NOT_EXIST"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="unknown calendar"):
        config.load_config("ds", V010)


def test_load_config_missing_calendar_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without calendar field raises ValueError."""
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
""",
    )
    with pytest.raises(ValueError, match="missing 'calendar' field"):
        config.load_config("ds", V010)


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


# --- dependency normalization tests ---


def test_load_config_dep_table_with_lookback(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Table dep with lookback normalizes correctly."""
    write_config(
        mock_scripts_dir,
        "dep-a",
        "0.0.2",
        """
name = "dep-a"
version = "0.0.2"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep-a = {version = "0.0.2", lookback = "5d"}
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.dependencies["dep-a"] == DependencyInfo(
        version=SemVer.parse("0.0.2"),
        lookback_subtract=timedelta(days=4),
    )


def test_load_config_dep_table_without_lookback(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Table dep without lookback gets None."""
    write_config(
        mock_scripts_dir,
        "dep-a",
        "0.0.2",
        """
name = "dep-a"
version = "0.0.2"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep-a = {version = "0.0.2"}
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.dependencies["dep-a"] == DependencyInfo(
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
calendar = "everyday"

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
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep-a = {version = "0.0.2", lookback = "bad"}
""",
    )
    with pytest.raises(ValueError, match="invalid lookback"):
        config.load_config("ds", V010)


# --- env-vars validation tests ---


def test_load_config_env_vars_default_false(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """env_vars defaults to False when not specified."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.env_vars is False


def test_load_config_env_vars_explicit_true(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """env_vars is True when set in config."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"
env-vars = true

[schema]
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg.env_vars is True


def test_load_config_env_vars_invalid_type_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """env-vars with non-bool type raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"
env-vars = "yes"

[schema]
price = "int"
""",
    )
    with pytest.raises(ValueError, match="'env-vars' must be a boolean"):
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
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep-a = 123
""",
    )
    with pytest.raises(ValueError, match="must be a version string or a table"):
        config.load_config("ds", V010)


def test_load_config_self_cycle_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """load_config raises ValueError when the dataset depends on itself."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
ds = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="ds/0.1.0"):
        config.load_config("ds", V010)


def test_load_config_two_node_cycle_raises(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """load_config raises ValueError for a two-node cycle (A->B->A)."""
    write_config(
        mock_scripts_dir,
        "a",
        "0.1.0",
        """
name = "a"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
b = "0.1.0"
""",
    )
    write_config(
        mock_scripts_dir,
        "b",
        "0.1.0",
        """
name = "b"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
a = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="a/0.1.0"):
        config.load_config("a", V010)


# --- check_dependency_graph_cycles tests ---

_MINIMAL_CFG = """\
name = "{name}"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
"""

_MINIMAL_CFG_WITH_DEP = """\
name = "{name}"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
{dep_name} = "0.1.0"
"""


def test_cycles_no_deps(mock_scripts_dir: Path, write_config: Callable) -> None:
    write_config(mock_scripts_dir, "a", "0.1.0", _MINIMAL_CFG.format(name="a"))
    config.check_dependency_graph_cycles("a", V010)  # no exception


def test_cycles_linear_chain(mock_scripts_dir: Path, write_config: Callable) -> None:
    write_config(mock_scripts_dir, "c", "0.1.0", _MINIMAL_CFG.format(name="c"))
    write_config(
        mock_scripts_dir,
        "b",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="b", dep_name="c"),
    )
    write_config(
        mock_scripts_dir,
        "a",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="a", dep_name="b"),
    )
    config.check_dependency_graph_cycles("a", V010)  # no exception


def test_cycles_self_cycle(mock_scripts_dir: Path, write_config: Callable) -> None:
    write_config(
        mock_scripts_dir,
        "a",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="a", dep_name="a"),
    )
    with pytest.raises(ValueError, match="a/0.1.0"):
        config.check_dependency_graph_cycles("a", V010)


def test_cycles_two_node_cycle(mock_scripts_dir: Path, write_config: Callable) -> None:
    write_config(
        mock_scripts_dir,
        "a",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="a", dep_name="b"),
    )
    write_config(
        mock_scripts_dir,
        "b",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="b", dep_name="a"),
    )
    with pytest.raises(ValueError, match="a/0.1.0"):
        config.check_dependency_graph_cycles("a", V010)


def test_cycles_three_node_cycle(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    write_config(
        mock_scripts_dir,
        "a",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="a", dep_name="b"),
    )
    write_config(
        mock_scripts_dir,
        "b",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="b", dep_name="c"),
    )
    write_config(
        mock_scripts_dir,
        "c",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="c", dep_name="a"),
    )
    with pytest.raises(ValueError, match="a/0.1.0"):
        config.check_dependency_graph_cycles("a", V010)


def test_cycles_diamond_no_cycle(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    # A->B, A->C, B->D, C->D (diamond shape, no cycle)
    write_config(mock_scripts_dir, "d", "0.1.0", _MINIMAL_CFG.format(name="d"))
    write_config(
        mock_scripts_dir,
        "b",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="b", dep_name="d"),
    )
    write_config(
        mock_scripts_dir,
        "c",
        "0.1.0",
        _MINIMAL_CFG_WITH_DEP.format(name="c", dep_name="d"),
    )
    # A depends on both B and C, needs a custom config for two deps
    write_config(
        mock_scripts_dir,
        "a",
        "0.1.0",
        """\
name = "a"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
b = "0.1.0"
c = "0.1.0"
""",
    )
    config.check_dependency_graph_cycles("a", V010)  # no exception
