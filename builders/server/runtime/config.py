import os
import re
import tomllib
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

import structlog
from calendars.interface import Calendar
from calendars.registry import CALENDARS_MAP
from utils.semver import SemVer

logger = structlog.get_logger()


@dataclass(frozen=True)
class DependencyInfo:
    """Parsed dependency with version and optional lookback window."""

    version: SemVer
    # pre-computed offset to subtract from T to get the inclusive window start
    # e.g. "5d" -> timedelta(days=4), so window is [T - 4d, T] (5 days inclusive)
    lookback_subtract: timedelta | None = None


class SchemaType(StrEnum):
    """Allowed types for dataset schema fields."""

    STR = "str"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"

    def to_type(self) -> type | tuple[type, ...]:
        """Convert `SchemaType` to its corresponding python type(s)."""
        TYPE_MAP: dict[str, type | tuple[type, ...]] = {
            SchemaType.STR: str,
            SchemaType.INT: int,
            SchemaType.FLOAT: (int, float),  # accept int as float
            SchemaType.BOOL: bool,
        }
        return TYPE_MAP[self]


@dataclass(frozen=True)
class DatasetConfig:
    """Parsed and validated dataset configuration."""

    name: str
    version: SemVer
    builder: str
    calendar: Calendar
    granularity: timedelta
    start_date: datetime
    schema: dict[str, SchemaType]
    dependencies: dict[str, DependencyInfo]
    env_vars: bool


# defaults for optional TOML fields
DEFAULT_BUILDER = "builder.py"

# overridable via env var for local dev, where the scripts dir
# is not a sibling of the server package
_default_scripts = str(Path(__file__).resolve().parent.parent / "scripts")
SCRIPTS_DIR = Path(os.environ.get("SCRIPTS_DIR", _default_scripts))

_GRANULARITY_MAP = {
    "1s": timedelta(seconds=1),
    "1m": timedelta(minutes=1),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
}

# maps duration unit suffixes to timedelta kwargs
DURATION_UNITS = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
}


def parse_lookback(value: str) -> timedelta:
    """Parse a duration string like '5d', '24h', '30m', '60s'.

    Returns the timedelta to subtract from T to get the inclusive
    window start. e.g. "5d" -> timedelta(days=4), so the window
    [T - 4d, T] contains exactly 5 days.
    """
    match = re.fullmatch(r"(\d+)([smhd])", value)
    if not match:
        raise ValueError(
            f"invalid lookback '{value}', "
            "expected format like '5d', '24h', '30m', '60s'"
        )
    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        raise ValueError(f"lookback must be positive, got '{value}'")
    return timedelta(**{DURATION_UNITS[unit]: amount - 1})


def _validate_name_version(
    config: dict, dataset_name: str, dataset_version: SemVer
) -> None:
    """Validate that name and version fields match the dataset path."""
    if "name" not in config:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} is missing 'name' field"
        )
    if "version" not in config:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} "
            "is missing 'version' field"
        )

    if config["name"] != dataset_name:
        raise ValueError(
            f"config.toml name '{config['name']}' does not match "
            f"directory name '{dataset_name}'"
        )

    config_version = SemVer.parse(config["version"])
    if config_version != dataset_version:
        raise ValueError(
            f"config.toml version '{config['version']}' does not match "
            f"directory version '{dataset_version}'"
        )


def _validate_schema(config: dict, dataset_name: str, dataset_version: SemVer) -> None:
    """Validate that the schema field is present, non-empty, and uses known types."""
    # check that the schema exists and is non-empty
    if "schema" not in config:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} "
            "is missing 'schema' field"
        )
    if not config["schema"]:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} "
            "'schema' must not be empty"
        )

    # validate each schema value
    for key, type_name in config["schema"].items():
        if type_name not in SchemaType:
            raise ValueError(
                f"config.toml for {dataset_name}/{dataset_version} has unknown "
                f"type '{type_name}' for schema key '{key}'"
            )


def _validate_granularity(
    config: dict, dataset_name: str, dataset_version: SemVer
) -> None:
    """Validate that the granularity field is present and a known value."""
    if "granularity" not in config:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} "
            "is missing 'granularity' field"
        )
    if config["granularity"] not in _GRANULARITY_MAP:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} has unknown "
            f"granularity '{config['granularity']}'"
        )


def _validate_start_date(
    config: dict, dataset_name: str, dataset_version: SemVer
) -> None:
    """Validate that start-date is present and a valid YYYY-MM-DD date."""
    if "start-date" not in config:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} "
            "is missing 'start-date' field"
        )
    value = config["start-date"]
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} has invalid "
            f"start-date '{value}', expected YYYY-MM-DD format"
        )
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as err:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} has invalid "
            f"start-date '{value}', not a real date"
        ) from err


def _validate_calendar(
    config: dict, dataset_name: str, dataset_version: SemVer
) -> None:
    """Validate that the calendar field is present and refers to a known calendar."""
    if "calendar" not in config:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} "
            "is missing 'calendar' field"
        )
    calendar_name = config["calendar"]
    if calendar_name not in CALENDARS_MAP:
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version} has unknown "
            f"calendar '{calendar_name}'"
        )


def _validate_env_vars(
    config: dict, dataset_name: str, dataset_version: SemVer
) -> None:
    """Validate that env-vars is a bool if present."""
    if "env-vars" in config and not isinstance(config["env-vars"], bool):
        raise ValueError(
            f"config.toml for {dataset_name}/{dataset_version}: "
            f"'env-vars' must be a boolean, got {type(config['env-vars']).__name__}"
        )


def _validate_dependencies_format(
    config: dict, dataset_name: str, dataset_version: SemVer
) -> None:
    """Validate that each dependency is a version string or a table with a
    'version' key."""
    deps = config.get("dependencies")
    if deps is None:
        return

    for dep_name, dep_value in deps.items():
        if isinstance(dep_value, str):
            pass  # simple format: dep = "0.1.0"
        elif isinstance(dep_value, dict):
            # table format: dep = {version = "0.1.0", lookback = "5d"}
            if "version" not in dep_value:
                raise ValueError(
                    f"config.toml for {dataset_name}/{dataset_version}: "
                    f"dependency '{dep_name}' table is missing 'version'"
                )
        else:
            raise ValueError(
                f"config.toml for {dataset_name}/{dataset_version}: "
                f"dependency '{dep_name}' must be a version string or a table"
            )


def _normalize_dependencies(config: dict) -> None:
    """Normalize dependencies in place to ``DependencyInfo`` instances.

    Assumes the config has already been validated.
    """
    deps = config.get("dependencies")
    if deps is None:
        return

    normalized: dict[str, DependencyInfo] = {}
    for dep_name, dep_value in deps.items():
        if isinstance(dep_value, str):
            normalized[dep_name] = DependencyInfo(version=SemVer.parse(dep_value))
        else:
            lookback_subtract: timedelta | None = None
            if "lookback" in dep_value:
                lookback_subtract = parse_lookback(dep_value["lookback"])
            normalized[dep_name] = DependencyInfo(
                version=SemVer.parse(dep_value["version"]),
                lookback_subtract=lookback_subtract,
            )

    config["dependencies"] = normalized


def validate_config(config: dict, dataset_name: str, dataset_version: SemVer) -> None:
    """Validate that a parsed config dict has required fields and matches
    the dataset path."""
    _validate_name_version(config, dataset_name, dataset_version)
    _validate_schema(config, dataset_name, dataset_version)
    _validate_granularity(config, dataset_name, dataset_version)
    _validate_start_date(config, dataset_name, dataset_version)
    _validate_calendar(config, dataset_name, dataset_version)
    _validate_env_vars(config, dataset_name, dataset_version)
    _validate_dependencies_format(config, dataset_name, dataset_version)


def _normalize_config_schema(config: dict) -> None:
    """
    Normalize **in place** a config's schema, by converting values from
    type `str` to type `SchemaType`.

    Assumes that config has already been validated. That is, the schema
    field exists, and all values are valid types.
    """
    normalized_schema: dict[str, SchemaType] = {}
    for key, type_name in config["schema"].items():
        normalized_schema[key] = SchemaType(type_name)
    config["schema"] = normalized_schema


def normalize_config(config: dict) -> None:
    """
    Normalize **in place** a config's values based off a set of rules.
    """
    _normalize_config_schema(config)
    _normalize_dependencies(config)


def _check_dependency_graph_cycles(
    dataset_name: str,
    dataset_version: SemVer,
    in_stack: set[tuple[str, SemVer]],
    visited: set[tuple[str, SemVer]],
) -> None:
    """Recursively walk the dependency graph, raising `ValueError` on a cycle.

    `in_stack` tracks nodes on the current DFS path; a dep already in
    `in_stack` means a cycle exists. `visited` tracks fully-explored nodes
    so shared dependencies (diamonds) are not re-visited.
    """
    node = (dataset_name, dataset_version)
    if node in visited:
        return
    in_stack.add(node)
    cfg = _load_config_no_cycles_check(dataset_name, dataset_version)
    for dep_name, dep_info in cfg.dependencies.items():
        dep_version = dep_info.version
        dep_node = (dep_name, dep_version)
        if dep_node in in_stack:
            raise ValueError(
                f"dependency cycle detected: {dep_name}/{dep_version} "
                f"is an ancestor of {dataset_name}/{dataset_version}"
            )
        _check_dependency_graph_cycles(dep_name, dep_version, in_stack, visited)
    in_stack.discard(node)
    visited.add(node)


def check_dependency_graph_cycles(dataset_name: str, dataset_version: SemVer) -> None:
    """Raise `ValueError` if the dependency graph rooted at the given dataset
    contains a cycle."""
    _check_dependency_graph_cycles(dataset_name, dataset_version, set(), set())


@lru_cache(maxsize=256)
def _load_config_no_cycles_check(
    dataset_name: str, dataset_version: SemVer
) -> DatasetConfig:
    """Load and validate config.toml without checking for dependency cycles."""
    config_path = SCRIPTS_DIR / dataset_name / str(dataset_version) / "config.toml"
    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    validate_config(raw, dataset_name, dataset_version)
    normalize_config(raw)

    logger.debug(
        "config loaded",
        dataset=dataset_name,
        version=str(dataset_version),
        path=str(config_path),
    )

    return DatasetConfig(
        name=raw["name"],
        version=SemVer.parse(raw["version"]),
        builder=raw.get("builder", DEFAULT_BUILDER),
        calendar=CALENDARS_MAP[raw["calendar"]],
        granularity=_GRANULARITY_MAP[raw["granularity"]],
        start_date=datetime.strptime(raw["start-date"], "%Y-%m-%d"),
        schema=raw["schema"],
        dependencies=raw.get("dependencies", {}),
        env_vars=raw.get("env-vars", False),
    )


@lru_cache(maxsize=256)
def _load_config_with_cycles_check(
    dataset_name: str, dataset_version: SemVer
) -> DatasetConfig:
    """Load config with dependency cycle validation."""
    check_dependency_graph_cycles(dataset_name, dataset_version)
    return _load_config_no_cycles_check(dataset_name, dataset_version)


def clear_config_caches() -> None:
    """Clear in-process config caches."""
    for fn in (_load_config_with_cycles_check, _load_config_no_cycles_check):
        cache_clear = getattr(fn, "cache_clear", None)
        if callable(cache_clear):
            cache_clear()


def load_config(dataset_name: str, dataset_version: SemVer) -> DatasetConfig:
    """Load and validate config.toml for a given dataset.

    Raises ValueError if the dependency graph contains a cycle.
    """
    return _load_config_with_cycles_check(dataset_name, dataset_version)
