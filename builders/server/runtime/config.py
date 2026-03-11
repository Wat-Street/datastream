import re
import tomllib
from datetime import datetime, timedelta
from pathlib import Path

from utils.semver import SemVer

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"

# values must be valid arguments to isinstance (type or tuple of types)
# keys define what strings are allowed in config.toml schema fields
TYPE_MAP = {
    "str": str,
    "int": int,
    "float": (int, float),  # accept int as valid float
    "bool": bool,
}

GRANULARITY_MAP = {
    "1s": timedelta(seconds=1),
    "1m": timedelta(minutes=1),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
}


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
    for key, type_name in config["schema"].items():
        if type_name not in TYPE_MAP:
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
    if config["granularity"] not in GRANULARITY_MAP:
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


def validate_config(config: dict, dataset_name: str, dataset_version: SemVer) -> None:
    """Validate that a parsed config dict has required fields and matches
    the dataset path."""
    # TODO (bryan): validate the existence of other fields in the config
    # toml such as builder, calender
    _validate_name_version(config, dataset_name, dataset_version)
    _validate_schema(config, dataset_name, dataset_version)
    _validate_granularity(config, dataset_name, dataset_version)
    _validate_start_date(config, dataset_name, dataset_version)


def load_config(dataset_name: str, dataset_version: SemVer) -> dict:
    """Load and validate config.toml for a given dataset."""
    config_path = SCRIPTS_DIR / dataset_name / str(dataset_version) / "config.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    validate_config(config, dataset_name, dataset_version)

    return config
