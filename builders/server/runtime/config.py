import tomllib
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


def validate_config(config: dict, dataset_name: str, dataset_version: SemVer) -> None:
    """Validate that a parsed config dict has required fields and matches
    the dataset path."""

    # TODO (bryan): validate the existence of other fields in the config
    # toml such as builder, calender, granularity
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


def load_config(dataset_name: str, dataset_version: SemVer) -> dict:
    """Load and validate config.toml for a given dataset."""
    config_path = SCRIPTS_DIR / dataset_name / str(dataset_version) / "config.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    validate_config(config, dataset_name, dataset_version)

    return config
