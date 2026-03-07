import tomllib
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent / "scripts"


def validate_config(config: dict, dataset_name: str, dataset_version: str) -> None:
    """Validate that a parsed config dict has required fields and matches
    the dataset path."""

    # TODO (bryan): validate the existence of other fields in the config
    # toml such as builder, calender, granularity, schema
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
    if config["version"] != dataset_version:
        raise ValueError(
            f"config.toml version '{config['version']}' does not match "
            f"directory version '{dataset_version}'"
        )


def load_config(dataset_name: str, dataset_version: str) -> dict:
    """Load and validate config.toml for a given dataset."""
    config_path = SCRIPTS_DIR / dataset_name / dataset_version / "config.toml"
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    validate_config(config, dataset_name, dataset_version)

    return config
