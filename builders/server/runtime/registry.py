from utils.semver import SemVer

from runtime.config import DatasetConfig

# populated once at startup by load_all_configs(); never mutated after that
_CONFIG_REGISTRY: dict[tuple[str, SemVer], DatasetConfig] = {}


def get_config(name: str, version: SemVer) -> DatasetConfig:
    """Return the pre-loaded config for a dataset. Raises ValueError if not found."""
    key = (name, version)
    if key not in _CONFIG_REGISTRY:
        raise ValueError(
            f"dataset {name}/{version} not found in config registry; "
            "ensure it exists in SCRIPTS_DIR and the server started successfully"
        )
    return _CONFIG_REGISTRY[key]
