import tomllib
from pathlib import Path

import structlog
from utils.semver import SemVer

from runtime.config import (
    DatasetConfig,
    normalize_config,
    validate_config,
)

logger = structlog.get_logger()

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


def _validate_deps_exist(name: str, version: SemVer) -> None:
    """Validate that all direct dependencies of a dataset exist in the registry."""
    cfg = _CONFIG_REGISTRY[(name, version)]
    for dep_name, dep_info in cfg.dependencies.items():
        if (dep_name, dep_info.version) not in _CONFIG_REGISTRY:
            raise ValueError(
                f"{name}/{version} references dependency "
                f"{dep_name}/{dep_info.version} which was not found in SCRIPTS_DIR"
            )


def _check_cycles(
    name: str,
    version: SemVer,
    path: set[tuple[str, SemVer]],
    visited: set[tuple[str, SemVer]],
) -> None:
    """DFS cycle detection over CONFIG_REGISTRY.

    `path` tracks ancestors in the current DFS path; a dep already in `path` means
    a cycle. `visited` tracks fully-explored nodes to skip re-traversal.
    """
    node = (name, version)
    path.add(node)
    cfg = _CONFIG_REGISTRY[node]
    for dep_name, dep_info in cfg.dependencies.items():
        dep_node = (dep_name, dep_info.version)
        if dep_node in path:
            raise ValueError(
                f"dependency cycle detected: {dep_name}/{dep_info.version} "
                f"is an ancestor of {name}/{version}"
            )
        if dep_node not in visited:
            _check_cycles(dep_name, dep_info.version, path, visited)
    path.discard(node)
    visited.add(node)


def _validate_granularity(name: str, version: SemVer) -> None:
    """Validate that a dataset's granularity is >= all dependencies'."""
    cfg = _CONFIG_REGISTRY[(name, version)]
    for dep_name, dep_info in cfg.dependencies.items():
        dep_cfg = _CONFIG_REGISTRY[(dep_name, dep_info.version)]
        if cfg.granularity < dep_cfg.granularity:
            raise ValueError(
                f"{name}/{version} has granularity '{cfg.granularity}' which is "
                f"finer than dependency '{dep_name}/{dep_info.version}' with "
                f"granularity '{dep_cfg.granularity}'"
            )


def _validate_start_date(name: str, version: SemVer) -> None:
    """Validate that a dataset's start_date is >= all direct dependencies'
    start_dates."""
    cfg = _CONFIG_REGISTRY[(name, version)]
    for dep_name, dep_info in cfg.dependencies.items():
        dep_cfg = _CONFIG_REGISTRY[(dep_name, dep_info.version)]
        if cfg.start_date < dep_cfg.start_date:
            raise ValueError(
                f"{name}/{version} has start date '{cfg.start_date}' which comes "
                f"before dependency {dep_name}/{dep_info.version} "
                f"with start date {dep_cfg.start_date}"
            )


def load_all_configs(scripts_dir: Path) -> None:
    """Discover, load, and validate every config in scripts_dir.

    Populates CONFIG_REGISTRY. Raises ValueError if any config is structurally
    invalid, a dependency is missing from scripts_dir, or the dependency graph
    has cycles, granularity violations, or start-date violations.

    The server lifespan calls this at startup; any error here prevents startup.
    """
    global _CONFIG_REGISTRY
    _CONFIG_REGISTRY = {}

    if not scripts_dir.is_dir():
        logger.warning(
            "scripts_dir not found, no configs loaded", path=str(scripts_dir)
        )
        return

    # step 1: discover and load all configs individually (structural validation)
    for name_dir in sorted(scripts_dir.iterdir()):
        if not name_dir.is_dir():
            continue
        for version_dir in sorted(name_dir.iterdir()):
            if not version_dir.is_dir():
                continue
            config_path = version_dir / "config.toml"
            if not config_path.exists():
                continue

            name = name_dir.name
            version = SemVer.parse(version_dir.name)

            with open(config_path, "rb") as f:
                raw = tomllib.load(f)

            validate_config(raw, name, version)
            normalize_config(raw)

            cfg = DatasetConfig.from_raw(raw)
            _CONFIG_REGISTRY[(name, version)] = cfg
            logger.debug("config loaded", dataset=name, version=str(version))

    logger.info("all configs loaded", count=len(_CONFIG_REGISTRY))

    # step 2: ensure every referenced dependency exists in the registry
    for name, version in _CONFIG_REGISTRY:
        _validate_deps_exist(name, version)

    # step 3: check for cycles across all datasets
    visited: set[tuple[str, SemVer]] = set()
    for name, version in _CONFIG_REGISTRY:
        if (name, version) not in visited:
            _check_cycles(name, version, set(), visited)

    # step 4: validate granularity constraints
    for name, version in _CONFIG_REGISTRY:
        _validate_granularity(name, version)

    # step 5: validate start-date constraints
    for name, version in _CONFIG_REGISTRY:
        _validate_start_date(name, version)

    logger.info("config graph validation passed", count=len(_CONFIG_REGISTRY))
