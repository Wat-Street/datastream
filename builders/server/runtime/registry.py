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
