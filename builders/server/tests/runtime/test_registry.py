from collections.abc import Callable
from pathlib import Path

import pytest
from runtime import registry
from runtime.config import DatasetConfig
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")
V020 = SemVer.parse("0.2.0")

_MINIMAL = """\
name = "{name}"
version = "{version}"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
"""

_WITH_DEP = """\
name = "{name}"
version = "{version}"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
{dep_name} = "{dep_version}"
"""


@pytest.fixture(autouse=True)
def clear_registry() -> None:
    """reset CONFIG_REGISTRY between tests."""
    registry.CONFIG_REGISTRY = {}


# --- happy path ---


def test_load_all_configs_empty_dir(tmp_path: Path) -> None:
    """empty scripts_dir loads zero configs without error."""
    registry.load_all_configs(tmp_path)
    assert registry.CONFIG_REGISTRY == {}


def test_load_all_configs_missing_dir(tmp_path: Path) -> None:
    """non-existent scripts_dir warns and loads zero configs."""
    registry.load_all_configs(tmp_path / "does-not-exist")
    assert registry.CONFIG_REGISTRY == {}


def test_load_all_configs_single(tmp_path: Path, write_config: Callable) -> None:
    """single valid config is discovered and stored."""
    write_config(tmp_path, "ds", "0.1.0", _MINIMAL.format(name="ds", version="0.1.0"))
    registry.load_all_configs(tmp_path)
    assert (("ds", V010)) in registry.CONFIG_REGISTRY
    cfg = registry.CONFIG_REGISTRY[("ds", V010)]
    assert isinstance(cfg, DatasetConfig)
    assert cfg.name == "ds"


def test_load_all_configs_multiple(tmp_path: Path, write_config: Callable) -> None:
    """multiple configs across datasets all loaded."""
    write_config(tmp_path, "a", "0.1.0", _MINIMAL.format(name="a", version="0.1.0"))
    write_config(tmp_path, "b", "0.1.0", _MINIMAL.format(name="b", version="0.1.0"))
    write_config(tmp_path, "b", "0.2.0", _MINIMAL.format(name="b", version="0.2.0"))
    registry.load_all_configs(tmp_path)
    assert len(registry.CONFIG_REGISTRY) == 3
    assert ("a", V010) in registry.CONFIG_REGISTRY
    assert ("b", V010) in registry.CONFIG_REGISTRY
    assert ("b", V020) in registry.CONFIG_REGISTRY


def test_load_all_configs_with_valid_dependency(
    tmp_path: Path, write_config: Callable
) -> None:
    """dataset with a valid dependency loads without error."""
    write_config(tmp_path, "dep", "0.1.0", _MINIMAL.format(name="dep", version="0.1.0"))
    write_config(
        tmp_path,
        "parent",
        "0.1.0",
        _WITH_DEP.format(
            name="parent", version="0.1.0", dep_name="dep", dep_version="0.1.0"
        ),
    )
    registry.load_all_configs(tmp_path)
    assert len(registry.CONFIG_REGISTRY) == 2


def test_load_all_configs_skips_dirs_without_config_toml(
    tmp_path: Path, write_config: Callable
) -> None:
    """directories without config.toml are silently skipped."""
    write_config(tmp_path, "ds", "0.1.0", _MINIMAL.format(name="ds", version="0.1.0"))
    # create a version dir with no config.toml
    (tmp_path / "ds" / "0.2.0").mkdir(parents=True)
    registry.load_all_configs(tmp_path)
    assert len(registry.CONFIG_REGISTRY) == 1


# --- get_config ---


def test_get_config_found(tmp_path: Path, write_config: Callable) -> None:
    """get_config returns the correct DatasetConfig."""
    write_config(tmp_path, "ds", "0.1.0", _MINIMAL.format(name="ds", version="0.1.0"))
    registry.load_all_configs(tmp_path)
    cfg = registry.get_config("ds", V010)
    assert cfg.name == "ds"
    assert cfg.version == V010


def test_get_config_not_found_raises(tmp_path: Path) -> None:
    """get_config raises ValueError for unknown dataset."""
    registry.load_all_configs(tmp_path)
    with pytest.raises(ValueError, match="not found in config registry"):
        registry.get_config("ghost", V010)


# --- structural validation failures ---


def test_load_all_configs_invalid_config_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """structurally invalid config (missing schema) raises ValueError at startup."""
    write_config(
        tmp_path,
        "ds",
        "0.1.0",
        """\
name = "ds"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"
""",
    )
    with pytest.raises(ValueError, match="missing 'schema' field"):
        registry.load_all_configs(tmp_path)


# --- dependency existence failures ---


def test_load_all_configs_missing_dependency_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """dataset referencing a dep not in scripts_dir raises ValueError."""
    write_config(
        tmp_path,
        "parent",
        "0.1.0",
        _WITH_DEP.format(
            name="parent", version="0.1.0", dep_name="missing-dep", dep_version="0.1.0"
        ),
    )
    with pytest.raises(ValueError, match="missing-dep/0.1.0"):
        registry.load_all_configs(tmp_path)


# --- cycle detection ---


def test_load_all_configs_self_cycle_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """dataset depending on itself raises ValueError."""
    write_config(
        tmp_path,
        "ds",
        "0.1.0",
        _WITH_DEP.format(
            name="ds", version="0.1.0", dep_name="ds", dep_version="0.1.0"
        ),
    )
    with pytest.raises(ValueError, match="cycle detected"):
        registry.load_all_configs(tmp_path)


def test_load_all_configs_two_node_cycle_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """A->B->A cycle raises ValueError."""
    write_config(
        tmp_path,
        "a",
        "0.1.0",
        _WITH_DEP.format(name="a", version="0.1.0", dep_name="b", dep_version="0.1.0"),
    )
    write_config(
        tmp_path,
        "b",
        "0.1.0",
        _WITH_DEP.format(name="b", version="0.1.0", dep_name="a", dep_version="0.1.0"),
    )
    with pytest.raises(ValueError, match="cycle detected"):
        registry.load_all_configs(tmp_path)


def test_load_all_configs_three_node_cycle_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """A->B->C->A cycle raises ValueError."""
    write_config(
        tmp_path,
        "a",
        "0.1.0",
        _WITH_DEP.format(name="a", version="0.1.0", dep_name="b", dep_version="0.1.0"),
    )
    write_config(
        tmp_path,
        "b",
        "0.1.0",
        _WITH_DEP.format(name="b", version="0.1.0", dep_name="c", dep_version="0.1.0"),
    )
    write_config(
        tmp_path,
        "c",
        "0.1.0",
        _WITH_DEP.format(name="c", version="0.1.0", dep_name="a", dep_version="0.1.0"),
    )
    with pytest.raises(ValueError, match="cycle detected"):
        registry.load_all_configs(tmp_path)


def test_load_all_configs_diamond_no_cycle(
    tmp_path: Path, write_config: Callable
) -> None:
    """diamond shape (A->B, A->C, B->D, C->D) is valid."""
    write_config(tmp_path, "d", "0.1.0", _MINIMAL.format(name="d", version="0.1.0"))
    write_config(
        tmp_path,
        "b",
        "0.1.0",
        _WITH_DEP.format(name="b", version="0.1.0", dep_name="d", dep_version="0.1.0"),
    )
    write_config(
        tmp_path,
        "c",
        "0.1.0",
        _WITH_DEP.format(name="c", version="0.1.0", dep_name="d", dep_version="0.1.0"),
    )
    write_config(
        tmp_path,
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
    registry.load_all_configs(tmp_path)  # no exception
    assert len(registry.CONFIG_REGISTRY) == 4


# --- granularity violation ---


def test_load_all_configs_granularity_violation_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """parent with finer granularity than dependency raises ValueError."""
    write_config(
        tmp_path,
        "dep",
        "0.1.0",
        """\
name = "dep"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    write_config(
        tmp_path,
        "parent",
        "0.1.0",
        """\
name = "parent"
version = "0.1.0"
granularity = "1h"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="finer than dependency"):
        registry.load_all_configs(tmp_path)


def test_load_all_configs_equal_granularity_ok(
    tmp_path: Path, write_config: Callable
) -> None:
    """equal granularity between parent and dep is valid."""
    write_config(
        tmp_path,
        "dep",
        "0.1.0",
        """\
name = "dep"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    write_config(
        tmp_path,
        "parent",
        "0.1.0",
        """\
name = "parent"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep = "0.1.0"
""",
    )
    registry.load_all_configs(tmp_path)  # no exception


# --- start-date violation ---


def test_load_all_configs_start_date_violation_raises(
    tmp_path: Path, write_config: Callable
) -> None:
    """parent with earlier start-date than dependency raises ValueError."""
    write_config(
        tmp_path,
        "dep",
        "0.1.0",
        """\
name = "dep"
version = "0.1.0"
granularity = "1d"
start-date = "2021-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    write_config(
        tmp_path,
        "parent",
        "0.1.0",
        """\
name = "parent"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="comes before dependency"):
        registry.load_all_configs(tmp_path)


def test_load_all_configs_start_date_equal_ok(
    tmp_path: Path, write_config: Callable
) -> None:
    """equal start-date between parent and dep is valid."""
    write_config(
        tmp_path,
        "dep",
        "0.1.0",
        """\
name = "dep"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"
""",
    )
    write_config(
        tmp_path,
        "parent",
        "0.1.0",
        """\
name = "parent"
version = "0.1.0"
granularity = "1d"
start-date = "2020-01-01"
calendar = "everyday"

[schema]
price = "int"

[dependencies]
dep = "0.1.0"
""",
    )
    registry.load_all_configs(tmp_path)  # no exception
