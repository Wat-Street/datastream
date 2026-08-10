"""Validate the real builders/scripts directory, not fixtures.

These tests gate dataset PRs (including ones opened by the dataset-creation
flow): green CI means every committed config passes the same validation the
server runs at startup, so a merged dataset cannot break boot.
"""

import ast
import tomllib
from pathlib import Path

import core.runtime.registry as registry
import pytest

# builders/server/tests/core/runtime/ -> builders/scripts
REAL_SCRIPTS_DIR = Path(__file__).resolve().parents[4] / "scripts"

VERSION_DIRS = sorted(
    (path.parent for path in REAL_SCRIPTS_DIR.glob("*/*/config.toml")),
    key=str,
)


@pytest.fixture(autouse=True)
def _reset_registry():
    """reset _CONFIG_REGISTRY between tests."""
    registry._CONFIG_REGISTRY = {}
    yield
    registry._CONFIG_REGISTRY = {}


def _version_dir_id(version_dir: Path) -> str:
    return f"{version_dir.parent.name}/{version_dir.name}"


def test_scripts_dir_exists() -> None:
    """the repo layout puts real builder scripts at builders/scripts."""
    assert REAL_SCRIPTS_DIR.is_dir(), f"{REAL_SCRIPTS_DIR} not found"
    assert VERSION_DIRS, "no config.toml found under builders/scripts"


def test_all_real_configs_load_and_validate() -> None:
    """every committed config passes the full startup validation

    (structure, deps exist, no cycles, granularity, start dates).
    """
    registry.load_all_configs(REAL_SCRIPTS_DIR)
    assert len(registry._CONFIG_REGISTRY) == len(VERSION_DIRS)


@pytest.mark.parametrize("version_dir", VERSION_DIRS, ids=_version_dir_id)
def test_builder_script_defines_build(version_dir: Path) -> None:
    """each builder script exists, parses, and defines a top-level

    build(dependencies, timestamp). ast-only so builder deps are never
    imported.
    """
    with open(version_dir / "config.toml", "rb") as f:
        raw = tomllib.load(f)
    builder_path = version_dir / raw.get("builder", "builder.py")
    assert builder_path.is_file(), f"{builder_path} missing"

    tree = ast.parse(builder_path.read_text(), filename=str(builder_path))
    build_fns = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build"
    ]
    assert build_fns, f"{builder_path} has no top-level build()"

    args = build_fns[0].args
    positional = args.posonlyargs + args.args
    assert len(positional) == 2, (
        f"{builder_path} build() must take (dependencies, timestamp), "
        f"got {[a.arg for a in positional]}"
    )
