import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from runtime import loader
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")


def test_load_builder_returns_callable(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Returns a callable."""
    write_builder(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
def build(dependencies, timestamp):
    return {"value": 1}
""",
    )
    fn = loader.load_builder("ds", V010)
    assert callable(fn)


def test_load_builder_function_works(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Returned function produces correct output."""
    write_builder(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
def build(dependencies, timestamp):
    return {"ticker": "AAPL", "price": 42}
""",
    )
    fn = loader.load_builder("ds", V010)
    result = fn({}, None)
    assert result == {"ticker": "AAPL", "price": 42}


def test_load_builder_cleans_up_sys_path(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Script dir is removed from sys.path after import."""
    write_builder(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
def build(dependencies, timestamp):
    return {}
""",
    )
    loader.load_builder("ds", V010)
    script_dir = str(mock_scripts_dir / "ds" / "0.1.0")
    assert script_dir not in sys.path


def test_load_builder_missing_file_raises(mock_scripts_dir: Path) -> None:
    """Nonexistent builder.py raises error."""
    d = mock_scripts_dir / "ds" / "0.1.0"
    d.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        loader.load_builder("ds", V010)


def test_load_builder_missing_build_function(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Builder.py without build function raises AttributeError."""
    write_builder(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
def not_build():
    pass
""",
    )
    with pytest.raises(AttributeError):
        loader.load_builder("ds", V010)


def test_load_builder_with_relative_import(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Builder that imports from a sibling module works."""
    d = mock_scripts_dir / "ds" / "0.1.0"
    d.mkdir(parents=True, exist_ok=True)
    (d / "helper.py").write_text("VALUE = 99\n")
    write_builder(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
import helper

def build(dependencies, timestamp):
    return {"value": helper.VALUE}
""",
    )
    fn = loader.load_builder("ds", V010)
    result = fn({}, None)
    assert result == {"value": 99}


def test_load_builder_no_sys_path_pollution(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Loading multiple datasets leaves sys.path unchanged."""
    for name in ["ds-a", "ds-b", "ds-c"]:
        write_builder(
            mock_scripts_dir,
            name,
            "0.1.0",
            """
def build(dependencies, timestamp):
    return {}
""",
        )

    path_before = sys.path.copy()
    for name in ["ds-a", "ds-b", "ds-c"]:
        loader.load_builder(name, V010)
    assert sys.path == path_before


def test_load_builder_missing_directory_raises() -> None:
    """Missing dataset directory raises FileNotFoundError with clear message."""
    with pytest.raises(FileNotFoundError, match="dataset directory not found"):
        loader.load_builder("nonexistent-dataset", V010)
