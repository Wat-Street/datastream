import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from runtime import loader


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
    fn = loader.load_builder("ds", "0.1.0")
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
    fn = loader.load_builder("ds", "0.1.0")
    result = fn({}, None)
    assert result == {"ticker": "AAPL", "price": 42}


def test_load_builder_adds_to_sys_path(
    mock_scripts_dir: Path, write_builder: Callable
) -> None:
    """Script dir is added to sys.path."""
    write_builder(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
def build(dependencies, timestamp):
    return {}
""",
    )
    loader.load_builder("ds", "0.1.0")
    script_dir = str(mock_scripts_dir / "ds" / "0.1.0")
    assert script_dir in sys.path


def test_load_builder_missing_file_raises(mock_scripts_dir: Path) -> None:
    """Nonexistent builder.py raises error."""
    d = mock_scripts_dir / "ds" / "0.1.0"
    d.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        loader.load_builder("ds", "0.1.0")


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
        loader.load_builder("ds", "0.1.0")


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
    fn = loader.load_builder("ds", "0.1.0")
    result = fn({}, None)
    assert result == {"value": 99}
