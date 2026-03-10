from collections.abc import Callable
from pathlib import Path

import pytest
from runtime import config
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")


def test_load_valid_config(mock_scripts_dir: Path, write_config: Callable) -> None:
    """Valid config.toml returns correct dict."""
    write_config(
        mock_scripts_dir,
        "my-dataset",
        "0.1.0",
        """
name = "my-dataset"
version = "0.1.0"
builder = "builder.py"
granularity = "1d"
""",
    )
    cfg = config.load_config("my-dataset", V010)
    assert cfg["name"] == "my-dataset"
    assert cfg["version"] == "0.1.0"
    assert cfg["granularity"] == "1d"


def test_load_config_missing_file_raises(mock_scripts_dir: Path) -> None:
    """Nonexistent file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        config.load_config("nonexistent", SemVer.parse("0.0.1"))


def test_load_config_missing_name_field(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without name raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
version = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="missing 'name' field"):
        config.load_config("ds", V010)


def test_load_config_missing_version_field(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Config without version raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
""",
    )
    with pytest.raises(ValueError, match="missing 'version' field"):
        config.load_config("ds", V010)


def test_load_config_name_mismatch(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Name doesn't match dir raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "wrong-name"
version = "0.1.0"
""",
    )
    with pytest.raises(ValueError, match="does not match"):
        config.load_config("ds", V010)


def test_load_config_version_mismatch(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Version doesn't match dir raises ValueError."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "9.9.9"
""",
    )
    with pytest.raises(ValueError, match="does not match"):
        config.load_config("ds", V010)


def test_load_config_with_dependencies(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Dependencies section parsed correctly."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"

[dependencies]
dep-a = "0.0.2"
dep-b = "1.0.0"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["dependencies"] == {"dep-a": "0.0.2", "dep-b": "1.0.0"}


def test_load_config_with_schema(
    mock_scripts_dir: Path, write_config: Callable
) -> None:
    """Schema section parsed correctly."""
    write_config(
        mock_scripts_dir,
        "ds",
        "0.1.0",
        """
name = "ds"
version = "0.1.0"

[schema]
ticker = "str"
price = "int"
""",
    )
    cfg = config.load_config("ds", V010)
    assert cfg["schema"] == {"ticker": "str", "price": "int"}
