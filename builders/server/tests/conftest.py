from collections.abc import Callable
from pathlib import Path

import pytest


@pytest.fixture
def mock_scripts_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a temp scripts dir and monkeypatch SCRIPTS_DIR."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    from runtime import config, loader

    monkeypatch.setattr(config, "SCRIPTS_DIR", scripts)
    monkeypatch.setattr(loader, "SCRIPTS_DIR", scripts)
    return scripts


@pytest.fixture
def write_config(tmp_path: Path) -> Callable[[Path, str, str, str], Path]:
    """Factory fixture to write config.toml files under the scripts dir."""

    def _write(
        scripts_dir: Path, dataset_name: str, dataset_version: str, content: str
    ) -> Path:
        d = scripts_dir / dataset_name / dataset_version
        d.mkdir(parents=True, exist_ok=True)
        (d / "config.toml").write_text(content)
        return d

    return _write


@pytest.fixture
def write_builder(tmp_path: Path) -> Callable[[Path, str, str, str], Path]:
    """Factory fixture to write builder.py files under the scripts dir."""

    def _write(
        scripts_dir: Path, dataset_name: str, dataset_version: str, content: str
    ) -> Path:
        d = scripts_dir / dataset_name / dataset_version
        d.mkdir(parents=True, exist_ok=True)
        (d / "builder.py").write_text(content)
        return d

    return _write
