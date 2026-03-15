from pathlib import Path
from unittest.mock import patch

from runtime.venv_management import _ensure_venv, setup_builder_venvs


def _make_builder(tmp_path: Path, name: str, version: str, reqs: str) -> Path:
    """Create a builder directory with requirements.txt."""
    d = tmp_path / name / version
    d.mkdir(parents=True)
    (d / "requirements.txt").write_text(reqs)
    (d / "builder.py").write_text("def build(d, t): return []")
    return d


@patch("runtime.venv_management.subprocess.run")
def test_ensure_venv_creates_venv(mock_run, tmp_path: Path):
    """First run creates venv and writes hash file."""
    builder_dir = _make_builder(tmp_path, "ds", "0.1.0", "requests==2.32.0\n")
    _ensure_venv(builder_dir)

    # uv venv and uv pip install both called
    assert mock_run.call_count == 2
    assert "venv" in mock_run.call_args_list[0][0][0]
    assert "install" in mock_run.call_args_list[1][0][0]

    # hash file written
    hash_file = builder_dir / ".venv" / ".requirements_hash"
    assert hash_file.exists()


@patch("runtime.venv_management.subprocess.run")
def test_ensure_venv_skips_when_hash_matches(mock_run, tmp_path: Path):
    """Second run with same requirements skips venv creation."""
    builder_dir = _make_builder(tmp_path, "ds", "0.1.0", "requests==2.32.0\n")

    # first run
    _ensure_venv(builder_dir)
    mock_run.reset_mock()

    # second run, same requirements
    _ensure_venv(builder_dir)
    mock_run.assert_not_called()


@patch("runtime.venv_management.subprocess.run")
def test_ensure_venv_rebuilds_on_requirements_change(mock_run, tmp_path: Path):
    """Changing requirements.txt triggers rebuild."""
    builder_dir = _make_builder(tmp_path, "ds", "0.1.0", "requests==2.32.0\n")
    _ensure_venv(builder_dir)
    mock_run.reset_mock()

    # change requirements
    (builder_dir / "requirements.txt").write_text("requests==2.33.0\n")
    _ensure_venv(builder_dir)

    # venv recreated
    assert mock_run.call_count == 2


@patch("runtime.venv_management._ensure_venv")
def test_setup_skips_dirs_without_requirements(mock_ensure, tmp_path: Path):
    """Directories without requirements.txt are skipped."""
    # builder with no requirements.txt
    d = tmp_path / "ds" / "0.1.0"
    d.mkdir(parents=True)
    (d / "builder.py").write_text("def build(d, t): return []")

    setup_builder_venvs(tmp_path)
    mock_ensure.assert_not_called()


@patch("runtime.venv_management._ensure_venv")
def test_setup_continues_on_error(mock_ensure, tmp_path: Path):
    """One builder's venv failure doesn't block others."""
    _make_builder(tmp_path, "ds1", "0.1.0", "bad\n")
    _make_builder(tmp_path, "ds2", "0.1.0", "good\n")

    mock_ensure.side_effect = [RuntimeError("fail"), None]
    # should not raise
    setup_builder_venvs(tmp_path)
    assert mock_ensure.call_count == 2
