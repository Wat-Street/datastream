import os
from datetime import datetime
from pathlib import Path

import pytest
from runtime import runner


def _write_builder(tmp_path: Path, code: str) -> Path:
    """Write a builder.py to tmp_path and return the directory."""
    builder_file = tmp_path / "builder.py"
    builder_file.write_text(code)
    return tmp_path


def test_successful_build(tmp_path: Path) -> None:
    """Simple builder returns expected list."""
    script_dir = _write_builder(
        tmp_path,
        """
def build(dependencies, timestamp):
    return [{"value": 42}]
""",
    )
    result = runner.run_builder(
        script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=None
    )
    assert result == [{"value": 42}]


def test_builder_exception_raises_runtime_error(tmp_path: Path) -> None:
    """Builder that raises propagates as RuntimeError."""
    script_dir = _write_builder(
        tmp_path,
        """
def build(dependencies, timestamp):
    raise ValueError("something went wrong")
""",
    )
    with pytest.raises(RuntimeError, match="something went wrong"):
        runner.run_builder(
            script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=None
        )


def test_builder_timeout_raises_runtime_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Builder that sleeps too long raises timeout error."""
    script_dir = _write_builder(
        tmp_path,
        """
import time
def build(dependencies, timestamp):
    time.sleep(10)
    return []
""",
    )
    monkeypatch.setattr(runner, "TIMEOUT_SECONDS", 1)
    with pytest.raises(RuntimeError, match="timed out"):
        runner.run_builder(
            script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=None
        )


def test_builder_crash_raises_runtime_error(tmp_path: Path) -> None:
    """Builder that calls os._exit(1) raises crash error."""
    script_dir = _write_builder(
        tmp_path,
        """
import os
def build(dependencies, timestamp):
    os._exit(1)
""",
    )
    with pytest.raises(RuntimeError, match="crashed"):
        runner.run_builder(
            script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=None
        )


def test_builder_receives_correct_args(tmp_path: Path) -> None:
    """Verify dependencies and timestamp are passed through correctly."""
    script_dir = _write_builder(
        tmp_path,
        """
def build(dependencies, timestamp):
    return [{"ts": str(timestamp)}]
""",
    )
    ts = datetime(2024, 6, 15)
    deps: dict = {"my-dep": {ts: [{"val": 1}]}}
    result = runner.run_builder(script_dir, "builder.py", deps, ts, env_file=None)
    assert result[0]["ts"] == str(ts)


def test_builder_with_dependencies(tmp_path: Path) -> None:
    """Builder that reads from multi-row dependencies works."""
    script_dir = _write_builder(
        tmp_path,
        """
def build(dependencies, timestamp):
    rows = []
    for ts_data in dependencies["prices"].values():
        rows.extend(ts_data)
    return [{"price": row["close"]} for row in rows]
""",
    )
    ts = datetime(2024, 1, 1)
    deps: dict = {"prices": {ts: [{"close": 150.5}, {"close": 200.0}]}}
    result = runner.run_builder(script_dir, "builder.py", deps, ts, env_file=None)
    assert result == [{"price": 150.5}, {"price": 200.0}]


# --- env file injection tests ---


def test_builder_env_vars_injected(tmp_path: Path) -> None:
    """Builder subprocess receives env vars from .env file."""
    script_dir = _write_builder(
        tmp_path,
        """
import os
def build(dependencies, timestamp):
    return [{
        "api_key": os.environ.get("TEST_API_KEY", ""),
        "secret": os.environ.get("TEST_SECRET", ""),
    }]
""",
    )
    env_file = tmp_path / ".env"
    env_file.write_text("TEST_API_KEY=abc123\nTEST_SECRET=s3cret\n")
    result = runner.run_builder(
        script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=env_file
    )
    assert result == [{"api_key": "abc123", "secret": "s3cret"}]


def test_builder_env_file_none_is_noop(tmp_path: Path) -> None:
    """Passing env_file=None does not affect the build."""
    script_dir = _write_builder(
        tmp_path,
        """
def build(dependencies, timestamp):
    return [{"value": 42}]
""",
    )
    result = runner.run_builder(
        script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=None
    )
    assert result == [{"value": 42}]


def test_builder_env_vars_dont_leak_to_parent(tmp_path: Path) -> None:
    """Env vars loaded in subprocess don't appear in the parent process."""
    script_dir = _write_builder(
        tmp_path,
        """
def build(dependencies, timestamp):
    return [{"value": 1}]
""",
    )
    env_file = tmp_path / ".env"
    env_file.write_text("TEST_LEAK_CHECK=should_not_leak\n")
    runner.run_builder(
        script_dir, "builder.py", {}, datetime(2024, 1, 1), env_file=env_file
    )
    assert "TEST_LEAK_CHECK" not in os.environ
