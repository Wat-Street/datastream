import os
import time
from typing import Any

import pandas as pd
import pytest

from runtime import runner

# Top-level functions so they are picklable for multiprocessing


def _simple_build(
    dependencies: dict[str, dict], timestamp: pd.Timestamp
) -> dict[str, Any]:
    """Return a simple dict."""
    return {"value": 42}


def _raising_build(
    dependencies: dict[str, dict], timestamp: pd.Timestamp
) -> dict[str, Any]:
    """Raise an exception."""
    raise ValueError("something went wrong")


def _sleeping_build(
    dependencies: dict[str, dict], timestamp: pd.Timestamp
) -> dict[str, Any]:
    """Sleep longer than the timeout."""
    time.sleep(10)
    return {}


def _crashing_build(
    dependencies: dict[str, dict], timestamp: pd.Timestamp
) -> dict[str, Any]:
    """Hard-crash the subprocess."""
    os._exit(1)


def _echo_build(
    dependencies: dict[str, dict], timestamp: pd.Timestamp
) -> dict[str, Any]:
    """Echo back args to verify they are passed correctly."""
    return {"deps": dependencies, "ts": str(timestamp)}


def _dep_read_build(
    dependencies: dict[str, dict], timestamp: pd.Timestamp
) -> dict[str, Any]:
    """Read from dependencies dict."""
    return {"price": dependencies["prices"]["close"]}


def test_successful_build() -> None:
    """Simple function returns expected dict."""
    result = runner.run_builder(_simple_build, {}, pd.Timestamp("2024-01-01"))
    assert result == {"value": 42}


def test_builder_exception_raises_runtime_error() -> None:
    """Function that raises propagates as RuntimeError."""
    with pytest.raises(RuntimeError, match="something went wrong"):
        runner.run_builder(_raising_build, {}, pd.Timestamp("2024-01-01"))


def test_builder_timeout_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Function that sleeps too long raises timeout error."""
    monkeypatch.setattr(runner, "TIMEOUT_SECONDS", 1)
    with pytest.raises(RuntimeError, match="timed out"):
        runner.run_builder(_sleeping_build, {}, pd.Timestamp("2024-01-01"))


def test_builder_crash_raises_runtime_error() -> None:
    """Function that calls os._exit(1) raises crash error."""
    with pytest.raises(RuntimeError, match="crashed"):
        runner.run_builder(_crashing_build, {}, pd.Timestamp("2024-01-01"))


def test_builder_receives_correct_args() -> None:
    """Verify dependencies and timestamp are passed through correctly."""
    deps = {"my-dep": {"val": 1}}
    ts = pd.Timestamp("2024-06-15")
    result = runner.run_builder(_echo_build, deps, ts)
    assert result["deps"] == deps
    assert result["ts"] == str(ts)


def test_builder_with_dependencies() -> None:
    """Function that reads from dependencies dict works."""
    deps = {"prices": {"close": 150.5}}
    result = runner.run_builder(_dep_read_build, deps, pd.Timestamp("2024-01-01"))
    assert result == {"price": 150.5}
