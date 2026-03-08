import os
import time
from datetime import datetime
from typing import Any

import pytest
from runtime import runner

# Top-level functions so they are picklable for multiprocessing


def _simple_build(
    dependencies: dict[str, list[dict]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Return a simple list of dicts."""
    return [{"value": 42}]


def _raising_build(
    dependencies: dict[str, list[dict]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Raise an exception."""
    raise ValueError("something went wrong")


def _sleeping_build(
    dependencies: dict[str, list[dict]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Sleep longer than the timeout."""
    time.sleep(10)
    return []


def _crashing_build(
    dependencies: dict[str, list[dict]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Hard-crash the subprocess."""
    os._exit(1)


def _echo_build(
    dependencies: dict[str, list[dict]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Echo back args to verify they are passed correctly."""
    return [{"deps": dependencies, "ts": str(timestamp)}]


def _dep_read_build(
    dependencies: dict[str, list[dict]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Read from dependencies dict (multi-row)."""
    return [{"price": row["close"]} for row in dependencies["prices"]]


def test_successful_build() -> None:
    """Simple function returns expected list."""
    result = runner.run_builder(_simple_build, {}, datetime(2024, 1, 1))
    assert result == [{"value": 42}]


def test_builder_exception_raises_runtime_error() -> None:
    """Function that raises propagates as RuntimeError."""
    with pytest.raises(RuntimeError, match="something went wrong"):
        runner.run_builder(_raising_build, {}, datetime(2024, 1, 1))


def test_builder_timeout_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Function that sleeps too long raises timeout error."""
    monkeypatch.setattr(runner, "TIMEOUT_SECONDS", 1)
    with pytest.raises(RuntimeError, match="timed out"):
        runner.run_builder(_sleeping_build, {}, datetime(2024, 1, 1))


def test_builder_crash_raises_runtime_error() -> None:
    """Function that calls os._exit(1) raises crash error."""
    with pytest.raises(RuntimeError, match="crashed"):
        runner.run_builder(_crashing_build, {}, datetime(2024, 1, 1))


def test_builder_receives_correct_args() -> None:
    """Verify dependencies and timestamp are passed through correctly."""
    deps: dict[str, list[dict]] = {"my-dep": [{"val": 1}]}
    ts = datetime(2024, 6, 15)
    result = runner.run_builder(_echo_build, deps, ts)
    assert result[0]["deps"] == deps
    assert result[0]["ts"] == str(ts)


def test_builder_with_dependencies() -> None:
    """Function that reads from multi-row dependencies works."""
    deps: dict[str, list[dict]] = {"prices": [{"close": 150.5}, {"close": 200.0}]}
    result = runner.run_builder(_dep_read_build, deps, datetime(2024, 1, 1))
    assert result == [{"price": 150.5}, {"price": 200.0}]
