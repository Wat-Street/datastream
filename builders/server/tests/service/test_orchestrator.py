from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from runtime.config import DependencyInfo
from service.orchestrator import run_build

from .conftest import V010, _cfg

JAN1 = datetime(2024, 1, 1)
JAN5 = datetime(2024, 1, 5)


# --- single root dataset works ---


@patch("service.orchestrator.execute_job")
@patch("service.scheduler.registry")
def test_single_root(mock_registry, mock_execute) -> None:
    """Root with no deps -> schedule + execute 1 job successfully."""
    mock_registry.get_config.return_value = _cfg(name="root")
    mock_execute.return_value = MagicMock(success=True)

    run_build("root", V010, JAN1, JAN5)

    assert mock_execute.call_count == 1
    job = mock_execute.call_args[0][0]
    assert job.dataset_name == "root"


# --- level-by-level execution order ---


@patch("service.orchestrator.execute_job")
@patch("service.scheduler.registry")
def test_level_order_execution(mock_registry, mock_execute) -> None:
    """A -> B -> C: C built first, then B, then A."""
    configs = {
        "A": _cfg(name="A", dependencies={"B": DependencyInfo(version=V010)}),
        "B": _cfg(name="B", dependencies={"C": DependencyInfo(version=V010)}),
        "C": _cfg(name="C"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]
    mock_execute.return_value = MagicMock(success=True)

    run_build("A", V010, JAN1, JAN5)

    assert mock_execute.call_count == 3
    executed_names = [c[0][0].dataset_name for c in mock_execute.call_args_list]
    # C at level 0, B at level 1, A at level 2
    assert executed_names == ["C", "B", "A"]


# --- failure at level N prevents level N+1 ---


@patch("service.orchestrator.execute_job")
@patch("service.scheduler.registry")
def test_failure_stops_subsequent_levels(mock_registry, mock_execute) -> None:
    """If B fails at level 1, A at level 2 never executes."""
    configs = {
        "A": _cfg(name="A", dependencies={"B": DependencyInfo(version=V010)}),
        "B": _cfg(name="B", dependencies={"C": DependencyInfo(version=V010)}),
        "C": _cfg(name="C"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    # C succeeds, B fails
    def mock_exec(job, cancelled):
        if job.dataset_name == "B":
            return MagicMock(success=False, error="B crashed")
        return MagicMock(success=True)

    mock_execute.side_effect = mock_exec

    with pytest.raises(RuntimeError, match="B crashed"):
        run_build("A", V010, JAN1, JAN5)

    # only C and B were attempted, A was never reached
    executed_names = [c[0][0].dataset_name for c in mock_execute.call_args_list]
    assert "C" in executed_names
    assert "B" in executed_names
    assert "A" not in executed_names


# --- NoValidTimestampsError propagates ---


@patch("service.orchestrator.execute_job")
@patch("service.scheduler.registry")
def test_no_valid_timestamps_propagates(mock_registry, mock_execute) -> None:
    """Worker failure with no-valid-timestamps error propagates as RuntimeError."""
    mock_registry.get_config.return_value = _cfg(name="ds")
    mock_execute.return_value = MagicMock(
        success=False, error="no valid calendar timestamps"
    )

    with pytest.raises(RuntimeError, match="no valid calendar timestamps"):
        run_build("ds", V010, JAN1, JAN5)


# --- diamond graph executes correctly ---


@patch("service.orchestrator.execute_job")
@patch("service.scheduler.registry")
def test_diamond_graph(mock_registry, mock_execute) -> None:
    """Diamond A -> {B, C}, B -> D, C -> D: D once at level 0, {B,C} at 1, A at 2."""
    configs = {
        "A": _cfg(
            name="A",
            dependencies={
                "B": DependencyInfo(version=V010),
                "C": DependencyInfo(version=V010),
            },
        ),
        "B": _cfg(name="B", dependencies={"D": DependencyInfo(version=V010)}),
        "C": _cfg(name="C", dependencies={"D": DependencyInfo(version=V010)}),
        "D": _cfg(name="D"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]
    mock_execute.return_value = MagicMock(success=True)

    run_build("A", V010, JAN1, JAN5)

    # 4 jobs total, D appears exactly once
    assert mock_execute.call_count == 4
    executed_names = [c[0][0].dataset_name for c in mock_execute.call_args_list]
    assert executed_names.count("D") == 1

    # D first, A last
    assert executed_names[0] == "D"
    assert executed_names[-1] == "A"

    # B and C in the middle (level 1, order doesn't matter)
    middle = set(executed_names[1:3])
    assert middle == {"B", "C"}


# --- cancelled event is set on failure ---


@patch("service.orchestrator.execute_job")
@patch("service.scheduler.registry")
def test_cancelled_event_set_on_failure(mock_registry, mock_execute) -> None:
    """When a job fails, the cancelled event passed to execute_job is set."""
    mock_registry.get_config.return_value = _cfg(name="ds")
    mock_execute.return_value = MagicMock(success=False, error="boom")

    with pytest.raises(RuntimeError):
        run_build("ds", V010, JAN1, JAN5)

    # verify the cancelled event passed to execute_job was set
    cancelled = mock_execute.call_args[0][1]
    assert cancelled.is_set()
