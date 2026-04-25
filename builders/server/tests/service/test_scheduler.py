from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from runtime.config import DependencyInfo
from service.scheduler import collect_graph

from .conftest import V010, _cfg

V020 = V010  # alias for readability in multi-version tests


# --- single root, no deps ---


@patch("service.scheduler.registry")
def test_single_root_no_deps(mock_registry) -> None:
    """Root dataset with no dependencies produces 1 node, no edges."""
    mock_registry.get_config.return_value = _cfg(name="root")

    graph = collect_graph("root", V010, datetime(2024, 1, 1), datetime(2024, 1, 5))

    assert len(graph.ranges) == 1
    assert ("root", V010) in graph.ranges
    assert graph.ranges[("root", V010)] == (datetime(2024, 1, 1), datetime(2024, 1, 5))
    assert graph.edges == {}


# --- linear chain ---


@patch("service.scheduler.registry")
def test_linear_chain(mock_registry) -> None:
    """A -> B -> C produces 3 nodes with correct edges and identical ranges."""
    configs = {
        "A": _cfg(
            name="A",
            dependencies={"B": DependencyInfo(version=V010)},
        ),
        "B": _cfg(
            name="B",
            dependencies={"C": DependencyInfo(version=V010)},
        ),
        "C": _cfg(name="C"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("A", V010, datetime(2024, 1, 1), datetime(2024, 1, 5))

    assert len(graph.ranges) == 3
    for name in ["A", "B", "C"]:
        assert (name, V010) in graph.ranges
        assert graph.ranges[(name, V010)] == (
            datetime(2024, 1, 1),
            datetime(2024, 1, 5),
        )

    assert graph.edges[("A", V010)] == {("B", V010)}
    assert graph.edges[("B", V010)] == {("C", V010)}
    assert ("C", V010) not in graph.edges


# --- diamond dependency ---


@patch("service.scheduler.registry")
def test_diamond_deduplication(mock_registry) -> None:
    """Diamond: A -> {B, C}, B -> D, C -> D. D appears once in ranges."""
    configs = {
        "A": _cfg(
            name="A",
            dependencies={
                "B": DependencyInfo(version=V010),
                "C": DependencyInfo(version=V010),
            },
        ),
        "B": _cfg(
            name="B",
            dependencies={"D": DependencyInfo(version=V010)},
        ),
        "C": _cfg(
            name="C",
            dependencies={"D": DependencyInfo(version=V010)},
        ),
        "D": _cfg(name="D"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("A", V010, datetime(2024, 1, 1), datetime(2024, 1, 5))

    assert len(graph.ranges) == 4
    # D appears exactly once
    assert ("D", V010) in graph.ranges
    assert graph.ranges[("D", V010)] == (datetime(2024, 1, 1), datetime(2024, 1, 5))

    # edges
    assert graph.edges[("A", V010)] == {("B", V010), ("C", V010)}
    assert graph.edges[("B", V010)] == {("D", V010)}
    assert graph.edges[("C", V010)] == {("D", V010)}


# --- lookback expansion ---


@patch("service.scheduler.registry")
def test_lookback_expands_dep_range(mock_registry) -> None:
    """Lookback on a dependency widens its build range backwards."""
    configs = {
        "parent": _cfg(
            name="parent",
            dependencies={
                "dep": DependencyInfo(
                    version=V010,
                    lookback_subtract=timedelta(days=4),
                ),
            },
        ),
        "dep": _cfg(name="dep"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("parent", V010, datetime(2024, 1, 10), datetime(2024, 1, 20))

    # parent range unchanged
    assert graph.ranges[("parent", V010)] == (
        datetime(2024, 1, 10),
        datetime(2024, 1, 20),
    )
    # dep range expanded: Jan 10 - 4d = Jan 6
    assert graph.ranges[("dep", V010)] == (
        datetime(2024, 1, 6),
        datetime(2024, 1, 20),
    )


@patch("service.scheduler.registry")
def test_lookback_propagates_through_chain(mock_registry) -> None:
    """Lookback expansion propagates: A (lookback=5d on B) -> B -> C.
    C's range should be expanded by A's lookback on B."""
    configs = {
        "A": _cfg(
            name="A",
            dependencies={
                "B": DependencyInfo(
                    version=V010,
                    lookback_subtract=timedelta(days=4),
                ),
            },
        ),
        "B": _cfg(
            name="B",
            dependencies={"C": DependencyInfo(version=V010)},
        ),
        "C": _cfg(name="C"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("A", V010, datetime(2024, 1, 10), datetime(2024, 1, 20))

    # A: unchanged
    assert graph.ranges[("A", V010)] == (
        datetime(2024, 1, 10),
        datetime(2024, 1, 20),
    )
    # B: expanded by A's lookback
    assert graph.ranges[("B", V010)] == (
        datetime(2024, 1, 6),
        datetime(2024, 1, 20),
    )
    # C: inherits B's expanded range (no additional lookback)
    assert graph.ranges[("C", V010)] == (
        datetime(2024, 1, 6),
        datetime(2024, 1, 20),
    )


# --- diamond with different lookbacks ---


@patch("service.scheduler.registry")
def test_diamond_different_lookbacks_union(mock_registry) -> None:
    """Diamond where B and C both depend on D with different lookbacks.
    D's range should be the union of both expanded ranges."""
    configs = {
        "A": _cfg(
            name="A",
            dependencies={
                "B": DependencyInfo(version=V010),
                "C": DependencyInfo(version=V010),
            },
        ),
        "B": _cfg(
            name="B",
            dependencies={
                # B needs D with 3d lookback -> dep_start = Jan 10 - 2d = Jan 8
                "D": DependencyInfo(version=V010, lookback_subtract=timedelta(days=2)),
            },
        ),
        "C": _cfg(
            name="C",
            dependencies={
                # C needs D with 7d lookback -> dep_start = Jan 10 - 6d = Jan 4
                "D": DependencyInfo(version=V010, lookback_subtract=timedelta(days=6)),
            },
        ),
        "D": _cfg(name="D"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("A", V010, datetime(2024, 1, 10), datetime(2024, 1, 20))

    # D's range should be union: min(Jan 8, Jan 4)=Jan 4, max(Jan 20, Jan 20)=Jan 20
    assert graph.ranges[("D", V010)] == (
        datetime(2024, 1, 4),
        datetime(2024, 1, 20),
    )


# --- start-date clamping ---


@patch("service.scheduler.registry")
def test_start_date_clamping(mock_registry) -> None:
    """Start before dataset start-date gets clamped forward."""
    mock_registry.get_config.return_value = _cfg(
        name="ds", start_date=datetime(2024, 1, 5)
    )

    graph = collect_graph("ds", V010, datetime(2024, 1, 1), datetime(2024, 1, 10))

    # start clamped to Jan 5
    assert graph.ranges[("ds", V010)] == (
        datetime(2024, 1, 5),
        datetime(2024, 1, 10),
    )


@patch("service.scheduler.registry")
def test_end_before_start_date_raises(mock_registry) -> None:
    """End before dataset start-date raises ValueError."""
    mock_registry.get_config.return_value = _cfg(
        name="ds", start_date=datetime(2024, 6, 1)
    )

    with pytest.raises(ValueError, match="before.*start-date"):
        collect_graph("ds", V010, datetime(2024, 5, 1), datetime(2024, 5, 15))


@patch("service.scheduler.registry")
def test_lookback_clamped_by_dep_start_date(mock_registry) -> None:
    """Lookback expansion that pushes start before dep's start-date gets clamped."""
    configs = {
        "parent": _cfg(
            name="parent",
            dependencies={
                "dep": DependencyInfo(
                    version=V010,
                    lookback_subtract=timedelta(days=30),
                ),
            },
        ),
        # dep has start-date Jan 1, lookback would push to Dec 2023
        "dep": _cfg(name="dep", start_date=datetime(2024, 1, 1)),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("parent", V010, datetime(2024, 1, 10), datetime(2024, 1, 20))

    # dep start clamped to its start-date, not pushed before it
    assert graph.ranges[("dep", V010)][0] == datetime(2024, 1, 1)


# --- diamond re-expansion propagation ---


@patch("service.scheduler.registry")
def test_diamond_reexpansion_propagates_to_grandchildren(mock_registry) -> None:
    """When a diamond dep's range expands on second visit, the expansion
    propagates to its children.

    Graph: A -> {B, C}. B -> D -> E. C -> D (with lookback).
    B is visited first, giving D range [Jan 10, Jan 20].
    C is visited second with lookback, giving D range [Jan 4, Jan 20].
    D's range expands, so E must also get the expanded range.
    """
    configs = {
        "A": _cfg(
            name="A",
            dependencies={
                "B": DependencyInfo(version=V010),
                "C": DependencyInfo(version=V010),
            },
        ),
        "B": _cfg(
            name="B",
            dependencies={"D": DependencyInfo(version=V010)},
        ),
        "C": _cfg(
            name="C",
            dependencies={
                "D": DependencyInfo(version=V010, lookback_subtract=timedelta(days=6)),
            },
        ),
        "D": _cfg(
            name="D",
            dependencies={"E": DependencyInfo(version=V010)},
        ),
        "E": _cfg(name="E"),
    }
    mock_registry.get_config.side_effect = lambda name, version: configs[name]

    graph = collect_graph("A", V010, datetime(2024, 1, 10), datetime(2024, 1, 20))

    # D expanded to Jan 4 via C's lookback
    assert graph.ranges[("D", V010)] == (
        datetime(2024, 1, 4),
        datetime(2024, 1, 20),
    )
    # E must also have the expanded range from D's re-walk
    assert graph.ranges[("E", V010)] == (
        datetime(2024, 1, 4),
        datetime(2024, 1, 20),
    )
