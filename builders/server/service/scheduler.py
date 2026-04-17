"""Dependency graph collection and topological scheduling for builds.

This module has two layers:

1. Graph collection (collect_graph): DFS walk that computes time ranges
   each dataset needs, accounting for lookback expansion, start-date
   clamping, and diamond dependency deduplication.

2. Topological scheduling (schedule_build): Kahn's algorithm that consumes
   the collected graph and produces a BuildPlan with jobs grouped by
   topological level. Level 0 = roots (no deps), level N+1 = datasets
   whose deps all live in levels 0..N. The orchestrator executes levels
   sequentially as barriers -- all jobs in level N must complete before
   any job in level N+1 starts.

Terminology:
    Node: a (dataset_name, SemVer) pair identifying one dataset.
    Edge: parent -> dependency. "A depends on B" means edge A -> B.
    Range: the [start, end] time interval a dataset must be built for.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import structlog
from runtime import registry
from utils.semver import SemVer

from service.models import BuildPlan, JobDescriptor

logger = structlog.get_logger()

# type alias for a node in the dependency graph
Node = tuple[str, SemVer]


@dataclass
class DependencyGraph:
    """The raw dependency graph with computed time ranges.

    Produced by collect_graph(). Contains all the information needed for
    Kahn's algorithm to produce a topologically ordered BuildPlan.

    Attributes:
        ranges: maps each node to its required (start, end) build range.
            Ranges account for lookback expansion and start-date clamping.
            For diamond dependencies, the range is the union (widest) of
            all ranges requested by different parents.
        edges: maps each parent node to the set of its direct dependencies.
            Only populated for nodes that have dependencies. Root nodes
            (no deps) appear in ranges but not in edges.
    """

    ranges: dict[Node, tuple[datetime, datetime]] = field(default_factory=dict)
    edges: dict[Node, set[Node]] = field(default_factory=dict)


def collect_graph(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> DependencyGraph:
    """Walk the dependency tree and collect all nodes with their required ranges.

    Starting from the root dataset, performs a DFS through the dependency
    graph. For each node it:
    1. Clamps start to the dataset's start_date (same as _build_recursive)
    2. Records the required [start, end] range
    3. Expands dependency ranges by lookback_subtract when applicable
    4. Recurses into dependencies

    Diamond dependencies (same dataset reachable via multiple paths) are
    handled by taking the union of required ranges. If a second visit
    widens a node's range, its subtree is re-walked so the expansion
    propagates to grandchildren. Since ranges only ever widen (never
    shrink), convergence is guaranteed for any acyclic graph.

    Args:
        dataset_name: root dataset to build
        dataset_version: version of the root dataset
        start: requested build start time
        end: requested build end time

    Returns:
        DependencyGraph with ranges and edges for all reachable datasets

    Raises:
        ValueError: if end < start_date for any dataset in the graph
    """
    graph = DependencyGraph()
    _collect(dataset_name, dataset_version, start, end, graph)
    return graph


def _collect(
    name: str,
    version: SemVer,
    start: datetime,
    end: datetime,
    graph: DependencyGraph,
) -> None:
    """Recursive DFS helper that populates the graph.

    For each node, this function:
    1. Loads the config from the startup registry
    2. Validates end >= start_date, clamps start to start_date
    3. Checks if we've visited this node before (diamond case):
       - If yes and the range didn't expand: return early (no re-walk needed)
       - If yes and the range expanded: update to union, re-walk children
       - If no: record the range and walk children
    4. For each dependency, computes the expanded range (lookback) and recurses

    Args:
        name: dataset name
        version: dataset version
        start: required start for this node (may be expanded by parent's lookback)
        end: required end for this node
        graph: mutable graph being built up across the DFS
    """
    cfg = registry.get_config(name, version)
    node: Node = (name, version)

    # enforce start-date: reject if end is before the dataset's start_date,
    # clamp start forward if it precedes start_date
    if end < cfg.start_date:
        raise ValueError(
            f"build request end date {end} is before "
            f"dataset {name}/{version} start-date {cfg.start_date}"
        )
    if start < cfg.start_date:
        logger.warning(
            "clamping start to dataset start-date",
            dataset=name,
            version=str(version),
            original_start=str(start),
            start_date=str(cfg.start_date),
        )
        start = cfg.start_date

    # diamond dependency handling: if we've seen this node before,
    # check whether the new range is wider than what we recorded
    if node in graph.ranges:
        existing_start, existing_end = graph.ranges[node]
        new_start = min(existing_start, start)
        new_end = max(existing_end, end)

        # if the range didn't actually expand, no need to re-walk
        # children since they already cover the required range
        if new_start >= existing_start and new_end <= existing_end:
            return

        # range expanded -- update and fall through to re-walk children
        graph.ranges[node] = (new_start, new_end)
        start = new_start
        end = new_end
    else:
        graph.ranges[node] = (start, end)

    # walk dependencies, expanding ranges for lookback
    if not cfg.dependencies:
        return

    deps: set[Node] = set()
    for dep_name, dep_info in cfg.dependencies.items():
        dep_node: Node = (dep_name, dep_info.version)
        deps.add(dep_node)

        # lookback expansion: if this dependency has a lookback window,
        # we need data starting earlier. e.g. "5d" lookback with
        # lookback_subtract=timedelta(days=4) means we need data from
        # [start - 4d, end] so the builder gets 5 days inclusive.
        dep_start = start
        if dep_info.lookback_subtract is not None:
            dep_start = start - dep_info.lookback_subtract

        _collect(dep_name, dep_info.version, dep_start, end, graph)

    # record edges (overwrite on re-walk is fine, deps don't change)
    graph.edges[node] = deps


def schedule_build(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> BuildPlan:
    """Produce a topologically ordered build plan for a dataset and its dependencies.

    Uses Kahn's algorithm (BFS-based topological sort) on the graph from
    collect_graph(). Kahn's is chosen over Tarjan's DFS-based sort because
    it naturally produces level-order grouping -- nodes are processed in
    waves by their distance from roots, which directly maps to the barrier
    model the orchestrator needs.

    Algorithm:
        1. Collect the dependency graph (nodes, ranges, edges)
        2. Compute in-degree for each node (number of dependencies it has)
        3. Seed level 0 with all zero-in-degree nodes (roots)
        4. For each level: remove those nodes, decrement in-degrees of their
           dependents (parents), and collect newly zero-in-degree nodes as
           the next level
        5. Convert each node + range into a JobDescriptor, grouped by level

    The result is a BuildPlan where:
        - levels[0] = root datasets (no deps, build first)
        - levels[-1] = the originally requested dataset (build last)
        - within each level, jobs are independent and safe to parallelize

    Args:
        dataset_name: root dataset to build
        dataset_version: version of the root dataset
        start: requested build start time
        end: requested build end time

    Returns:
        BuildPlan with jobs grouped by topological level

    Raises:
        ValueError: if end < start_date for any dataset in the graph
    """
    graph = collect_graph(dataset_name, dataset_version, start, end)

    # build reverse_edges: dep -> set of parents that depend on it.
    # this lets us efficiently find which parents to update when a
    # dependency completes (in-degree decrement).
    reverse_edges: dict[Node, set[Node]] = defaultdict(set)
    for parent, deps in graph.edges.items():
        for dep in deps:
            reverse_edges[dep].add(parent)

    # compute in-degree: how many dependencies each node has.
    # nodes with in_degree 0 are roots -- they have no deps and can
    # be built immediately.
    in_degree: dict[Node, int] = {}
    for node in graph.ranges:
        in_degree[node] = len(graph.edges.get(node, set()))

    # Kahn's algorithm: process nodes level by level.
    # each "level" is a wave of nodes whose dependencies have all been
    # processed in prior levels.
    levels: list[list[JobDescriptor]] = []
    current_level_nodes = [node for node, deg in in_degree.items() if deg == 0]

    while current_level_nodes:
        # convert this level's nodes to JobDescriptors
        jobs = []
        for node in current_level_nodes:
            name, version = node
            range_start, range_end = graph.ranges[node]
            jobs.append(
                JobDescriptor(
                    dataset_name=name,
                    dataset_version=version,
                    start=range_start,
                    end=range_end,
                )
            )
        levels.append(jobs)

        # find the next level: for each node we just processed, decrement
        # the in-degree of its parents (nodes that depend on it). any
        # parent whose in-degree hits 0 is ready for the next level.
        next_level_nodes: list[Node] = []
        for node in current_level_nodes:
            for parent in reverse_edges.get(node, set()):
                in_degree[parent] -= 1
                if in_degree[parent] == 0:
                    next_level_nodes.append(parent)

        current_level_nodes = next_level_nodes

    return BuildPlan(levels=levels)
