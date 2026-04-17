from datetime import datetime

import pytest
from service.models import BuildPlan, JobDescriptor, JobResult
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")
V020 = SemVer.parse("0.2.0")
JAN1 = datetime(2024, 1, 1)
JAN5 = datetime(2024, 1, 5)
JAN10 = datetime(2024, 1, 10)


# --- JobDescriptor ---


def test_job_descriptor_equality() -> None:
    """Identical fields produce equal descriptors."""
    a = JobDescriptor("ds", V010, JAN1, JAN5)
    b = JobDescriptor("ds", V010, JAN1, JAN5)
    assert a == b


def test_job_descriptor_inequality_name() -> None:
    """Different names produce unequal descriptors."""
    a = JobDescriptor("ds-a", V010, JAN1, JAN5)
    b = JobDescriptor("ds-b", V010, JAN1, JAN5)
    assert a != b


def test_job_descriptor_inequality_version() -> None:
    """Different versions produce unequal descriptors."""
    a = JobDescriptor("ds", V010, JAN1, JAN5)
    b = JobDescriptor("ds", V020, JAN1, JAN5)
    assert a != b


def test_job_descriptor_inequality_range() -> None:
    """Different time ranges produce unequal descriptors."""
    a = JobDescriptor("ds", V010, JAN1, JAN5)
    b = JobDescriptor("ds", V010, JAN1, JAN10)
    assert a != b


def test_job_descriptor_hashable() -> None:
    """JobDescriptor can be used as a dict key and in sets."""
    a = JobDescriptor("ds", V010, JAN1, JAN5)
    b = JobDescriptor("ds", V010, JAN1, JAN5)
    c = JobDescriptor("other", V010, JAN1, JAN5)

    s = {a, b, c}
    assert len(s) == 2

    d = {a: "result"}
    assert d[b] == "result"


def test_job_descriptor_frozen() -> None:
    """JobDescriptor fields cannot be reassigned."""
    j = JobDescriptor("ds", V010, JAN1, JAN5)
    with pytest.raises(AttributeError):
        j.dataset_name = "other"  # type: ignore[misc]


# --- JobResult ---


def test_job_result_success() -> None:
    """Successful result has no error."""
    job = JobDescriptor("ds", V010, JAN1, JAN5)
    result = JobResult(job=job, success=True)
    assert result.success is True
    assert result.error is None


def test_job_result_failure() -> None:
    """Failed result carries an error message."""
    job = JobDescriptor("ds", V010, JAN1, JAN5)
    result = JobResult(job=job, success=False, error="builder crashed")
    assert result.success is False
    assert result.error == "builder crashed"


def test_job_result_frozen() -> None:
    """JobResult fields cannot be reassigned."""
    job = JobDescriptor("ds", V010, JAN1, JAN5)
    result = JobResult(job=job, success=True)
    with pytest.raises(AttributeError):
        result.success = False  # type: ignore[misc]


# --- BuildPlan ---


def test_build_plan_single_level() -> None:
    """Root dataset with no deps produces 1 level."""
    job = JobDescriptor("root", V010, JAN1, JAN5)
    plan = BuildPlan(levels=[[job]])
    assert len(plan.levels) == 1
    assert plan.levels[0] == [job]


def test_build_plan_multiple_levels() -> None:
    """Levels are ordered: roots first, requested dataset last."""
    root = JobDescriptor("root", V010, JAN1, JAN10)
    mid = JobDescriptor("mid", V010, JAN1, JAN10)
    leaf = JobDescriptor("leaf", V010, JAN1, JAN5)

    plan = BuildPlan(levels=[[root], [mid], [leaf]])

    assert plan.levels[0] == [root]
    assert plan.levels[-1] == [leaf]
    assert len(plan.levels) == 3


def test_build_plan_level_with_multiple_jobs() -> None:
    """A single level can contain multiple independent jobs."""
    a = JobDescriptor("ds-a", V010, JAN1, JAN5)
    b = JobDescriptor("ds-b", V010, JAN1, JAN5)

    plan = BuildPlan(levels=[[a, b]])
    assert len(plan.levels[0]) == 2
