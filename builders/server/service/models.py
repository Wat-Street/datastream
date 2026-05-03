from dataclasses import dataclass
from datetime import datetime

from utils.semver import SemVer


@dataclass(frozen=True)
class JobDescriptor:
    """A single unit of build work: one dataset over one time range.

    Frozen and hashable so it can be used as a dict key for result tracking.
    """

    dataset_name: str
    dataset_version: SemVer
    start: datetime
    end: datetime


@dataclass(frozen=True)
class JobResult:
    """Outcome of executing a JobDescriptor.

    The worker handles its own DB insert, so this only carries
    success/failure status -- no row data.
    """

    job: JobDescriptor
    success: bool
    error: str | None = None


@dataclass
class BuildPlan:
    """Topologically ordered build schedule.

    levels[0] contains root datasets (no dependencies).
    levels[-1] contains the originally requested dataset.
    Each level must complete before the next begins (barrier model).
    Jobs within a level have no edges between them and are safe to
    parallelize in a future multi-worker setup.
    """

    levels: list[list[JobDescriptor]]
