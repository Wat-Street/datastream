from datetime import datetime, timedelta

from calendars.interface import Calendar
from calendars.registry import CALENDARS_MAP
from runtime.config import DEFAULT_BUILDER, DatasetConfig, DependencyInfo, SchemaType
from utils.semver import SemVer

V010 = SemVer.parse("0.1.0")
_1D = timedelta(days=1)
_DEFAULT_START = datetime(2020, 1, 1)


def _cfg(
    name: str = "ds",
    version: SemVer = V010,
    granularity: timedelta = _1D,
    start_date: datetime = _DEFAULT_START,
    schema: dict[str, SchemaType] | None = None,
    dependencies: dict[str, DependencyInfo] | None = None,
    calendar: "Calendar | None" = None,
) -> DatasetConfig:
    """shared test helper to build a DatasetConfig with sensible defaults."""
    return DatasetConfig(
        name=name,
        version=version,
        builder=DEFAULT_BUILDER,
        calendar=calendar if calendar is not None else CALENDARS_MAP["everyday"],
        granularity=granularity,
        start_date=start_date,
        schema=schema or {},
        dependencies=dependencies or {},
        env_vars=False,
    )
