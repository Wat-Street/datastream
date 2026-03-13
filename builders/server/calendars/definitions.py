from datetime import datetime, timedelta

from calendars.interface import Calendar
from calendars.utils import is_midnight


class AlwaysOpenCalendar(Calendar):
    """Calendar that accepts any timestamp without restriction."""

    @property
    def name(self) -> str:
        return "always-open"

    @property
    def granularity(self) -> timedelta:
        return timedelta(seconds=1)

    def is_open(self, timestamp: datetime) -> bool:
        return True

    def next_open(self, timestamp: datetime) -> datetime | None:
        """Every timestamp is open, so return timestamp as-is."""
        return timestamp


class EverydayCalendar(Calendar):
    """Calendar where every day is valid."""

    @property
    def name(self) -> str:
        return "everyday"

    @property
    def granularity(self) -> timedelta:
        return timedelta(days=1)

    def is_open(self, timestamp: datetime) -> bool:
        return is_midnight(timestamp)

    def next_open(self, timestamp: datetime) -> datetime | None:
        """Return timestamp if already midnight, else advance to next midnight."""
        midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        if midnight < timestamp:
            midnight += timedelta(days=1)
        return midnight


class WeekdayCalendar(Calendar):
    """Calendar where only Monday-Friday are valid."""

    @property
    def name(self) -> str:
        return "weekday"

    @property
    def granularity(self) -> timedelta:
        return timedelta(days=1)

    def is_open(self, timestamp: datetime) -> bool:
        # weekday() returns 0=Mon ... 4=Fri, 5=Sat, 6=Sun
        return is_midnight(timestamp) and timestamp.weekday() < 5

    def next_open(self, timestamp: datetime) -> datetime | None:
        """Return next weekday midnight >= timestamp."""
        midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        if midnight < timestamp:
            midnight += timedelta(days=1)
        while midnight.weekday() >= 5:
            midnight += timedelta(days=1)
        return midnight
