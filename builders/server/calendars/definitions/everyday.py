from datetime import datetime, timedelta

from calendars.interface import Calendar
from calendars.utils import is_midnight


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
