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
