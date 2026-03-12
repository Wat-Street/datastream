from datetime import datetime, timedelta

from calendars.interface import Calendar


def _is_midnight(timestamp: datetime) -> bool:
    """Check if a timestamp is aligned to midnight."""
    return (
        timestamp.hour == 0
        and timestamp.minute == 0
        and timestamp.second == 0
        and timestamp.microsecond == 0
    )


class EverydayCalendar(Calendar):
    """Calendar where every day is valid."""

    @property
    def name(self) -> str:
        return "everyday"

    @property
    def granularity(self) -> timedelta:
        return timedelta(days=1)

    def is_open(self, timestamp: datetime) -> bool:
        return _is_midnight(timestamp)


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
        return _is_midnight(timestamp) and timestamp.weekday() < 5
