from datetime import datetime, timedelta

from calendars.interface import Calendar


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
