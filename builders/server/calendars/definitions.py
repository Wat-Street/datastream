from datetime import datetime, timedelta

from calendars.interface import Calendar


class EverydayCalendar(Calendar):
    """Calendar where every day is valid."""

    @property
    def name(self) -> str:
        return "everyday"

    @property
    def granularity(self) -> timedelta:
        return timedelta(days=1)

    def is_open(self, timestamp: datetime) -> bool:
        # reject timestamps that aren't aligned to midnight
        return (
            timestamp.hour == 0
            and timestamp.minute == 0
            and timestamp.second == 0
            and timestamp.microsecond == 0
        )
