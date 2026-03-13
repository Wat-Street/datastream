from datetime import datetime, timedelta

import exchange_calendars as xcals
import pandas as pd

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


class NyseDailyCalendar(Calendar):
    """Calendar for NYSE trading days (excludes weekends and NYSE holidays).

    Uses midnight-aligned timestamps rather than market close (4pm ET) to stay
    consistent with all other daily calendars and avoid timezone awareness since
    all datetimes in the system are naive. This calendar answers "is this day
    a trading day?", not "is the market open right now?".
    """

    def __init__(self) -> None:
        self._xcal = xcals.get_calendar("XNYS")

    @property
    def name(self) -> str:
        return "nyse-daily"

    @property
    def granularity(self) -> timedelta:
        return timedelta(days=1)

    def is_open(self, timestamp: datetime) -> bool:
        if not is_midnight(timestamp):
            return False
        return self._xcal.is_session(pd.Timestamp(timestamp))

    def next_open(self, timestamp: datetime) -> datetime | None:
        """Return next NYSE trading day midnight >= timestamp."""
        midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        if midnight < timestamp:
            midnight += timedelta(days=1)
        ts = pd.Timestamp(midnight)
        if self._xcal.is_session(ts):
            return midnight
        # date_to_session with direction="next" finds the next valid session
        next_session = self._xcal.date_to_session(ts, direction="next")
        return next_session.to_pydatetime().replace(tzinfo=None)
