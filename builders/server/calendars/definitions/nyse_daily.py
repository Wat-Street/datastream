from datetime import datetime, timedelta

import exchange_calendars as xcals
import pandas as pd

from calendars.interface import Calendar
from calendars.utils import is_midnight


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
