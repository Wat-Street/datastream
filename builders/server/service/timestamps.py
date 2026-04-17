"""Timestamp generation and related exceptions.

Separated from builder.py to avoid circular imports between
builder -> orchestrator -> worker -> builder.
"""

from datetime import datetime, timedelta

from calendars.interface import Calendar


class NoValidTimestampsError(Exception):
    """raised when generate_timestamps returns no valid calendar dates for the range."""


# TODO (bryan): benchmark this, and optimize if needed
def generate_timestamps(
    start: datetime,
    end: datetime,
    granularity: timedelta,
    calendar: Calendar,
) -> list[datetime]:
    """Generate timestamps in [start, end], filtered by calendar."""
    current = calendar.next_open(start)
    if current is None:
        # no open timestamps
        return []

    timestamps = []
    while current <= end:
        if calendar.is_open(current):
            timestamps.append(current)
        current += granularity
    return timestamps
