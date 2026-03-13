from datetime import datetime, timedelta

from calendars.definitions import AlwaysOpenCalendar
from calendars.registry import CALENDARS_MAP


def test_always_open_is_open_always_true() -> None:
    """AlwaysOpenCalendar.is_open returns True for any timestamp."""
    cal = AlwaysOpenCalendar()
    assert cal.is_open(datetime(2024, 1, 1))  # midnight
    assert cal.is_open(datetime(2024, 1, 1, 12, 30, 45))  # mid-day
    assert cal.is_open(datetime(2024, 1, 6))  # saturday
    assert cal.is_open(datetime(2024, 1, 7, 23, 59, 59))  # sunday, non-midnight


def test_always_open_name() -> None:
    cal = AlwaysOpenCalendar()
    assert cal.name == "always-open"


def test_always_open_granularity() -> None:
    cal = AlwaysOpenCalendar()
    assert cal.granularity == timedelta(seconds=1)


def test_always_open_in_registry() -> None:
    assert "always-open" in CALENDARS_MAP
    assert isinstance(CALENDARS_MAP["always-open"], AlwaysOpenCalendar)


def test_always_open_next_open_returns_same_timestamp() -> None:
    """next_open returns the timestamp itself since everything is open."""
    cal = AlwaysOpenCalendar()
    ts = datetime(2024, 1, 1, 12, 30, 45)
    assert cal.next_open(ts) == ts


def test_always_open_next_open_midnight() -> None:
    cal = AlwaysOpenCalendar()
    ts = datetime(2024, 1, 1)
    assert cal.next_open(ts) == ts


def test_always_open_next_open_weekend() -> None:
    cal = AlwaysOpenCalendar()
    ts = datetime(2024, 1, 6, 15, 0, 0)  # saturday afternoon
    assert cal.next_open(ts) == ts
