from datetime import datetime, timedelta

from calendars.definitions import EverydayCalendar
from calendars.registry import CALENDARS_MAP


def test_everyday_is_open_always_true() -> None:
    """EverydayCalendar.is_open returns True for any date."""
    cal = EverydayCalendar()
    assert cal.is_open(datetime(2024, 1, 1))  # monday
    assert cal.is_open(datetime(2024, 1, 6))  # saturday
    assert cal.is_open(datetime(2024, 1, 7))  # sunday
    assert cal.is_open(datetime(2024, 12, 25))  # holiday


def test_everyday_name() -> None:
    cal = EverydayCalendar()
    assert cal.name == "everyday"


def test_everyday_granularity() -> None:
    cal = EverydayCalendar()
    assert cal.granularity == timedelta(days=1)


def test_everyday_rejects_nonmidnight_timestamps() -> None:
    """EverydayCalendar.is_open returns False for timestamps not at midnight."""
    cal = EverydayCalendar()
    assert not cal.is_open(datetime(2024, 1, 1, 12, 0, 0))
    assert not cal.is_open(datetime(2024, 1, 1, 0, 30, 0))
    assert not cal.is_open(datetime(2024, 1, 1, 0, 0, 1))
    assert not cal.is_open(datetime(2024, 1, 1, 0, 0, 0, 1))


def test_everyday_in_registry() -> None:
    assert "everyday" in CALENDARS_MAP
    assert isinstance(CALENDARS_MAP["everyday"], EverydayCalendar)
