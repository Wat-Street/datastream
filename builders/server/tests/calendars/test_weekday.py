from datetime import datetime, timedelta

from calendars.definitions import WeekdayCalendar
from calendars.registry import CALENDARS_MAP


def test_weekday_open_on_weekdays() -> None:
    """WeekdayCalendar.is_open returns True for Mon-Fri."""
    cal = WeekdayCalendar()
    # 2024-01-01 is a Monday
    for i in range(5):
        assert cal.is_open(datetime(2024, 1, 1 + i))


def test_weekday_closed_on_weekends() -> None:
    """WeekdayCalendar.is_open returns False for Sat-Sun."""
    cal = WeekdayCalendar()
    # 2024-01-06 is Saturday, 2024-01-07 is Sunday
    assert not cal.is_open(datetime(2024, 1, 6))
    assert not cal.is_open(datetime(2024, 1, 7))


def test_weekday_name() -> None:
    cal = WeekdayCalendar()
    assert cal.name == "weekday"


def test_weekday_granularity() -> None:
    cal = WeekdayCalendar()
    assert cal.granularity == timedelta(days=1)


def test_weekday_rejects_nonmidnight_timestamps() -> None:
    """WeekdayCalendar.is_open returns False for non-midnight weekdays."""
    cal = WeekdayCalendar()
    # 2024-01-01 is a Monday but not at midnight
    assert not cal.is_open(datetime(2024, 1, 1, 9, 30, 0))
    assert not cal.is_open(datetime(2024, 1, 1, 0, 0, 1))


def test_weekday_in_registry() -> None:
    assert "weekday" in CALENDARS_MAP
    assert isinstance(CALENDARS_MAP["weekday"], WeekdayCalendar)
