from datetime import datetime, timedelta

from calendars.definitions.nyse_daily import NyseDailyCalendar
from calendars.registry import CALENDARS_MAP


def test_nyse_daily_name() -> None:
    cal = NyseDailyCalendar()
    assert cal.name == "nyse-daily"


def test_nyse_daily_granularity() -> None:
    cal = NyseDailyCalendar()
    assert cal.granularity == timedelta(days=1)


def test_nyse_daily_in_registry() -> None:
    assert "nyse-daily" in CALENDARS_MAP
    assert isinstance(CALENDARS_MAP["nyse-daily"], NyseDailyCalendar)


def test_nyse_daily_open_regular_trading_day() -> None:
    cal = NyseDailyCalendar()
    # 2024-01-02 is a Tuesday, normal trading day
    assert cal.is_open(datetime(2024, 1, 2))


def test_nyse_daily_closed_on_weekends() -> None:
    cal = NyseDailyCalendar()
    # 2024-01-06 is Saturday, 2024-01-07 is Sunday
    assert not cal.is_open(datetime(2024, 1, 6))
    assert not cal.is_open(datetime(2024, 1, 7))


def test_nyse_daily_closed_new_years_day() -> None:
    cal = NyseDailyCalendar()
    # 2024-01-01 is New Year's Day (Monday)
    assert not cal.is_open(datetime(2024, 1, 1))


def test_nyse_daily_closed_christmas() -> None:
    cal = NyseDailyCalendar()
    # 2024-12-25 is Christmas (Wednesday)
    assert not cal.is_open(datetime(2024, 12, 25))


def test_nyse_daily_closed_mlk_day() -> None:
    cal = NyseDailyCalendar()
    # 2024-01-15 is MLK Day (third Monday of January)
    assert not cal.is_open(datetime(2024, 1, 15))


def test_nyse_daily_closed_july_fourth() -> None:
    cal = NyseDailyCalendar()
    # 2024-07-04 is Independence Day (Thursday)
    assert not cal.is_open(datetime(2024, 7, 4))


def test_nyse_daily_closed_thanksgiving() -> None:
    cal = NyseDailyCalendar()
    # 2024-11-28 is Thanksgiving (fourth Thursday of November)
    assert not cal.is_open(datetime(2024, 11, 28))


def test_nyse_daily_rejects_nonmidnight() -> None:
    cal = NyseDailyCalendar()
    # 2024-01-02 is a valid trading day but not at midnight
    assert not cal.is_open(datetime(2024, 1, 2, 9, 30, 0))
    assert not cal.is_open(datetime(2024, 1, 2, 0, 0, 1))


def test_nyse_daily_next_open_already_trading_day() -> None:
    """next_open returns the same timestamp when already at a trading day midnight."""
    cal = NyseDailyCalendar()
    ts = datetime(2024, 1, 2)  # tuesday, normal trading day
    assert cal.next_open(ts) == ts


def test_nyse_daily_next_open_from_weekend() -> None:
    """next_open from saturday skips to monday."""
    cal = NyseDailyCalendar()
    ts = datetime(2024, 1, 6)  # saturday
    assert cal.next_open(ts) == datetime(2024, 1, 8)  # monday


def test_nyse_daily_next_open_from_holiday() -> None:
    """next_open from a holiday advances to next trading day."""
    cal = NyseDailyCalendar()
    ts = datetime(2024, 1, 1)  # new year's day (monday)
    assert cal.next_open(ts) == datetime(2024, 1, 2)  # tuesday


def test_nyse_daily_next_open_from_non_midnight() -> None:
    """next_open from non-midnight advances to next trading day midnight."""
    cal = NyseDailyCalendar()
    ts = datetime(2024, 1, 2, 9, 30, 0)  # tuesday 9:30am
    assert cal.next_open(ts) == datetime(2024, 1, 3)  # wednesday


def test_nyse_daily_next_open_multi_day_skip() -> None:
    """next_open from friday evening before MLK monday skips to tuesday."""
    cal = NyseDailyCalendar()
    # 2024-01-12 is Friday, 2024-01-15 is MLK Day (Monday)
    ts = datetime(2024, 1, 12, 17, 0, 0)  # friday 5pm
    assert cal.next_open(ts) == datetime(2024, 1, 16)  # tuesday
