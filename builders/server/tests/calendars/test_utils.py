from datetime import datetime

from calendars.utils import is_midnight


def test_is_midnight_positive() -> None:
    """is_midnight returns True for timestamps exactly at midnight."""
    assert is_midnight(datetime(2024, 1, 1, 0, 0, 0, 0))
    assert is_midnight(datetime(2025, 12, 31, 0, 0, 0, 0))


def test_is_midnight_negative() -> None:
    """is_midnight returns False for timestamps not at midnight."""
    # Hours
    assert not is_midnight(datetime(2024, 1, 1, 1, 0, 0, 0))
    # Minutes
    assert not is_midnight(datetime(2024, 1, 1, 0, 1, 0, 0))
    # Seconds
    assert not is_midnight(datetime(2024, 1, 1, 0, 0, 1, 0))
    # Microseconds
    assert not is_midnight(datetime(2024, 1, 1, 0, 0, 0, 1))
    # Noon
    assert not is_midnight(datetime(2024, 1, 1, 12, 0, 0, 0))
