from datetime import datetime


def is_midnight(timestamp: datetime) -> bool:
    """Check if a timestamp is aligned to midnight."""
    return (
        timestamp.hour == 0
        and timestamp.minute == 0
        and timestamp.second == 0
        and timestamp.microsecond == 0
    )
