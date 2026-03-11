from datetime import datetime


def build(
    dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime
) -> list[dict]:
    """Extract close prices from multi-row OHLC dependency."""
    return [
        {"ticker": row["ticker"], "close": row["close"]}
        for row in dependencies["mock-multi-ohlc"][timestamp]
    ]
