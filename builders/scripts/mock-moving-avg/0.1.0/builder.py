from datetime import datetime


def build(
    dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime
) -> list[dict]:
    """Compute 5-day moving average of close prices from lookback data."""
    close_data = dependencies["mock-daily-close"]
    prices = [rows[0]["close"] for rows in close_data.values()]

    if not prices:
        return [{"ticker": "AAPL", "average": 0.0}]

    average = sum(prices) / len(prices)
    return [{"ticker": "AAPL", "average": round(average, 2)}]
