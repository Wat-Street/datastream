from datetime import datetime


def build(dependencies: dict[str, list[dict]], timestamp: datetime) -> list[dict]:
    """Extract the close price from the mock-ohlc dependency."""
    ohlc = dependencies["mock-ohlc"][0]
    return [
        {
            "ticker": ohlc["ticker"],
            "close": ohlc["close"],
        }
    ]
