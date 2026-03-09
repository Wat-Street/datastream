import random
from datetime import datetime

TICKERS = ["AAPL", "MSFT", "GOOG"]


def build(dependencies: dict[str, list[dict]], timestamp: datetime) -> list[dict]:
    """Generate deterministic mock OHLC data for multiple tickers."""
    rows = []
    for ticker in TICKERS:
        random.seed(f"{ticker}-{timestamp}")
        base = round(random.uniform(100, 300), 2)
        rows.append(
            {
                "ticker": ticker,
                "open": base,
                "high": round(base + random.uniform(0, 50), 2),
                "low": round(base - random.uniform(0, 30), 2),
                "close": round(base + random.uniform(-10, 20), 2),
            }
        )
    return rows
