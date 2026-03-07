import random

import pandas as pd


def build(dependencies: dict[str, dict], timestamp: pd.Timestamp) -> dict:
    """Generate deterministic mock OHLC data for AAPL based on the timestamp."""
    random.seed(str(timestamp))
    base = round(random.uniform(100, 300), 2)

    return {
        "ticker": "AAPL",
        "open": base,
        "high": round(base + random.uniform(0, 50), 2),
        "low": round(base - random.uniform(0, 30), 2),
        "close": round(base + random.uniform(-10, 20), 2),
    }
