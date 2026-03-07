import pandas as pd


def build(dependencies: dict[str, dict], timestamp: pd.Timestamp) -> dict:
    """Extract the close price from the mock-ohlc dependency."""
    ohlc = dependencies["mock-ohlc"]
    return {
        "ticker": ohlc["ticker"],
        "close": ohlc["close"],
    }
