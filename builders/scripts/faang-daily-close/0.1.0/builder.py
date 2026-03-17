import os
from datetime import datetime
from typing import Any

from eodhd import APIClient

TICKERS = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]


def build(
    dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime
) -> list[dict[str, Any]]:
    """Fetch NYSE daily close prices for FAANG stocks from EODHD."""
    # APIClient is instantiated here, not at module level — isolated_worker.py
    # loads the .env before calling build() but after importing the module,
    # so module-level env access would fail
    api = APIClient(os.environ["EODHD_API_KEY"])
    date_str = timestamp.strftime("%Y-%m-%d")

    rows = []
    for ticker in TICKERS:
        data = api.get_eod_historical_stock_market_data(
            symbol=f"{ticker}.US", period="d", from_date=date_str, to_date=date_str
        )
        # api returns empty list if ticker was suspended or no data for date
        if not data:
            continue
        rows.append({"ticker": ticker, "close": float(data[0]["close"])})

    return rows
