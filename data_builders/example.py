from datetime import date
from datastream.builders import (
    data_source,
    dataset,
    Field,
    FieldType,
    Dependency,
)
from datastream.trading_calendar import NyseTradingCalendar

import pandas as pd


@data_source(
    name="example_close",
    version="1",
    calendar=NyseTradingCalendar(),
    start_date=date(2020, 1, 1),
    fields=[
        Field("symbol", _type=FieldType._str),
        Field("close", _type=FieldType._float),
    ],
)
def example_close_builder(today: date):
    return pd.DataFrame({"symbol": ["TKR1", "TKR2"], "close": [412.30, 85.68]})


@dataset(
    name="example_mav5",
    version="1",
    calendar=NyseTradingCalendar(),
    start_date=date(2021, 8, 14),
    fields=[
        Field("symbol", _type=FieldType._str),
        Field("mav5", _type=FieldType._float),
    ],
    dependencies=[Dependency("example_close", version="1", lookback=5)],
)
def example_mav5_builder(today: date, closes: pd.DataFrame):
    return (
        closes.groupby("symbol")
        .apply(lambda df: df["close"].sum() / 5)
        .rename(columns={"close": "mav5"})
    )
