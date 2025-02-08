from datetime import date
from typing import Protocol


class TradingCalendar(Protocol):
    def is_open(self, d: date) -> bool:
        ...


class NyseTradingCalendar(TradingCalendar):
    def is_open(self, d: date) -> bool:
        # TODO
        raise NotImplementedError()
