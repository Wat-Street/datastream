from abc import ABC, abstractmethod
from datetime import datetime, timedelta


class Calendar(ABC):
    """Base class for calendars that determine valid timestamps for a dataset."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this calendar."""

    @property
    @abstractmethod
    def granularity(self) -> timedelta:
        """Smallest time step this calendar operates on."""

    @abstractmethod
    def is_open(self, timestamp: datetime) -> bool:
        """Return True if the given timestamp is a valid data point."""
