from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, NewType

DatasetName = NewType("DatasetName", str)

_SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


@dataclass(frozen=True)
class DatasetVersion:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, version: str) -> DatasetVersion:
        """Parse a semver string like '1.2.3' into a DatasetVersion."""
        match = _SEMVER_RE.match(version)
        if not match:
            raise ValueError(f"invalid semver: {version!r}")
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
        )

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass
class DatasetRow:
    timestamp: datetime
    data: list[dict[str, Any]]


@dataclass
class DatasetResponse:
    dataset_name: str
    dataset_version: DatasetVersion
    total_timestamps: int
    returned_timestamps: int
    rows: list[DatasetRow]
