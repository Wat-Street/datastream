from datetime import datetime
from typing import Any


def build(
    dependencies: dict[str, dict[datetime, list[dict]]], timestamp: datetime
) -> list[dict[str, Any]]:
    return [{"value": 1}]
