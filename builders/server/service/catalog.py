from dataclasses import dataclass

import db.datasets
from runtime.registry import CONFIG_REGISTRY


@dataclass
class DatasetInfo:
    """A discovered dataset with its data presence status."""

    name: str
    version: str
    has_data: bool


def list_datasets() -> list[DatasetInfo]:
    """Return all pre-loaded datasets annotated with whether they have DB rows."""
    has_data = db.datasets.get_datasets_with_data()
    return sorted(
        [
            DatasetInfo(
                name=name,
                version=str(version),
                has_data=(name, str(version)) in has_data,
            )
            for name, version in CONFIG_REGISTRY
        ],
        key=lambda d: (d.name, d.version),
    )
