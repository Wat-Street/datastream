from dataclasses import dataclass

import db.datasets
import runtime.config


@dataclass
class DatasetInfo:
    """A discovered dataset with its data presence status."""

    name: str
    version: str
    has_data: bool


def discover_datasets() -> list[tuple[str, str]]:
    """Scan SCRIPTS_DIR and return (name, version) pairs that have a config.toml.

    Skips non-directories at both levels.
    """
    results: list[tuple[str, str]] = []
    scripts_dir = runtime.config.SCRIPTS_DIR
    if not scripts_dir.is_dir():
        return results
    for name_dir in sorted(scripts_dir.iterdir()):
        if not name_dir.is_dir():
            continue
        for version_dir in sorted(name_dir.iterdir()):
            if not version_dir.is_dir():
                continue
            if (version_dir / "config.toml").exists():
                results.append((name_dir.name, version_dir.name))
    return results


def list_datasets() -> list[DatasetInfo]:
    """Return all discovered datasets annotated with whether they have DB rows."""
    discovered = discover_datasets()
    has_data = db.datasets.get_datasets_with_data()
    return [
        DatasetInfo(name=name, version=version, has_data=(name, version) in has_data)
        for name, version in discovered
    ]
