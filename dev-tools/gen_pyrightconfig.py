"""Generate a root pyrightconfig.json with per-builder execution environments.

Scans builders/scripts/<name>/<version>/ for directories with requirements.txt
and generates executionEnvironments entries pointing to each builder's .venv.
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "builders" / "scripts"
OUTPUT = REPO_ROOT / "pyrightconfig.json"


def find_builder_dirs(scripts_dir: Path) -> list[Path]:
    """Find all builder directories that have a requirements.txt."""
    builder_dirs = []
    if not scripts_dir.is_dir():
        return builder_dirs

    for dataset_dir in sorted(scripts_dir.iterdir()):
        if not dataset_dir.is_dir():
            continue
        for version_dir in sorted(dataset_dir.iterdir()):
            if not version_dir.is_dir():
                continue
            if (version_dir / "requirements.txt").exists():
                builder_dirs.append(version_dir)

    return builder_dirs


def _find_site_packages(builder_dir: Path) -> Path | None:
    """Find the site-packages directory inside a builder's .venv."""
    lib_dir = builder_dir / ".venv" / "lib"
    if not lib_dir.is_dir():
        return None
    # expect exactly one pythonX.Y directory
    python_dirs = [d for d in lib_dir.iterdir() if d.name.startswith("python")]
    if not python_dirs:
        return None
    return python_dirs[0] / "site-packages"


def generate_config(builder_dirs: list[Path]) -> dict:
    """Build the pyrightconfig.json content."""
    environments = []
    for builder_dir in builder_dirs:
        rel = builder_dir.relative_to(REPO_ROOT)
        entry: dict = {"root": str(rel)}
        site_packages = _find_site_packages(builder_dir)
        if site_packages and site_packages.is_dir():
            entry["extraPaths"] = [str(site_packages.relative_to(REPO_ROOT))]
        environments.append(entry)

    # extra fields applied globally across all environments
    return {"typeCheckingMode": "basic", "executionEnvironments": environments}


def main() -> None:
    builder_dirs = find_builder_dirs(SCRIPTS_DIR)
    config = generate_config(builder_dirs)
    OUTPUT.write_text(json.dumps(config, indent=2) + "\n")
    print(f"wrote {OUTPUT} with {len(builder_dirs)} builder environment(s)")  # noqa: T201
    for d in builder_dirs:
        print(f"  - {d.relative_to(REPO_ROOT)}")  # noqa: T201
    print("If you're in an IDE, please reload it to pick up the changes.")  # noqa: T201


if __name__ == "__main__":
    sys.exit(main() or 0)
