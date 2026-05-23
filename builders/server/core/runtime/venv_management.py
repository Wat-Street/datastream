"""Per-builder virtual environment management.

Scans builder directories for requirements.txt and creates/updates venvs
using uv. Venvs are cached based on a hash of requirements.txt.
"""

import subprocess
import zlib
from pathlib import Path

import structlog

logger = structlog.get_logger()

REQUIREMENTS_FILE = "requirements.txt"
VENV_DIR = ".venv"
HASH_FILE = ".requirements_hash"


def _compute_hash(path: Path) -> str:
    """Compute crc32 hash of a file's contents."""
    data = path.read_bytes()
    return str(zlib.crc32(data))


def _ensure_venv(builder_dir: Path) -> None:
    """Create or update a venv for a builder directory.

    Skips if .venv/.requirements_hash matches the current requirements.txt hash.
    """
    requirements = builder_dir / REQUIREMENTS_FILE
    venv_path = builder_dir / VENV_DIR
    hash_path = venv_path / HASH_FILE

    current_hash = _compute_hash(requirements)

    # skip if hash matches
    if hash_path.exists() and hash_path.read_text().strip() == current_hash:
        logger.info("venv up to date", builder_dir=str(builder_dir))
        return

    logger.info("creating venv", builder_dir=str(builder_dir))

    # create venv
    subprocess.run(
        ["uv", "venv", str(venv_path)],
        check=True,
        capture_output=True,
    )

    # install dependencies
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "-r",
            str(requirements),
            "-p",
            str(venv_path / "bin" / "python"),
        ],
        check=True,
        capture_output=True,
    )

    # write hash (venv_path created by `uv venv` above)
    venv_path.mkdir(parents=True, exist_ok=True)
    hash_path.write_text(current_hash)
    logger.info("venv ready", builder_dir=str(builder_dir))


def setup_builder_venvs(scripts_dir: Path) -> None:
    """Scan all builder directories and create venvs where needed."""
    created = 0
    skipped = 0

    if not scripts_dir.is_dir():
        logger.warning("scripts directory not found", path=str(scripts_dir))
        return

    for dataset_dir in sorted(scripts_dir.iterdir()):
        if not dataset_dir.is_dir():
            continue
        for version_dir in sorted(dataset_dir.iterdir()):
            if not version_dir.is_dir():
                continue
            requirements = version_dir / REQUIREMENTS_FILE
            if not requirements.exists():
                skipped += 1
                continue
            try:
                _ensure_venv(version_dir)
                created += 1
            except Exception:
                logger.exception("failed to create venv", builder_dir=str(version_dir))

    logger.info(
        "venv setup complete",
        created_or_updated=created,
        skipped=skipped,
    )
