import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path

from utils.semver import SemVer

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
BUILDER_FILENAME = "builder.py"


def load_builder(dataset_name: str, dataset_version: SemVer) -> Callable:
    """Dynamically import the build function from a builder script."""
    script_dir = SCRIPTS_DIR / dataset_name / str(dataset_version)
    builder_path = script_dir / BUILDER_FILENAME

    if not script_dir.is_dir():
        raise FileNotFoundError(f"dataset directory not found: {script_dir}")
    if not builder_path.is_file():
        raise FileNotFoundError(f"builder script not found: {builder_path}")

    # temporarily add script dir to sys.path so relative imports work
    str_dir = str(script_dir)
    added_to_path = str_dir not in sys.path
    if added_to_path:
        # insert at start for fast lookup when importing
        sys.path.insert(0, str_dir)

    try:
        spec = importlib.util.spec_from_file_location(
            f"builder_{dataset_name}_{dataset_version}", builder_path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]  # loader is checked non-None above
    finally:
        if added_to_path:
            sys.path.remove(str_dir)

    return module.build
