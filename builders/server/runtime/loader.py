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

    if not builder_path.exists():
        raise FileNotFoundError(f"builder not found: {builder_path}")

    spec = importlib.util.spec_from_file_location(
        f"builder_{dataset_name}_{dataset_version}", builder_path
    )
    assert spec is not None and spec.loader is not None

    # Add the script's directory to sys.path so relative imports work.
    # done after existence check so a missing path never pollutes sys.path.
    str_dir = str(script_dir)
    if str_dir not in sys.path:
        sys.path.insert(0, str_dir)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]  # loader is checked non-None above

    return module.build
