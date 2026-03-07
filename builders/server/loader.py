import importlib.util
import sys
from pathlib import Path
from typing import Callable

SCRIPTS_DIR = Path(__file__).parent / "scripts"


def load_builder(dataset_name: str, dataset_version: str) -> Callable:
    """Dynamically import the build function from a builder script."""
    script_dir = SCRIPTS_DIR / dataset_name / dataset_version
    builder_path = script_dir / "builder.py"

    # Add the script's directory to sys.path so relative imports work
    str_dir = str(script_dir)
    if str_dir not in sys.path:
        sys.path.insert(0, str_dir)

    spec = importlib.util.spec_from_file_location(
        f"builder_{dataset_name}_{dataset_version}", builder_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module.build
