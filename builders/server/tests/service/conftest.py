from contextlib import contextmanager
from unittest.mock import patch

import pytest


@contextmanager
def _noop_lock(name, version):
    yield None


@pytest.fixture(autouse=True)
def patch_build_lock():
    """Replace advisory locks with a no-op for unit tests."""
    with patch("service.builder.build_lock", _noop_lock):
        yield
