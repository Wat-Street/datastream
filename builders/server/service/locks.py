import threading

_lock_map: dict[tuple[str, str], threading.Lock] = {}
_lock_map_lock = threading.Lock()


def get_build_lock(dataset_name: str, dataset_version: str) -> threading.Lock:
    """Return the build lock for a (name, version) pair, creating it if needed."""
    key = (dataset_name, dataset_version)
    with _lock_map_lock:
        if key not in _lock_map:
            _lock_map[key] = threading.Lock()
        return _lock_map[key]
