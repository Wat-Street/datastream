import threading
from unittest.mock import patch

from service.locks import get_build_lock


def test_get_build_lock_returns_same_lock_for_same_key() -> None:
    """Same (name, version) returns the same Lock instance."""
    with patch("service.locks._lock_map", {}):
        lock1 = get_build_lock("ds", "0.1.0")
        lock2 = get_build_lock("ds", "0.1.0")
        assert lock1 is lock2


def test_get_build_lock_returns_different_locks_for_different_keys() -> None:
    """Different (name, version) pairs return distinct Lock instances."""
    with patch("service.locks._lock_map", {}):
        lock_a = get_build_lock("ds-a", "0.1.0")
        lock_b = get_build_lock("ds-b", "0.1.0")
        lock_a_v2 = get_build_lock("ds-a", "0.2.0")
        assert lock_a is not lock_b
        assert lock_a is not lock_a_v2


def test_get_build_lock_is_thread_safe() -> None:
    """Concurrent calls from multiple threads all get the same lock."""
    with patch("service.locks._lock_map", {}):
        results: list[threading.Lock] = []
        barrier = threading.Barrier(10)

        def grab_lock() -> None:
            barrier.wait()
            results.append(get_build_lock("ds", "0.1.0"))

        threads = [threading.Thread(target=grab_lock) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 10
        assert all(lock is results[0] for lock in results)
