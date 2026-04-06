from unittest.mock import MagicMock, patch

import pytest
from utils.retry import retry_with_backoff


def test_succeeds_first_try() -> None:
    """No retries needed when fn succeeds immediately."""
    fn = MagicMock(return_value=42)
    result = retry_with_backoff(
        fn, max_retries=3, initial_delay=1.0, description="test"
    )
    assert result == 42
    assert fn.call_count == 1


@patch("utils.retry.time.sleep")
def test_succeeds_after_retries(_mock_sleep: MagicMock) -> None:
    """Retries until fn succeeds."""
    fn = MagicMock(side_effect=[ValueError("fail"), ValueError("fail"), "ok"])
    result = retry_with_backoff(
        fn, max_retries=3, initial_delay=1.0, description="test"
    )
    assert result == "ok"
    assert fn.call_count == 3


@patch("utils.retry.time.sleep")
def test_exhausts_retries(_mock_sleep: MagicMock) -> None:
    """Raises last exception after all retries exhausted."""
    fn = MagicMock(side_effect=ValueError("persistent failure"))
    with pytest.raises(ValueError, match="persistent failure"):
        retry_with_backoff(fn, max_retries=2, initial_delay=1.0, description="test")
    # initial attempt + 2 retries = 3 calls
    assert fn.call_count == 3


@patch("utils.retry.time.sleep")
def test_exponential_delay(mock_sleep: MagicMock) -> None:
    """Sleep delays follow exponential backoff pattern."""
    fn = MagicMock(side_effect=[RuntimeError] * 5 + ["ok"])
    retry_with_backoff(
        fn, max_retries=5, initial_delay=2.0, backoff_factor=2.0, description="test"
    )
    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert delays == [2.0, 4.0, 8.0, 16.0, 32.0]


@patch("utils.retry.logger")
@patch("utils.retry.time.sleep")
def test_logs_retry_attempts(_mock_sleep: MagicMock, mock_logger: MagicMock) -> None:
    """Warning logged on each retry attempt."""
    fn = MagicMock(side_effect=[RuntimeError("oops"), "ok"])
    retry_with_backoff(
        fn, max_retries=3, initial_delay=1.0, description="builder subprocess"
    )
    mock_logger.warning.assert_called_once()
    call_kwargs = mock_logger.warning.call_args[1]
    assert call_kwargs["description"] == "builder subprocess"
    assert call_kwargs["attempt"] == 1
