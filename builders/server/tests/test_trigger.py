import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent


@pytest.fixture
def trigger_module() -> types.ModuleType:
    """Import trigger.py from repo root via importlib."""
    spec = importlib.util.spec_from_file_location("trigger", REPO_ROOT / "trigger.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]  # loader is checked non-None above
    return mod


@patch("requests.post")
def test_trigger_success(
    mock_post: MagicMock,
    trigger_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Correct URL and params, prints success."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {"status": "ok"}
    mock_post.return_value = mock_resp

    monkeypatch.setattr(
        sys, "argv", ["trigger.py", "ds", "0.1.0", "2024-01-01", "2024-01-31"]
    )
    # Patch requests in the trigger module's namespace
    monkeypatch.setattr(trigger_module, "requests", sys.modules["requests"])

    trigger_module.main()
    captured = capsys.readouterr()
    assert "Success" in captured.out


def test_trigger_wrong_arg_count_exits(
    trigger_module: types.ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Too few args raises SystemExit(1)."""
    monkeypatch.setattr(sys, "argv", ["trigger.py", "ds"])
    with pytest.raises(SystemExit) as exc_info:
        trigger_module.main()
    assert exc_info.value.code == 1


@patch("requests.post")
def test_trigger_server_error_exits(
    mock_post: MagicMock,
    trigger_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-ok response raises SystemExit(1)."""
    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"
    mock_post.return_value = mock_resp

    monkeypatch.setattr(
        sys, "argv", ["trigger.py", "ds", "0.1.0", "2024-01-01", "2024-01-31"]
    )
    monkeypatch.setattr(trigger_module, "requests", sys.modules["requests"])

    with pytest.raises(SystemExit) as exc_info:
        trigger_module.main()
    assert exc_info.value.code == 1


@patch("requests.post")
def test_trigger_correct_url_format(
    mock_post: MagicMock,
    trigger_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """URL is http://localhost:8000/build/{name}/{version}."""
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {"status": "ok"}
    mock_post.return_value = mock_resp

    monkeypatch.setattr(
        sys, "argv", ["trigger.py", "my-ds", "1.2.3", "2024-01-01", "2024-01-31"]
    )
    monkeypatch.setattr(trigger_module, "requests", sys.modules["requests"])

    trigger_module.main()
    called_url = mock_post.call_args[0][0]
    assert called_url == "http://localhost:3000/build/my-ds/1.2.3"
