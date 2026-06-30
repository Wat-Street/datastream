from datastream import config


def test_default_base_url():
    assert config.get_base_url() == "http://localhost:3000/api/v1"


def test_get_api_key_defaults_to_env(monkeypatch):
    monkeypatch.setattr(config, "_api_key", None)
    monkeypatch.setenv(config.API_KEY_ENV, "envkey")
    assert config.get_api_key() == "envkey"


def test_get_api_key_none_when_unset(monkeypatch):
    monkeypatch.setattr(config, "_api_key", None)
    monkeypatch.delenv(config.API_KEY_ENV, raising=False)
    assert config.get_api_key() is None


def test_configure_api_key_overrides_env(monkeypatch):
    monkeypatch.setattr(config, "_api_key", None)
    monkeypatch.setenv(config.API_KEY_ENV, "envkey")
    config.configure(api_key="explicit")
    assert config.get_api_key() == "explicit"


def test_configure_base_url_only_keeps_api_key(monkeypatch):
    monkeypatch.setattr(config, "_api_key", "keep-me")
    config.configure(base_url="http://x/api/v1")
    assert config.get_api_key() == "keep-me"
    assert config.get_base_url() == "http://x/api/v1"


def test_configure_changes_url():
    original = config.get_base_url()
    try:
        config.configure("https://example.com/api/v1")
        assert config.get_base_url() == "https://example.com/api/v1"
    finally:
        config.configure(original)


def test_get_base_url_returns_configured():
    original = config.get_base_url()
    try:
        config.configure("http://custom:8080")
        assert config.get_base_url() == "http://custom:8080"
    finally:
        config.configure(original)
