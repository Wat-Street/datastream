from datastream import config


def test_default_base_url():
    assert config.get_base_url() == "http://localhost:3000/api/v1"


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


def test_api_key_defaults_to_none(monkeypatch):
    monkeypatch.delenv("DATASTREAM_API_KEY", raising=False)
    monkeypatch.setattr("datastream.config._api_key", None)
    assert config.get_api_key() is None


def test_api_key_from_env(monkeypatch):
    monkeypatch.setattr("datastream.config._api_key", None)
    monkeypatch.setenv("DATASTREAM_API_KEY", "env-key")
    assert config.get_api_key() == "env-key"


def test_configure_api_key_overrides_env(monkeypatch):
    monkeypatch.setattr("datastream.config._api_key", None)
    monkeypatch.setenv("DATASTREAM_API_KEY", "env-key")
    try:
        config.configure(api_key="explicit-key")
        assert config.get_api_key() == "explicit-key"
    finally:
        monkeypatch.setattr("datastream.config._api_key", None)
