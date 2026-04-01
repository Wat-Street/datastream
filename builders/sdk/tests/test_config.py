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
