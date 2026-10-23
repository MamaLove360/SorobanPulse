import pytest

from soroban_pulse.client import SorobanPulseClient
from soroban_pulse.exceptions import AuthenticationError


def test_requires_api_key():
    with pytest.raises(AuthenticationError):
        SorobanPulseClient(api_key="")


def test_default_base_url_and_headers():
    client = SorobanPulseClient(api_key="sp_test_123")
    headers = client._headers()
    assert headers["Authorization"] == "Bearer sp_test_123"
    assert client.base_url.startswith("https://")


def test_base_url_trailing_slash_stripped():
    client = SorobanPulseClient(api_key="sp_test_123", base_url="https://example.com/v1/")
    assert client.base_url == "https://example.com/v1"


def test_get_meta_returns_typed_response(monkeypatch):
    from soroban_pulse.models import MetaResponse

    client = SorobanPulseClient(api_key="sp_test_123")
    calls = []

    def fake_request(method, path, params=None, body=None):
        calls.append((method, path))
        return {"features": ["default"], "version": "1.0.0", "build_sha": "abc", "api_version": "v1"}

    monkeypatch.setattr(client, "_request", fake_request)
    meta = client.get_meta()

    assert calls == [("GET", "/meta")]
    assert meta == MetaResponse(features=["default"], version="1.0.0", build_sha="abc", api_version="v1")
