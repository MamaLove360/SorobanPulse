import json

import httpx
import pytest

from soroban_pulse.async_client import AsyncSorobanPulseClient
from soroban_pulse.exceptions import ApiError, AuthenticationError

BASE = "https://example.test/v1"


def client_with(handler):
    return AsyncSorobanPulseClient(api_key="sp_test_123", base_url=BASE, transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_list_events_sends_auth_and_drops_none_params():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers["Authorization"]
        return httpx.Response(200, json={"data": [{"id": "e1"}], "next_cursor": None})

    async with client_with(handler) as client:
        page = await client.list_events(contract_id="CABC", limit=10)

    assert page["data"] == [{"id": "e1"}]
    assert seen["auth"] == "Bearer sp_test_123"
    assert seen["url"] == f"{BASE}/events?contract_id=CABC&limit=10"


@pytest.mark.asyncio
async def test_iter_events_follows_cursor():
    pages = {
        None: {"data": [{"id": "e1"}, {"id": "e2"}], "next_cursor": "c2"},
        "c2": {"data": [{"id": "e3"}], "next_cursor": None},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=pages[request.url.params.get("cursor")])

    async with client_with(handler) as client:
        ids = [event["id"] async for event in client.iter_events()]

    assert ids == ["e1", "e2", "e3"]


@pytest.mark.asyncio
async def test_create_subscription_posts_json_body():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["body"] = json.loads(request.content)
        return httpx.Response(201, json={"id": "sub_1"})

    async with client_with(handler) as client:
        result = await client.create_subscription("CABC", "https://hook.test")

    assert result == {"id": "sub_1"}
    assert seen["method"] == "POST"
    assert seen["body"] == {"contract_id": "CABC", "webhook_url": "https://hook.test", "event_types": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_auth_failures_raise_authentication_error(status):
    async with client_with(lambda r: httpx.Response(status, json={"message": "bad key"})) as client:
        with pytest.raises(AuthenticationError, match="bad key"):
            await client.list_events()


@pytest.mark.asyncio
async def test_other_errors_raise_api_error_with_payload():
    async with client_with(lambda r: httpx.Response(500, text="upstream exploded")) as client:
        with pytest.raises(ApiError) as info:
            await client.list_events()

    assert info.value.status_code == 500
    assert info.value.payload == {"message": "upstream exploded"}


@pytest.mark.asyncio
async def test_network_errors_become_api_error_and_wait_until_ready_gives_up():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with client_with(handler) as client:
        with pytest.raises(ApiError) as info:
            await client.list_events()
        assert info.value.status_code == 0
        assert await client.wait_until_ready(poll_interval=0, attempts=2) is False


@pytest.mark.asyncio
async def test_request_outside_context_manager_is_rejected():
    client = AsyncSorobanPulseClient(api_key="sp_test_123")
    with pytest.raises(RuntimeError):
        await client.list_events()


@pytest.mark.asyncio
async def test_get_meta_returns_typed_response():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={
            "features": ["classical-crypto", "dashboard", "default"],
            "version": "1.0.0",
            "build_sha": "076772f1a2b3",
            "api_version": "v1",
        })

    async with client_with(handler) as client:
        meta = await client.get_meta()

    assert seen["url"] == f"{BASE}/meta"
    assert meta.features == ["classical-crypto", "dashboard", "default"]
    assert meta.version == "1.0.0"
    assert meta.build_sha == "076772f1a2b3"
    assert meta.api_version == "v1"
    assert meta.has_feature("dashboard")
    assert not meta.has_feature("kafka")
