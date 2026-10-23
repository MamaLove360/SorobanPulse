"""Async (asyncio) SorobanPulse API client, mirroring `SorobanPulseClient`."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx

from .exceptions import ApiError, AuthenticationError
from .client import DEFAULT_BASE_URL, DEFAULT_TIMEOUT
from .models import MetaResponse


class AsyncSorobanPulseClient:
    """Async client built on `httpx.AsyncClient`, with the same error
    semantics as `SorobanPulseClient` (401/403 -> `AuthenticationError`,
    other non-2xx and network failures -> `ApiError`).

    Example:
        async with AsyncSorobanPulseClient(api_key="sp_live_...") as client:
            events = await client.list_events(contract_id="C...")
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        if not api_key:
            raise AuthenticationError("api_key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._transport = transport
        self._session: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "AsyncSorobanPulseClient":
        self._session = httpx.AsyncClient(
            headers=self._headers(),
            timeout=self.timeout,
            transport=self._transport,
        )
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        if self._session is not None:
            await self._session.aclose()
            self._session = None

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": "soroban-pulse-python-sdk/0.1.0",
        }

    async def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                       body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self._session is None:
            raise RuntimeError("AsyncSorobanPulseClient must be used as an async context manager")

        url = f"{self.base_url}{path}"
        clean_params = {k: v for k, v in (params or {}).items() if v is not None}

        try:
            resp = await self._session.request(method, url, params=clean_params, json=body)
        except httpx.HTTPError as exc:
            raise ApiError(0, f"network error: {exc}") from exc

        text = resp.text
        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError:
            payload = {"message": text}
        if resp.status_code >= 400:
            if resp.status_code in (401, 403):
                raise AuthenticationError(payload.get("message", "authentication failed"))
            raise ApiError(resp.status_code, payload.get("message", "request failed"), payload=payload)
        return payload

    async def get_meta(self) -> MetaResponse:
        """Build metadata and enabled features (``GET /v1/meta``, no auth needed)."""
        return MetaResponse.from_dict(await self._request("GET", "/meta"))

    async def list_events(
        self,
        contract_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "GET",
            "/events",
            params={
                "contract_id": contract_id,
                "event_type": event_type,
                "limit": limit,
                "cursor": cursor,
            },
        )

    async def iter_events(self, **kwargs: Any) -> AsyncIterator[Dict[str, Any]]:
        cursor = kwargs.pop("cursor", None)
        while True:
            page = await self.list_events(cursor=cursor, **kwargs)
            for event in page.get("data", []):
                yield event
            cursor = page.get("next_cursor")
            if not cursor:
                break

    async def async_events_stream(self, contract_id: Optional[str] = None) -> AsyncIterator[Dict[str, Any]]:
        """Stream events using Server-Sent Events with automatic reconnection."""
        if self._session is None:
            raise RuntimeError("AsyncSorobanPulseClient must be used as an async context manager")

        url = f"{self.base_url}/v1/events/stream"
        params = {}
        if contract_id:
            params["contract_id"] = contract_id

        last_event_id = ""
        while True:
            headers = dict(self._headers())
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id

            try:
                async with self._session.stream("GET", url, params=params, headers=headers) as response:
                    if response.status_code >= 400:
                        if response.status_code in (401, 403):
                            raise AuthenticationError("authentication failed")
                        raise ApiError(response.status_code, "stream failed")

                    async for line in response.aiter_lines():
                        if not line or line.startswith(":"):
                            continue

                        if line == ":close":
                            return

                        if line.startswith("id:"):
                            last_event_id = line[4:].strip()
                            continue

                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            try:
                                event = json.loads(data_str)
                                yield event
                            except json.JSONDecodeError:
                                pass
            except (httpx.RequestError, httpx.HTTPError) as e:
                raise ApiError(0, f"stream error: {e}") from e
            except asyncio.CancelledError:
                return

    async def create_subscription(self, contract_id: str, webhook_url: str,
                                   event_types: Optional[List[str]] = None) -> Dict[str, Any]:
        body = {
            "contract_id": contract_id,
            "webhook_url": webhook_url,
            "event_types": event_types or [],
        }
        return await self._request("POST", "/subscriptions", body=body)

    async def wait_until_ready(self, poll_interval: float = 1.0, attempts: int = 5) -> bool:
        """Poll the health endpoint until the API is reachable."""
        for _ in range(attempts):
            try:
                await self._request("GET", "/health")
                return True
            except ApiError:
                await asyncio.sleep(poll_interval)
        return False
