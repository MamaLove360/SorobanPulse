"""Synchronous SorobanPulse API client."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from .exceptions import ApiError, AuthenticationError
from .models import MetaResponse

DEFAULT_BASE_URL = "https://api.sorobanpulse.io/v1"
DEFAULT_TIMEOUT = 30


class SorobanPulseClient:
    """Blocking client for the SorobanPulse REST API.

    Example:
        client = SorobanPulseClient(api_key="sp_live_...")
        events = client.list_events(contract_id="C...", limit=50)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        if not api_key:
            raise AuthenticationError("api_key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": "soroban-pulse-python-sdk/0.1.0",
        }

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                 body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
            if query:
                url = f"{url}?{query}"

        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib_request.Request(url, data=data, headers=self._headers(), method=method)

        try:
            with urllib_request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            payload = exc.read()
            try:
                parsed = json.loads(payload) if payload else {}
            except json.JSONDecodeError:
                parsed = {"message": payload.decode("utf-8", "replace")}
            if exc.code in (401, 403):
                raise AuthenticationError(parsed.get("message", "authentication failed")) from exc
            raise ApiError(exc.code, parsed.get("message", str(exc)), payload=parsed) from exc
        except URLError as exc:
            raise ApiError(0, f"network error: {exc.reason}") from exc

    # -- System -----------------------------------------------------------

    def get_meta(self) -> MetaResponse:
        """Build metadata and enabled features (``GET /v1/meta``, no auth needed)."""
        return MetaResponse.from_dict(self._request("GET", "/meta"))

    # -- Events -----------------------------------------------------------

    def list_events(
        self,
        contract_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        params = {
            "contract_id": contract_id,
            "event_type": event_type,
            "limit": limit,
            "cursor": cursor,
        }
        return self._request("GET", "/events", params=params)

    def iter_events(self, **kwargs: Any) -> Iterable[Dict[str, Any]]:
        """Yield events across all pages, following `next_cursor`."""
        cursor = kwargs.pop("cursor", None)
        while True:
            page = self.list_events(cursor=cursor, **kwargs)
            for event in page.get("data", []):
                yield event
            cursor = page.get("next_cursor")
            if not cursor:
                break

    def get_event(self, event_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/events/{event_id}")

    # -- Subscriptions ------------------------------------------------------

    def list_subscriptions(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/subscriptions").get("data", [])

    def create_subscription(self, contract_id: str, webhook_url: str,
                             event_types: Optional[List[str]] = None) -> Dict[str, Any]:
        body = {
            "contract_id": contract_id,
            "webhook_url": webhook_url,
            "event_types": event_types or [],
        }
        return self._request("POST", "/subscriptions", body=body)

    def delete_subscription(self, subscription_id: str) -> None:
        self._request("DELETE", f"/subscriptions/{subscription_id}")
