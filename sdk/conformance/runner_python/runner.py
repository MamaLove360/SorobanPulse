#!/usr/bin/env python3
"""
sdk/conformance/runner_python/runner.py

Python SDK conformance runner for SorobanPulse.
Reads ../scenarios.yaml, executes every scenario against the live server, and
reports pass/fail in GitHub Actions step-summary format.

Usage:
    CONFORMANCE_BASE_URL=http://localhost:3000 python runner.py

Exit code: 0 = all pass, 1 = one or more failures.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sys
import time
import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import yaml  # PyYAML, listed in requirements.txt

# ── Types ─────────────────────────────────────────────────────────────────────

ScenarioDict = dict[str, Any]
AssertionDict = dict[str, Any]


# ── Assertion engine ──────────────────────────────────────────────────────────

def _resolve_field(obj: Any, field_path: str) -> Any:
    """
    Resolve a dotted/bracketed path like "data[0].id" in a decoded JSON object.
    Returns the value, or a sentinel ``_MISSING`` if not found.
    """
    _MISSING = object()
    parts = re.sub(r"\[(\d+)\]", r".\1", field_path).split(".")
    parts = [p for p in parts if p]
    current = obj
    for part in parts:
        if current is _MISSING:
            return _MISSING
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (IndexError, ValueError):
                return _MISSING
        elif isinstance(current, dict):
            if part not in current:
                return _MISSING
            current = current[part]
        else:
            return _MISSING
    return current


_MISSING = object()  # module-level sentinel


def _resolve_field_or_missing(obj: Any, field_path: str) -> Any:
    sentinel = object()
    parts = re.sub(r"\[(\d+)\]", r".\1", field_path).split(".")
    parts = [p for p in parts if p]
    current = obj
    for part in parts:
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (IndexError, ValueError):
                return sentinel
        elif isinstance(current, dict):
            if part not in current:
                return sentinel
            current = current[part]
        else:
            return sentinel
    return current


def check_assertion(body: Any, assertion: AssertionDict) -> str | None:
    """Return an error string if the assertion fails, else None."""
    op: str = assertion["op"]
    field: str = assertion["field"]
    expected = assertion.get("value")

    sentinel = object()
    value = _resolve_field_or_missing(body, field)
    missing = isinstance(value, object) and value.__class__ is object.__class__ and id(value) != id(body)

    # Re-do with simpler sentinel
    _s = object()

    def get_value(obj: Any, path: str) -> Any:
        """Return a tuple (value, found: bool)."""
        parts = re.sub(r"\[(\d+)\]", r".\1", path).split(".")
        parts = [p for p in parts if p]
        cur = obj
        for p in parts:
            if isinstance(cur, list):
                try:
                    cur = cur[int(p)]
                except (IndexError, ValueError):
                    return None, False
            elif isinstance(cur, dict):
                if p not in cur:
                    return None, False
                cur = cur[p]
            else:
                return None, False
        return cur, True

    val, found = get_value(body, field)

    if op == "exists":
        if not found or val is None:
            return f'field "{field}" does not exist or is null'
        return None

    if op == "type":
        if not found:
            return f'field "{field}" does not exist'
        if expected == "array":
            if not isinstance(val, list):
                return f'field "{field}" expected type "array", got "{type(val).__name__}"'
        elif expected == "object":
            if not isinstance(val, dict):
                return f'field "{field}" expected type "object", got "{type(val).__name__}"'
        elif expected == "number":
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                return f'field "{field}" expected type "number", got "{type(val).__name__}"'
        elif expected == "string":
            if not isinstance(val, str):
                return f'field "{field}" expected type "string", got "{type(val).__name__}"'
        elif expected == "boolean":
            if not isinstance(val, bool):
                return f'field "{field}" expected type "boolean", got "{type(val).__name__}"'
        return None

    if op == "eq":
        # Compare JSON representations to handle list/dict equality
        if json.dumps(val, sort_keys=True) != json.dumps(expected, sort_keys=True):
            return f'field "{field}" expected {json.dumps(expected)}, got {json.dumps(val)}'
        return None

    if op == "gte":
        if not found or not isinstance(val, (int, float)):
            return f'field "{field}" is not a number'
        if val < expected:
            return f'field "{field}" expected >= {expected}, got {val}'
        return None

    if op == "lte":
        if not found or not isinstance(val, (int, float)):
            return f'field "{field}" is not a number'
        if val > expected:
            return f'field "{field}" expected <= {expected}, got {val}'
        return None

    if op == "len_gte":
        if not found or not isinstance(val, list):
            return f'field "{field}" is not an array'
        if len(val) < expected:
            return f'field "{field}" expected length >= {expected}, got {len(val)}'
        return None

    if op == "contains":
        if not found:
            return f'field "{field}" does not exist'
        if isinstance(val, str):
            if not isinstance(expected, str) or expected not in val:
                return f'field "{field}" expected to contain "{expected}", got "{val}"'
            return None
        if isinstance(val, list):
            if expected not in val:
                return f'field "{field}" expected to contain {json.dumps(expected)}'
            return None
        return f'field "{field}" is not a string or array'

    return f'unknown assertion op "{op}"'


# ── Webhook signature verification ───────────────────────────────────────────

def verify_webhook_signature(
    payload: str,
    signature_header: str,
    secret: str,
    timestamp: str | None = None,
) -> bool:
    """
    Verify an HMAC-SHA256 webhook signature in the form ``sha256=<hex>``.
    When a timestamp is supplied the signed message is ``<timestamp>.<payload>``
    (Stripe-style scheme); otherwise the raw payload is signed.
    """
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    provided = signature_header[len("sha256="):].strip()
    message = f"{timestamp}.{payload}" if timestamp else payload
    expected = hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, provided)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def build_url(base: str, path: str, query: dict[str, Any] | None = None) -> str:
    base = base.rstrip("/")
    url = base + path
    if query:
        url += "?" + urlencode({k: str(v) for k, v in query.items()})
    return url


def http_request(
    url: str,
    method: str,
    headers: dict[str, str] | None = None,
    body: Any = None,
) -> tuple[int, dict[str, str], str]:
    """Returns (status_code, response_headers, body_text)."""
    req_headers = {
        "User-Agent": "soroban-pulse-conformance-py/1.0",
        "Accept": "application/json",
        **(headers or {}),
    }

    body_bytes: bytes | None = None
    if body is not None:
        body_bytes = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
        req_headers["Content-Length"] = str(len(body_bytes))

    req = Request(url, data=body_bytes, headers=req_headers, method=method)
    try:
        with urlopen(req, timeout=30) as resp:
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, resp_headers, resp.read().decode("utf-8")
    except HTTPError as e:
        resp_headers = {k.lower(): v for k, v in e.headers.items()}
        return e.code, resp_headers, e.read().decode("utf-8")


def collect_sse(
    url: str,
    timeout_secs: int,
    min_events: int,
) -> tuple[int, dict[str, str], list[str]]:
    """
    Connect to an SSE endpoint and collect data lines until the timeout or
    min_events threshold is reached.
    Returns (status_code, response_headers, list_of_data_payloads).
    """
    import socket

    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path_qs = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")

    status_code: int = 0
    resp_headers: dict[str, str] = {}
    events: list[str] = []
    error: Exception | None = None

    def _run() -> None:
        nonlocal status_code, resp_headers

        raw_request = (
            f"GET {path_qs} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Accept: text/event-stream\r\n"
            "Cache-Control: no-cache\r\n"
            "User-Agent: soroban-pulse-conformance-py/1.0\r\n"
            "Connection: close\r\n"
            "\r\n"
        )

        try:
            sock = socket.create_connection((host, port), timeout=timeout_secs + 2)
            sock.sendall(raw_request.encode())

            buf = b""
            # Read status line + headers
            while b"\r\n\r\n" not in buf:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf += chunk

            header_part, _, body_start = buf.partition(b"\r\n\r\n")
            header_lines = header_part.decode("utf-8", errors="replace").splitlines()
            if header_lines:
                m = re.match(r"HTTP/[\d.]+ (\d+)", header_lines[0])
                if m:
                    status_code = int(m.group(1))
            for line in header_lines[1:]:
                if ":" in line:
                    k, _, v = line.partition(":")
                    resp_headers[k.strip().lower()] = v.strip()

            # Read body line by line looking for "data: " SSE lines
            sock.settimeout(timeout_secs)
            stream_buf = body_start.decode("utf-8", errors="replace")
            try:
                while len(events) < min_events:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    stream_buf += chunk.decode("utf-8", errors="replace")
                    lines = stream_buf.split("\n")
                    stream_buf = lines[-1]
                    for line in lines[:-1]:
                        if line.startswith("data: "):
                            data = line[6:].strip()
                            if data and data != "[DONE]":
                                events.append(data)
            except socket.timeout:
                pass  # Normal – we hit the deadline
            finally:
                sock.close()
        except Exception as exc:
            nonlocal error
            error = exc

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout_secs + 5)

    return status_code, resp_headers, events


# ── Result model ──────────────────────────────────────────────────────────────

class Result:
    def __init__(
        self,
        id: str,
        description: str,
        passed: bool,
        error: str | None,
        duration_ms: int,
    ) -> None:
        self.id = id
        self.description = description
        self.passed = passed
        self.error = error
        self.duration_ms = duration_ms


# ── Shared runner state ───────────────────────────────────────────────────────

state: dict[str, str] = {}


# ── Core runner ───────────────────────────────────────────────────────────────

def run_scenario(scenario: ScenarioDict, base_url: str) -> Result:
    start = time.monotonic()

    try:
        # Resolve path
        resolved_path: str = scenario.get("path") or ""
        if "path_template" in scenario:
            def replace_token(m: re.Match) -> str:
                key = m.group(1)
                return state.get(key, f"{{{key}}}")
            resolved_path = re.sub(r"\{(\w+)\}", replace_token, scenario["path_template"])

        errors: list[str] = []

        # ── SSE scenario ─────────────────────────────────────────────────────
        if scenario.get("sse"):
            url = build_url(base_url, resolved_path, scenario.get("query"))
            timeout_secs = scenario.get("sse_timeout_secs", 10)
            min_events = scenario.get("sse_min_events", 1)
            status, resp_headers, events = collect_sse(url, timeout_secs, min_events)

            expected_status: int = scenario["expect"]["status"]
            if status != expected_status:
                errors.append(f"status: expected {expected_status}, got {status}")

            for k, v in (scenario["expect"].get("headers") or {}).items():
                actual = resp_headers.get(k.lower(), "")
                if v not in actual:
                    errors.append(f'header "{k}": expected to include "{v}", got "{actual}"')

            if len(events) < min_events:
                errors.append(
                    f"SSE: expected at least {min_events} event(s), got {len(events)} within {timeout_secs}s"
                )

            return Result(
                id=scenario["id"],
                description=scenario["description"],
                passed=len(errors) == 0,
                error="; ".join(errors) or None,
                duration_ms=int((time.monotonic() - start) * 1000),
            )

        # ── Webhook signature scenario ───────────────────────────────────────
        if scenario.get("kind") == "webhook_signature":
            secret = os.environ.get(
                scenario.get("secret_env", "CONFORMANCE_WEBHOOK_SECRET"),
                scenario.get("secret", "conformance-secret"),
            )
            payload = scenario.get("payload", '{"event":"test"}')
            timestamp = scenario.get("timestamp", "1700000000")
            message = f"{timestamp}.{payload}"
            signature = "sha256=" + hmac.new(
                secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
            ).hexdigest()

            if not verify_webhook_signature(payload, signature, secret, timestamp):
                errors.append("valid signature was rejected")
            if verify_webhook_signature(payload + "tampered", signature, secret, timestamp):
                errors.append("tampered payload was accepted")
            if verify_webhook_signature(payload, "sha256=deadbeef", secret, timestamp):
                errors.append("invalid signature was accepted")

            return Result(
                id=scenario["id"],
                description=scenario["description"],
                passed=len(errors) == 0,
                error="; ".join(errors) or None,
                duration_ms=int((time.monotonic() - start) * 1000),
            )

        # ── HTTP scenario ─────────────────────────────────────────────────────
        url = build_url(base_url, resolved_path, scenario.get("query"))
        status, resp_headers, body_text = http_request(
            url,
            scenario.get("method", "GET"),
            headers=scenario.get("headers"),
            body=scenario.get("body"),
        )

        expected_status = scenario["expect"]["status"]
        if status != expected_status:
            errors.append(f"status: expected {expected_status}, got {status}")

        for k, v in (scenario["expect"].get("headers") or {}).items():
            actual = resp_headers.get(k.lower(), "")
            if v not in actual:
                errors.append(f'header "{k}": expected to include "{v}", got "{actual}"')

        assertions = scenario["expect"].get("body") or []
        if assertions:
            try:
                parsed_body = json.loads(body_text)
            except json.JSONDecodeError:
                errors.append(f"body is not valid JSON: {body_text[:200]}")
                parsed_body = None

            if parsed_body is not None:
                for assertion in assertions:
                    err = check_assertion(parsed_body, assertion)
                    if err:
                        errors.append(err)

                # Persist subscription ID for delete_subscription
                if scenario["id"] == "create_subscription" and not errors:
                    if isinstance(parsed_body, dict) and "id" in parsed_body:
                        state["subscription_id"] = str(parsed_body["id"])

        return Result(
            id=scenario["id"],
            description=scenario["description"],
            passed=len(errors) == 0,
            error="; ".join(errors) or None,
            duration_ms=int((time.monotonic() - start) * 1000),
        )

    except Exception as exc:
        return Result(
            id=scenario["id"],
            description=scenario.get("description", ""),
            passed=False,
            error=str(exc),
            duration_ms=int((time.monotonic() - start) * 1000),
        )


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_result(r: Result) -> None:
    icon = "✅" if r.passed else "❌"
    print(f"{icon} [{r.id}] {r.description} ({r.duration_ms}ms)")
    if not r.passed and r.error:
        print(f"   └─ {r.error}")


def to_json_result(r: Result) -> dict[str, Any]:
    """Normalized result shape shared with the TypeScript and Go runners."""
    return {
        "id": r.id,
        "description": r.description,
        "passed": r.passed,
        "error": r.error,
        "durationMs": r.duration_ms,
    }


def write_json_report(results: list[Result]) -> None:
    report_path = os.environ.get("CONFORMANCE_REPORT_PATH")
    if not report_path:
        return
    payload = {
        "sdk": "python",
        "results": [to_json_result(r) for r in results],
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def write_summary(results: list[Result]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    total = len(results)
    badge = "🟢" if failed == 0 else "🔴"

    lines = [
        f"## {badge} SDK Conformance — Python ({passed}/{total} passed)\n",
        "| # | Scenario | Status | Duration | Error |",
        "|---|----------|--------|----------|-------|",
    ]
    for r in results:
        status = "✅ Pass" if r.passed else "❌ Fail"
        error = (r.error or "").replace("|", "\\|")
        lines.append(f"| | `{r.id}` | {status} | {r.duration_ms}ms | {error} |")
    lines.append(
        f"\n**Total:** {total} &nbsp;|&nbsp; **Passed:** {passed} &nbsp;|&nbsp; **Failed:** {failed}\n"
    )

    with open(summary_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    scenarios_path = Path(__file__).parent.parent / "scenarios.yaml"
    with open(scenarios_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    env_key: str = raw.get("base_url_env", "CONFORMANCE_BASE_URL")
    base_url: str = os.environ.get(env_key, "http://localhost:3000")

    scenarios: list[ScenarioDict] = raw.get("scenarios", [])

    print(f"\n=== SorobanPulse SDK Conformance — Python ===")
    print(f"Base URL: {base_url}")
    print(f"Scenarios: {len(scenarios)}\n")

    all_results: list[Result] = []
    for scenario in scenarios:
        result = run_scenario(scenario, base_url)
        all_results.append(result)
        print_result(result)

    passed = sum(1 for r in all_results if r.passed)
    failed = sum(1 for r in all_results if not r.passed)

    print(f"\n--- Summary ---")
    print(f"Total: {len(all_results)} | Passed: {passed} | Failed: {failed}")

    write_json_report(all_results)
    write_summary(all_results)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
