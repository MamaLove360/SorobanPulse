# Tutorial: Build a Token Transfer Alert Bot with SorobanPulse

This tutorial builds a complete alert bot that monitors SEP-41 token transfer
events on the Stellar testnet and sends a Slack message every time a transfer
occurs on a given contract. The full receiver is under 100 lines of code.

**What you'll build:**

```
Stellar testnet ──► SorobanPulse ──► HTTP webhook ──► Bot (Python / Flask) ──► Slack
```

**Prerequisites:**

- SorobanPulse running locally (see [Quickstart Tutorial](tutorial-quickstart.md))
- Python 3.10+ with `pip`
- A Slack workspace where you can add an Incoming Webhook
- `curl` and `ngrok` (or any tunnel that exposes a local port publicly)

---

## Step 1 — Create a Slack Incoming Webhook

1. Go to **https://api.slack.com/apps** → **Create New App → From scratch**.
2. Name it `SorobanPulse Alerts`, pick your workspace, click **Create App**.
3. Under **Features** click **Incoming Webhooks**, toggle it on, then click
   **Add New Webhook to Workspace**.
4. Choose a channel (e.g. `#stellar-alerts`) and click **Allow**.
5. Copy the webhook URL — it looks like:

```
https://<your-slack-incoming-webhook-url>
```

You will use this as `SLACK_WEBHOOK_URL` below.

---

## Step 2 — Expose your local port with ngrok

SorobanPulse needs a publicly reachable URL to POST webhooks to.
[ngrok](https://ngrok.com/download) creates a temporary HTTPS tunnel:

```bash
ngrok http 5000
```

Expected output:

```
Forwarding  https://a1b2c3d4.ngrok.io -> http://localhost:5000
```

Copy the `https://` URL — this is your `WEBHOOK_RECEIVER_URL`.

---

## Step 3 — Write the receiver bot

Create `alert_bot.py` with the following content (89 lines including comments):

```python
#!/usr/bin/env python3
"""
SorobanPulse → Slack token-transfer alert bot.

Receives webhook POSTs from SorobanPulse, validates the HMAC-SHA256
signature, filters for SEP-41 'transfer' topics, and forwards each
matching event to Slack as a formatted message.

Install:  pip install flask requests
Run:
    SLACK_WEBHOOK_URL=https://<your-slack-incoming-webhook-url> \
    SOROBAN_WEBHOOK_SECRET=my-secret \
    WATCHED_CONTRACT_ID=CDLZFC3... \
    python alert_bot.py
"""

import hashlib
import hmac
import json
import os
import sys

import requests
from flask import Flask, abort, jsonify, request

app = Flask(__name__)

# ── Configuration ──────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL   = os.environ.get("SLACK_WEBHOOK_URL", "")
WEBHOOK_SECRET      = os.environ.get("SOROBAN_WEBHOOK_SECRET", "")
WATCHED_CONTRACT_ID = os.environ.get("WATCHED_CONTRACT_ID", "")  # empty = all


# ── HMAC signature verification ────────────────────────────────────────────
def verify_signature(body: bytes, header: str) -> bool:
    """Return True when X-Signature-256 matches the request body."""
    if not WEBHOOK_SECRET:
        return True  # dev mode — skip verification
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    received = header.removeprefix("sha256=")
    return hmac.compare_digest(expected, received)  # constant-time comparison


# ── Slack helper ───────────────────────────────────────────────────────────
def post_to_slack(event: dict) -> None:
    """Format one SEP-41 transfer event and POST it to Slack."""
    val = event.get("event_data", {}).get("value", {})
    if isinstance(val, dict) and "I128" in val:
        i128   = val["I128"]
        amount = (i128["hi"] << 64) | i128["lo"]
    else:
        amount = val

    text = (
        "*SEP-41 Transfer* :money_with_wings:\n"
        f"Contract: `{event.get('contract_id', '?')}`\n"
        f"Amount:   `{amount}`\n"
        f"Ledger:   `{event.get('ledger', '?')}`\n"
        f"Tx:       `{event.get('tx_hash', '?')}`"
    )
    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5)
    resp.raise_for_status()


# ── Webhook endpoint ───────────────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    body   = request.get_data()
    sig    = request.headers.get("X-Signature-256", "")

    if not verify_signature(body, sig):
        app.logger.warning("Rejected request: invalid signature")
        abort(401)

    try:
        event = json.loads(body)
    except json.JSONDecodeError:
        abort(400)

    # Ignore events from other contracts when a filter is configured.
    if WATCHED_CONTRACT_ID and event.get("contract_id") != WATCHED_CONTRACT_ID:
        return jsonify({"status": "ignored", "reason": "contract_id mismatch"})

    # Only forward SEP-41 transfer events (topic[0] == ["Symbol", "transfer"]).
    topics      = event.get("event_data", {}).get("topic", [])
    is_transfer = any(t == ["Symbol", "transfer"] or t == "transfer" for t in topics)
    if not is_transfer:
        return jsonify({"status": "ignored", "reason": "not a transfer event"})

    try:
        post_to_slack(event)
    except requests.RequestException as exc:
        app.logger.error("Slack delivery failed: %s", exc)
        # Return 500 so SorobanPulse retries delivery (exponential backoff).
        return jsonify({"status": "error", "detail": str(exc)}), 500

    return jsonify({"status": "ok"})


# ── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not SLACK_WEBHOOK_URL:
        sys.exit("Error: SLACK_WEBHOOK_URL is not set.")
    app.run(host="0.0.0.0", port=5000, debug=False)
```

---

## Step 4 — Install dependencies and start the bot

```bash
pip install flask requests

SLACK_WEBHOOK_URL="https://<your-slack-incoming-webhook-url>" \
SOROBAN_WEBHOOK_SECRET="my-secret" \
WATCHED_CONTRACT_ID="CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC" \
python alert_bot.py
```

Expected output:

```
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:5000
```

---

## Step 5 — Register the webhook with SorobanPulse

Add these three lines to your `.env`, then run `make docker-up` to apply:

```ini
WEBHOOK_URL=https://a1b2c3d4.ngrok.io/webhook
WEBHOOK_SECRET=my-secret
WEBHOOK_CONTRACT_FILTER=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
```

> `WEBHOOK_CONTRACT_FILTER` limits delivery to your watched contract only —
> this keeps Slack quiet and reduces load on the bot. Remove it to receive
> events from every indexed contract.

Confirm the service picked up the new config:

```bash
curl -s http://localhost:3000/health | python3 -m json.tool
```

```json
{
    "status": "ok",
    "db": "ok",
    "indexer": "ok"
}
```

---

## Step 6 — Watch the alerts flow

Within one indexer poll cycle (≤ 5 seconds) SorobanPulse will POST each new
matching event to your bot. When a SEP-41 transfer is indexed you'll see:

**Bot terminal:**

```
127.0.0.1 - - [20/Oct/2026 12:34:56] "POST /webhook HTTP/1.1" 200 -
```

**Slack channel (`#stellar-alerts`):**

```
SEP-41 Transfer 💸
Contract: `CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC`
Amount:   `1000000`
Ledger:   `5943210`
Tx:       `a1b2c3d4e5f6...`
```

---

## Error handling and retry behaviour

SorobanPulse retries failed webhook deliveries with **exponential backoff**
(1 s → 2 s → 4 s, up to 3 attempts). Returning `500` from the bot signals a
transient failure and triggers a retry. Returning `200` marks the delivery as
accepted even if you chose to drop the event.

Track persistent failures via the Prometheus counter:

```bash
curl -s http://localhost:3000/metrics | grep webhook_failures
# soroban_pulse_webhook_failures_total 0
```

---

## Verifying the HMAC signature manually

```bash
# Re-create the signature SorobanPulse sends
echo -n '{"id":"test","contract_id":"CABC..."}' \
  | openssl dgst -sha256 -hmac "my-secret"
# SHA2-256(stdin)= 7a3fbd...

# The X-Signature-256 header value will be: sha256=7a3fbd...
```

---

## What's next?

| Feature | Where to go |
|---|---|
| Stream events in real time without polling | [SSE Reconnection](sse-reconnection.md) |
| Filter by event topic or payload fields | [Subscription Best Practices](subscription-best-practices.md) |
| Avoid Slack flooding with rate limits | [Notification Rate Limiting](notification-rate-limiting.md) |
| Full webhook signing reference | [Webhook Signing](webhook_signing.md) |
| Deploy to production | [Deployment Guide](deployment.md) |
