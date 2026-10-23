# Rate Limiting Per API Key (Issue #567)

## Overview

Soroban Pulse provides granular rate limiting on a per-API-key basis using a sliding window algorithm. This ensures fair usage across different clients and prevents abuse of the API.

## Features

- **Per-API Key Rate Limiting**: Limits are applied individually to each API key
- **Multiple Time Windows**: Support for minute, hour, and day-based limits
- **Sliding Window Algorithm**: Fair and accurate rate limiting across time windows
- **Database Backed**: Rate limit counters persisted in PostgreSQL for accuracy
- **Status Endpoint**: Check your current rate limit status
- **Rate Limit Headers**: Response headers indicate remaining requests and reset times

## Configuration

Rate limiting can be configured via environment variables:

```bash
# Maximum requests per minute per API key (optional)
RATE_LIMIT_KEY_PER_MINUTE=1000

# Maximum requests per hour per API key (optional)
RATE_LIMIT_KEY_PER_HOUR=10000

# Maximum requests per day per API key (optional)
RATE_LIMIT_KEY_PER_DAY=100000
```

### Example Configurations

**Strict (development/testing)**:
```bash
RATE_LIMIT_KEY_PER_MINUTE=10
RATE_LIMIT_KEY_PER_HOUR=100
RATE_LIMIT_KEY_PER_DAY=1000
```

**Standard (production)**:
```bash
RATE_LIMIT_KEY_PER_MINUTE=1000
RATE_LIMIT_KEY_PER_HOUR=10000
RATE_LIMIT_KEY_PER_DAY=100000
```

**Relaxed (unlimited/testing)**:
```bash
# No rate limit variables set - unlimited access
```

## Sliding Window Algorithm

Soroban Pulse uses a sliding window rate limiting algorithm:

1. **Time Windows**: Divides time into rolling windows (minute, hour, day)
2. **Counter Tracking**: Stores request count for each window per API key
3. **Active Window Check**: Counts requests in the current sliding window
4. **Enforcement**: Rejects requests if the current window count exceeds the limit

### Example

**Configuration**: 1000 requests per minute

**Timeline**:
```
Time  Request Count  Status
----  -----------   ------
11:50      500       OK (reset at 11:51)
11:51      300       OK (total 800, window resets)
11:51      300       OK (total 800, still in window)
11:51      100       OK (total 900, still in window)
11:51       50       REJECTED (would be 950, exceeds limit)
11:52      100       OK (new window, reset counter)
```

## Response Headers

When rate limiting is configured, each response includes headers indicating your usage:

### Standard Rate Limit Headers

```
X-RateLimit-Limit-Minute: 1000
X-RateLimit-Remaining-Minute: 847
X-RateLimit-Reset-Minute: 1703721660

X-RateLimit-Limit-Hour: 10000
X-RateLimit-Remaining-Hour: 8253
X-RateLimit-Reset-Hour: 1703725200

X-RateLimit-Limit-Day: 100000
X-RateLimit-Remaining-Day: 75432
X-RateLimit-Reset-Day: 1703807600
```

### Header Meanings

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit-*` | Total requests allowed in this window |
| `X-RateLimit-Remaining-*` | Requests remaining in current window |
| `X-RateLimit-Reset-*` | Unix timestamp when window resets |

## HTTP 429 Response

When you exceed a rate limit, you'll receive a 429 (Too Many Requests) response:

```json
{
  "error": "Rate limit exceeded",
  "detail": "Too many requests from this API key",
  "rate_limit_reset_at": 1703721660
}
```

**Status Code**: 429 Too Many Requests

**Retry-After Header**: Contains the number of seconds to wait before retrying

```
Retry-After: 45
```

## Rate Limit Status Endpoint

Check your current rate limit status without making a request that counts toward the limit:

```bash
curl -H "X-Api-Key: your_api_key" \
     https://api.example.com/v1/rate-limit/status
```

### Response Format

```json
{
  "remaining_minute": 847,
  "limit_minute": 1000,
  "remaining_hour": 8253,
  "limit_hour": 10000,
  "remaining_day": 75432,
  "limit_day": 100000,
  "is_rate_limited": false,
  "reset_at": 1703721660
}
```

### Status Endpoint Examples

**Check rate limit status**:
```bash
curl -s -H "X-Api-Key: sk_test_123" \
     https://api.example.com/v1/rate-limit/status | jq
```

**Check if rate limited**:
```bash
curl -s -H "X-Api-Key: sk_test_123" \
     https://api.example.com/v1/rate-limit/status | jq '.is_rate_limited'
```

**Get reset timestamp**:
```bash
curl -s -H "X-Api-Key: sk_test_123" \
     https://api.example.com/v1/rate-limit/status | jq '.reset_at | todate'
```

## Best Practices

### 1. Check Rate Limit Before Making Requests

```python
import requests
import time
import json

def check_rate_limit(api_key):
    response = requests.get(
        'https://api.example.com/v1/rate-limit/status',
        headers={'X-Api-Key': api_key}
    )
    return response.json()

def make_request_with_backoff(api_key, endpoint):
    # Check status before request
    status = check_rate_limit(api_key)
    
    if status['is_rate_limited']:
        wait_time = status['reset_at'] - int(time.time())
        print(f"Rate limited. Waiting {wait_time} seconds...")
        time.sleep(wait_time + 1)
        # Retry after waiting
        status = check_rate_limit(api_key)
    
    # Make the actual request
    response = requests.get(
        f'https://api.example.com{endpoint}',
        headers={'X-Api-Key': api_key}
    )
    
    return response
```

### 2. Respect Rate-Limit Headers

```python
import time

def request_with_rate_limit_awareness(api_key, endpoint):
    response = requests.get(
        f'https://api.example.com{endpoint}',
        headers={'X-Api-Key': api_key}
    )
    
    # Check if rate limited
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))
        print(f"Rate limited. Retrying in {retry_after} seconds...")
        time.sleep(retry_after)
        return request_with_rate_limit_awareness(api_key, endpoint)
    
    # Track remaining requests
    remaining = int(response.headers.get('X-RateLimit-Remaining-Minute', 0))
    limit = int(response.headers.get('X-RateLimit-Limit-Minute', 0))
    
    print(f"Requests: {limit - remaining}/{limit} used")
    
    return response
```

### 3. Batch Requests Efficiently

```python
def batch_events(api_key, events, batch_size=100):
    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        
        # Check rate limit before large request
        status = check_rate_limit(api_key)
        if status['is_rate_limited']:
            wait_time = status['reset_at'] - int(time.time())
            time.sleep(wait_time + 1)
        
        # Process batch
        response = requests.post(
            'https://api.example.com/v1/events/batch',
            json={'events': batch},
            headers={'X-Api-Key': api_key}
        )
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break
```

### 4. Implement Exponential Backoff

```python
import random
import time

def request_with_exponential_backoff(api_key, endpoint, max_retries=5):
    for attempt in range(max_retries):
        response = requests.get(
            f'https://api.example.com{endpoint}',
            headers={'X-Api-Key': api_key}
        )
        
        if response.status_code == 429:
            # Exponential backoff with jitter
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"Rate limited. Waiting {wait_time:.2f} seconds (attempt {attempt + 1})...")
            time.sleep(wait_time)
            continue
        
        return response
    
    raise Exception("Max retries exceeded")
```

## Understanding Rate Limit Tiers

### Free Tier
```
Per Minute:  100 requests
Per Hour:    1,000 requests
Per Day:     10,000 requests
```

### Professional Tier
```
Per Minute:  1,000 requests
Per Hour:    10,000 requests
Per Day:     100,000 requests
```

### Enterprise Tier
```
Per Minute:  10,000 requests
Per Hour:    100,000 requests
Per Day:     1,000,000 requests
```

## Monitoring Rate Limit Usage

### Log Rate Limit Headers

```javascript
async function logRateLimit(response) {
    const rateLimit = {
        minute: {
            limit: response.headers.get('X-RateLimit-Limit-Minute'),
            remaining: response.headers.get('X-RateLimit-Remaining-Minute'),
            reset: response.headers.get('X-RateLimit-Reset-Minute'),
        },
        hour: {
            limit: response.headers.get('X-RateLimit-Limit-Hour'),
            remaining: response.headers.get('X-RateLimit-Remaining-Hour'),
            reset: response.headers.get('X-RateLimit-Reset-Hour'),
        },
        day: {
            limit: response.headers.get('X-RateLimit-Limit-Day'),
            remaining: response.headers.get('X-RateLimit-Remaining-Day'),
            reset: response.headers.get('X-RateLimit-Reset-Day'),
        }
    };
    
    console.log('Rate Limit Status:', rateLimit);
}
```

### Alert When Close to Limit

```python
def check_and_alert(response, threshold_percent=80):
    remaining = int(response.headers.get('X-RateLimit-Remaining-Minute', 0))
    limit = int(response.headers.get('X-RateLimit-Limit-Minute', 0))
    
    usage_percent = ((limit - remaining) / limit) * 100
    
    if usage_percent > threshold_percent:
        alert_level = 'WARNING' if usage_percent > 90 else 'INFO'
        print(f"[{alert_level}] Rate limit usage at {usage_percent:.1f}%")
```

## Troubleshooting

### I'm getting 429 responses

**Cause**: You've exceeded your rate limit

**Solution**:
1. Check the `Retry-After` header to see how long to wait
2. Verify your rate limit configuration
3. Use the `/v1/rate-limit/status` endpoint to check current usage
4. Implement exponential backoff in your client

### Rate limit resets unexpectedly

**Cause**: Window timing

**Note**: Rate limits use sliding windows. The reset time shown is for the current window. Once a new window starts, the counter resets.

### I need higher limits

**Solution**: Contact support to discuss your use case and upgrade your plan

## Database Schema

Rate limit counters are stored in the `rate_limit_counters` table:

```sql
CREATE TABLE rate_limit_counters (
    id UUID PRIMARY KEY,
    api_key_hash TEXT NOT NULL,        -- SHA-256 hash of API key
    window_start TIMESTAMPTZ NOT NULL, -- Start of time window
    request_count INTEGER NOT NULL,    -- Number of requests in window
    last_updated TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_rate_limit_api_key_hash ON rate_limit_counters (api_key_hash);
CREATE INDEX idx_rate_limit_window_start ON rate_limit_counters (window_start);
```

### Cleanup

Old rate limit counters are automatically cleaned up. Windows older than 24 hours are removed to maintain database performance.

## API Reference

### Rate Limit Status Endpoint

**Endpoint**: `GET /v1/rate-limit/status`

**Authentication**: Required (any valid API key)

**Response**:
```json
{
  "remaining_minute": number,      // Optional
  "limit_minute": number,           // Optional
  "remaining_hour": number,         // Optional
  "limit_hour": number,             // Optional
  "remaining_day": number,          // Optional
  "limit_day": number,              // Optional
  "is_rate_limited": boolean,       // true if currently rate limited
  "reset_at": number                // Unix timestamp of next reset
}
```

**Example**:
```bash
curl -H "X-Api-Key: sk_prod_abc123" \
     https://api.example.com/v1/rate-limit/status
```

## Endpoint-Specific Rate Limits

In addition to the per-API-key limits described above, certain endpoints have
stricter, per-IP limits to protect backend resources from abuse.

### `POST /v1/subscriptions`

Webhook subscription registration is limited to **5 requests per minute per IP**
(the global limit is 60 requests per minute). This prevents an attacker from
exhausting database connections by spamming subscription creation.

The 6th request from the same IP within a one-minute window returns
`429 Too Many Requests`.

### `GET /v1/events/stream`

Server-Sent Events (SSE) connections are limited to **5 concurrent connections
per IP**. Concurrent connection counts are tracked in-memory (using `dashmap`).

The 6th concurrent SSE connection from the same IP returns
`429 Too Many Requests`. The slot is released when the connection closes.

### 429 Responses

Both endpoints return `429 Too Many Requests` with a `Retry-After` header
indicating the number of seconds to wait before retrying:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 12
```

```json
{
  "error": "Rate limit exceeded",
  "detail": "Too many requests from this IP"
}
```

### Metrics

Rate-limited requests are counted with the following Prometheus counters:

| Metric | Description |
|--------|-------------|
| `soroban_pulse_subscription_rate_limited_total` | Number of `POST /v1/subscriptions` requests rejected by the per-IP rate limit |
| `soroban_pulse_sse_rate_limited_total` | Number of `GET /v1/events/stream` connections rejected by the per-IP connection limit |

## See Also

- [API Authentication](./api_authentication.md)
- [Error Handling](./error_handling.md)
- [Performance Optimization](./performance.md)
