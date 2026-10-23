# Webhook Failures Runbook

## Symptom
Webhook delivery failures are occurring. The `soroban_pulse_webhook_failures_total` metric is increasing, indicating that webhooks are not being successfully delivered to subscribers after all retry attempts are exhausted.

## Likely Causes
1. **Subscriber endpoint is down**: The webhook subscriber's endpoint is unreachable or returning errors
2. **Network connectivity issues**: Network partition or firewall blocking webhook delivery
3. **Subscriber endpoint is slow**: The endpoint is timing out before the indexer's timeout expires
4. **Invalid webhook configuration**: Incorrect subscriber URL or authentication credentials
5. **Webhook payload is too large**: The payload exceeds the subscriber's size limits
6. **Rate limiting**: The subscriber is rate-limiting webhook requests

## Investigation Steps

### 1. Check webhook failure rate
```bash
promtool query instant 'rate(soroban_pulse_webhook_failures_total[5m])'
```

### 2. Review indexer logs for webhook errors
```bash
kubectl logs -l app=soroban-pulse -c soroban-pulse --tail=200 | grep -i "webhook\|delivery\|subscriber"
```

### 3. Check subscriber endpoint health
```bash
# Test connectivity to subscriber endpoint
curl -v -X POST https://subscriber-endpoint.example.com/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": true}'
```

### 4. Check webhook configuration
```bash
# Query the database for webhook subscriptions
psql $DATABASE_URL -c "SELECT id, url, status, last_error FROM webhooks LIMIT 10;"
```

### 5. Monitor network connectivity
```bash
# From the indexer pod
kubectl exec -it <pod-name> -- bash
curl -v https://subscriber-endpoint.example.com/health
```

## Resolution Steps

### If subscriber endpoint is down
- Contact the subscriber to restore their endpoint
- Temporarily disable the webhook subscription
- Update the webhook URL to a working endpoint

### If network connectivity is the issue
- Check firewall rules and security groups
- Verify DNS resolution of the subscriber endpoint
- Check for network latency or packet loss

### If the endpoint is slow
- Increase the webhook timeout in the indexer configuration
- Ask the subscriber to optimize their endpoint performance
- Implement async webhook processing on the subscriber side

### If webhook configuration is invalid
- Verify the subscriber URL is correct
- Check authentication credentials (API keys, tokens)
- Test the webhook manually with `curl`

### If payload is too large
- Reduce the amount of data in the webhook payload
- Implement payload compression
- Ask the subscriber to increase their size limits

### If rate limiting is occurring
- Reduce the webhook delivery frequency
- Implement exponential backoff for retries
- Contact the subscriber to increase rate limits

## Dead-Letter Queue (DLQ) Procedures

When a webhook exhausts all retry attempts, it is no longer discarded silently. The failed delivery is moved into the `webhook_dlq` table, which stores the `subscription_id`, `event_id`, `payload`, `last_error`, `failed_at`, and `replayed_at` for each entry. Failed webhooks appear in the DLQ within one retry cycle.

### 1. Monitor DLQ depth
```bash
promtool query instant 'soroban_pulse_webhook_dlq_depth'
```
A steadily growing `soroban_pulse_webhook_dlq_depth` gauge indicates deliveries are failing and not being recovered. Alert when the gauge exceeds a threshold or grows consistently over time.

### 2. Inspect DLQ entries
```bash
# Paginated listing of DLQ entries
curl -s "$ADMIN_API_URL/v1/admin/webhook-dlq?page=1&page_size=50" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq .
```
Review `last_error` and `failed_at` to identify the root cause (see Investigation Steps above). Filter by subscription if needed:
```bash
curl -s "$ADMIN_API_URL/v1/admin/webhook-dlq?subscription_id=<subscription-id>" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq .
```

### 3. Replay a single entry
Once the subscriber endpoint is healthy, replay an individual delivery. Replayed webhooks re-enter the normal delivery queue.
```bash
curl -s -X POST "$ADMIN_API_URL/v1/admin/webhook-dlq/<dlq-id>/replay" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq .
```

### 4. Bulk replay
Replay all pending DLQ entries, optionally scoped to a single subscription:
```bash
# Replay everything
curl -s -X POST "$ADMIN_API_URL/v1/admin/webhook-dlq/replay" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq .

# Replay only entries for one subscription
curl -s -X POST "$ADMIN_API_URL/v1/admin/webhook-dlq/replay" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"subscription_id": "<subscription-id>"}' | jq .
```
After replay, confirm entries have a non-null `replayed_at` and that `soroban_pulse_webhook_dlq_depth` decreases.

### 5. Delete DLQ entries
If a delivery is unrecoverable (e.g. the subscription was deleted or the payload is obsolete), remove it from the DLQ:
```bash
curl -s -X DELETE "$ADMIN_API_URL/v1/admin/webhook-dlq/<dlq-id>" \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

## Prevention
- Monitor webhook failure rate continuously
- Monitor `soroban_pulse_webhook_dlq_depth` and alert on sustained growth
- Set up alerts for webhook delivery failures
- Implement webhook retry logic with exponential backoff
- Test webhook endpoints regularly
- Implement webhook signature verification for security
- Document webhook payload format and size limits
- Provide webhook testing tools for subscribers
- Periodically review and replay or prune DLQ entries
