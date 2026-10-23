# Notification Delivery Troubleshooting

## NotificationDeliverySLABreach

**Alert**: Notification delivery p95 latency exceeds 30s SLA
**Severity**: Critical

### Symptoms
- Notifications arriving late to users
- P95 delivery latency above 30 seconds
- Potential user-facing impact on alert delivery

### Investigation Steps

1. **Check notification queue depth**:
   ```sql
   SELECT COUNT(*) FROM notifications WHERE status = 'pending';
   SELECT COUNT(*) FROM notifications WHERE status = 'queued';
   ```

2. **Check notification pipeline metrics**:
   ```bash
   curl -s http://localhost:3000/metrics | grep notification
   ```

3. **Review downstream service health**:
   - Email provider status
   - Webhook endpoint availability
   - Slack/PagerDuty API status

### Common Causes

1. **High notification volume**: Spike in events causing queue backup
2. **Downstream service outage**: Email or webhook service unavailable
3. **Rate limiting**: External API rate limits being hit
4. **Database contention**: Lock contention on notification table

### Resolution

1. **Immediate**: Scale up notification workers
   ```bash
   kubectl scale deployment soroban-pulse-notifications --replicas=5
   ```

2. **Short-term**: Increase worker concurrency
   ```bash
   kubectl set env deployment/soroban-pulse NOTIFICATION_WORKERS=10
   ```

3. **Long-term**: Review notification batching strategy

### Runbook References
- [Email Notifications](../email-notifications.md)
- [Notification Channels](../notification-channels.md)
- [Rate Limiting](../notification-rate-limiting.md)

---

## NotificationDeliveryLatencyHigh

**Alert**: Notification delivery p99 latency exceeds 60s
**Severity**: Warning

### Symptoms
- Occasional delayed notifications
- P99 latency approaching SLA breach

### Investigation Steps

1. **Check for slow consumers**:
   ```sql
   SELECT channel, AVG(delivery_time) 
   FROM notification_delivery_log 
   WHERE created_at > NOW() - INTERVAL '1 hour'
   GROUP BY channel;
   ```

2. **Review webhook endpoint response times**

3. **Check for network issues**

### Resolution
- Optimize slow webhook endpoints
- Consider async processing for heavy payloads
- Implement circuit breakers for unreliable downstream services

---

## NotificationDeliverySuccessRateLow

**Alert**: Notification delivery success rate below 95%
**Severity**: Critical

### Symptoms
- `NotificationDeliverySuccessRateLow` firing for more than 5 minutes
- Users report missing emails, SMS or webhook calls
- `soroban_pulse_notification_delivery_failure_total` rising faster than usual

Follow **Channel failover** below if one channel is clearly down.

---

## Channel failover

Use this procedure when the primary notification channel (for example email
via SMTP) is down and notifications must be routed through a healthy channel.
Target time to complete: **under 5 minutes**.

### Prerequisites

```bash
export PULSE_URL=https://your-host        # Soroban Pulse base URL
export ADMIN_TOKEN=...                    # admin API key
```

### Step 1 — Confirm which channel is failing (≈1 min)

Open the **Soroban Pulse** dashboard in Grafana (`docs/grafana-dashboard.json`)
and check delivery success rate per channel. To add the panel ad hoc, use
**Explore** with the Prometheus data source and run:

```promql
# Success rate per channel over the last 5 minutes (0–1)
sum by (channel) (rate(soroban_pulse_notification_delivery_success_total[5m]))
/
(
  sum by (channel) (rate(soroban_pulse_notification_delivery_success_total[5m]))
  +
  sum by (channel) (rate(soroban_pulse_notification_delivery_failure_total[5m]))
)
```

Supporting queries:

```promql
# Channel health gauge: 1 = healthy, 0 = unhealthy
soroban_pulse_notification_channel_healthy

# p95 delivery latency per channel
histogram_quantile(0.95,
  sum by (channel, le) (rate(soroban_pulse_notification_delivery_latency_seconds_bucket[5m])))

# Failovers already performed
sum by (channel) (increase(soroban_pulse_notification_failover_total[15m]))
```

A channel whose success rate is below 0.95 while others are near 1.0 is the
failing channel. Cross-check in the database:

```sql
SELECT channel_type,
       COUNT(*) FILTER (WHERE status = 'success') AS ok,
       COUNT(*) FILTER (WHERE status = 'failure') AS failed,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'success') / NULLIF(COUNT(*), 0), 1) AS success_pct
FROM notification_deliveries
WHERE delivered_at > NOW() - INTERVAL '15 minutes'
GROUP BY channel_type
ORDER BY success_pct;
```

### Step 2 — Switch the default channel (≈1 min)

Switch the default notification channel to a healthy one (`webhook`, `email`,
`sms` or `pagerduty`):

```bash
curl -sS -X POST "$PULSE_URL/v1/admin/notifications/channel" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"channel": "webhook"}'
```

Verify the change:

```bash
curl -sS "$PULSE_URL/v1/admin/notifications/dashboard" \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Within 1–2 minutes the success rate for the new channel should rise in Grafana
and `soroban_pulse_notification_delivery_failure_total` should stop climbing.

### Step 3 — Clear the notification DLQ (≈2 min)

Notifications that exhausted their retries while the primary channel was down
are parked in the dead-letter queue. Once the new channel is delivering:

1. **Inspect the backlog**:
   ```bash
   curl -sS "$PULSE_URL/v1/admin/webhook-dlq" \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Or check the `soroban_pulse_webhook_dlq_depth` gauge.

2. **Replay all pending entries** through the healthy channel (use `limit` to
   replay in smaller batches if the backlog is large):
   ```bash
   curl -sS -X POST "$PULSE_URL/v1/admin/webhook-dlq/replay" \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"limit": 500}'
   ```

3. **Discard entries that should not be resent** (for example, stale alerts):
   ```bash
   curl -sS -X DELETE "$PULSE_URL/v1/admin/webhook-dlq/<id>" \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```

4. **Confirm the DLQ is drained**:
   ```sql
   SELECT COUNT(*) FROM webhook_dlq WHERE replayed_at IS NULL;
   ```
   The result should be `0` and `soroban_pulse_webhook_dlq_depth` should drop to 0.

### Step 4 — Fail back once the primary recovers

1. Confirm the primary provider is healthy (provider status page, and
   `POST /v1/admin/notifications/channels/{id}/test` returns success).
2. Switch the default back:
   ```bash
   curl -sS -X POST "$PULSE_URL/v1/admin/notifications/channel" \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"channel": "email"}'
   ```
3. Watch the per-channel success rate panel for 10 minutes and confirm
   `NotificationDeliverySuccessRateLow` has resolved.
4. Record the incident timeline, affected window and DLQ replay counts in the
   incident ticket.

---

## Common Remediation Commands

### Flush stuck notifications
```sql
UPDATE notifications SET status = 'pending' 
WHERE status = 'queued' AND created_at < NOW() - INTERVAL '1 hour';
```

### Check notification worker health
```bash
kubectl get pods -l app=soroban-pulse -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}'
```

### Restart notification workers
```bash
kubectl rollout restart deployment/soroban-pulse-notifications
```
