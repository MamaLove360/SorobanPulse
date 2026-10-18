# Frequently Asked Questions (Issue #999)

> **Status: implementation only, not test-verified.** This document was
> written to satisfy the issue #999 checklist without running the service or
> validating the referenced doc links against a live build. Cross-check any
> command or config value below against the linked guide before relying on
> it operationally.

Soroban Pulse indexes Soroban smart contract events on the Stellar network
and exposes them via a REST API, Server-Sent Events, and webhook/notification
channels. This FAQ collects the questions that come up most often from both
**users** (people querying/subscribing to events) and **operators** (people
running an instance).

## Table of contents

- [General](#general)
- [Getting started](#getting-started)
- [Subscriptions and notifications](#subscriptions-and-notifications)
- [Integrations](#integrations)
- [Troubleshooting](#troubleshooting)
- [Performance tuning](#performance-tuning)
- [Security](#security)
- [Video walkthroughs](#video-walkthroughs)

---

## General

**What is Soroban Pulse?**
A Rust backend service that polls the Stellar Soroban RPC for contract
events, indexes them in PostgreSQL, and re-exposes them via a REST API,
real-time SSE streams, and outbound notification channels (webhooks, email,
SMS, push, and chat integrations). See the [README](../README.md) and
[architecture.md](architecture.md) for the full component breakdown.

**Is Soroban Pulse an alternative to Stellar Horizon?**
It's complementary rather than a drop-in replacement — Soroban Pulse
specializes in Soroban contract *events* with subscription/notification
delivery on top, whereas Horizon covers the broader ledger/transaction API
surface. If you're migrating event-watching workflows off Horizon, see
[migration-guides/from-stellar-horizon.md](migration-guides/from-stellar-horizon.md).

**What database does it require?**
PostgreSQL. Migrations live in `migrations/` and are applied automatically
on startup (see [development-setup.md](development-setup.md)).

**Where do I report a bug or request a feature?**
Open a GitHub issue against this repository. Include your Soroban Pulse
version, deployment platform, and (for bugs) relevant log lines with
correlation IDs — see [correlation-ids.md](correlation-ids.md).

## Getting started

**How do I run Soroban Pulse locally?**
See [development-setup.md](development-setup.md) and
[onboarding.md](onboarding.md) for the full local setup
(PostgreSQL, environment variables, running migrations, starting the
service).

**How do I know the indexer is healthy and caught up?**
Check the `/health` endpoint and the `soroban_pulse_indexer_lag_ledgers`
gauge (current ledger vs. latest ledger from RPC). See
[kubernetes-probes.md](kubernetes-probes.md) and
[metrics-reference.md](metrics-reference.md).

**How do I create my first subscription?**
See [subscription-best-practices.md](subscription-best-practices.md) and
the [API guide](api-guide.md) for the subscription endpoints and filter
syntax ([filter-dsl.md](filter-dsl.md)).

**Is there a client SDK?**
Yes — see [client-libraries.md](client-libraries.md),
[js-sdk.md](js-sdk.md), and [python-sdk.md](python-sdk.md).

## Subscriptions and notifications

**What notification channels are supported?**
Webhook, email, SMS, and push (FCM/APNs/Web), plus chat integrations
(Discord, Slack, Microsoft Teams, Telegram, GitHub, PagerDuty). See
[notification-channels.md](notification-channels.md) and, for how delivery
retry/error-handling is implemented across email/SMS/push, see
[notification-architecture.md](notification-architecture.md).

**Why didn't I receive a notification I expected?**
Common causes, roughly in order of likelihood:
1. The subscription's filter didn't match the event — check
   [filter-dsl.md](filter-dsl.md) and [subscription-validator](subscription-validator.md).
2. Quiet hours or a maintenance window suppressed it — see
   [notification-batching.md](notification-batching.md).
3. The target (email/phone/push token) is on a suppression list or has
   unsubscribed.
4. The channel is unhealthy — check the
   `soroban_pulse_notification_channel_healthy` gauge.
5. It was rate-limited or deduplicated — see
   [notification-rate-limiting.md](notification-rate-limiting.md) and
   [notification-deduplication.md](notification-deduplication.md).

**Can I batch notifications instead of getting one per event?**
Yes, for email — see the `Schedule` options (`immediate`, `hourly_digest`,
`daily_digest`, custom cron) in [email-notifications.md](email-notifications.md).

**How does notification priority work?**
Critical-priority events bypass batching and are sent immediately across
supported channels; see [priority-queueing.md](priority-queueing.md).

**Can I pause a subscription temporarily instead of deleting it?**
Yes — see the pause/resume endpoints described in the API guide (issue
#884 in `metrics.rs` tracks `soroban_pulse_subscriptions_paused_total` /
`_resumed_total` for this).

## Integrations

**Which outbound integrations exist today?**
Slack, Discord, Microsoft Teams, Telegram, GitHub, PagerDuty, generic
webhooks, Kafka, AWS Kinesis, AWS EventBridge, Google Pub/Sub, AWS SQS, and
Prometheus remote-write. See the respective `docs/*-integration.md` /
`docs/*.md` pages (e.g. [slack-integration.md](slack-integration.md),
[pagerduty-integration.md](pagerduty-integration.md),
[kafka-event-publishing.md](kafka-event-publishing.md)).

**How do I verify a webhook is genuinely from Soroban Pulse?**
Every webhook is HMAC-signed; verify it using the steps in
[webhook-verification.md](webhook-verification.md) and
[webhook-signing.md](webhook-signing.md).

**What happens if a webhook endpoint is unreachable?**
Delivery retries with backoff per [retry-policies.md](retry-policies.md); if
retries are exhausted the event fails over according to
[graceful-degradation.md](graceful-degradation.md), and
`soroban_pulse_webhook_failures_total` is incremented.

**Is there a GraphQL API in addition to REST?**
Yes — see [graphql_api.md](graphql_api.md) and
[graphql_subscriptions.md](graphql_subscriptions.md).

## Troubleshooting

**The indexer is stuck / not advancing.**
See the "Indexer lag" section of
[troubleshooting-guide.md](troubleshooting-guide.md) and check
`soroban_pulse_indexer_current_ledger` vs.
`soroban_pulse_indexer_latest_ledger`. Also check
`soroban_pulse_indexer_is_leader` if running multiple replicas — only the
advisory-lock holder indexes; see
[advisory-lock-behavior.md](advisory-lock-behavior.md).

**I'm seeing database connection pool exhaustion.**
See [connection-pool.md](connection-pool.md) and the pool-utilization
metrics in [metrics-reference.md](metrics-reference.md)
(`soroban_pulse_db_pool_utilization`, `_pool_exhaustion_alerts_total`).

**Events are duplicated / missing.**
See [event-deduplication.md](event-deduplication.md) (bloom-filter and
content-fingerprint dedup) and [data-quality-monitoring.md](data-quality-monitoring.md).

**Where do I find general log-based troubleshooting steps?**
[troubleshooting.md](troubleshooting.md),
[log-analysis-tool.md](log-analysis-tool.md), and
[log-aggregation.md](log-aggregation.md).

**A counter metric looks like it reset to a small number.**
As of issue #993, long-running counters (e.g. push delivery analytics) use
a saturating counter that will not silently wrap; check
`soroban_pulse_counter_overflow_total` — if it's non-zero for that counter,
it genuinely saturated at `u64::MAX` rather than wrapping. See
[metrics-design.md](metrics-design.md).

## Performance tuning

**How do I tune throughput for a high-volume contract?**
See [performance-tuning.md](performance-tuning.md), particularly the
connection-pool and indexer-performance sections.

**Are there benchmarks I can run myself?**
Yes — see `benches/` and
[load-testing-guide.md](load-testing-guide.md) /
[load-testing-runbook.md](load-testing-runbook.md).

**How do I reduce query latency for large event ranges?**
See [query-plan-tuning.md](query-plan-tuning.md),
[table-partitioning.md](table-partitioning.md), and
[index-analysis.md](index-analysis.md).

**Does caching help, and where is it configurable?**
See [query-caching.md](query-caching.md) and
[serialization-caching.md](serialization-caching.md).

## Security

**How is data encrypted?**
See [encryption.md](encryption.md) and
[event-encryption.md](event-encryption.md) for at-rest/in-transit coverage,
and [key-rotation.md](key-rotation.md) for key lifecycle.

**Is Soroban Pulse GDPR-compliant for stored event data?**
See [gdpr-compliance.md](gdpr-compliance.md) for the right-to-erasure and
anonymization flows.

**How do I restrict which IPs can reach the API?**
See [ip-access-control.md](ip-access-control.md) and
[security-headers.md](security-headers.md) /
[owasp_security_headers.md](owasp_security_headers.md).

**Has this project had a penetration test / security review?**
See [penetration-testing.md](penetration-testing.md) and
[security-testing.md](security-testing.md) for the methodology; check the
repository's issue tracker for the latest review status, since point-in-time
results age quickly.

**Is SOC 2 documentation available?**
See [soc2-compliance.md](soc2-compliance.md).

## Video walkthroughs

The canonical index of recorded walkthroughs (getting started, running an
indexer locally, the API/SSE tour, production operations, and a
troubleshooting clinic) lives in
[video-tutorials.md](video-tutorials.md) — linked here rather than
duplicated so the FAQ doesn't go stale if episodes are added or re-recorded.

---

*Didn't find your question here? Check
[troubleshooting-guide.md](troubleshooting-guide.md) or open an issue.*
