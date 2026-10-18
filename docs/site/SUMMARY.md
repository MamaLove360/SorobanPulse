# SorobanPulse Docs — Information Architecture

> **This file is the canonical site map.** Every existing document is mapped to one of five top-level sections. Sidebar configuration files in `docs/site/sidebars/` are generated from this structure.
>
> Sections: [Getting Started](#1-getting-started) · [Guides](#2-guides) · [API Reference](#3-api-reference) · [Operations](#4-operations) · [Contributing](#5-contributing)

---

## 1. Getting Started

*Goal: a developer can be running the service and querying events within 30 minutes.*

| Page | File | Notes |
|---|---|---|
| **Welcome / Overview** | *(landing page `index.tsx`)* | Hero + feature grid |
| **Onboarding** | `docs/onboarding.md` | Day-1 checklist, common first-build fixes |
| **Development Environment Setup** | `docs/development-setup.md` | OS-specific, IDE, pre-commit, profiling |
| **Quick Start Guide** | `docs/onboarding.md` | Docker Compose fast path |
| **FAQ** | `docs/FAQ.md` | Most-asked questions |
| **Changelog** | `CHANGELOG.md` | Release history |

---

## 2. Guides

Concept-level and how-to documentation grouped by capability area.

### 2.1 API Usage

| Page | File |
|---|---|
| API Guide | `docs/api-guide.md` |
| API Usage (examples) | `docs/api-usage.md` |
| API Versioning | `docs/api-versioning.md` |
| API Deprecation | `docs/api-deprecation.md` |
| API Changelog | `docs/api-changelog.md` |
| Rate Limiting | `docs/rate_limiting.md` |
| Rate Limiting (advanced) | `docs/rate-limiting-advanced.md` |
| Cursor Pagination | `docs/cursor-pagination.md` |
| Idempotency | `docs/idempotency.md` |
| HTTP Caching | `docs/http-caching.md` |
| Compression Optimisation | `docs/compression-optimization.md` |
| Postman Collection | `docs/postman.md` |
| API Sandbox | `docs/api-sandbox.md` |
| API Cookbook | `docs/api-cookbook.md` |

### 2.2 Event Indexing

| Page | File |
|---|---|
| Architecture Overview | `docs/architecture.md` |
| Contract Event Schemas | `docs/contract-event-schemas.md` |
| Event Normalization | `docs/event-normalization.md` |
| Event Deduplication | `docs/event-deduplication.md` |
| Event Deduplication (replicas) | `docs/event_deduplication_replicas.md` |
| Event Aggregation | `docs/event-aggregation.md` |
| Event Tagging | `docs/event-tagging.md` |
| Event Encryption | `docs/event-encryption.md` |
| Batch Operations | `docs/batch-operations.md` |
| Bulk Export | `docs/bulk_export.md` |
| Advisory Lock Behaviour | `docs/advisory-lock-behavior.md` |
| Filter DSL | `docs/filter-dsl.md` |
| Temporal Queries | `docs/temporal-queries.md` |
| Contract Event Simulation | `docs/contract-event-simulation.md` |
| Event Simulator | `docs/event-simulator.md` |
| Cross-chain Events | `docs/cross-chain-events.md` |
| Cross-chain Correlation | `docs/cross_chain_correlation.md` |

### 2.3 Streaming (SSE)

| Page | File |
|---|---|
| SSE Reconnection & Last-Event-ID | `docs/sse-reconnection.md` |
| SSE Reverse Proxy Config | `docs/sse_reverse_proxy_configuration.md` |
| Streaming Optimisation | `docs/streaming-optimization.md` |
| Streaming Responses | `docs/STREAMING_RESPONSES.md` |
| Query Streaming | `docs/query-streaming.md` |
| Stream Statistics | `docs/stream-statistics.md` |
| Event Feeds | `docs/event-feeds.md` |

### 2.4 Webhooks

| Page | File |
|---|---|
| Webhook Verification | `docs/webhook-verification.md` |
| Webhook Signing | `docs/webhook-signing.md` (+ `docs/webhook_signing.md`) |
| Webhook Circuit Breaker | `docs/webhook_circuit_breaker.md` |
| Webhook Endpoint Rate Limits | `docs/webhook-endpoint-rate-limits.md` |
| Webhook Logging | `docs/webhook-logging.md` |
| Subscription Best Practices | `docs/subscription-best-practices.md` |
| Subscription Validator | `docs/subscription-validator.md` |
| Retry Policies | `docs/retry-policies.md` |
| Priority Queueing | `docs/priority-queueing.md` |

### 2.5 Notifications

| Page | File |
|---|---|
| Notification Architecture | `docs/notification-architecture.md` |
| Notification Channels | `docs/notification-channels.md` |
| Notification Features | `docs/notification-features.md` |
| Notification Rate Limiting | `docs/notification-rate-limiting.md` |
| Notification Batching | `docs/notification-batching.md` |
| Notification Deduplication | `docs/notification-deduplication.md` |
| Notification Content Filter | `docs/notification-content-filter.md` |
| Notification Delivery Receipts | `docs/notification-delivery-receipts.md` |
| Delivery Receipts | `docs/delivery-receipts.md` |
| Email Notifications | `docs/email-notifications.md` |
| Email Quick Start | `docs/email-quick-start.md` |
| Slack Integration | `docs/slack-integration.md` |
| Discord Integration | `docs/discord-integration.md` |
| Teams Integration | `docs/teams-integration.md` |
| PagerDuty Integration | `docs/pagerduty-integration.md` |

### 2.6 SDKs & Client Libraries

| Page | File |
|---|---|
| SDK Integration Guide | `docs/sdk-integration-guide.md` |
| SDK Development Guide | `docs/sdk-development.md` |
| Client Libraries Overview | `docs/client-libraries.md` |
| JavaScript SDK | `docs/js-sdk.md` |
| Python SDK | `docs/python-sdk.md` |
| OpenAPI Code Generation | `docs/openapi-codegen.md` |
| Code Generation | `docs/codegen.md` |
| CLI Usage | `docs/cli-usage.md` |
| VS Code Extension | `docs/vscode-extension.md` |

### 2.7 Advanced Features

| Page | File |
|---|---|
| Schema Versioning | `docs/schema.md` |
| Database Schema | `docs/schema.md` |
| Filter DSL (advanced) | `docs/filter-dsl.md` |
| Cursor Pagination | `docs/cursor-pagination.md` |
| Saved Queries | `docs/query-builder-pattern.md` |
| Temporal Queries | `docs/temporal-queries.md` |
| GraphQL API | `docs/graphql_api.md` |
| GraphQL Subscriptions | `docs/graphql_subscriptions.md` |
| Multi-tenancy | `docs/multi-tenancy.md` |
| Feature Flags | `docs/feature-flags.md` |
| Feature Flag Rollback | `docs/feature-flag-rollback.md` |
| Lua Transformation | `docs/lua-transformation.md` |
| Encryption | `docs/encryption.md` |
| Event Replay | *(via `docs/scheduled-replay.md`)* |
| Scheduled Replay | `docs/scheduled-replay.md` |
| Anomaly Detection | *(referenced in metrics docs)* |
| ML Integration | `docs/ML_INTEGRATION.md` |
| SaaS Platform | `docs/SAAS_PLATFORM.md` |

### 2.8 Migration Guides

| Page | File |
|---|---|
| Migration Guide Index | `docs/migration-guides/README.md` |
| From Stellar Horizon | `docs/migration-guides/from-stellar-horizon.md` |
| From Other Indexers | `docs/migration-guides/from-other-indexers.md` |
| Data Migration Procedures | `docs/migration-guides/data-migration-procedures.md` |
| Subscription Mapping | `docs/migration-guides/subscription-mapping.md` |
| Rollback Procedures | `docs/migration-guides/rollback-procedures.md` |
| Validation | `docs/migration-guides/validation.md` |

---

## 3. API Reference

*Machine-readable and interactive documentation for the REST API.*

| Page | Source |
|---|---|
| **Interactive Docs (Swagger UI)** | Served at `/docs` by the Rust service |
| **OpenAPI 3.0 JSON** | `/openapi.json` (generated by `cargo run --bin gen_openapi`) |
| API SLA | `docs/api-sla.md` |
| API Compliance Testing | `docs/api-compliance-testing.md` |
| Contract Testing | `docs/contract-testing.md` |
| API Contract Testing | `docs/api-contract-testing.md` |
| API Compatibility Testing | `docs/api-compatibility-testing.md` |
| Correlation IDs | `docs/correlation-ids.md` |
| Idempotency | `docs/idempotency.md` |
| Error Handling | `docs/error-handling.md` |

---

## 4. Operations

*Running, monitoring, and scaling SorobanPulse in production.*

### 4.1 Deployment

| Page | File |
|---|---|
| Deployment Overview | `docs/deployment.md` |
| Deployment Platforms | `docs/deployment-platforms.md` |
| Docker / Docker Compose | *(in `docs/deployment.md`)* |
| Kubernetes Probes | `docs/kubernetes-probes.md` |
| Helm Chart | `helm/soroban-pulse/README.md` |
| Terraform | `docs/terraform.md` |
| Multi-region | `docs/multi-region.md` |
| Multi-deployment Architecture | `docs/multi-deployment-architecture.md` |
| GitOps | `docs/gitops.md` |
| Edge Computing | `docs/edge-computing-guide.md` |
| Service Mesh | `docs/service-mesh-guide.md` |
| HTTP/3 Support | `docs/http3-support.md` |
| **Deployment Runbooks** | `docs/deployment-runbooks/` |
| ↳ AWS | `docs/deployment-runbooks/aws.md` |
| ↳ GCP | `docs/deployment-runbooks/gcp.md` |
| ↳ Azure | `docs/deployment-runbooks/azure.md` |
| ↳ Self-hosted | `docs/deployment-runbooks/self-hosted.md` |

### 4.2 Observability

| Page | File |
|---|---|
| Architecture | `docs/architecture.md` |
| Metrics Reference | `docs/metrics-reference.md` |
| Metrics Design | `docs/metrics-design.md` |
| Alerting | `docs/alerting.md` |
| Tracing | `docs/tracing.md` |
| Logging | `docs/logging.md` |
| Log Aggregation | `docs/log-aggregation.md` |
| SLI / SLO | `docs/sli-slo.md` |
| Grafana Dashboard | `docs/grafana-dashboard.json` |
| Prometheus Alerts | `docs/alerts.yml` |
| Prometheus Remote Write | `docs/prometheus-remote-write.md` |
| Replica Monitoring | `docs/replica-monitoring.md` |
| Correlation IDs | `docs/correlation-ids.md` |
| Structured Logging | *(in `docs/logging.md`)* |

### 4.3 Database

| Page | File |
|---|---|
| Schema | `docs/schema.md` |
| Connection Pool | `docs/connection-pool.md` |
| Pool Optimisation | `docs/pool-optimization.md` |
| Table Partitioning | `docs/table-partitioning.md` |
| Index Analysis | `docs/index-analysis.md` |
| Index Maintenance | `docs/index-maintenance.md` |
| Query Caching | `docs/query-caching.md` |
| Query Plan Tuning | `docs/query-plan-tuning.md` |
| Query Profiler | `docs/query-profiler.md` |
| Statistics Auto-analysis | `docs/statistics-auto-analysis.md` |
| Database Configuration Tuning | `docs/database-configuration-tuning.md` |
| Backup Verification | `docs/backup-verification.md` |
| Data Retention | `docs/data-retention.md` |
| Data Retention Tiers | `docs/data-retention-tiers.md` |
| Serialization & Caching | `docs/serialization-caching.md` |

### 4.4 Security

| Page | File |
|---|---|
| Security Headers | `docs/security-headers.md` |
| OWASP Security Headers | `docs/owasp_security_headers.md` |
| Security Testing | `docs/security-testing.md` |
| IP Access Control | `docs/ip-access-control.md` |
| Key Rotation | `docs/key-rotation.md` |
| Secret Management | `docs/secret-management.md` |
| Encryption | `docs/encryption.md` |
| Zero Trust | `docs/zero-trust.md` |
| Image Security | `docs/image-security.md` |
| Penetration Testing | `docs/penetration-testing.md` |
| SOC 2 Compliance | `docs/soc2-compliance.md` |
| GDPR Compliance | `docs/gdpr-compliance.md` |
| Audit Trail | `docs/audit-trail.md` |
| Audit Logging | `docs/audit_logging.md` |

### 4.5 Reliability & Scale

| Page | File |
|---|---|
| Capacity Planning | `docs/capacity-planning.md` |
| Performance Tuning | `docs/performance-tuning.md` |
| Load Testing Guide | `docs/load-testing-guide.md` |
| Disaster Recovery | `docs/disaster-recovery.md` |
| Graceful Degradation | `docs/graceful-degradation.md` |
| Chaos Testing | `docs/chaos-testing.md` |
| Dedup Memory Management | `docs/dedup-memory-management.md` |

### 4.6 Runbooks

| Page | File |
|---|---|
| Operator Runbook | `docs/runbooks/operator-runbook.md` |
| Indexer Lag | `docs/runbooks/indexer-lag.md` |
| DB Pool Exhaustion | `docs/runbooks/db-pool-exhaustion.md` |
| RPC Errors | `docs/runbooks/rpc-errors.md` |
| Webhook Failures | `docs/runbooks/webhook-failures.md` |
| Notifications | `docs/runbooks/notifications.md` |
| SSE Connections | `docs/runbooks/sse-connections.md` |
| Feature Flag Rollback | `docs/runbooks/feature-flag-rollback.md` |
| Query Plan Cache | `docs/runbooks/query-plan-cache.md` |
| Schema Consolidation | `docs/runbooks/schema-consolidation.md` |
| **Emergency Runbooks** | `docs/emergency-runbooks/` |
| ↳ Security Breach | `docs/emergency-runbooks/security-breach-response.md` |
| ↳ DB Corruption | `docs/emergency-runbooks/database-corruption-recovery.md` |
| ↳ Data Loss | `docs/emergency-runbooks/data-loss-recovery.md` |
| ↳ Service Failure | `docs/emergency-runbooks/service-failure-recovery.md` |

### 4.7 Troubleshooting

| Page | File |
|---|---|
| Troubleshooting Guide | `docs/troubleshooting-guide.md` |
| Troubleshooting Reference | `docs/troubleshooting.md` |

---

## 5. Contributing

*For contributors to the SorobanPulse codebase and documentation.*

| Page | File |
|---|---|
| **CONTRIBUTING.md** | `CONTRIBUTING.md` |
| Development Setup | `docs/development-setup.md` |
| Architecture Decision Records | `docs/adr/README.md` |
| ↳ ADR-0000 Template | `docs/adr/0000-template.md` |
| ↳ ADR-0001 ADR system | `docs/adr/0001-adr-system.md` |
| ↳ ADR-0002 Multi-replica indexing | `docs/adr/0002-multi-replica-indexing.md` |
| ↳ ADR-0003 Webhook retry strategy | `docs/adr/0003-webhook-retry-strategy.md` |
| ↳ ADR-0004 Event compression | `docs/adr/0004-event-compression.md` |
| ADR Review Guidelines | `docs/adr/review-guidelines.md` |
| Property-based Testing | `docs/property-testing.md` |
| Mutation Testing | `docs/mutation-testing.md` |
| Security Testing | `docs/security-testing.md` |
| E2E Testing | `docs/e2e-testing.md` |
| Contract Testing | `docs/contract-testing.md` |
| Video Tutorials | `docs/video-tutorials.md` |
| **Design Tokens** | `docs/design-tokens.md` |
| **Diagram Style Guide** | `docs/diagram-style-guide.md` |
