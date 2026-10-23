# Summary

<!-- Canonical site map for the documentation site (Issue #1170).
     docs/site/sidebars.js builds every sidebar from this file, and
     `npm run check-nav` fails when a doc under docs/ is missing here.
     Each top-level `##` heading becomes a navbar section, `###` a category,
     and a nested list a category whose page is its parent link.

     Merged duplicates (client-side redirects in docusaurus.config.js):
       /docs/property-testing     → /docs/property-based-testing
       /docs/api-contract-testing → /docs/contract-testing -->

## Getting Started

- [Developer Onboarding](../onboarding.md)
- [Development Environment Setup](../development-setup.md)
- [Tutorial: Token Transfer Alert Bot](../tutorials/token-alert-bot.md)
- [Frequently Asked Questions](../FAQ.md)
- [Video Tutorials and Demonstrations](../video-tutorials.md)

## Guides

### Events and Indexing

- [SorobanPulse Architecture](../architecture.md)
- [System Design](../system-design.md)
- [Contract Event Schemas](../contract-event-schemas.md)
- [Event Normalization](../event-normalization.md)
- [Event Deduplication](../event-deduplication.md)
- [Event Deduplication Across Replicas](../event_deduplication_replicas.md)
- [Event Aggregation](../event-aggregation.md)
- [Event Tagging](../event-tagging.md)
- [Batch Event Operations](../batch-operations.md)
- [Bulk Event Export with Compression](../bulk_export.md)
- [Historical backfill from the Stellar ledger data lake (Galexie)](../backfill.md)
- [Scheduled Event Replay](../scheduled-replay.md)
- [Webhook Dead-Letter Replay](../dead-letter-replay.md)
- [Background Job Framework](../background-jobs.md)
- [Advisory Lock Behavior & Leader Election Race Fix](../advisory-lock-behavior.md)
- [Latency-Scored RPC Endpoint Selection](../rpc-endpoint-selection.md)
- [Stellar Asset Contract Detection](../sac-detection.md)
- [Event Filtering DSL](../filter-dsl.md)
- [Temporal Event Queries](../temporal-queries.md)
- [Time Series Analysis](../time-series-analysis.md)
- [Derived Metrics DSL](../derived-metrics-dsl.md)
- [Lua Event Transformation](../lua-transformation.md)
- [Contract Event Simulation](../contract-event-simulation.md)
- [Soroban Event Simulator](../event-simulator.md)
- [Cross-Chain Event Correlation](../cross-chain-events.md)
- [Cross-Chain Event Correlation](../cross_chain_correlation.md)
- [Data Quality Monitoring](../data-quality-monitoring.md)
- [Data Warehouse Export](../data-warehouse.md)
- [Event Explorer & Event Detail — Design Specification](../event-explorer-design.md)

### Streaming (SSE)

- [SSE Client Reconnection](../sse-reconnection.md)
- [SSE Connection Timeout Configuration for Reverse Proxies](../sse_reverse_proxy_configuration.md)
- [Streaming Response Optimization](../streaming-optimization.md)
- [Query Response Streaming](../STREAMING_RESPONSES.md)
- [Query Result Streaming](../query-streaming.md)
- [Real-Time Event Stream Statistics](../stream-statistics.md)
- [Event feeds](../event-feeds.md)

### Webhooks and Subscriptions

- [Webhook Signature Verification](../webhook-verification.md)
- [Webhook Signing](../webhook-signing.md)
- [Webhook Request Signing and Verification](../webhook_signing.md)
- [Webhook Endpoint Circuit Breaker](../webhook_circuit_breaker.md)
- [Webhook endpoint rate limits](../webhook-endpoint-rate-limits.md)
- [Webhook Request/Response Logging](../webhook-logging.md)
- [Subscription Best Practices](../subscription-best-practices.md)
- [Subscription Configuration Validator](../subscription-validator.md)
- [Webhook Retry Policies](../retry-policies.md)
- [Priority-Based Webhook Delivery](../priority-queueing.md)
- [Customer Delivery History API](../delivery-history-api.md)
- [Notification Delivery Receipts](../delivery-receipts.md)

### Notifications

- [Notification Delivery Architecture](../notification-architecture.md)
- [Notification Channels – Advanced Features](../notification-channels.md)
- [Notification Channel Interface](../notification-channel-interface.md)
- [Notification System — Advanced Features](../notification-features.md)
- [Per-Channel Notification Rate Limiting](../notification-rate-limiting.md)
- [Notification Batching](../notification-batching.md)
- [Notification Deduplication](../notification-deduplication.md)
- [Content-Based Notification Filtering](../notification-content-filter.md)
- [Notification Delivery Receipts](../notification-delivery-receipts.md)
- [Email Notifications](../email-notifications.md)
- [Email Notifications - Quick Start](../email-quick-start.md)
- [Incident Correlation Engine](../incident-correlation.md)

### Dashboard and Tools

- [Web dashboard](../dashboard.md)
- [Web Operational Dashboard](../web-dashboard.md)
- [App Builder](../app-builder.md)
- [spulse CLI Usage](../cli-usage.md)
- [Soroban Pulse VSCode Extension](../vscode-extension.md)
- [Log Analysis Tool](../log-analysis-tool.md)
- [GET /v1/meta](../meta-capabilities.md)

### Advanced

- [Multi-Tenant Access Isolation](../multi-tenancy.md)
- [SaaS Platform & Multi-Tenant Hosting](../SAAS_PLATFORM.md)
- [AI/ML Integration & Intelligence Features](../ML_INTEGRATION.md)
- [ML Model Serving](../ml-model-serving.md)
- [Quantum-Ready Cryptography](../quantum-cryptography.md)
- [Edge Computing & CDN Integration Guide](../edge-computing-guide.md)

### Migration Guides

- [Migration Guides](../migration-guides/README.md)
  - [Migrating from Stellar Horizon](../migration-guides/from-stellar-horizon.md)
  - [Migrating from other event indexers](../migration-guides/from-other-indexers.md)
  - [Data migration procedures](../migration-guides/data-migration-procedures.md)
  - [Subscription mapping](../migration-guides/subscription-mapping.md)
  - [Rollback procedures](../migration-guides/rollback-procedures.md)
  - [Migration validation](../migration-guides/validation.md)

## API

- [API Guide](../api-guide.md)
- [API Usage Guide](../api-usage.md)
- [API Cookbook](../api-cookbook.md)
- [API Versioning Policy](../api-versioning.md)
- [API Deprecation Policy and Timeline](../api-deprecation.md)
- [API Changelog Process](../api-changelog.md)
- [API Response Time SLA](../api-sla.md)
- [Local API Sandbox](../api-sandbox.md)
- [Postman Collection Generator](../postman.md)

### Querying

- [Cursor (Keyset) Pagination](../cursor-pagination.md)
- [Relay Cursor Pagination](../relay-pagination.md)
- [HTTP Caching](../http-caching.md)
- [HTTP Response Compression](../compression-optimization.md)
- [Idempotency Keys / Request Deduplication](../idempotency.md)
- [Idempotency-Key Support](../idempotency-keys.md)

### Auth, Limits and Errors

- [Scoped API Keys](../scoped-api-keys.md)
- [Rate Limiting Per API Key](../rate_limiting.md)
- [Advanced per-API-key rate limiting](../rate-limiting-advanced.md)
- [Error Handling Documentation](../error-handling.md)
- [RFC 9457 Problem Details](../problem-json-errors.md)
- [Correlation IDs](../correlation-ids.md)

### GraphQL

- [GraphQL API Documentation](../graphql_api.md)
- [GraphQL Subscriptions for Real-Time Events](../graphql_subscriptions.md)
- [GraphQL Safety Limits](../graphql-safety.md)

### OpenAPI and SDKs

- [OpenAPI 3.1 Upgrade](../openapi-31-upgrade.md)
- [OpenAPI Client Library Code Generation](../openapi-codegen.md)
- [Subscription Code Generator](../codegen.md)
- [SDK Integration Guide](../sdk-integration-guide.md)
- [SDK Development Guide](../sdk-development.md)
- [Client Libraries](../client-libraries.md)
- [JavaScript / TypeScript SDK](../js-sdk.md)
- [Python SDK](../python-sdk.md)

## Operations

### Configuration

- [Configuration Management](../configuration-management.md)
- [Cargo Feature Matrix](../features.md)
- [Feature Flags](../feature-flags.md)
- [Feature Flag Rollback Automation](../feature-flag-rollback.md)
- [Secret Management and Secrets Scanning](../secret-management.md)
- [API key rotation](../key-rotation.md)

### Deployment

- [Deployment Guide](../deployment.md)
- [Deployment Guide: Common Platforms](../deployment-platforms.md)
- [Kubernetes Probes](../kubernetes-probes.md)
- [Helm Migration Job](../helm-migration-job.md)
- [Terraform Infrastructure Guide](../terraform.md)
- [Infrastructure as Code Testing](../iac-testing.md)
- [GitOps for Infrastructure Management](../gitops.md)
- [Multi-Region Deployment](../multi-region.md)
- [Multi-Deployment Architecture Guide](../multi-deployment-architecture.md)
- [Service Mesh Integration Guide](../service-mesh-guide.md)
- [HTTP/3 Support](../http3-support.md)
- [Deployment Runbooks](../deployment-runbooks/README.md)
  - [AWS Deployment Runbook (EC2 + RDS + ALB)](../deployment-runbooks/aws.md)
  - [GCP Deployment Runbook (Compute Engine + Cloud SQL + Load Balancing)](../deployment-runbooks/gcp.md)
  - [Azure Deployment Runbook (VM + Azure Database for PostgreSQL + Application Gateway)](../deployment-runbooks/azure.md)
  - [Self-Hosted Deployment Runbook](../deployment-runbooks/self-hosted.md)
  - [Runbook Template](../deployment-runbooks/template.md)
  - [Testing the Deployment Runbooks](../deployment-runbooks/testing-framework.md)

### Observability

- [Real-time Alerting Configuration](../alerting.md)
- [Distributed Tracing Configuration](../tracing.md)
- [Structured Logging Convention](../logging.md)
- [Log Aggregation Setup](../log-aggregation.md)
- [Metrics Counter Design](../metrics-design.md)
- [SLI / SLO Dashboard](../sli-slo.md)
- [Prometheus Remote Write Integration](../prometheus-remote-write.md)
- [Replica Sync Monitoring](../replica-monitoring.md)

### Database

- [Connection Pool Configuration & Optimization](../connection-pool.md)
- [Connection Pool Optimization](../pool-optimization.md)
- [Table Partitioning Strategy](../table-partitioning.md)
- [Index Usage Analysis and Recommendations](../index-analysis.md)
- [Index Maintenance Guide](../index-maintenance.md)
- [Query Result Caching & Cache Invalidation](../query-caching.md)
- [PostgreSQL Query Plan Caching](../QUERY_PLAN_CACHING.md)
- [Query Plan Caching and Tuning](../query-plan-tuning.md)
- [Query Profiler](../query-profiler.md)
- [Database Statistics Auto-Analysis Configuration](../statistics-auto-analysis.md)
- [Database Configuration Tuning](../database-configuration-tuning.md)
- [Backup Verification](../backup-verification.md)
- [Data Retention Policy](../data-retention.md)
- [Multi-Tier Data Retention](../data-retention-tiers.md)
- [Serialization Caching](../serialization-caching.md)
- [JSON Serialization Optimization](../SERIALIZATION_OPTIMIZATION.md)
- [Bloom Filter Deduplication Memory Management](../dedup-memory-management.md)

### Security and Compliance

- [SorobanPulse Threat Model](../security/threat-model.md)
- [Security Headers](../security-headers.md)
- [OWASP API Security Headers](../owasp_security_headers.md)
- [IP allow/deny list enforcement](../ip-access-control.md)
- [Event Encryption at Rest](../encryption.md)
- [Event Encryption at Rest](../event-encryption.md)
- [Zero-trust security model](../zero-trust.md)
- [Container Image Scanning and Signing](../image-security.md)
- [Penetration testing and security assessment](../penetration-testing.md)
- [SOC 2 Compliance Checklist](../soc2-compliance.md)
- [GDPR Data Handling Compliance](../gdpr-compliance.md)
- [Audit Trail for Compliance](../audit-trail.md)
- [Audit Logging for Sensitive Operations](../audit_logging.md)

### Reliability and Scale

- [Capacity Planning Automation](../capacity-planning.md)
- [Performance Tuning Guide](../performance-tuning.md)
- [Load Testing Guide](../load-testing-guide.md)
- [Load Testing Runbook](../load-testing-runbook.md)
- [Disaster Recovery Runbook](../disaster-recovery.md)
- [Graceful Degradation](../graceful-degradation.md)

### Runbooks

- [Operator Runbook](../runbooks/operator-runbook.md)
- [Indexer Lag Runbook](../runbooks/indexer-lag.md)
- [Database Connection Pool Exhaustion Runbook](../runbooks/db-pool-exhaustion.md)
- [RPC Errors Runbook](../runbooks/rpc-errors.md)
- [Webhook Failures Runbook](../runbooks/webhook-failures.md)
- [Notification Delivery Troubleshooting](../runbooks/notifications.md)
- [SSE Connections Runbook](../runbooks/sse-connections.md)
- [Feature Flag Rollback Troubleshooting](../runbooks/feature-flag-rollback.md)
- [Runbook: Query Plan Cache](../runbooks/query-plan-cache.md)
- [Runbook: Schema Consolidation](../runbooks/schema-consolidation.md)
- [Runbook: RPC Retention Gap](../runbooks/rpc-retention-gap.md)
- [Emergency Runbooks](../emergency-runbooks/README.md)
  - [Runbook: Security Breach Response](../emergency-runbooks/security-breach-response.md)
  - [Runbook: Database Corruption Recovery](../emergency-runbooks/database-corruption-recovery.md)
  - [Runbook: Complete Data Loss Recovery](../emergency-runbooks/data-loss-recovery.md)
  - [Runbook: Widespread Service Failure Recovery](../emergency-runbooks/service-failure-recovery.md)

### Troubleshooting

- [Troubleshooting Decision Tree](../troubleshooting-guide.md)
- [Troubleshooting and Debugging Guide](../troubleshooting.md)

## Integrations

### Chat and Incident Tools

- [Slack integration](../slack-integration.md)
- [Discord integration](../discord-integration.md)
- [Microsoft Teams integration](../teams-integration.md)
- [PagerDuty Integration](../pagerduty-integration.md)
- [Platform Integration Guide](../guides/integration-guide.md)

### Event Streaming

- [Kafka Event Publishing](../kafka-event-publishing.md)
- [AWS SQS integration](../sqs-integration.md)
- [Azure Event Hubs integration](../event-hubs-integration.md)
- [AWS EventBridge Integration](../eventbridge-integration.md)

## Contributing

- [Module Map](../module-map.md)
- [Archive: Implementation Reports](../archive/README.md)

### Architecture Decisions

- [Architecture Decision Records](../adr/README.md)
  - [ADR Template](../adr/0000-template.md)
  - [0001 — Use a numbered Markdown ADR system](../adr/0001-adr-system.md)
  - [0002 — Single-writer indexing with monitored read replicas](../adr/0002-multi-replica-indexing.md)
  - [0003 — Layered retry, backoff, and circuit breaking for webhook delivery](../adr/0003-webhook-retry-strategy.md)
  - [0004 — Optional gzip compression for stored event data](../adr/0004-event-compression.md)
  - [ADR review guidelines](../adr/review-guidelines.md)

### Testing

- [E2E Testing](../e2e-testing.md)
- [Property-Based Testing Guide](../property-based-testing.md)
- [API Contract Testing with Pact](../contract-testing.md)
- [API Compatibility Testing](../api-compatibility-testing.md)
- [API Compliance Testing](../api-compliance-testing.md)
- [Mutation Testing Guide](../mutation-testing.md)
- [Mutation Testing Guide](../mutation-testing-guide.md)
- [Chaos Engineering Tests](../chaos-testing.md)
- [Security Testing](../security-testing.md)
- [Performance Regression Testing](../performance-regression-testing.md)
- [Integration Test Parallelization](../test-parallelization.md)

### Design

- [SorobanPulse design system](../design/design-system.md)
- [Data-Visualisation Guidelines](../design/dataviz.md)
- [SorobanPulse Design Tokens](../design-tokens.md)
- [SorobanPulse Diagram Style Guide](../diagram-style-guide.md)
- [Repository assets](../assets/README.md)
- [SorobanPulse brand assets](../assets/brand/README.md)

### Engineering Plans

- [Aggregation Merge Plan](../aggregation-merge-plan.md)
- [Cargo Workspace Plan](../cargo-workspace-plan.md)
- [Connection Pool Consolidation](../connection-pool-consolidation.md)
- [Feature Gate Plan](../feature-gate-plan.md)
- [Handler Split Plan](../handler-split-plan.md)
- [Module Pruning Audit](../module-pruning-audit.md)
- [Router Consolidation](../router-consolidation.md)
- [Schema Audit — SorobanPulse](../schema-audit.md)
- [Typed Error Migration](../typed-error-migration.md)
- [Unified Query Builder Pattern](../query-builder-pattern.md)

### Design Specs / RFCs

- [Design Specs / RFCs](../rfcs/README.md)
  - [Webhook delivery improvements: spec](../rfcs/webhook-delivery-improvements/spec.md)
  - [Webhook delivery improvements: executive summary](../rfcs/webhook-delivery-improvements/executive-summary.md)
  - [Webhook delivery improvements: quick start](../rfcs/webhook-delivery-improvements/quick-start.md)
  - [Webhook delivery improvements: testing and validation](../rfcs/webhook-delivery-improvements/testing-and-validation.md)

### Pull Request Notes

- [Chideraisiguzor 1137 1140](../pr/chideraisiguzor-1137-1140.md)
- [Misrasamuelisiguzor Oss 1121 1124](../pr/misrasamuelisiguzor-oss-1121-1124.md)
- [Samuelisi 1133 1136](../pr/samuelisi-1133-1136.md)
- [Spotkorner Dot 1125 1128](../pr/spotkorner-dot-1125-1128.md)
- [Techisigu 1129 1132](../pr/techisigu-1129-1132.md)

## Reference

- [Configuration Reference](../configuration.md)
- [Operational Metrics Reference](../metrics-reference.md)
- [Database Schema](../schema.md)
- [Stellar & Soroban Glossary](../glossary.md)
