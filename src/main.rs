// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use soroban_pulse::{audit_logging, bloom_filter, compliance_report, compression_config, config, content_filter, costs, cross_chain_correlation, cursor_expiry_handler, db, db_pool, derived_metrics, incident_correlation, advisory_lock, query_streaming, serialization_cache, streaming_response, dedup, distributed_tracing, email, encryption, error, event_hubs, graceful_shutdown, handlers, idempotency, log_analysis_tool, index_monitor, indexer, kafka, kinesis, metrics, prometheus_remote_write, eventbridge, middleware, models, normalizer, notification_dedup, warehouse, pruner, pubsub, queue_publisher, rate_limiter, reencrypt, resource_metrics, routes, rpc_client, schema_validator, sqs, stats_refresh, subscriptions, webhook, webhook_verification, notification_rate_limit, notification_formatter, pagerduty, github, discord, slack, teams, telegram, notification_channel, notification_delivery, integration_handlers, retry_policy, sms, aggregation, saved_queries, abi, oncall, xdr_validation, replica_monitor, feature_flags, event_dedup_replicas, bulk_export, sse_ring_buffer, query_cache, query_plan_cache, query_optimizer, partition_manager, query_builder, notification_admin, financial_accuracy, webhook_template, event_aggregation, anomaly_detection, push_notification, slo_tracker, anonymization, event_compression, health_check, ledger_hashes, networks, zero_trust, push_preload, statistics_management, cloud_provider, cloud_replication, deployment_orchestrator};
#[cfg(feature = "lua")]
use soroban_pulse::lua_transform;
#[cfg(feature = "parquet")]
use soroban_pulse::parquet_export;
#[cfg(feature = "graphql")]
use soroban_pulse::graphql;

#[cfg(feature = "graphql")]
use soroban_pulse::graphql_subscriptions;
#[cfg(feature = "archive")]
use soroban_pulse::archiver;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let log_format = std::env::var("RUST_LOG_FORMAT").unwrap_or_else(|_| "text".to_string());

    let registry = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()));

    #[cfg(feature = "otel")]
    let registry = {
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4317".to_string());

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(otel_endpoint),
            )
            .install_simple()
            .context("Failed to initialize OpenTelemetry tracer")?;

        registry.with(OpenTelemetryLayer::new(tracer))
    };

    if log_format == "json" {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }

    // Initialize metrics exporter
    let prometheus_handle = metrics::init_metrics();

    // Issue #1248: every background task is registered with the scheduler so
    // it is listed by `GET /v1/admin/jobs` and aborted on shutdown.
    let jobs = soroban_pulse::job_scheduler::global().clone();

    #[cfg(target_os = "linux")]
    jobs.adopt("memory-collector", metrics::spawn_memory_collector());

    // Issue #1247: parse + validate the configuration exactly once. Every
    // parse and semantic error is reported together in `ConfigError`.
    let config = config::ConfigBuilder::from_env().build().inspect_err(|e| {
        error!("{e}");
        eprintln!("Startup aborted due to configuration errors. Fix the above and restart.");
    })?;

    info!(
        rpc_url = %config.stellar_rpc_url,
        rpc_fallback_urls = ?config.stellar_rpc_fallback_urls,
        rpc_headers = ?config.safe_rpc_headers(),
        start_ledger = config.start_ledger,
        port = config.port,
        db_url = %config.safe_db_url(),
        db_max_connections = config.db_max_connections,
        db_min_connections = config.db_min_connections,
        indexer_event_types = ?config.indexer_event_types,
        "Resolved configuration",
    );

    // Issue #1244: the single pool initialisation path — connects with
    // retries and starts the pool monitor + adaptive tuner exactly once.
    let pool_settings = db_pool::DbPoolSettings::from_config(&config);
    let pool = match db_pool::DbPool::init(&config.database_url, &pool_settings, 3, Duration::from_secs(2)).await {
        Ok(db) => db.into_pool(),
        Err(e) => {
            tracing::error!(error = %e, "Failed to connect to database after 3 attempts");
            return Err(anyhow::anyhow!("Database connection failed after 3 attempts"));
        }
    };

    // Issue #1147: MIGRATE_ONLY=true (or the `migrate` subcommand) runs
    // migrations then exits with code 0 on success, 1 on failure.
    // This is used by the Helm pre-install/pre-upgrade Job so that schema
    // changes are applied before any application pods start.
    let migrate_only = std::env::var("MIGRATE_ONLY")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false)
        || std::env::args().nth(1).as_deref() == Some("migrate");

    if migrate_only {
        info!("MIGRATE_ONLY mode: running migrations and exiting");
        match db::run_migrations(&pool).await {
            Ok(count) => {
                info!(count = count, "Migration complete — exiting");
                std::process::exit(0);
            }
            Err(e) => {
                tracing::error!(error = %e, "Migration failed — exiting with code 1");
                std::process::exit(1);
            }
        }
    }

    // Issue #1147: skip startup migrations when the Helm migration Job is
    // responsible for running them (RUN_MIGRATIONS_ON_STARTUP=false).
    if config.run_migrations_on_startup {
        if let Err(e) = db::run_migrations(&pool).await {
            tracing::error!(error = %e, "Startup migrations failed");
            return Err(e.into());
        }
    } else {
        info!("RUN_MIGRATIONS_ON_STARTUP=false — skipping startup migrations (expected to be run by the Helm migration Job)");
    }

    // Issue #1252: `--backfill-from <ledger> --backfill-to <ledger>` replays
    // historical ledgers from the Galexie data lake through the indexer's
    // insert path, then exits. Safe to run alongside a live indexer.
    match soroban_pulse::backfill::parse_cli_range(std::env::args().skip(1)) {
        Ok(None) => {}
        Ok(Some((from, to))) => {
            let cfg = match soroban_pulse::backfill::BackfillConfig::from_env(from, to, &config.chain_id) {
                Ok(cfg) => cfg,
                Err(e) => {
                    error!(error = %e, "invalid backfill configuration");
                    std::process::exit(2);
                }
            };
            let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let sink = std::sync::Arc::new(soroban_pulse::indexer::Indexer::new(
                pool.clone(),
                config.clone(),
                shutdown_rx,
                soroban_pulse::indexer::SorobanRpcClient::new(&config),
            ));
            match soroban_pulse::backfill::run(pool.clone(), sink, cfg).await {
                Ok(summary) => {
                    info!(ledgers = summary.ledgers, events_inserted = summary.events_inserted, "Backfill complete — exiting");
                    std::process::exit(0);
                }
                Err(e) => {
                    error!(error = %e, "Backfill failed — re-run the same command to resume");
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            error!(error = %e, "invalid backfill arguments");
            std::process::exit(2);
        }
    }

    // Create read pool: use replica URL if configured, otherwise reuse primary pool.
    let read_pool = if let Some(ref replica_url) = config.database_replica_url {
        info!("DATABASE_REPLICA_URL set — HTTP handlers will use read replica");
        match db_pool::connect(replica_url, &pool_settings).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "Failed to connect to read replica, falling back to primary");
                pool.clone()
            }
        }
    } else {
        pool.clone()
    };

    info!("Migrations applied successfully");
    info!(environment = ?config.environment, "Running environment");

    // Warm the query plan cache with the five canonical query patterns so
    // the first real requests are never cold misses. (#802)
    {
        let plan_cache = query_plan_cache::QueryPlanCache::with_defaults();
        match plan_cache.warm_cache(&pool).await {
            Ok(n) => info!(warmed = n, "Query plan cache warmed at startup"),
            Err(e) => warn!(error = %e, "Query plan cache warming failed; continuing"),
        }
    }

    // Initialize schema validator and load schemas
    let schema_validator = Arc::new(schema_validator::SchemaValidator::new(pool.clone()));
    if let Err(e) = schema_validator.load_schemas().await {
        warn!(error = %e, "Failed to load schemas from database");
    } else {
        info!("Schema validator initialized");
    }

    // Create shared health state for indexer and HTTP handlers
    let health_state = Arc::new(config::HealthState::new(config.indexer_stall_timeout_secs));

    // Create shared indexer state for /status endpoint
    let indexer_state = Arc::new(config::IndexerState::new());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut shutdown_rx_axum = shutdown_rx.clone();

    // Broadcast channel for real-time SSE streaming (capacity 256 events)
    let (event_tx, _) = tokio::sync::broadcast::channel::<models::SorobanEvent>(256);

    // Issue #1264: Shared operational cost calculator. Wired into the indexer
    // (RPC + DB costs), webhook delivery and email paths so that every
    // component records its spend into a single in-memory store. Hourly
    // snapshots are persisted by the collector spawned further below.
    let cost_calculator = Arc::new(costs::calculator::CostCalculator::new(
        costs::calculator::CostRates::from_env(),
    ));

    // Issue #1266: In-memory idempotency key store with a 24-hour TTL. A
    // periodic sweeper removes expired entries so the map does not grow
    // unbounded on long-running pods.
    let idempotency_store = idempotency::shared_store();
    {
        let store = idempotency_store.clone();
        let mut idem_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let removed = store.remove_expired();
                        if removed > 0 {
                            tracing::debug!(removed = removed, "Pruned expired idempotency keys");
                        }
                    }
                    _ = idem_shutdown_rx.changed() => break,
                }
            }
        });
    }

    // Spawn webhook delivery task if WEBHOOK_URL is configured.
    if let Some(ref webhook_url) = config.webhook_url {
        let webhook_rx = event_tx.subscribe();
        let webhook_url = webhook_url.clone();
        let webhook_secret = config.webhook_secret.clone();
        let webhook_contract_filter = config.webhook_contract_filter.clone();
        let webhook_retry_policy = config.webhook_retry_policy.clone();
        let webhook_pool = pool.clone();
        let webhook_cost_calculator = cost_calculator.clone();
        // Per-channel rate limiter (Issue #476). `None` when no limit configured.
        let webhook_rate_limiter = notification_rate_limit::ChannelRateLimiter::from_config(
            &notification_rate_limit::RateLimitConfig::new(
                config.webhook_rate_limit_per_minute,
                config.webhook_rate_limit_per_hour,
            ),
        );
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("Failed to build webhook HTTP client")?;

        info!(
            url = %webhook_url,
            rate_limit_per_minute = ?config.webhook_rate_limit_per_minute,
            rate_limit_per_hour = ?config.webhook_rate_limit_per_hour,
            "Webhook delivery enabled"
        );

        jobs.spawn("webhook-delivery", async move {
            let mut rx = webhook_rx;
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        // Apply contract filter if configured
                        if !webhook_contract_filter.is_empty()
                            && !webhook_contract_filter.contains(&event.contract_id)
                        {
                            continue;
                        }
                        // Apply content filter if configured (Issue #477).
                        if let Some(ref cf) = webhook_content_filter {
                            if !cf.evaluate(&event.value) {
                                continue;
                            }
                        }
                        let client = http_client.clone();
                        let url = webhook_url.clone();
                        let secret = webhook_secret.clone();
                        let pool_ref = Some(pool.as_ref());
                        let cost_calc = webhook_cost_calculator.clone();
                        let priority = webhook::evaluate_priority(
                            &event,
                            config.notification_priority_rule_path.as_deref(),
                            config.notification_priority_rule_value.as_deref(),
                            config.notification_priority_rule_priority.as_deref(),
                            &config.notification_default_priority,
                        ).to_string();
                        tokio::spawn(async move {
                            let started = std::time::Instant::now();
                            webhook::deliver_with_retry_policy(
                                client,
                                url,
                                secret,
                                event,
                                pool_ref,
                                &config.webhook_retry_policy,
                                priority,
                            )
                            .await;
                            // Issue #1264: record the delivery cost (one unit per
                            // webhook HTTP call, priced by the configured rate).
                            cost_calc.record_notification(
                                costs::calculator::Component::Webhook,
                                1,
                                started.elapsed(),
                            );
                        });
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            skipped = n,
                            "Webhook subscriber lagged, some events skipped"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    // Spawn email notification task if EMAIL_SMTP_HOST is configured.
    if let Some(ref smtp_host) = config.email_smtp_host {
        if let Some(ref from) = config.email_from {
            if !config.email_to.is_empty() {
                let email_rx = event_tx.subscribe();
                // Base URL for unsubscribe links (Issue #483): honor the
                // configured public URL, otherwise fall back to localhost:<port>.
                let base_url = config
                    .email_public_base_url
                    .clone()
                    .unwrap_or_else(|| format!("http://localhost:{}", config.port));
                let mut notifier = email::EmailNotifier::new(
                    smtp_host.clone(),
                    config.email_smtp_port,
                    config.email_smtp_user.clone(),
                    config.email_smtp_password.clone(),
                    from.clone(),
                    config.email_to.clone(),
                    config.email_contract_filter.clone(),
                    config.email_retry_policy.clone(),
                    email::Schedule::parse(
                        &config.email_schedule,
                        config.email_daily_digest_hour,
                        config.email_cron.clone(),
                    ),
                    email::QuietHours::parse(
                        config.email_quiet_hours_start.as_deref(),
                        config.email_quiet_hours_end.as_deref(),
                    ),
                    config.email_language.clone(),
                    pool.clone(),
                    base_url,
                );
                // Issue #1264: attribute email delivery spend to the cost tracker.
                notifier.set_cost_calculator(cost_calculator.clone());

                info!(
                    smtp_host = %smtp_host,
                    recipients = config.email_to.len(),
                    schedule = %config.email_schedule,
                    language = %config.email_language,
                    "Email notifications enabled"
                );

                // SPF deliverability check (Issue #486): warn at startup if the
                // sending domain has no SPF record. Best-effort and non-blocking.
                let spf_from = from.clone();
                jobs.spawn_oneshot("email-spf-check", async move {
                    email::verify_spf_record(&spf_from).await;
                });

                jobs.adopt("email-notifier", notifier.spawn(email_rx));
            } else {
                warn!(
                    "EMAIL_SMTP_HOST is set but EMAIL_TO is empty — email notifications disabled"
                );
            }
        } else {
            warn!("EMAIL_SMTP_HOST is set but EMAIL_FROM is not — email notifications disabled");
        }
    }

    // Issue #1245: environment-configured notification channels are
    // registered with a single dispatcher that consumes the event stream.
    // New channels only need a `NotificationChannel` impl and a `register`.
    let mut notification_dispatcher = notification_channel::NotificationDispatcher::new();

    // SMS notifications if Twilio is configured (Issue #473)
    if let (Some(account_sid), Some(auth_token), Some(from_number)) = (
        &config.twilio_account_sid,
        &config.twilio_auth_token,
        &config.twilio_from_number,
    ) {
        if !config.sms_to_numbers.is_empty() {
            let twilio_config = sms::TwilioConfig {
                account_sid: account_sid.clone(),
                auth_token: auth_token.clone(),
                from_number: from_number.clone(),
                to_numbers: config.sms_to_numbers.clone(),
            };

            match sms::SmsNotifier::new(
                twilio_config,
                config.sms_contract_filter.clone(),
                config.sms_retry_policy.clone(),
                pool.clone(),
            ) {
                Ok(notifier) => {
                    info!(
                        from_number = %from_number,
                        recipients = config.sms_to_numbers.len(),
                        "SMS notifications enabled"
                    );
                    notification_dispatcher.register(std::sync::Arc::new(notifier));
                }
                Err(e) => error!(error = %e, "SMS notifications disabled"),
            }
        } else {
            warn!("Twilio credentials set but SMS_TO_NUMBERS is empty — SMS notifications disabled");
        }
    }

    if !notification_dispatcher.is_empty() {
        info!(channels = ?notification_dispatcher.channel_names(), "Notification dispatcher started");
        notification_dispatcher.spawn(event_tx.subscribe());
    }

    // Issue #619: Spawn subscription email delivery worker.
    {
        let email_pool = pool.clone();
        jobs.spawn("subscription-email-delivery", subscriptions::run_email_delivery_worker(email_pool));
    }

    // Issue #1162: Encrypt integration credentials at rest with a dedicated key,
    // then backfill any rows that were stored in plaintext before this change.
    soroban_pulse::integration_secrets::init_keys(
        config.integration_encryption_key,
        config.integration_encryption_key_old,
    );
    {
        let backfill_pool = pool.clone();
        jobs.spawn_oneshot("integration-secrets-backfill", async move {
            if let Err(e) = soroban_pulse::integration_secrets::backfill_plaintext(&backfill_pool).await {
                error!(error = %e, "Failed to backfill integration credential encryption");
            }
        });
    }

    // Issue #1057: Auto-fetch contract specs (gated by AUTO_FETCH_CONTRACT_SPECS).
    if soroban_pulse::contract_specs::enabled() {
        jobs.spawn("contract-specs", soroban_pulse::contract_specs::run_worker(pool.clone(), config.stellar_rpc_url.clone()));
    }

    // Issue #1058: Track contract WASM versions and emit contract_upgraded events.
    jobs.spawn("contract-versions", soroban_pulse::contract_versions::run_worker(pool.clone(), config.stellar_rpc_url.clone(), event_tx.clone()));

    // Issue #620: Spawn push notification delivery worker.
    {
        let push_pool = pool.clone();
        jobs.spawn("push-delivery", push_notification::run_push_delivery_worker(push_pool));
    }

    // Issue #622 / #1244: the pool monitor and adaptive tuner were started by
    // `db_pool::DbPool::init` above.

    // Spawn Redis publisher if configured
    if let (Some(redis_url), Some(redis_stream_key)) = (&config.redis_url, &config.redis_stream_key)
    {
        let redis_rx = event_tx.subscribe();
        let redis_url = redis_url.clone();
        let redis_stream_key = redis_stream_key.clone();

        info!(stream_key = %redis_stream_key, "Redis publisher enabled");

        let redis_buffer_max_size = config.redis_buffer_max_size;
        jobs.spawn("redis-publisher", async move {
            queue_publisher::spawn_redis_publisher(
                redis_url,
                redis_stream_key,
                redis_buffer_max_size,
                redis_rx,
            )
            .await;
        });
    }

    // ── Role-gated: indexer subsystem ────────────────────────────────────────
    // ROLE=api  → indexer is skipped; this pod is a pure HTTP API replica.
    // ROLE=indexer | all → indexer starts; health/metrics HTTP server also binds.
    info!(role = ?config.role, "Starting with deployment role");

    // Create the SSE ring buffer unconditionally — the HTTP server always needs
    // a valid (possibly empty) buffer for Last-Event-ID replay, even on api pods.
    let sse_ring_buf = sse_ring_buffer::SseRingBuffer::new(config.sse_ring_buffer_capacity);

    // Build and wire up the indexer only when this instance is configured to run it.
    let rpc_client = indexer::SorobanRpcClient::new(&config)?;
    // Issue #1256: measures endpoint latency for routing; spawned with the indexer.
    let rpc_latency_probe = rpc_client.latency_probe();
    let mut indexer = indexer::Indexer::new(
        pool.clone(),
        config.clone(),
        shutdown_rx.clone(),
        rpc_client,
    );
    indexer.set_health_state(health_state.clone());
    indexer.set_indexer_state(indexer_state.clone());
    indexer.set_event_tx(event_tx.clone());
    indexer.set_sse_ring_buffer(std::sync::Arc::clone(&sse_ring_buf));
    // Issue #1264: track RPC call and DB query costs from the indexer.
    indexer.set_cost_calculator(cost_calculator.clone());

    // Issue #266: Initialize and seed bloom filter
    {
        // Determine bloom filter parameters: auto-size if BLOOM_EXPECTED_ITEMS is set, else use configured values
        let (capacity, fp_rate) = if config.bloom_expected_items > 0 {
            bloom_filter::auto_size_bloom_filter(config.bloom_expected_items, config.bloom_false_positive_rate)
        } else {
            (config.bloom_filter_capacity, config.bloom_filter_fp_rate)
        };

        let bloom = std::sync::Arc::new(bloom_filter::EventBloomFilter::new(capacity, fp_rate)?);
        match bloom_filter::seed_from_db(&bloom, &pool, 100_000).await {
            Ok(n) => info!(seeded = n, capacity = capacity, fp_rate = fp_rate, "Bloom filter seeded from DB"),
            Err(e) => tracing::warn!(error = %e, "Failed to seed bloom filter from DB"),
        }
        indexer.set_bloom_filter(bloom);
    }

    // Issue #265: Initialize Kinesis publisher if configured
    #[cfg(feature = "kinesis")]
    if let (Some(stream_name), Some(region)) = (
        config.kinesis_stream_name.clone(),
        config.aws_region.clone(),
    ) {
        let partition_key_field = config.kinesis_partition_key_field.clone();
        let publisher =
            kinesis::aws::AwsKinesisPublisher::from_env(stream_name, region, partition_key_field)
                .await;
        indexer.set_kinesis_publisher(std::sync::Arc::new(publisher));
        info!("Kinesis publisher enabled");
    }

    // Issue #264: Initialize Pub/Sub publisher if configured
    #[cfg(feature = "pubsub")]
    if let (Some(project_id), Some(topic_id)) = (
        config.pubsub_project_id.clone(),
        config.pubsub_topic_id.clone(),
    ) {
        match pubsub::gcp::GcpPubSubPublisher::from_env(project_id, topic_id, config.pubsub_enable_message_ordering).await {
            Ok(publisher) => {
                indexer.set_pubsub_publisher(std::sync::Arc::new(publisher));
                info!("Pub/Sub publisher enabled");
            }
            Err(e) => tracing::warn!(error = %e, "Failed to initialize Pub/Sub publisher"),
        }
    }

    // Issue #706: Initialize SQS publisher if configured
    #[cfg(feature = "sqs")]
    if let (Some(queue_url), Some(region)) =
        (config.sqs_queue_url.clone(), config.aws_region.clone())
    {
        let publisher = sqs::aws::AwsSqsPublisher::from_env(
            queue_url,
            region,
            config.sqs_dlq_url.clone(),
            config.sqs_batch_size,
        )
        .await;
        indexer.set_sqs_publisher(std::sync::Arc::new(publisher));
        info!("SQS publisher enabled");
    }

    // Issue #707: Initialize Azure Event Hubs publisher if configured
    if let Some(connection_string) = config.event_hubs_connection_string.clone() {
        match event_hubs::HttpEventHubsPublisher::from_connection_string(
            &connection_string,
            config.event_hub_name.clone().unwrap_or_default(),
            config.event_hubs_partition_strategy.clone(),
        ) {
            Ok(publisher) => {
                indexer.set_event_hubs_publisher(std::sync::Arc::new(publisher));
                info!("Event Hubs publisher enabled");
            }
            Err(e) => warn!(error = %e, "Failed to initialize Event Hubs publisher"),
        }
    }

    #[cfg(feature = "kafka")]
    if let (Some(brokers), Some(topic)) = (&config.kafka_brokers, &config.kafka_topic) {
        match soroban_pulse::kafka::RdKafkaProducer::new(
            brokers,
            config.kafka_batch_size,
            config.kafka_linger_ms,
        ) {
            Ok(producer) => {
                info!(brokers = %brokers, topic = %topic, "Kafka publishing enabled");
                indexer.set_kafka_publisher(std::sync::Arc::new(producer), topic.clone());
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to create Kafka producer — Kafka publishing disabled");
            }
        }
    }

    // Initialize Lua transformer if configured
    #[cfg(feature = "lua")]
    if let Some(ref script_path) = config.event_transform_script {
        match soroban_pulse::lua_transform::LuaTransformer::new(
            std::path::Path::new(script_path),
            config.event_transform_timeout_ms,
        ) {
            Ok(transformer) => {
                info!(
                    script_path = %script_path,
                    timeout_ms = config.event_transform_timeout_ms,
                    "Lua event transformer enabled"
                );
                indexer.set_lua_transformer(std::sync::Arc::new(transformer));
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to initialize Lua transformer — transformation disabled");
            }
        }
    }

    let mut indexer_job_names = Vec::new();
    if config.role.runs_indexer() {
        // Spawn the indexer background task.
        jobs.spawn("indexer", async move {
            indexer.run().await;
        });
        tokio::spawn(rpc_latency_probe.run(shutdown_rx.clone()));
        indexer_job_names.push("indexer".to_string());

        // Issue #1063: index additional networks (e.g. testnet alongside
        // mainnet) concurrently from this same deployment. Each additional
        // network gets its own indexer task, RPC client, advisory lock key
        // (see `indexer::lock_key_for_chain`) and checkpoint row (the indexer
        // checkpoint/state tables are already keyed off `chain_id`), but shares
        // the connection pool, event broadcast channel and SSE ring buffer with
        // the primary network so subscribers see all configured networks.
        let secondary_targets: Vec<_> = config.network_targets().into_iter().skip(1).collect();
        for target in secondary_targets {
            info!(
                chain_id = %target.chain_id,
                rpc_url = %target.rpc_url,
                "Starting additional network indexer"
            );
            let mut network_config = config.clone();
            network_config.chain_id = target.chain_id.clone();
            network_config.stellar_rpc_url = target.rpc_url.clone();
            // The primary network's fallbacks belong to a different chain;
            // never route this network's requests to them.
            network_config.stellar_rpc_fallback_urls = Vec::new();

            let network_rpc_client = match indexer::SorobanRpcClient::new(&network_config) {
                Ok(client) => client,
                Err(e) => {
                    error!(chain_id = %target.chain_id, error = %e, "Skipping additional network indexer");
                    continue;
                }
            };
            tokio::spawn(network_rpc_client.latency_probe().run(shutdown_rx.clone()));
            let mut network_indexer = indexer::Indexer::new(
                pool.clone(),
                network_config,
                shutdown_rx.clone(),
                network_rpc_client,
            );
            network_indexer.set_health_state(health_state.clone());
            network_indexer.set_indexer_state(indexer_state.clone());
            network_indexer.set_event_tx(event_tx.clone());
            network_indexer.set_sse_ring_buffer(std::sync::Arc::clone(&sse_ring_buf));
            network_indexer.set_cost_calculator(cost_calculator.clone());

            let job_name = format!("indexer:{}", target.chain_id);
            jobs.spawn(job_name.clone(), async move {
                network_indexer.run().await;
            });
            indexer_job_names.push(job_name);
        }

        // Spawn index usage monitoring background task (indexer role only)
        jobs.adopt("index-monitor", index_monitor::spawn(
            pool.clone(),
            config.index_check_interval_hours,
            shutdown_rx.clone(),
            index_monitor::FragmentationThresholds {
                warn_ratio: config.fragmentation_warn_threshold,
                critical_ratio: config.fragmentation_critical_threshold,
                auto_reindex: config.fragmentation_auto_reindex,
            },
        ));

        // Spawn replica sync monitoring background task (#586)
        jobs.adopt("replica-monitor", replica_monitor::spawn(pool.clone(), 60, shutdown_rx.clone()));

        // Spawn feature flag rollback watcher (#587)
        jobs.adopt("feature-flag-watcher", feature_flags::spawn(pool.clone(), 60, shutdown_rx.clone()));

        // Spawn materialized-view refresh background task
        jobs.adopt(
            "stats-refresh",
            stats_refresh::spawn(
                pool.clone(),
                config.stats_refresh_interval_secs,
                shutdown_rx.clone(),
            ),
        );
    } else {
        info!("ROLE=api: indexer and indexer-only background tasks skipped");
        // Drop the un-spawned indexer so its resources are freed.
        drop(indexer);
    }

    // Spawn resource metrics collector (all roles — useful on api pods too)
    jobs.adopt("resource-metrics", resource_metrics::spawn_resource_collector(shutdown_rx.clone()));

    // Issue #1262: Spawn the derived metrics evaluation job. It evaluates each
    // user-defined metric on its configured schedule, publishes the results to
    // the Prometheus registry and persists snapshots for history.
    {
        let derived_metrics_config = derived_metrics::DerivedMetricsConfig::from_config(&config);
        if derived_metrics_config.metrics.is_empty() {
            info!("No derived metrics configured — derived metrics job not started");
        } else {
            info!(
                count = derived_metrics_config.metrics.len(),
                "Starting derived metrics evaluation job"
            );
            derived_metrics::spawn_evaluation_job(
                pool.clone(),
                derived_metrics_config,
                shutdown_rx.clone(),
            );
        }
    }

    // Issue #1263: Spawn the incident correlation detector loop. It correlates
    // anomaly alerts, RPC errors and indexer lag into incidents.
    {
        let incident_config = incident_correlation::IncidentCorrelationConfig::from_config(&config);
        info!(
            window_secs = incident_config.correlation_window_secs,
            interval_secs = incident_config.evaluation_interval_secs,
            "Starting incident correlation detector"
        );
        incident_correlation::spawn_detector(pool.clone(), incident_config, shutdown_rx.clone());
    }

    // #496: Spawn audit log purge job
    {
        let audit_pool = pool.clone();
        let retention_days = config.notification_audit_log_retention_days;
        jobs.spawn("audit-log-purge", notification_admin::purge_old_audit_log_entries(audit_pool, retention_days));
    }

    // #498: Spawn channel health checker
    {
        let health_pool = pool.clone();
        let health_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("Failed to build health check HTTP client")?;
        let interval_secs = config.notification_health_check_interval_secs;
        let health_check_email = config.notification_health_check_email.clone();
        jobs.adopt(
            "channel-health-checker",
            notification_admin::spawn_channel_health_checker(
                health_pool,
                health_client,
                interval_secs,
                health_check_email,
            ),
        );
    }

    // #696: Install the SLO tracker and spawn its background evaluator. Must
    // happen before the HTTP server begins serving so that the
    // `/v1/admin/slo/report` endpoint can resolve `slo_tracker::current()`.
    {
        let slo_tracker = slo_tracker::shared_tracker();
        let installed = slo_tracker::install(slo_tracker.clone());
        if !installed {
            warn!("SLO tracker was already installed — replacing with the default tracker");
            // Re-install by clearing the OnceLock via a fresh tracker. In
            // practice this branch only fires if main is re-entered, which
            // does not happen, so we just log and move on.
        }
        if let Some(handle) = slo_tracker::SloTracker::spawn_evaluator(
            slo_tracker.clone(),
            shutdown_rx.clone(),
            slo_tracker::DEFAULT_EVAL_INTERVAL_SECS,
        ) {
            jobs.adopt("slo-evaluator", handle);
        }
        info!(
            interval_secs = slo_tracker::DEFAULT_EVAL_INTERVAL_SECS,
            slos_tracked = slo_tracker.definition_count(),
            "SLO tracker installed and evaluator started",
        );
    }

    async fn shutdown_signal(
        event_tx: broadcast::Sender<models::SorobanEvent>,
        sse_drain_timeout_secs: u64,
        sse_shutdown_grace_period_ms: u64,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
    ) {
        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {},
                        _ = sigterm.recv() => {},
                    }
                }
                Err(e) => {
                    // Without a SIGTERM handler we can still shut down on Ctrl-C.
                    tracing::warn!(error = %e, "Failed to install SIGTERM handler; listening for Ctrl-C only");
                    tokio::signal::ctrl_c().await.ok();
                }
            }
        }

        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }

        tracing::info!("Graceful shutdown initiated, sending close event to SSE clients...");

        // Signal SSE streams to emit close event
        let _ = shutdown_tx.send(true);

        // Wait for SSE clients to receive the close event
        tokio::time::sleep(Duration::from_millis(sse_shutdown_grace_period_ms)).await;

        // Drop the broadcast sender to signal SSE streams to close
        drop(event_tx);

        // Wait for SSE connections to drain
        tokio::time::sleep(Duration::from_secs(sse_drain_timeout_secs)).await;

        soroban_pulse::job_scheduler::global().spawn("shutdown-timeout", async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            tracing::info!("Graceful shutdown timeout reached (30s), forcing exit");
            std::process::exit(0);
        });
    }

    let event_tx_clone = event_tx.clone();
    let sse_drain_timeout = config.sse_drain_timeout_secs;
    let sse_shutdown_grace_period = config.sse_shutdown_grace_period_ms;
    let shutdown_tx_clone = shutdown_tx.clone();
    jobs.spawn("shutdown-signal", async move {
        shutdown_signal(event_tx_clone, sse_drain_timeout, sse_shutdown_grace_period, shutdown_tx_clone).await;
    });
    jobs.spawn(
        "jobs-metrics",
        soroban_pulse::job_scheduler::run_metrics_refresher(jobs.clone(), shutdown_rx.clone()),
    );

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(role = ?config.role, addr = %addr, "Binding HTTP server");

    // Clone pool before it is moved into the router for the tenant map load.
    let pool_for_tenant_map = pool.clone();

    let tenant_map = if config.multi_tenant && config.role.runs_full_api() {
        match routes::load_tenant_map(&pool_for_tenant_map).await {
            Ok(map) => {
                info!(count = map.len(), "Loaded tenant map from database");
                std::sync::Arc::new(map)
            }
            Err(e) => {
                error!(error = %e, "Failed to load tenant map — aborting startup");
                return Err(e.into());
            }
        }
    } else {
        std::sync::Arc::new(std::collections::HashMap::new())
    };

    // ROLE=indexer: expose only health + metrics so Kubernetes probes work
    // and Prometheus can scrape the pod, but skip all API routes.
    // ROLE=all|api: build the full router.
    let router = if config.role.runs_full_api() {
        info!(origins = ?config.allowed_origins, "Allowed CORS origins");
        info!(rate_limit = config.rate_limit_per_minute, "Rate limit per IP");
        routes::create_router_with_tx_and_tenant_map(
            pool,
            read_pool,
            config.api_keys.clone(),
            &config.allowed_origins,
            config.rate_limit_per_minute,
            config.behind_proxy,
            health_state,
            indexer_state,
            prometheus_handle,
            event_tx,
            config.sse_keepalive_interval_ms,
            config.sse_max_connections,
            2000,
            config.event_data_encryption_key,
            config.event_data_encryption_key_old,
            config.clone(),
            Some(schema_validator),
            tenant_map,
            shutdown_rx.clone(),
            sse_ring_buf,
        )
    } else {
        // Indexer-only role: minimal router with health + metrics routes.
        info!("ROLE=indexer: binding minimal HTTP server (health + metrics only)");
        routes::create_minimal_router(
            pool,
            health_state,
            indexer_state,
            prometheus_handle,
            config.clone(),
            shutdown_rx.clone(),
            sse_ring_buf,
        )
    };

    info!(addr = %addr, "Soroban Pulse listening");

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        error!(addr = %addr, "Address already in use");
        e
    })?;

    info!(
        behind_proxy = config.behind_proxy,
        "Running server - trusting X-Forwarded-For"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx_axum.changed().await;
        })
        .await?;

    // Wait for the indexer tasks to finish (only present when role runs indexer),
    // then abort the remaining background jobs.
    for name in &indexer_job_names {
        if let Some(handle) = jobs.take(name) {
            let _ = handle.await;
        }
    }
    jobs.shutdown_all();

    #[cfg(feature = "otel")]
    global::shutdown_tracer_provider();

    Ok(())
}
