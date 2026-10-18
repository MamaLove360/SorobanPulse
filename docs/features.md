# Cargo Feature Matrix

Soroban Pulse uses cargo features to keep optional integrations out of the
default binary. This page lists every feature declared in
[`Cargo.toml`](../Cargo.toml) and every feature name the source code gates on.

## Declared features

| Feature | What it enables | Extra dependencies / system requirements | Maturity |
|---|---|---|---|
| `default` | `classical-crypto` + `dashboard` | — | stable |
| `dashboard` | Serves the built web dashboard at `/ui` (`src/dashboard.rs`). Runtime opt-in with `SERVE_DASHBOARD=true`. See [dashboard.md](dashboard.md). | `tower-http/fs` | stable |
| `classical-crypto` | Marker for the classical (non post-quantum) crypto path. No code is gated on it. | — | stable |
| `quantum-crypto` | Pulls in post-quantum crypto crates. See [quantum-cryptography.md](quantum-cryptography.md). | `pqcrypto-dilithium`, `pqcrypto-kyber`, `pqcrypto-falcon`, `pqcrypto-sphincsplus`, `pqcrypto-traits` (C toolchain required) | experimental |
| `hybrid-crypto` | `classical-crypto` + `quantum-crypto` | as `quantum-crypto` | experimental |
| `istio` | Service mesh (gRPC) support. See [service-mesh-guide.md](service-mesh-guide.md). | `tonic`, `prost` | experimental |
| `linkerd` | Service mesh (gRPC) support. See [service-mesh-guide.md](service-mesh-guide.md). | `tonic`, `prost` | experimental |
| `full-observability` | Jaeger exporter for OpenTelemetry tracing. See [tracing.md](tracing.md). | `tracing-opentelemetry`, `opentelemetry-jaeger` | beta |
| `kafka` | Kafka publisher (`src/kafka.rs`), wired into the indexer. Env: `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_BATCH_SIZE`, `KAFKA_LINGER_MS`. See [kafka-event-publishing.md](kafka-event-publishing.md). | `rdkafka` (builds librdkafka from source: `cmake` and a C/C++ toolchain) | beta |
| `sqs` | AWS SQS publisher (`src/sqs.rs`). Env: `SQS_QUEUE_URL`, `AWS_REGION`, `SQS_BATCH_SIZE`, `SQS_DLQ_URL`. See [sqs-integration.md](sqs-integration.md). | none (see below) | beta |
| `pubsub` | Google Cloud Pub/Sub publisher (`src/pubsub.rs`). Env: `PUBSUB_PROJECT_ID`, `PUBSUB_TOPIC_ID`, `PUBSUB_ENABLE_MESSAGE_ORDERING`. | `google-cloud-pubsub`, `google-cloud-googleapis` | beta |

Features that must not be combined: `otel` and `zipkin` (enforced in
[`build.rs`](../build.rs)).

### Empty features

- **`sqs = []`** — intentionally empty. `src/sqs.rs` signs requests to the
  SQS Query API itself using the existing `reqwest`, `sha2` and `hmac`
  dependencies, so no AWS SDK crate is needed. The feature only compiles the
  publisher in.
- **`classical-crypto = []`** — a marker so `hybrid-crypto` can express
  "classical + quantum". No code checks it.

## Gated in code but not declared

The following names appear in `#[cfg(feature = "...")]` attributes but are
**not** declared in `Cargo.toml`. They cannot be enabled today
(`cargo build --features graphql` fails with "does not have that feature"),
so the gated code is never compiled. They are flagged for review: each needs
to be declared with its dependencies, or the gated code removed.

| Feature | Gated code | Dependencies it would need | Maturity |
|---|---|---|---|
| `graphql` | `src/graphql.rs`, `src/graphql_subscriptions.rs`. See [graphql_api.md](graphql_api.md). | `async-graphql` (already a non-optional dependency) | experimental |
| `otel` | OTLP tracing layer in `src/main.rs`, `src/distributed_tracing.rs`. Env: `OTEL_EXPORTER_OTLP_ENDPOINT`. | `opentelemetry`, `opentelemetry-otlp` (non-optional), `tracing-opentelemetry` | experimental |
| `zipkin` | Only referenced by the `otel`/`zipkin` conflict check in `build.rs` | a Zipkin exporter crate | experimental |
| `encryption` | AES-256-GCM `event_data` encryption in `src/encryption.rs`, `src/handlers.rs`. See [encryption.md](encryption.md). | `aes-gcm` (non-optional) | experimental |
| `kinesis` | AWS Kinesis publisher (`src/kinesis.rs`). Env: `KINESIS_STREAM_NAME`, `AWS_REGION`. | an AWS Kinesis client | experimental |
| `archive` | S3 archival job (`src/archiver.rs`). Env: `ARCHIVE_AFTER_DAYS`. See [data-retention-tiers.md](data-retention-tiers.md). | `aws-sdk-s3`, `flate2` | experimental |
| `parquet` | Parquet export (`src/parquet_export.rs`). | `parquet`, `arrow` | experimental |
| `lua` | Lua event transforms (`src/lua_transform.rs`). Env: `EVENT_TRANSFORM_SCRIPT`, `EVENT_TRANSFORM_TIMEOUT_MS`. See [lua-transformation.md](lua-transformation.md). | `mlua` | experimental |
| `redis-queue` | Redis Streams publisher in `src/queue_publisher.rs`. Env: `REDIS_URL`, `REDIS_STREAM_KEY`, `REDIS_BUFFER_MAX_SIZE`. | `redis` | experimental |

## Docker image and Helm chart

The published Docker image is built from the [`Dockerfile`](../Dockerfile)
with **default features only** (`classical-crypto`, `dashboard`). The
[Helm chart](../helm/soroban-pulse) deploys that same image and does not
enable any extra features. To run an integration such as Kafka or Pub/Sub in
Kubernetes, build a custom image (below) and point `image.repository` /
`image.tag` in the chart values at it.

## Building with custom features

Locally:

```bash
# Default features plus Kafka and Pub/Sub
cargo build --release --features "kafka pubsub"

# Minimal binary without the dashboard
cargo build --release --no-default-features --features classical-crypto

# Check that every feature compiles on its own (as CI does)
cargo hack check --each-feature --no-dev-deps
```

Docker, through the `CARGO_FEATURES` build arg (added on top of the defaults):

```bash
docker build --build-arg CARGO_FEATURES="kafka pubsub" -t soroban-pulse:custom .
```

The `kafka` feature also needs `cmake` and a C++ compiler in the builder
stage to build librdkafka.
