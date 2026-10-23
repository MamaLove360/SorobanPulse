# Cargo Feature Matrix

SorobanPulse uses [Cargo features](https://doc.rust-lang.org/cargo/reference/features.html)
to keep the default binary lean while making every optional subsystem available
when you need it. This page documents each feature, the env vars it requires,
its approximate effect on binary size, and when you would reach for it.

## Quick Reference

| Feature | Default | Description |
|---|---|---|
| `dashboard` | ✅ | Serve the built-in ops dashboard at `/ui` |
| `classical-crypto` | ✅ | HMAC-SHA256 + AES-GCM webhook signing and field encryption |
| `quantum-crypto` | ❌ | Post-quantum algorithm suite (Dilithium, Kyber, Falcon, SPHINCS+) |
| `hybrid-crypto` | ❌ | Both classical and post-quantum in parallel |
| `istio` | ❌ | Istio service-mesh health check and mTLS probes |
| `linkerd` | ❌ | Linkerd service-mesh health check and mTLS probes |
| `full-observability` | ❌ | OpenTelemetry + Jaeger distributed tracing |
| `kafka` | ❌ | Publish indexed events to an Apache Kafka topic |
| `sqs` | ❌ | Publish indexed events to an AWS SQS queue |
| `pubsub` | ❌ | Publish indexed events to a Google Cloud Pub/Sub topic |
| `chaos` | ❌ | Enable chaos/resilience test gates (test-only) |

---

## Individual Features

### `dashboard`

**Enabled by default.** Bundles tower-http's static file service (`tower-http/fs`)
so the React ops dashboard can be served directly from the binary at `/ui`.

**When to disable:** Production deployments where the dashboard is served by
a dedicated CDN or reverse proxy, or images where you want to minimise the
dependency footprint.

```bash
# Build without the dashboard
cargo build --no-default-features --features classical-crypto

# Runtime opt-in (binary must have been built with the feature)
SERVE_DASHBOARD=true cargo run
```

**Required env vars:** `SERVE_DASHBOARD=true` (defaults to `false`).

**Approximate binary size impact:** +1.2 MB (tower-http fs + deps).

---

### `classical-crypto`

**Enabled by default.** Provides HMAC-SHA256 webhook signature generation /
verification, AES-GCM field-level encryption, and Ed25519 signing via the
`ed25519-dalek` crate.

This feature is required for:
- Webhook HMAC signatures (`WEBHOOK_SIGNING_SECRET`)
- Encrypted notification-channel credentials (`ENCRYPTION_KEY`)
- All standard API key hashing

**When to disable:** Only in minimal test builds where you need neither
webhooks nor field encryption.

```bash
cargo build --features classical-crypto
cargo check --no-default-features --features classical-crypto
```

**Required env vars:**
- `WEBHOOK_SIGNING_SECRET` — HMAC key for webhook delivery signatures
- `ENCRYPTION_KEY` — 32-byte hex key for field-level AES-GCM encryption

**Approximate binary size impact:** baseline (always linked unless explicitly excluded).

---

### `quantum-crypto`

**Disabled by default.** Adds post-quantum cryptographic primitives via the
`pqcrypto` family of crates:

| Algorithm | Role |
|---|---|
| Dilithium3 | Digital signatures |
| Kyber1024 | Key encapsulation / hybrid KEM |
| Falcon-1024 | Compact signatures (embedded / IoT use cases) |
| SPHINCS+-SHA2-256s | Hash-based signatures (stateless, highest assurance) |

**When to use:** Environments that must be resistant to future quantum
computers, or when you need to comply with NIST PQC standards.

```bash
cargo build --features quantum-crypto
```

**Required env vars:** None beyond `ENCRYPTION_KEY`; algorithm selection is
controlled via `CRYPTO_ALGORITHM` (default: `dilithium3`).
See [docs/quantum-cryptography.md](quantum-cryptography.md) for full options.

**Approximate binary size impact:** +6–8 MB (native C library compilations for
each pqcrypto crate).

**CI note:** The `features` job runs `cargo hack check --each-feature`, which
checks `quantum-crypto` independently. Building the pqcrypto crates requires
a C compiler and takes ~2–3 minutes extra on a cold cache.

---

### `hybrid-crypto`

**Disabled by default.** A convenience alias for `classical-crypto +
quantum-crypto`. Enables the hybrid key-exchange mode described in
[docs/encryption.md](encryption.md): the classical ECDH path and the
quantum-safe KEM path are run simultaneously; the shared secret is derived
from both via HKDF.

```bash
cargo build --features hybrid-crypto
```

**Required env vars:** Same as `classical-crypto` plus `quantum-crypto`.
Set `CRYPTO_MODE=hybrid` to activate the hybrid code path at runtime.

**Approximate binary size impact:** +6–8 MB (sum of both crypto sets).

---

### `istio`

**Disabled by default.** Activates the gRPC health check server
(`tonic`/`prost`) that Istio expects at `[::]:8090` for sidecar liveness and
mTLS peer-authentication probes. Also exposes the `grpc_health_v1` service
used by `istioctl proxy-status`.

```bash
cargo build --features istio
```

**Required env vars:**
- `ISTIO_GRPC_PORT` — port for the gRPC health service (default: `8090`)
- mTLS certificates are injected by the Istio sidecar; no manual cert config
  is needed.

**Kubernetes manifests:** `k8s/istio-peerauthentication.yaml`,
`k8s/istio-destinationrule.yaml`, `k8s/istio-gateway.yaml`.
See [docs/service-mesh-guide.md](service-mesh-guide.md) for a complete walkthrough.

**Approximate binary size impact:** +3 MB (`tonic` + `prost` + generated protos).

---

### `linkerd`

**Disabled by default.** Same gRPC health check plumbing as `istio` but
configured for Linkerd 2.x's `ServiceProfile` and `TrafficSplit` primitives.
Linkerd does not require a custom sidecar probe port; the standard `/healthz/ready`
HTTP endpoint is used for liveness.

```bash
cargo build --features linkerd
```

**Required env vars:** None beyond standard health check settings.

**Kubernetes manifests:** `k8s/linkerd-server.yaml`,
`k8s/linkerd-serviceprofile.yaml`, `k8s/linkerd-trafficsplit.yaml`.

**Approximate binary size impact:** +3 MB (same `tonic`/`prost` stack as `istio`).

**Note:** `istio` and `linkerd` both pull in `tonic` and `prost`. Enabling both
simultaneously compiles fine but is operationally unusual — Istio and Linkerd
are mutually exclusive in practice.

---

### `full-observability`

**Disabled by default.** Wires up OpenTelemetry distributed tracing via
`tracing-opentelemetry` and adds an OTLP exporter plus a Jaeger exporter
(`opentelemetry-jaeger`). Each indexer poll cycle, RPC call, and DB query
becomes a child span.

```bash
cargo build --features full-observability
# or
cargo run --features full-observability
```

**Required env vars:**
- `OTEL_EXPORTER_OTLP_ENDPOINT` — OTLP receiver endpoint (e.g. `http://localhost:4317`)
- `OTEL_SERVICE_NAME` — service name in traces (default: `soroban-pulse`)
- `JAEGER_ENDPOINT` — Jaeger UDP thrift endpoint, if using Jaeger directly
  (e.g. `http://localhost:14268/api/traces`)

See [docs/tracing.md](tracing.md) for a full OpenTelemetry + Jaeger setup guide.

**Approximate binary size impact:** +4 MB (`opentelemetry` + exporters).

---

### `kafka`

**Disabled by default.** After each successful indexer batch, publishes events
to a Kafka topic using `rdkafka` (which links against `librdkafka`). The
`cmake-build` cargo feature is used so `librdkafka` is compiled from source,
making the build self-contained.

```bash
# cmake is required to compile librdkafka
sudo apt-get install cmake
cargo build --features kafka
```

**Required env vars:**
- `KAFKA_BROKERS` — comma-separated list of broker addresses (e.g. `localhost:9092`)
- `KAFKA_TOPIC` — target topic name (default: `soroban-events`)
- `KAFKA_CLIENT_ID` — optional client identifier (default: `soroban-pulse`)

See [docs/kafka-event-publishing.md](kafka-event-publishing.md) for producer
configuration, TLS/SASL options, and Confluent Cloud setup.

**Approximate binary size impact:** +8–12 MB (`librdkafka` compiled from source).

**Build time note:** `librdkafka` via cmake adds ~3–5 minutes to a cold build.
CI caches the `target/` directory to keep incremental builds fast.

---

### `sqs`

**Disabled by default.** Publishes events to an AWS SQS queue. Unlike `kafka`,
this feature has no native C dependency — it uses `reqwest` (already in the
default dep graph) to call the SQS HTTP API.

```bash
cargo build --features sqs
```

**Required env vars:**
- `SQS_QUEUE_URL` — full SQS queue URL
  (e.g. `https://sqs.us-east-1.amazonaws.com/123456789/soroban-events`)
- `AWS_REGION` — AWS region (e.g. `us-east-1`)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — credentials, or use an IAM
  instance role / EKS IRSA for keyless auth.

See [docs/sqs-integration.md](sqs-integration.md) for IAM policy examples
and FIFO vs standard queue trade-offs.

**Approximate binary size impact:** negligible (uses existing `reqwest` client).

---

### `pubsub`

**Disabled by default.** Publishes events to a Google Cloud Pub/Sub topic via
`google-cloud-pubsub`.

```bash
cargo build --features pubsub
```

**Required env vars:**
- `PUBSUB_PROJECT_ID` — GCP project ID
- `PUBSUB_TOPIC` — topic name (e.g. `soroban-events`)
- `GOOGLE_APPLICATION_CREDENTIALS` — path to a service-account JSON key file,
  or omit to use the GKE metadata server / Workload Identity.

**Approximate binary size impact:** +2–3 MB (`google-cloud-pubsub` + gRPC transport).

---

### `chaos`

**Disabled by default.** A test-only feature that gates the advisory-lock
failover chaos tests in `tests/chaos_tests.rs`. It has no runtime effect and
does not add dependencies to production builds.

```bash
cargo test --features chaos
```

**Required env vars:** None.

**Approximate binary size impact:** 0 (test code only, stripped from release builds).

---

## Feature Combinations

### Minimal production (no dashboard)

```bash
cargo build --release --no-default-features --features classical-crypto
```

Suitable for environments where the dashboard is served by a separate service
or CDN. Produces the smallest possible binary.

### Standard production (default)

```bash
cargo build --release
# equivalent to: --features "dashboard,classical-crypto"
```

The right choice for most single-host and Kubernetes deployments.

### Quantum-ready + observability

```bash
cargo build --release --features "hybrid-crypto,full-observability"
```

Use in regulated environments that need forward-secure cryptography and want
full distributed traces in Jaeger/Honeycomb.

### Kafka + SQS fan-out

```bash
cargo build --release --features "kafka,sqs"
```

Publishes each indexed event to both a Kafka topic and an SQS queue
simultaneously. Useful for hybrid cloud architectures where consumers span
AWS and non-AWS infrastructure.

### Istio service mesh

```bash
cargo build --release --features "istio,full-observability"
```

Pairs well with the Istio manifests in `k8s/` and the Jaeger/Kiali dashboards
described in `k8s/observability-*.yaml`.

### Everything on

```bash
cargo build --release --features "dashboard,hybrid-crypto,istio,full-observability,kafka,sqs,pubsub"
```

Useful for integration testing all subsystems simultaneously. Not recommended
for production — only enable the features your deployment actually uses.

---

## How features are verified in CI

The `features` job in `.github/workflows/ci.yml` runs:

```bash
cargo hack check --each-feature --no-dev-deps
```

This checks every feature in isolation (plus `--no-default-features`) to
catch missing conditional compilation guards. The job requires cmake and
libpq-dev because `kafka` (librdkafka) and `sqs`/`pubsub` (sqlx) need native
build tools.

Each feature is also exercised in the nightly `release.yml` workflow with a
full `cargo build --features <feature>` to catch linker errors that `check`
alone would miss.

---

## Frequently Asked Questions

**Can I combine `istio` and `linkerd`?**
Yes, both features compile independently. In practice you would only deploy
with one — your Kubernetes cluster runs either Istio or Linkerd, not both.

**Does `quantum-crypto` replace `classical-crypto`?**
No. Use `hybrid-crypto` to run both simultaneously during a migration period.
Disable `classical-crypto` only after all clients have migrated to PQC keys.

**Is `chaos` safe to include in a production build?**
Yes — it is purely a test gate with no runtime code. The release profile
(`strip = true`) ensures the test code is stripped from the final binary.
That said, there is no reason to include it in production; leaving it out
keeps the build faster.

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

### Custom Docker build

Docker, through the `CARGO_FEATURES` build arg (added on top of the defaults):

```bash
docker build --build-arg CARGO_FEATURES="kafka pubsub" -t soroban-pulse:custom .
```

The `kafka` feature also needs `cmake` and a C++ compiler in the builder
stage to build librdkafka.
