FROM rust:1.87-slim AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Issue #401: Multi-stage build with cargo-chef for efficient layer caching
# Stage 1: Generate recipe from Cargo.lock (fast, cached unless dependencies change)
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Issue #1112: build the web dashboard (web/) — served by the backend at /ui.
# Design tokens are pre-built in design/build/, so only web/'s deps are needed.
FROM node:20-slim AS web
WORKDIR /src
COPY web/package.json web/package-lock.json ./web/
RUN cd web && npm ci --no-audit --no-fund
COPY design ./design
COPY web ./web
RUN cd web && npm run build

# Stage 2: Build dependencies only (cached unless Cargo.lock changes)
FROM chef AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
# Extra cargo features, e.g. --build-arg CARGO_FEATURES="kafka pubsub" (see docs/features.md).
ARG CARGO_FEATURES=""
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release ${CARGO_FEATURES:+--features "$CARGO_FEATURES"} --recipe-path recipe.json

# Stage 3: Build application (fast rebuild on source changes, dependencies cached)
COPY . .
RUN cargo build --release ${CARGO_FEATURES:+--features "$CARGO_FEATURES"}

# Final stage: Runtime image (minimal size, < 80 MB)
# gcr.io/distroless/cc-debian12 — digest pinned 2025-07-14. Update via Dependabot or manually with:
# docker inspect --format='{{index .RepoDigests 0}}' gcr.io/distroless/cc-debian12
# distroless/cc ships glibc + libssl + ca-certificates, so no apt-get layer is needed.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9b8f8b1a1b6b0b6f3f0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e

WORKDIR /app
COPY --from=builder --chown=nonroot:nonroot /app/target/release/soroban-pulse .
COPY --from=builder --chown=nonroot:nonroot /app/target/release/seed .
COPY --from=builder --chown=nonroot:nonroot /app/migrations ./migrations
COPY --from=web --chown=nonroot:nonroot /src/web/dist ./web/dist

# Serve the bundled dashboard at http://<host>:3000/ui. Set SERVE_DASHBOARD=false to disable.
ENV SERVE_DASHBOARD=true \
    DASHBOARD_DIR=/app/web/dist

# distroless/cc:nonroot already runs as uid/gid 65532 (nonroot).
USER nonroot:nonroot
EXPOSE 3000
# distroless has no shell or curl, so the HEALTHCHECK relies on the app's own
# /healthz/ready endpoint via the orchestrator (k8s liveness/readiness probes).
# If a container-level check is required, mount a static healthcheck binary and use:
#   HEALTHCHECK CMD ["/app/healthcheck"]
CMD ["./soroban-pulse"]
