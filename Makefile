.DEFAULT_GOAL := help

.PHONY: help build test test-db lint fmt run docker-up docker-down migrate clean gen-openapi gen-postman gen-config-docs docs deny

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Compile the project
	cargo build

test: ## Run the full test suite with cargo-nextest (requires DATABASE_URL)
	cargo nextest run

test-db: ## Start a test Postgres container and run the full test suite with cargo-nextest
	docker compose -f docker-compose.test.yml up -d --wait
	DATABASE_URL=postgres://postgres:postgres@localhost/soroban_pulse_test cargo nextest run; \
	  EXIT=$$?; \
	  docker compose -f docker-compose.test.yml down; \
	  exit $$EXIT

lint: ## Run clippy with warnings as errors
	cargo clippy -- -D warnings

fmt: ## Format source code
	cargo fmt

deny: ## Run cargo-deny checks (advisories, bans, licenses, sources)
	cargo deny check advisories
	cargo deny check bans
	cargo deny check licenses
	cargo deny check sources

audit: ## Run cargo-audit vulnerability scan against the RustSec advisory database
	@command -v cargo-audit >/dev/null 2>&1 || cargo install cargo-audit --locked
	cargo audit --deny warnings

security-tests: ## Run the dedicated security test suite (no DATABASE_URL needed)
	cargo test --test security -- --nocapture

check-secrets: ## Scan for hardcoded secrets and credentials in the codebase
	@bash scripts/check_secrets.sh

security: deny audit security-tests check-secrets ## Run all security checks (deny + audit + tests + secrets scan)

geiger: ## Audit unsafe code usage with cargo-geiger (advisory, non-blocking)
	@command -v cargo-geiger >/dev/null 2>&1 || cargo install cargo-geiger --locked
	cargo geiger --all-features 2>&1 | tee geiger-report.txt || true
	@echo "Geiger report saved to geiger-report.txt"

run: ## Start the development server
	cargo run

docker-up: ## Start the full stack via Docker Compose and wait for app to be healthy
	docker-compose up --build -d
	docker-compose wait app

docker-down: ## Tear down the Docker Compose stack
	docker-compose down

migrate: ## Run pending database migrations
	cargo sqlx migrate run

migrate-down: ## Rollback the most recent migration
	cargo sqlx migrate revert

seed: ## Seed the database with realistic dev/demo data (EVENTS=500 to override volume)
	cargo run --bin seed -- --events $${EVENTS:-500}

seed-large: ## Seed 100 000 events for performance testing
	cargo run --bin seed -- --events 100000

check-migrations: ## Check for duplicate migration timestamps
	@bash scripts/check-migrations.sh

clean: ## Remove build artifacts
	cargo clean
	rm -f openapi.json

gen-env-docs: ## Generate docs/configuration.md from src/config.rs @env tags
	cargo run --bin gen_env_docs -- --write

check-env-docs: ## Fail if docs/configuration.md is out of sync with src/config.rs
	cargo run --bin gen_env_docs -- --check

gen-openapi: ## Regenerate openapi.json from handler signatures and sync docs/openapi.json
	cargo run --bin gen_openapi > openapi.json
	cp openapi.json docs/openapi.json

gen-config-docs: ## Regenerate docs/configuration.md and .env.example from src/config/reference.rs
	cargo run --bin gen_config_docs

docs: ## Build the documentation site (docs/site) and check every doc is in the navigation
	cp openapi.json docs/site/static/openapi.json
	cd docs/site && npm ci && npm run check-nav && npm run build

gen-postman: gen-openapi ## Regenerate Postman collection and environment files from OpenAPI spec
	cargo run --bin gen_postman -- --input openapi.json --output-dir postman/
changelog: ## Generate changelog from git history (requires git-cliff)
	@command -v git-cliff >/dev/null 2>&1 || { echo "git-cliff not installed. Install with: cargo install git-cliff"; exit 1; }
	git-cliff --unreleased

generate-sdk: ## Generate TypeScript and Python SDKs from OpenAPI spec
	cargo run --bin gen_openapi > openapi.json
	# Generate TypeScript SDK
	npx @openapitools/openapi-generator-cli generate \
		-i openapi.json \
		-g typescript-fetch \
		-o sdk/typescript \
		--additional-properties=typescriptThreePlus=true,supportsES6=true
	# Generate Python SDK
	npx @openapitools/openapi-generator-cli generate \
		-i openapi.json \
		-g python \
		-o sdk/python \
		--additional-properties=library=httpx

sbom: ## Generate a CycloneDX SBOM for the Rust crate (requires cargo-cyclonedx)
	@command -v cargo-cyclonedx >/dev/null 2>&1 || cargo install cargo-cyclonedx --locked
	cargo cyclonedx --format json --output-cdx soroban-pulse-sbom.cdx.json
	@echo "CycloneDX SBOM written to soroban-pulse-sbom.cdx.json"

vacuum: ## Run VACUUM ANALYZE on the events table
	@if [ -z "$$DATABASE_URL" ]; then echo "DATABASE_URL is not set"; exit 1; fi
	psql "$$DATABASE_URL" -c "VACUUM ANALYZE events;"

.PHONY: run-zipkin zipkin-up zipkin-down
run-zipkin: ## Run with Zipkin tracing
	ZIPKIN_ENDPOINT=$${ZIPKIN_ENDPOINT:-http://localhost:9411/api/v2/spans} \
	cargo run --features zipkin

zipkin-up: ## Start Zipkin container
	docker run -d -p 9411:9411 --name zipkin openzipkin/zipkin

zipkin-down: ## Stop Zipkin container
	docker stop zipkin || true && docker rm zipkin || true

web-dev: ## Install web dependencies and start the dashboard dev server
	cd web && npm ci && npm run dev

web-build: ## Build the web dashboard for production
	cd web && npm ci && npm run build

web-lint: ## Lint and type-check the web dashboard
	cd web && npm ci && npm run lint && npm run typecheck

web-test: ## Run web dashboard tests
	cd web && npm ci && npm run test

.PHONY: web-dev web-build web-lint web-test

fuzz: ## Run fuzz tests locally (60 seconds each)
	@command -v cargo-fuzz >/dev/null 2>&1 || cargo install cargo-fuzz
	cd fuzz && cargo fuzz run fuzz_validate_contract_id -- -max_total_time=60
	cd fuzz && cargo fuzz run fuzz_validate_tx_hash -- -max_total_time=60
	cd fuzz && cargo fuzz run fuzz_pagination_params -- -max_total_time=60

# ── Load testing targets (Issue #923) ──────────────────────────────────────
# Requires k6: https://k6.io/docs/get-started/installation/
# All scripts read BASE_URL from the environment (default: http://localhost:3000).

.PHONY: load-test load-test-quick load-test-constant load-test-ramp \
        load-test-burst load-test-overload load-test-stress load-test-soak \
        load-test-spike

load-test-quick: ## Quick smoke check — constant load for 60 s (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed. See https://k6.io/docs/get-started/installation/"; exit 1; }
	@echo "Running 60-second steady-state smoke check..."
	@mkdir -p tests/load/results
	k6 run -e DURATION=60s ${K6_FLAGS} tests/load/constant_load.js

load-test-constant: ## Constant load — 50 VUs for 5 min (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run ${K6_FLAGS} tests/load/constant_load.js

load-test-ramp: ## Ramp-up — 0→100 VUs / 5 min ramp, 2 min hold (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run ${K6_FLAGS} tests/load/ramp_up.js

load-test-burst: ## Burst — 10 VU baseline + two 200 req/s spikes (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run ${K6_FLAGS} tests/load/burst.js

load-test-overload: ## Sustained overload — 200 req/s for 3 min, observational (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run ${K6_FLAGS} tests/load/sustained_overload.js

load-test-stress: ## Stress test — ramp from 200 to 1000 req/s (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run ${K6_FLAGS} tests/load/stress.js

load-test-soak: ## Soak test — 24-hour stability run (requires k6; use SOAK_DURATION=30m for a short run)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run -e SOAK_DURATION=$${SOAK_DURATION:-24h} ${K6_FLAGS} tests/load/soak.js

load-test-spike: ## Spike test — instant 10× burst + recovery (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed."; exit 1; }
	@mkdir -p tests/load/results
	k6 run ${K6_FLAGS} tests/load/spike.js

load-test: ## Run all load test scenarios sequentially and print summary (requires k6)
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed. See https://k6.io/docs/get-started/installation/"; exit 1; }
	@mkdir -p tests/load/results
	@echo "================================================================"
	@echo "  SorobanPulse Load Test Suite — Issue #923"
	@echo "  BASE_URL = $${BASE_URL:-http://localhost:3000}"
	@echo "================================================================"
	@echo ""
	@echo "[1/6] Constant Load (50 VUs, 5 min)..."
	@k6 run ${K6_FLAGS} tests/load/constant_load.js    && echo "  ✅ constant_load passed"    || echo "  ❌ constant_load FAILED"
	@echo ""
	@echo "[2/6] Ramp-Up (0→100 VUs, 9 min total)..."
	@k6 run ${K6_FLAGS} tests/load/ramp_up.js          && echo "  ✅ ramp_up passed"          || echo "  ❌ ramp_up FAILED"
	@echo ""
	@echo "[3/6] Burst (10 VU baseline + two 200 req/s spikes)..."
	@k6 run ${K6_FLAGS} tests/load/burst.js            && echo "  ✅ burst passed"            || echo "  ❌ burst FAILED"
	@echo ""
	@echo "[4/6] Sustained Overload (200 req/s, 3 min, observational)..."
	@k6 run ${K6_FLAGS} tests/load/sustained_overload.js && echo "  ✅ sustained_overload done" || echo "  ⚠️  sustained_overload done (observational — check summary)"
	@echo ""
	@echo "[5/6] Stress Test (200→1000 req/s ramp)..."
	@k6 run ${K6_FLAGS} tests/load/stress.js           && echo "  ✅ stress done"             || echo "  ⚠️  stress done (observational — check summary)"
	@echo ""
	@echo "[6/6] Spike Test (10× burst)..."
	@k6 run ${K6_FLAGS} tests/load/spike.js            && echo "  ✅ spike passed"            || echo "  ❌ spike FAILED"
	@echo ""
	@echo "================================================================"
	@echo "  All scenarios complete. Results in tests/load/results/"
	@echo "================================================================"
