# Developer Onboarding (Issue #826)

A "start here" guide that ties together the setup instructions in the
[README](../README.md), the workflow in [CONTRIBUTING.md](../CONTRIBUTING.md), and
the [Architecture Guide](architecture.md) into one path for a new contributor's
first day.

For deeper environment configuration beyond this day-1 checklist — OS-specific setup, IDE/editor configuration, debugging tools, and performance profiling — see [Development Environment Setup](development-setup.md).

> **In a hurry?** Skip the manual setup below entirely and use the
> [Dev Container / Codespaces](#dev-container--codespaces) — one click gives you
> Rust, PostgreSQL, `sqlx-cli`, and a migrated database, with no local installs.

## Before you start

- **Rust** (stable toolchain — this project does not pin a specific version via
  `rust-toolchain.toml`, so `rustup update stable` before starting is a good idea)
- **PostgreSQL 14+** (or Docker, to run one via `docker-compose.yml`)
- **Docker** (optional but recommended — `make docker-up` gets the full stack
  running without installing Postgres locally)

## Day-1 checklist

1. **Clone and configure** — or skip steps 1–3 entirely by opening the repo in a
   [Dev Container / Codespace](#dev-container--codespaces), which does all of
   this for you.
   ```bash
   git clone <repo-url> && cd SorobanPulse
   cp .env.example .env   # fill in real values — see README § Setup
   ```
2. **Get a database running before your first build.** This is the step most
   likely to trip up a first build — see [Why does `cargo build` need a
   database?](#why-does-cargo-build-need-a-database) below.
   ```bash
   docker compose up -d postgres   # or point DATABASE_URL at an existing instance
   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/soroban_pulse
   ```
3. **Build.**
   ```bash
   cargo build
   ```
4. **Run the full test suite** (spins up its own throwaway database — see
   [CONTRIBUTING.md § Running Integration Tests](../CONTRIBUTING.md#running-integration-tests)):
   ```bash
   make test-db
   ```
5. **Run the server locally:**
   ```bash
   make run          # local binary, migrations run automatically on startup
   # or
   make docker-up    # full stack via Docker Compose
   ```
6. **Confirm it's alive:**
   ```bash
   curl http://localhost:3000/healthz/live
   curl http://localhost:3000/v1/events
   ```
7. **Install the pre-commit hooks** so `cargo fmt`/`clippy`/`check` run locally
   before every commit, catching most CI failures before you push — see
   [CONTRIBUTING.md § Pre-commit Hooks](../CONTRIBUTING.md#pre-commit-hooks).

If everything above works, you're set up. If something fails, check
[Common First-Build Issues](#common-first-build-issues) below before
[docs/troubleshooting.md](troubleshooting.md), which covers *running* issues
(indexer lag, rate limiting, SSE) rather than *build* issues.

## Dev Container / Codespaces

The repo ships a [Dev Container](https://containers.dev/) configuration in
`.devcontainer/`, so you can get a fully working environment without installing
Rust, PostgreSQL, or `sqlx-cli` on your machine. This is the fastest path for a
first contribution and the recommended way to try the project.

### Opening the environment

- **GitHub Codespaces:** on the repo page, click **Code → Codespaces → Create
  codespace on main** (or use the badge/link in the README). The container is
  built and provisioned automatically.
- **VS Code locally:** install the
  [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers),
  then run **Dev Containers: Reopen in Container** from the command palette
  (or click the green status-bar button in the bottom-left corner). Docker must
  be running locally.

The container is based on the `mcr.microsoft.com/devcontainers/rust` image and
starts PostgreSQL as a Docker Compose service (`.devcontainer/docker-compose.yml`),
so the database is reachable at `postgres:5432` from inside the container.

### What the post-create script does

On first creation, `postCreateCommand` runs automatically and:

1. Copies `.env.example` to `.env` (if `.env` doesn't already exist) so
   `DATABASE_URL` and friends are set for the container's Postgres service.
2. Waits for PostgreSQL to accept connections.
3. Runs `cargo build` to compile the project (this also warms the dependency
   cache and validates the compile-time `sqlx::query!` macros).
4. Runs `sqlx migrate run` to apply all migrations to the dev database.
5. Seeds the database with sample data so `curl http://localhost:3000/v1/events`
   returns something useful immediately.
6. Installs Node dependencies (if the repo has a `package.json`) for any
   tooling that needs them.

Expect the first build to take **under 5 minutes** on a Codespaces default
machine; subsequent starts reuse the cached build and are near-instant.

### Running the tests

Inside the container, the same commands from the Day-1 checklist apply:

```bash
make test        # full test suite
make test-db     # integration tests against a throwaway database
```

`make test` is expected to pass out of the box inside the Dev Container — if it
doesn't, that's a bug worth reporting (see [Feedback](#feedback)).

### Troubleshooting

- **Port already in use (`5432` or `3000`).** Another Postgres or app instance
  is bound to the port. Stop it (`docker compose down` on the host, or
  `lsof -i :5432`) and rebuild the container. Inside the container, the app
  should talk to the Compose Postgres service by hostname (`postgres`), not
  `localhost`.
- **Database not ready / `connection refused` during post-create.** The
  Postgres service can take a few seconds to become healthy on a cold start.
  Rebuild the container (**Dev Containers: Rebuild Container**) or run the
  post-create steps manually:
  ```bash
  sqlx migrate run
  cargo build
  ```
- **`sqlx` compile errors mentioning `DATABASE_URL`.** The `.env` file is
  missing or points at the wrong host. Confirm `.env` exists and that
  `DATABASE_URL` uses the `postgres` service hostname, then re-run
  `sqlx migrate run && cargo build`.
- **Stale build after pulling changes.** Run **Dev Containers: Rebuild
  Container** to re-run the post-create script against the updated repo.

## Why does `cargo build` need a database?

Most queries in this codebase use `sqlx::query_as` with a plain SQL string,
checked only at runtime — no database is needed to compile those. A handful
of call sites (e.g. `src/abi.rs`) use the compile-time–checked `sqlx::query!`
macro instead, which connects to `DATABASE_URL` **while compiling** to verify
the query against the live schema. If that database isn't reachable and
already migrated, `cargo build`/`cargo check` fails before you even get to
running anything.

If you hit an error mentioning `sqlx` and a connection failure or `SQLX_OFFLINE`
during a **build** (not a test run), it means: bring up Postgres and run
migrations first, *then* build.

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/soroban_pulse
cargo sqlx migrate run --source migrations   # or: cargo run (runs migrations on startup)
cargo build
```

## Common First-Build Issues

### `cargo build` fails immediately with a dependency resolution error

```
error: failed to select a version for the requirement `<crate> = "..."`
candidate versions found which didn't match: ...
```

This means a dependency pin in `Cargo.toml` no longer resolves against the
current crates.io index (the requested version was never published, or has
since been fully removed). This is a manifest problem, not something wrong
with your environment — `cargo update -p <crate> --precise <version>` will
not help if no compatible version exists. Check the project's issue tracker
for an open report before filing a new one, since a stale/incorrect version
pin affects every contributor identically until it's corrected in
`Cargo.toml`/`Cargo.lock`.

### `cargo build` fails with unresolved `crate::` paths in a binary target

If `cargo build` (not `cargo build --lib`) fails with `unresolved module` or
`cannot find X in this scope` errors pointing at `src/main.rs`, a module that
exists under `src/` and is wired into the library target (`src/lib.rs`) was
not also declared with `mod ...;` in `src/main.rs` — the binary and library
targets each maintain their own module tree from the same `src/` files (see
`src/main.rs` vs. `src/lib.rs`), so a new module has to be added to *both*
when it's needed from handler code. If you hit this on a module we haven't
covered yet, adding the missing `mod <name>;` line to `src/main.rs` is
usually the entire fix.

### `docker-compose up` fails with a port conflict

Postgres (`5432`) or the app port (`3000`) is already bound by another local
process or a previous Compose run that wasn't torn down cleanly.

```bash
docker compose down
lsof -i :5432   # or :3000 — find what's holding the port
```

### `make test-db` hangs or fails to connect

The throwaway Postgres container may not be healthy yet, or Docker itself
isn't running. Check `docker compose -f docker-compose.test.yml ps` and
`docker compose -f docker-compose.test.yml logs postgres`.

### Migrations fail with a permissions error

See [docs/troubleshooting.md § Migrations fail on startup](troubleshooting.md#migrations-fail-on-startup)
for the exact grant statement needed.

## Where to go next

- **Understand the system**: [Architecture Guide](architecture.md) — component
  descriptions, event flow, the multi-replica advisory lock mechanism, and
  deployment architecture.
- **Understand the schema**: [Database Schema](schema.md) — table structure,
  indexes, and an ER diagram.
- **Understand Stellar/Soroban terminology**: [Glossary](glossary.md) — plain-language
  definitions of ledger, TOID, ScVal, XDR, Strkey, SAC, SEP-41, TTL/archival,
  and Soroban events 101. A good read before diving into `src/indexer.rs`.
- **Understand the codebase layout**: [Module Map](module-map.md) — every `src/`
  module grouped by domain with one-line descriptions and "where do I start?"
  guidance for common contribution types.
- **Understand testing conventions**: [CONTRIBUTING.md](../CONTRIBUTING.md) —
  commit message format, migration naming, fuzzing, mutation testing, and the
  PR checklist.
- **Keep fuzzed functions `pub`**: the `fuzz/` crate links `soroban_pulse` as
  an external dependency, so any function a fuzz target calls (for example
  `handlers::validate_contract_id` / `handlers::validate_tx_hash`) must be
  `pub`, not `pub(crate)` — otherwise the nightly fuzz job fails to build and
  is reported as a crash. Regression inputs for fuzz targets live in
  `tests/fixtures/fuzz_seeds/<target>/` (`fuzz/corpus/` is git-ignored).
- **Understand performance work**: [Performance Tuning Guide](performance-tuning.md)
  and [Database Configuration Tuning](database-configuration-tuning.md).
- **Pick a first issue**: look for issues labeled `good first issue` (if none
  are labeled yet, start with something in `docs/` — documentation fixes are
  a low-risk way to learn the codebase layout before touching `src/`).

## Feedback

Found a step here that's wrong, missing, or took longer than expected? Open a
PR against this file directly, or open an issue describing what tripped you
up — a "this confused me" report from someone on their first day is exactly
the signal this document needs to stay accurate. See
[CONTRIBUTING.md § Pull Requests](../CONTRIBUTING.md#pull-requests) for the
process.
