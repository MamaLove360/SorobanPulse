# Contributing to Soroban Pulse

New to the project? See [docs/onboarding.md](docs/onboarding.md) for a day-1
setup checklist and fixes for the most common first-build issues before
diving into the sections below.

By participating in this project you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md). How decisions are made, how RFCs work,
and how to become a reviewer or maintainer are described in
[GOVERNANCE.md](GOVERNANCE.md); area owners are listed in
[MAINTAINERS.md](MAINTAINERS.md).

**Found a security vulnerability?** Do not open a public issue — follow
[SECURITY.md](SECURITY.md) to report it privately.

Contributions are accepted under the project's [MIT License](LICENSE).

## Running Integration Tests

Integration tests require a live PostgreSQL instance. The easiest way is `make test-db`, which starts a throwaway container, runs the full suite, then tears it down:

```bash
make test-db   # only Docker required — no local Postgres needed
```

Under the hood this runs:

```bash
docker compose -f docker-compose.test.yml up -d --wait
DATABASE_URL=postgres://postgres:postgres@localhost/soroban_pulse_test cargo test
docker compose -f docker-compose.test.yml down
```

If you already have a Postgres instance running, set `DATABASE_URL` directly:

```bash
export DATABASE_URL=postgres://<user>:<password>@localhost/<dbname>
cargo test
```

`sqlx::test`-annotated tests create and tear down their own isolated database automatically — no manual schema setup needed.

## Getting Started

1. Fork the repo and create a branch from `main`.
2. Copy `.env.example` to `.env` and fill in your values. **Never commit `.env` or any `.env.*` file.**
3. Run `make docker-up` to start the full stack, or `make run` for the dev server only.

## Common Tasks

Run `make help` to see all available targets:

```
make build       # Compile the project
make test        # Run the full test suite (requires DATABASE_URL)
make lint        # Run clippy with warnings as errors
make fmt         # Format source code
make run         # Start the development server
make docker-up   # Start the full stack via Docker Compose
make docker-down # Tear down the Docker Compose stack
make migrate     # Run pending database migrations
make check-migrations # Check for duplicate migration timestamps
make clean       # Remove build artifacts
```

## Database Migrations

### Naming Convention

Migration files must follow the naming convention:

```
<YYYYMMDDHHMMSS>_<description>.sql
```

Where:
- `YYYYMMDDHHMMSS` is a unique timestamp (14 digits)
- `<description>` is a short, snake_case description of the migration

**Important:** Each migration must have a **unique timestamp prefix**. SQLx applies migrations in lexicographic order by filename. Duplicate timestamps cause non-deterministic apply order and can lead to schema inconsistencies between fresh and incrementally-migrated databases.

### Upgrading deployed databases

Colliding versions were renumbered (see `scripts/remap-migration-versions.sql`). `_sqlx_migrations` stores version numbers, so run that script once against any already-migrated database before deploying. Fresh databases need nothing.

### Checking for Duplicates

Before committing a new migration, run:

```bash
make check-migrations
```

This ensures no two migration files share the same timestamp prefix. The CI pipeline also runs this check automatically.

### Example

```
20260527024126_add_composite_indexes.sql
20260527024127_add_webhook_failures_table.sql
```

Not:

```
20260527000000_add_composite_indexes.sql
20260527000000_add_webhook_failures_table.sql  # ❌ Duplicate timestamp!
```

## Working with Compile-Time SQL (`sqlx` macros)

Hot-path queries use `sqlx::query_scalar!` and `sqlx::query_as!` macros so that
typos and schema drift are caught at **compile time** instead of at runtime.

### How it works

sqlx checks macro queries against a committed `.sqlx/` cache directory so that
**you do not need a live database just to build or run `cargo clippy`**.  
The cache is a set of `query-<sha256>.json` files — one per macro call — that
record the SQL text, parameter types, and column types Postgres returned when
`cargo sqlx prepare` last ran.

```
.sqlx/
  query-26d64aa4….json   # get_events_by_contract
  query-4a4bf0cf….json   # get_events_by_tx
  query-c1326d8f….json   # store_event INSERT
  query-f73bfc8e….json   # count by contract_id
  query-4da09329….json   # approximate event count
  query-a11236f1….json   # exact event count
```

### Building without a database

The `SQLX_OFFLINE=true` environment variable tells sqlx to use only the
committed cache and skip any connection attempt.  CI's `clippy` job sets this
flag automatically.  You can do the same locally:

```bash
SQLX_OFFLINE=true cargo build
SQLX_OFFLINE=true cargo clippy -- -D warnings
```

### Updating the cache after changing a query

Whenever you add a new macro call or change the SQL text inside an existing one
you must regenerate the cache so CI doesn't fail:

1. Start a local Postgres instance (Docker is the easiest path):

```bash
make test-db   # starts a throwaway container and runs migrations
```

   Or, if you already have Postgres running:

```bash
export DATABASE_URL=postgres://<user>:<password>@localhost/<dbname>
sqlx migrate run   # make sure the schema is up to date
```

2. Regenerate the cache:

```bash
cargo sqlx prepare
```

   This updates `.sqlx/` in-place.  For a workspace-wide regeneration use
   `cargo sqlx prepare --workspace`.

3. Commit the updated `.sqlx/` files together with your source change:

```bash
git add .sqlx/
git commit -m "chore(db): update sqlx offline cache"
```

### CI enforcement

The `sqlx-prepare-check` CI job runs `cargo sqlx prepare --check` against a
live Postgres 15 container on every push and pull request.  It exits non-zero
if the committed `.sqlx/` cache is stale — i.e., a query was changed without
regenerating the cache, or the database schema drifted from what the cache
describes.

**You do not need a database to build** — only to change queries.

## Fuzzing

Fuzz targets live in `fuzz/fuzz_targets/` and cover the primary input-validation boundary:

| Target | What it tests |
|--------|---------------|
| `fuzz_validate_contract_id` | `validate_contract_id` — no panics, deterministic, valid inputs accepted |
| `fuzz_validate_tx_hash` | `validate_tx_hash` — no panics, deterministic, valid inputs accepted |
| `fuzz_pagination_params` | `PaginationParams` deserialization — no panics, `limit`/`offset` in range |

Requires a nightly toolchain and `cargo-fuzz`:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

Run all fuzz targets locally (60 seconds each):

```bash
make fuzz
```

Or run a single target:

```bash
cd fuzz
cargo fuzz run fuzz_validate_contract_id -- -max_total_time=60
cargo fuzz run fuzz_validate_tx_hash     -- -max_total_time=60
cargo fuzz run fuzz_pagination_params   -- -max_total_time=60
```

To run indefinitely (until a crash is found):

```bash
cargo fuzz run fuzz_validate_contract_id
```

Corpus and crash artifacts are stored under `fuzz/corpus/` and `fuzz/artifacts/` respectively (both git-ignored). The CI pipeline runs fuzz tests for 60 seconds per target on every push and PR, and stores corpus artifacts for 30 days to accumulate interesting inputs over time.

## Pre-commit Hooks

This project uses [lefthook](https://github.com/evilmartians/lefthook) to run `cargo check`, `cargo fmt --check`, and `cargo clippy` before every commit.

Install lefthook and register the hooks once:

```bash
# macOS
brew install lefthook

# Linux / other (via cargo)
cargo install lefthook

# Register hooks in your local clone
lefthook install
```

Hooks typically complete in under 30 seconds on a typical change. If a hook fails, fix the reported issue and re-commit.

## Security: Never Log Sensitive Values

Never log passwords, API keys, tokens, or other credentials at any log level. This includes:

- `DATABASE_URL` — use `config.safe_db_url()` which strips credentials before logging
- `STELLAR_RPC_URL` — already sanitized by `validate_rpc_url()` before being stored in `Config`; the stored `config.stellar_rpc_url` is safe to log
- `API_KEY` / `API_KEY_SECONDARY` — never log these values
- Any request header that may contain `Authorization` or `X-Api-Key` values

When adding new log statements, double-check that no field contains a raw secret. If in doubt, strip credentials before logging (see `Config::safe_db_url()` for the pattern).

## Code Style

- Formatting is enforced by `rustfmt` using the project's `rustfmt.toml` (`max_width = 100`, `edition = "2021"`).
- Run `make fmt` before pushing, or let the pre-commit hook handle it.
- CI will reject any PR where `cargo fmt --check` fails.

## Commit Message Format

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to structure commit messages. This enables automated changelog generation and semantic versioning.

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat:` A new feature
- `fix:` A bug fix
- `perf:` A performance improvement
- `docs:` Documentation only
- `chore:` Build, CI, or dependency updates
- `refactor:` Code refactoring without feature changes
- `test:` Adding or updating tests

### Breaking Changes

Prefix the footer with `BREAKING CHANGE:` to indicate a breaking change:

```
feat(api): change event response format

BREAKING CHANGE: The /v1/events response now returns event_data as base64-encoded gzip by default.
Use ?compact=false to get uncompressed JSON.
```

### Examples

```
feat(indexer): add Lua script transformation support

- Load Lua scripts from configurable path
- Transform events before storage
- Skip events by returning nil

Closes #123
```

```
fix(handlers): prevent PII leakage in export endpoint

Filter payments by patientId in addition to clinicId.

Closes #456
```

```
perf(db): add GIN index on event_data JSONB column

Improves topic filtering query performance by 10x.
```

## Contribution Workflow

The preferred contribution path is: open or find an issue, confirm the intended scope, create a branch from the latest `main`, make the smallest complete change, add regression coverage or documentation, run the local checks, and open a pull request. If the change affects API behavior, database schema, deployment, security, or architecture, explain the impact in the issue and link the relevant design or runbook documentation.

### Choosing the right change

| Change | Expected additions |
|---|---|
| Bug fix | A focused test that fails before the fix and passes after it |
| API or event change | Updated API examples, compatibility notes, and affected integration tests |
| Database change | Forward and rollback migration where applicable, migration check, and query/test coverage |
| Operational change | Updated deployment or runbook documentation and a safe rollback procedure |
| Architecture change | An [ADR](docs/adr/README.md) linked from the pull request |
| Documentation-only change | A reproducible example checked against the current commands and configuration |

### When a pull request needs an ADR

A pull request needs an Architecture Decision Record when it changes a public API or event contract, storage or migration behavior, deployment topology, data retention, a security boundary, or an operational dependency — or introduces a decision future contributors could reasonably revisit. Routine bug fixes, dependency bumps, and localized refactors do not need one. If you are unsure whether your change qualifies, ask in the issue before opening the pull request rather than guessing.

To propose an ADR, copy [`docs/adr/0000-template.md`](docs/adr/0000-template.md) to the next available number under `docs/adr/`, fill in the context, decision, alternatives, consequences, and rollout sections, add it to the index in [`docs/adr/README.md`](docs/adr/README.md), and link it from your pull request description. New ADRs start as `Proposed`.

An ADR is reviewed and approved like any other code change: reviewers check that the decision is specific, that rejected alternatives are documented, and that consequences and rollback are addressed, before the pull request (and the ADR's status) can move to `Accepted`. See [`docs/adr/README.md`](docs/adr/README.md) for the full authoring workflow and [`docs/adr/review-guidelines.md`](docs/adr/review-guidelines.md) for what reviewers look for.

### Example: branch, checks, and pull request

```bash
git switch main
git pull --ff-only origin main
git switch -c fix/describe-the-change

# Make the change, then run the narrowest useful checks first.
make fmt
cargo test --lib
make lint
make test

git diff --check
git push --set-upstream origin fix/describe-the-change
```

Use a descriptive Conventional Commit such as `fix(indexer): handle empty event pages`. In the pull request, summarize the behavior before and after the change, list the checks that ran, call out migrations or operational effects, and link the issue with `Closes #<number>` when the pull request completes it.

### Example: adding a migration

```bash
# Choose a unique UTC timestamp and descriptive snake_case name.
touch migrations/20260827090000_add_event_retention_policy.sql
make check-migrations
# Add the matching down migration when the project convention requires it.
```

Never put credentials in examples. Use placeholders such as `postgres://<user>:<password>@localhost/<dbname>`, and confirm that generated logs and test fixtures contain no tokens, API keys, or personal data.

### Review-ready checklist

Before requesting review, verify that the change is scoped to the issue, tests cover the changed behavior, documentation matches the implementation, public interfaces remain backward compatible or are explicitly versioned, migrations are ordered uniquely, and `git diff --check` is clean. Describe any unavailable local dependency or skipped integration test rather than implying it passed.

## Pull Requests

- Keep PRs focused — one logical change per PR.
- Ensure `make test` and `make lint` pass locally before opening a PR.
- Write a clear PR description referencing the relevant issue (e.g. `Closes #75`).
- Use Conventional Commits for all commit messages in the PR.
- Put PR summaries and implementation reports in the PR description, not in new files at the repository root. Only `README`, `CONTRIBUTING`, `CHANGELOG`, `LICENSE`, `SECURITY`, `CODE_OF_CONDUCT` and other standard project files belong there; user guides go in `docs/guides/`, design specs in `docs/rfcs/`.

## Releases

See [RELEASE.md](RELEASE.md) for the complete release process, including:
- Version numbering (Semantic Versioning)
- Updating CHANGELOG.md
- Creating git tags and GitHub releases
- Docker image publishing

Only maintainers can cut releases. If you'd like to propose a release, open an issue or contact the maintainers.
