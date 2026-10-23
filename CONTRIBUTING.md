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

## Contributing Your First PR

Welcome! This section walks you through the complete journey from finding an
issue to getting your first PR merged. No prior contribution experience needed.

### Step 1 — Find an issue to work on

Start with issues labelled
[`good first issue`](https://github.com/Soroban-Pulse/SorobanPulse/issues?q=is%3Aissue+is%3Aopen+no%3Aassignee+label%3A%22good+first+issue%22).
These are intentionally small, well-scoped, and have clear acceptance criteria.
Every `good first issue` comes with enough context to complete it without deep
project knowledge.

### Step 2 — Claim the issue

Post a comment on the issue:

> "I'd like to work on this. Planning to start today."

Wait until a maintainer assigns the issue to you before you start coding.
This avoids duplicate work. If no one responds within 48 hours during
weekdays, feel free to ping in the comment thread.

### Step 3 — Create your branch

Always branch off the latest `main`:

```bash
git switch main
git pull --ff-only origin main
git switch -c fix/1234-describe-the-fix     # for bug fixes
# or
git switch -c feat/1324-feature-description # for new features
# or
git switch -c docs/1326-contributor-guide   # for documentation
```

**Branch naming convention:**

```
<type>/<issue-number>-<short-description>
```

| Type | When to use |
|---|---|
| `fix/` | Bug fixes |
| `feat/` | New features |
| `docs/` | Documentation-only changes |
| `chore/` | Build, CI, or dependency changes |
| `refactor/` | Code reorganization without behavior changes |
| `test/` | Adding or improving tests |
| `perf/` | Performance improvements |

Examples:
- `fix/1234-indexer-panic-on-empty-events`
- `feat/1324-cargo-feature-docs`
- `docs/1326-contributor-workflow`

### Step 4 — Write your change

- Keep the change focused on the issue. Don't fix unrelated issues in the same PR.
- Follow the code style (`make fmt` enforces rustfmt; `make lint` enforces clippy).
- Add tests: a failing test before your fix, a passing test after.
- Update documentation if your change affects user-facing behavior.

### Step 5 — Use Conventional Commits

Commit messages must follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description in present tense>

<optional body explaining the why, not the what>

Closes #<issue-number>
```

Examples:

```
docs(contributing): add first PR walkthrough and branch naming guide

Closes #1326
```

```
fix(indexer): handle empty event pages without panicking

Closes #1234
```

```
feat(api): add ?event_type filter to GET /v1/events

Closes #1300
```

Rules:
- Use present tense: "add feature" not "added feature"
- Keep the subject under 72 characters
- Reference the issue with `Closes #N` so it closes automatically on merge

### Step 6 — Run checks locally before pushing

```bash
make fmt       # auto-format code
make lint      # clippy with -D warnings
make test      # full test suite (needs DATABASE_URL or make test-db)
```

If you do not have a local Postgres instance:

```bash
make test-db   # starts a Docker Postgres, runs tests, tears it down
```

### Step 7 — Open the PR

```bash
git push --set-upstream origin fix/1234-describe-the-fix
```

Then open a pull request on GitHub:

- **Title:** use a Conventional Commit style title: `fix(indexer): handle empty event pages`
- **Description:** fill in all sections of the PR template (Summary, Related Issue, Changes, Testing)
- **Link the issue:** add `Closes #1234` to the description so the issue closes when your PR merges
- **Assign yourself** (GitHub auto-assigns if you opened the branch from the issue)

### Step 8 — Respond to review feedback

Reviewers may ask questions or request changes. Respond within 7 days.

- Address each comment individually — either make the change or explain why you disagree.
- Push new commits rather than force-pushing; this keeps the review history intact.
- Once all conversations are resolved, the reviewer will approve and a maintainer will merge.

### What happens after merge

- The linked issue closes automatically.
- You will appear in the contributor list and the release notes.
- You are now eligible to claim issues with `help wanted` and slightly larger scope.

---

## Label Definitions

Labels define what type of work an issue involves, how hard it is, which part
of the codebase it touches, and whether it is open for contribution.
They are defined in [`.github/labels.yml`](.github/labels.yml) and synced
by the [Sync Labels](.github/workflows/labels.yml) workflow.

### Type labels

| Label | Meaning |
|---|---|
| `bug` | Confirmed defect; something is broken |
| `enhancement` | New capability or improvement to an existing one |
| `documentation` | Change to docs only — no code |
| `refactoring` | Code reorganization without behavior change |
| `security` | Security-related issue or CVE |

### Contribution-readiness labels

| Label | Meaning |
|---|---|
| `good first issue` | Small, well-scoped task with clear acceptance criteria; ideal for a first PR. No prior project knowledge needed. |
| `help wanted` | Maintainers welcome outside contributions. Also applied automatically when a stale claim is released (7-day inactivity). |
| `Stellar Wave` | A curated batch of beginner-to-intermediate issues released periodically to grow the contributor community. Issues in a Stellar Wave are grouped by theme and announced in Discussions. |

### Difficulty labels

| Label | Meaning |
|---|---|
| `difficulty: easy` or `🟢 Beginner` | Localized change; minimal project context required |
| `difficulty: medium` or `🟡 Intermediate` | Requires understanding one subsystem; moderate context needed |
| `difficulty: hard` or `🔴 Advanced` | Touches multiple subsystems or requires deep domain knowledge |

### Priority labels

| Label | Meaning |
|---|---|
| `priority: critical` | Production-impacting; reviewed first; exempt from the stale-claim release |
| `priority: high` | Should be addressed in the current sprint |
| `priority: low` | Nice to have; no urgency |

### Area labels

| Label | The part of the codebase |
|---|---|
| `area: indexer` | `src/indexer.rs` and related polling / RPC code |
| `area: api` | HTTP handlers, routes, and middleware (`src/handlers.rs`, `src/routes.rs`) |
| `area: database` | Migrations, query builders, schema (`migrations/`, `src/db.rs`) |
| `area: integrations` | Third-party integrations: Kafka, SQS, webhooks, Slack, etc. |
| `area: sdk` | Client SDKs under `sdk/` |
| `area: frontend` | React dashboard (`frontend/`) and dashboard (`dashboard/`) |
| `area: infra` | Terraform, Helm, Kubernetes, Docker, CI/CD |
| `area: docs` | Documentation under `docs/`, README, CONTRIBUTING |



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

### Rollback (`.down.sql`) files

Every forward migration `<timestamp>_<name>.sql` must ship with a matching `<timestamp>_<name>.down.sql` that reverses it (at minimum `DROP TABLE IF EXISTS` / `DROP INDEX IF EXISTS` / `ALTER TABLE ... DROP COLUMN IF EXISTS`). `make check-migrations` (`scripts/check-migrations.sh`) fails if any migration lacks a down file, and CI runs it on every pull request.

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

## Benchmarking

Performance benchmarks use Criterion and run automatically on every PR. A regression gate fails CI if any benchmark regresses by more than 15%.

**Benchmark gate policy:**
- PRs that regress p99 latency by >15% fail CI
- Baseline is always the latest commit on `main`
- A comment is posted to the PR with regression details

Run benchmarks locally:

```bash
cargo bench
```

The `bench` job in CI is non-blocking for main branch builds, but **blocking** for PRs with a 15% regression threshold.

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

## Dependency Management

### Automated Dependency Updates

Dependabot runs daily for Cargo, Docker, and GitHub Actions dependencies. Patch-level updates that pass CI are automatically merged without manual review.

**Auto-merge policy:**
- Patch version bumps (e.g., `1.2.3` → `1.2.4`) merge automatically after CI passes
- Minor and major updates require manual review
- All auto-merged PRs must have the `dependencies` label

**Security audits:**
- `cargo-audit` runs daily at 2 AM UTC
- New CVEs trigger a Security Advisory issue within 24 hours
- Issues are tagged with `security`, `automated`, and `priority: critical`

Run security audits locally:

```bash
cargo install cargo-audit
cargo audit
```

## Dependency License Policy

This project is licensed under the [MIT License](LICENSE). All dependencies must
be compatible with that license.

- **GPL and AGPL licensed dependencies are not permitted.** `cargo-deny` is
  configured in `deny.toml` to deny these licenses, along with any license not
  on the allow-list.
- **Known vulnerabilities and unmaintained crates are denied.** The `advisories`
  section of `deny.toml` denies `vulnerability` and `unmaintained` advisories
  from the RustSec database.
- **CI enforces this.** The `cargo-deny` CI job runs `cargo-deny check` on every
  push and pull request. A new dependency that introduces a license violation or
  a known advisory will fail CI and cannot be merged.

Run the check locally before pushing:

```bash
cargo install cargo-deny   # once
cargo deny check
```

### Adding a justified exception

If a dependency genuinely cannot be avoided, add a narrowly scoped exception to
`deny.toml` rather than disabling the check:

- **License exceptions** go under `licenses.exceptions` with the crate name and
  the license being allowed, and must include a `reason` explaining why the
  dependency is required and why the license is acceptable.
- **Advisory exceptions** go under `advisories.ignore` with the advisory ID and
  must include a `reason` and a target removal date, so the exception is
  revisited rather than forgotten.

```toml
[licenses]
exceptions = [
    { name = "example-crate", allow = ["GPL-3.0"], reason = "Build-time only; not distributed" },
]

[advisories]
ignore = [
    { id = "RUSTSEC-0000-0000", reason = "Not reachable from our code paths", until = "2026-01-01" },
]
```

Exceptions are reviewed like any other code change; expect reviewers to ask
whether the dependency can be replaced or removed instead.

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

## Issue Lifecycle

Every contribution follows the same path from issue to merge:

1. **Find** an open, unassigned issue — start with
   [`good first issue`](https://github.com/Soroban-Pulse/SorobanPulse/issues?q=is%3Aissue+is%3Aopen+no%3Aassignee+label%3A%22good+first+issue%22)
   or [`help wanted`](https://github.com/Soroban-Pulse/SorobanPulse/issues?q=is%3Aissue+is%3Aopen+no%3Aassignee+label%3A%22help+wanted%22).
2. **Claim** it by commenting (see [Claiming an Issue](#claiming-an-issue)) and wait to be assigned.
3. **Build** the change on a branch from the latest `main` (see [Contribution Workflow](#contribution-workflow)).
4. **Open a PR** that follows the [Pull Request rules](#pull-requests).
5. **Review**: address feedback with new commits; a maintainer or area owner from
   [MAINTAINERS.md](MAINTAINERS.md) approves.
6. **Merge**: a maintainer squash-merges once required checks are green; the linked issue closes automatically.

### Claiming an Issue

- Comment on the issue asking to be assigned (e.g. "I'd like to work on this").
  Do not start work until a maintainer assigns you — unassigned work may collide with someone else's.
- An issue that already has an assignee is taken. Do not open a competing PR; ask in the issue if you think the claim is abandoned.
- **New contributors** (no merged PR yet) may hold **at most 2 assigned issues** at a time. Open a PR for one before claiming another.
- **7-day inactivity release**: if an assigned issue has no activity (comment, linked PR or push) for 7 days,
  the [Stale Claims](.github/workflows/stale-claims.yml) workflow unassigns it and adds `help wanted`.
  Post a short progress update to keep a claim alive; issues labelled `pinned` or `priority: critical` are exempt.
- If you can no longer work on an issue, comment so it can be reassigned.

### Issue Labels

Labels are defined in [`.github/labels.yml`](.github/labels.yml) and synced to the repository by the
[Sync Labels](.github/workflows/labels.yml) workflow. Add or change labels in that file, not in the GitHub UI.

| Label | Meaning |
|---|---|
| `bug`, `enhancement`, `documentation` | Type of change |
| `good first issue` | Small, well-scoped task with clear acceptance criteria; ideal for a first PR |
| `help wanted` | Maintainers welcome outside contributions; also applied when a stale claim is released |
| `difficulty: easy` / `medium` / `hard` | Expected effort and required domain knowledge |
| `priority: critical` | Production-impacting; reviewed first and exempt from the stale-claim release |
| `area: indexer`, `area: api`, `area: database`, `area: integrations`, `area: sdk`, `area: frontend`, `area: infra`, `area: docs` | The part of the codebase the issue touches; routes review to the owners in [MAINTAINERS.md](MAINTAINERS.md) |
| `stale`, `pinned` | Stale-claim workflow state; `pinned` opts an issue out |
| `security`, `fuzzing`, `automated` | Applied by automation (e.g. the nightly fuzz workflow) |

Every triaged issue should have one type label, one `area:` label and one `difficulty:` label.

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

- **One issue per PR.** Only bundle issues that are tightly related (e.g. the same bug reported twice,
  or changes that cannot land separately) and explain why in the description. Unrelated bundles will be asked to split.
- **Link every issue** with `Closes #N` in the PR description — one line per issue — so it closes on merge.
  Only open a PR for issues you are assigned to.
- **Required checks** must be green before merge: `Format`, `Clippy`, `cargo-deny`, `Check Migrations`,
  `sqlx prepare --check` and `Test (rust stable, …)` from [CI](.github/workflows/ci.yml),
  plus any area-specific workflow your change triggers (e.g. `Dashboard Build`, `Validate SDKs`).
  Run `make test` and `make lint` locally first.
- Fill in every section of the [PR template](.github/pull_request_template.md).
- Use Conventional Commits for all commit messages in the PR.
- Put PR summaries and implementation reports in the PR description, not in new files at the repository root. Only `README`, `CONTRIBUTING`, `CHANGELOG`, `LICENSE`, `SECURITY`, `CODE_OF_CONDUCT` and other standard project files belong there; user guides go in `docs/guides/`, design specs in `docs/rfcs/`.
- Respond to review feedback within 7 days; PRs left without a response may be closed and the issue released.

### Path-Filtered CI

CI jobs use path filters to reduce wall-clock time. Only jobs relevant to your changes will run:

| Path Changed | Triggered Jobs |
|--------------|----------------|
| `src/**`, `tests/**`, `Cargo.*` | Rust tests, clippy, format |
| `sdk/typescript/**` | TypeScript SDK CI only |
| `sdk/python/**` | Python SDK CI only |
| `terraform/**` | Terraform validation only |
| `migrations/**` | Migration checks only |

The `ci-success` job always runs to keep branch protection requirements stable, even when specific jobs are skipped.

## Releases

See [RELEASE.md](RELEASE.md) for the complete release process, including:
- Version numbering (Semantic Versioning)
- Updating CHANGELOG.md
- Creating git tags and GitHub releases
- Docker image publishing

Only maintainers can cut releases. If you'd like to propose a release, open an issue or contact the maintainers.
