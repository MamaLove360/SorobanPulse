# Development Environment Setup

A reference for configuring a full local development environment for Soroban Pulse: OS-specific toolchain installation, editor/IDE configuration, pre-commit hooks, database setup, debugging tools, test environments, and performance profiling.

New to the project? Start with [docs/onboarding.md](onboarding.md) for the day-1 checklist (clone, build, run, first test pass). This guide is the deeper reference for configuring the environment behind each of those steps and for tooling that onboarding doesn't cover — IDE setup, debuggers, profilers.

## Table of Contents

- [OS-Specific Setup](#os-specific-setup)
- [IDE / Editor Configuration](#ide--editor-configuration)
- [Pre-commit Hooks](#pre-commit-hooks)
- [Database Setup](#database-setup)
- [Debugging Tools](#debugging-tools)
- [Test Environment Setup](#test-environment-setup)
- [Performance Profiling Setup](#performance-profiling-setup)

---

## OS-Specific Setup

All platforms need: the Rust stable toolchain, Docker (for Postgres and integration tests), and `lefthook` (pre-commit hooks). The project does not pin a Rust version via `rust-toolchain.toml`, so `rustup update stable` picks up whatever the CI image uses.

### macOS

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

# Docker Desktop
brew install --cask docker
open -a Docker   # start the Docker daemon

# Postgres client (psql) for manual inspection — the server itself runs in Docker
brew install libpq && brew link --force libpq

# lefthook (pre-commit hooks)
brew install lefthook
```

### Linux (Debian/Ubuntu)

```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

# Build dependencies (needed for some crate native deps)
sudo apt-get update && sudo apt-get install -y build-essential pkg-config libssl-dev postgresql-client

# Docker Engine — follow https://docs.docker.com/engine/install/ubuntu/ for your distro,
# then add your user to the docker group so you don't need sudo for every command:
sudo usermod -aG docker $USER && newgrp docker

# lefthook (via cargo, no native package on most distros)
cargo install lefthook
```

Other distros: swap `apt-get` for your package manager (`dnf`, `pacman`, `zypper`); package names are the same or close (`openssl-devel` on Fedora, for example).

### Windows

Native Windows builds work, but WSL2 with an Ubuntu distro is the path most contributors use day to day — it matches CI (Linux) most closely and avoids path/line-ending friction with the shell scripts under `scripts/`.

**WSL2 (recommended):**
```powershell
wsl --install -d Ubuntu
```
Then open the Ubuntu shell and follow the [Linux](#linux-debianubuntu) steps above. Install Docker Desktop for Windows and enable **Settings → Resources → WSL Integration** for your distro instead of installing Docker inside WSL directly.

**Native Windows (if you must):**
- Install Rust via [rustup-init.exe](https://rustup.rs) — choose the MSVC toolchain (`stable-x86_64-pc-windows-msvc`) and install the "Desktop development with C++" Visual Studio Build Tools workload it prompts for.
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) with the WSL2 backend.
- Use PowerShell or Git Bash for the `make` targets — plain `cmd.exe` will not run the Makefile; install `make` via `choco install make` or run the underlying commands directly (see [Makefile](../Makefile) for what each target expands to).
- Line endings: set `git config --global core.autocrlf input` before cloning to avoid CRLF diffs in shell scripts.

### Verify the toolchain

Regardless of OS:
```bash
rustc --version   # stable, matches CI
docker --version
docker compose version
cargo install lefthook   # if not already installed via a package manager
```

---

## IDE / Editor Configuration

### VS Code

Install the [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer) extension — do not use the older deprecated "Rust" extension. Recommended `.vscode/settings.json` additions for this repo:

```json
{
  "rust-analyzer.check.command": "clippy",
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.cargo.extraEnv": {
    "SQLX_OFFLINE": "true"
  },
  "editor.formatOnSave": true,
  "[rust]": {
    "editor.defaultFormatter": "rust-lang.rust-analyzer"
  }
}
```

`SQLX_OFFLINE=true` matters here specifically: without it, rust-analyzer's background `cargo check` runs will try to connect to `DATABASE_URL` to validate `sqlx::query!` macros every time it re-checks the project, which is slow and fails outright if Postgres isn't running. See [Why does `cargo build` need a database?](onboarding.md#why-does-cargo-build-need-a-database) in the onboarding guide for the underlying cause; run `cargo sqlx prepare` after changing a compile-time-checked query so the offline cache (`.sqlx/`) stays in sync.

Useful extra extensions:
- **Even Better TOML** — for `Cargo.toml`, `mutants.toml`, `deny.toml`
- **SQLTools** (with the PostgreSQL driver) — browse the schema without leaving the editor
- **Docker** (Microsoft) — manage the Compose stack from the sidebar

A minimal `.vscode/launch.json` for debugging the server (requires the [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb) extension — see [Debugging Tools](#debugging-tools)):

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug soroban-pulse",
      "cargo": { "args": ["build", "--bin=soroban-pulse"] },
      "env": { "RUST_LOG": "debug" },
      "envFile": "${workspaceFolder}/.env",
      "cwd": "${workspaceFolder}"
    }
  ]
}
```

### Vim / Neovim

Use rust-analyzer via any LSP client. With built-in Neovim LSP (`nvim-lspconfig`):

```lua
require('lspconfig').rust_analyzer.setup({
  settings = {
    ['rust-analyzer'] = {
      check = { command = "clippy" },
      cargo = {
        extraEnv = { SQLX_OFFLINE = "true" },
      },
    },
  },
})
```

For debugging, [nvim-dap](https://github.com/mfussenegger/nvim-dap) with the `codelldb` adapter mirrors the VS Code `launch.json` above. Format-on-save can be wired through `conform.nvim` calling `rustfmt` (the project's `rustfmt.toml` is picked up automatically since it lives at the repo root).

### IntelliJ IDEA / RustRover

RustRover has first-class Rust support out of the box; for IntelliJ IDEA install the **Rust** plugin from the marketplace.

- **Settings → Languages & Frameworks → Rust**: point "External linter" at `clippy` to get inline clippy warnings matching CI.
- **Settings → Languages & Frameworks → Rust → Environment variables** (per run configuration): add `SQLX_OFFLINE=true` for the same reason as VS Code above.
- Enable **"Use rustfmt instead of built-in formatter"** so formatting matches `rustfmt.toml` exactly.
- Add a **PostgreSQL data source** (Database tool window) pointed at your local `DATABASE_URL` to browse tables and run ad-hoc queries against the schema described in [docs/schema.md](schema.md).

---

## Pre-commit Hooks

Hooks are managed by [lefthook](https://github.com/evilmartians/lefthook) and configured in [lefthook.yml](../lefthook.yml) at the repo root — they run `cargo fmt --check`, `cargo clippy`, and `cargo check` before every commit, catching most CI failures locally.

```bash
# Install lefthook itself — see OS-specific instructions above
lefthook install   # registers the git hooks in this clone
```

Run the same checks manually without committing:
```bash
lefthook run pre-commit
```

If a hook fails: fix the reported issue (usually `cargo fmt` or a clippy lint) and re-commit — hooks run again automatically. To skip hooks for a single commit in a genuine emergency, use `git commit --no-verify`, but treat this as exceptional; CI runs the same checks and will reject the push regardless.

Hooks complete in well under 30 seconds on a typical incremental change. If they're consistently slow, confirm `SQLX_OFFLINE=true` is set (see above) — otherwise `cargo check` inside the hook is hitting the database on every commit.

---

## Database Setup

Two supported paths: Docker (recommended for day-to-day development) or a native PostgreSQL install.

### Docker (recommended)

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/soroban_pulse
cargo sqlx migrate run --source migrations
```

`make docker-up` starts the full stack (app + Postgres) instead of just the database, if you want the server running in a container too rather than via `cargo run`.

### Native PostgreSQL

Install Postgres 14+ for your OS (`brew install postgresql@14`, `apt-get install postgresql`, or the Windows installer from postgresql.org), then:

```bash
createdb soroban_pulse
export DATABASE_URL=postgres://<user>:<password>@localhost:5432/soroban_pulse
cargo sqlx migrate run --source migrations
```

### Verifying the setup

```bash
psql $DATABASE_URL -c "SELECT 1;"
psql $DATABASE_URL -c "\dt"   # list tables — should show events, subscriptions, delivery_logs, etc.
```

If this fails, see [docs/troubleshooting.md § DATABASE_URL connection refused](troubleshooting.md#database_url-connection-refused) and [§ Migrations fail on startup](troubleshooting.md#migrations-fail-on-startup).

### Isolated test database

Don't reuse your development database for tests — `make test-db` provisions a disposable Postgres container specifically for the test suite (see [Test Environment Setup](#test-environment-setup) below), so there's no risk of test data polluting your local dev data or vice versa.

### Schema reference

See [docs/schema.md](schema.md) for the full table structure, indexes, and an ER diagram, and [docs/database-configuration-tuning.md](database-configuration-tuning.md) for connection pool and `postgresql.conf` tuning once the basic setup works.

---

## Debugging Tools

### Interactive debuggers

| Platform | Debugger | Notes |
|----------|----------|-------|
| macOS / Linux | [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb) (VS Code) or raw `lldb` | Works with the `launch.json` in [IDE Configuration](#vs-code) above |
| Linux | `gdb` | `rust-gdb target/debug/soroban-pulse` gives Rust-aware pretty-printing |
| Windows (MSVC toolchain) | Visual Studio debugger or CodeLLDB | `cargo build` then attach, or use the VS Code launch config |

Command-line session with `rust-gdb`:
```bash
cargo build
rust-gdb target/debug/soroban-pulse
(gdb) break src/indexer.rs:120
(gdb) run
```

### Logging as a debugging tool

Most day-to-day debugging in this codebase happens through `tracing`, not a step debugger — the indexer and request handlers are async, and stepping through `.await` points is often less useful than targeted log output.

```bash
# Trace one module in isolation
RUST_LOG=soroban_pulse::indexer=debug,info cargo run

# Structured JSON output for piping into jq or an aggregator
RUST_LOG_FORMAT=json RUST_LOG=debug cargo run 2>&1 | jq 'select(.target | startswith("soroban_pulse"))'
```

See [docs/troubleshooting.md § Logging Configuration](troubleshooting.md#logging-configuration) and [docs/logging.md](logging.md) for the full field conventions.

### Database query debugging

```bash
# Enable slow query logging above 200ms
SLOW_QUERY_THRESHOLD_MS=200 cargo run

# Inspect what Postgres itself thinks is slow
psql $DATABASE_URL -c "SELECT query, mean_exec_time, calls FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;"
```

### Inspecting the running service

```bash
curl http://localhost:3000/healthz/ready | jq .
curl http://localhost:3000/metrics | grep soroban_pulse_indexer
```

### Tracing distributed requests

For request-level tracing across the indexer and HTTP handlers, run with Zipkin locally:
```bash
make zipkin-up
make run-zipkin
# UI at http://localhost:9411
```

---

## Test Environment Setup

```bash
# Full suite against a disposable container — no local Postgres required
make test-db

# Against an already-running Postgres instance
export DATABASE_URL=postgres://<user>:<password>@localhost/<dbname>
cargo test

# Fast unit-only pass (no database needed for pure-logic tests)
cargo test --lib
```

`sqlx::test`-annotated integration tests create and tear down their own isolated schema per test automatically — you don't need to reset state between runs.

Other test surfaces available once the basic suite is green:

| Kind | Command | Purpose |
|------|---------|---------|
| Property-based tests | `cargo test --features proptest` (see [docs/property-based-testing.md](property-based-testing.md)) | Fuzz-style invariant checks over generated inputs |
| Mutation testing | `cargo mutants` (config in [mutants.toml](../mutants.toml)) | Measures whether tests actually catch injected bugs — see [docs/mutation-testing.md](mutation-testing.md) |
| Fuzzing | `make fuzz` | Targets input-validation boundaries — see [CONTRIBUTING.md § Fuzzing](../CONTRIBUTING.md#fuzzing) |
| API contract tests | see [docs/contract-testing.md](contract-testing.md) | Validates responses against the OpenAPI spec |
| End-to-end | `docker-compose.e2e.yml` | Full stack, black-box HTTP tests |

Run the narrowest suite that covers your change first (`cargo test --lib`, then `make test-db`) rather than the full matrix on every save.

---

## Performance Profiling Setup

### Micro-benchmarks (Criterion)

```bash
cargo bench --bench pagination     # requires no DB
cargo bench --bench db_queries     # requires DATABASE_URL
cargo bench --bench compression
```

Results land in `target/criterion/`; open `target/criterion/report/index.html` in a browser for the full HTML report with before/after comparisons across runs.

### CPU flamegraphs

**Linux:**
```bash
cargo install flamegraph
sudo cargo flamegraph --bin soroban-pulse
# open flamegraph.svg
```
`sudo` is required because flamegraph uses `perf` under the hood, which needs elevated privileges to read kernel performance counters on most distros. If you'd rather not run `cargo` as root, lower `/proc/sys/kernel/perf_event_paranoid` to `1` instead.

**macOS:** flamegraph uses `dtrace`, which requires disabling System Integrity Protection restrictions for it — most contributors profile on Linux (e.g. in a VM or CI) for this reason. Alternatively, use Instruments (bundled with Xcode): Product → Profile, choose the "Time Profiler" template, target the built binary in `target/debug/` or `target/release/`.

**Windows:** use [Windows Performance Analyzer](https://learn.microsoft.com/en-us/windows-hardware/test/wpt/windows-performance-analyzer) or profile inside WSL2 using the Linux instructions above.

### Memory profiling

```bash
# Linux: heaptrack gives a full allocation timeline
sudo apt-get install heaptrack heaptrack-gui
heaptrack target/debug/soroban-pulse
heaptrack_gui heaptrack.soroban-pulse.<pid>.zst

# Cross-platform: valgrind/massif (slower, very detailed)
valgrind --tool=massif target/debug/soroban-pulse
ms_print massif.out.<pid>
```

For a quick live signal without a dedicated profiler, watch `soroban_pulse_process_memory_bytes` on `/metrics` over time — see [docs/troubleshooting.md § Diagnose memory growth](troubleshooting.md#diagnose-memory-growth).

### Load testing

Once micro-benchmarks look healthy, validate end-to-end throughput against a running instance using the procedure in [docs/load-testing-runbook.md](load-testing-runbook.md).

### Where results feed back in

Compare new results against the interpretation guidance in [docs/performance-tuning.md § Benchmark Interpretation](performance-tuning.md#benchmark-interpretation) before concluding a change is a regression or an improvement — single-run noise on a shared or virtualized machine is common; re-run a few times.

---

## Related Documentation

- [Developer onboarding guide](onboarding.md) — day-1 checklist and first-build issues
- [CONTRIBUTING.md](../CONTRIBUTING.md) — workflow, commit conventions, migrations
- [Architecture Guide](architecture.md)
- [Database Schema](schema.md)
- [Performance Tuning Guide](performance-tuning.md)
- [Troubleshooting and Debugging Guide](troubleshooting.md)
