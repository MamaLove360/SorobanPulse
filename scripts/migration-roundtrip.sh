#!/bin/bash
# Migration up/down round-trip check (issue #1152).
#
# 1. Every up migration must have a matching .down.sql file.
# 2. Per migration: apply N, revert N, apply N. The revert must restore the
#    schema from before N and the reapply must reproduce the schema after N.
# 3. Full round trip: apply all, revert all (must leave an empty schema),
#    apply all again (must match the first apply).
#
# Schemas are compared with `pg_dump --schema-only`. `_sqlx_migrations` is
# excluded. Files starting with "-- no-transaction" run outside a transaction,
# matching SQLx.
#
# Connection uses the standard libpq env vars (PGHOST, PGPORT, PGUSER,
# PGPASSWORD). Scratch databases are dropped and recreated on every run.
#
# Usage: scripts/migration-roundtrip.sh [--check-only] [migrations-dir]
#
# --check-only validates that every up migration has a matching .down.sql file
# without connecting to a database. CI uses it as a fast pre-check.

set -euo pipefail

CHECK_ONLY=0
MIGRATIONS_DIR=""
for arg in "$@"; do
    case "$arg" in
        --check-only) CHECK_ONLY=1 ;;
        -h|--help)
            echo "Usage: $0 [--check-only] [migrations-dir]"
            exit 0
            ;;
        -*)
            echo "ERROR: unknown option: $arg" >&2
            exit 2
            ;;
        *)
            if [ -n "$MIGRATIONS_DIR" ]; then
                echo "ERROR: unexpected argument: $arg" >&2
                exit 2
            fi
            MIGRATIONS_DIR="$arg"
            ;;
    esac
done
MIGRATIONS_DIR="${MIGRATIONS_DIR:-migrations}"
STEP_DB="migration_roundtrip_step"
FULL_DB="migration_roundtrip_full"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# Prints a GitHub annotation in CI and a plain error locally, then exits.
die() {
    local file="$1" message="$2" diff_file="${3:-}"
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::error file=$file::$message"
    else
        echo "ERROR: $file: $message" >&2
    fi
    if [ -n "$diff_file" ] && [ -s "$diff_file" ]; then
        echo "Schema diff (expected vs actual):"
        cat "$diff_file"
    fi
    exit 1
}

admin_sql() {
    psql -X -q -v ON_ERROR_STOP=1 -d postgres -c "$1" >/dev/null
}

recreate_db() {
    admin_sql "DROP DATABASE IF EXISTS \"$1\""
    admin_sql "CREATE DATABASE \"$1\""
}

run_file() {
    local db="$1" file="$2"
    local args=(-X -q -v ON_ERROR_STOP=1 -d "$db" -f "$file")
    if ! head -n 1 "$file" | grep -q -- '-- no-transaction'; then
        args+=(--single-transaction)
    fi
    if ! psql "${args[@]}" >"$WORK_DIR/psql.log" 2>&1; then
        cat "$WORK_DIR/psql.log"
        return 1
    fi
}

# Strips comments, SET lines and pg_dump's per-run \restrict tokens so two
# dumps of the same schema compare equal.
dump_schema() {
    pg_dump --schema-only --no-owner --no-privileges \
        --exclude-table=_sqlx_migrations -d "$1" \
        | grep -v -E '^(--|SET |SELECT pg_catalog\.set_config|\\(un)?restrict )' \
        | sed '/^$/d' >"$2"
}

down_for() {
    echo "${1%.sql}.down.sql"
}

# === Discover migrations

mapfile -t UPS < <(find "$MIGRATIONS_DIR" -maxdepth 1 -name '*.sql' ! -name '*.down.sql' | sort)

if [ "${#UPS[@]}" -eq 0 ]; then
    die "$MIGRATIONS_DIR" "no migrations found"
fi

MISSING=()
for up in "${UPS[@]}"; do
    [ -f "$(down_for "$up")" ] || MISSING+=("$up")
done

if [ "${#MISSING[@]}" -gt 0 ]; then
    for up in "${MISSING[@]}"; do
        if [ -n "${GITHUB_ACTIONS:-}" ]; then
            echo "::error file=$up::missing down migration $(down_for "$up")"
        else
            echo "ERROR: $up: missing down migration $(down_for "$up")" >&2
        fi
    done
    echo "${#MISSING[@]} migration(s) have no down migration"
    exit 1
fi

echo "Checking ${#UPS[@]} migrations"

if [ "$CHECK_ONLY" -eq 1 ]; then
    echo "✓ All ${#UPS[@]} migrations have a matching down migration"
    exit 0
fi

# === Per migration: apply N, revert N, apply N

recreate_db "$STEP_DB"
dump_schema "$STEP_DB" "$WORK_DIR/before.sql"

for up in "${UPS[@]}"; do
    down="$(down_for "$up")"
    echo "--- $(basename "$up")"

    run_file "$STEP_DB" "$up" || die "$up" "up migration failed"
    dump_schema "$STEP_DB" "$WORK_DIR/after.sql"

    if ! run_file "$STEP_DB" "$down"; then
        die "$down" "down migration failed while reverting $(basename "$up")"
    fi
    dump_schema "$STEP_DB" "$WORK_DIR/reverted.sql"
    diff -u "$WORK_DIR/before.sql" "$WORK_DIR/reverted.sql" >"$WORK_DIR/diff.txt" \
        || die "$down" "down migration does not restore the schema from before $(basename "$up")" "$WORK_DIR/diff.txt"

    run_file "$STEP_DB" "$up" || die "$up" "up migration failed when reapplied after its down migration"
    dump_schema "$STEP_DB" "$WORK_DIR/reapplied.sql"
    diff -u "$WORK_DIR/after.sql" "$WORK_DIR/reapplied.sql" >"$WORK_DIR/diff.txt" \
        || die "$up" "reapplying after a revert produced a different schema" "$WORK_DIR/diff.txt"

    mv "$WORK_DIR/after.sql" "$WORK_DIR/before.sql"
done

# === Full round trip: up all, down all, up all

recreate_db "$FULL_DB"
dump_schema "$FULL_DB" "$WORK_DIR/empty.sql"

for up in "${UPS[@]}"; do
    run_file "$FULL_DB" "$up" || die "$up" "up migration failed on a clean database"
done
dump_schema "$FULL_DB" "$WORK_DIR/first.sql"

for ((i = ${#UPS[@]} - 1; i >= 0; i--)); do
    down="$(down_for "${UPS[$i]}")"
    if ! run_file "$FULL_DB" "$down"; then
        die "$down" "down migration failed while reverting all migrations (reverting $(basename "${UPS[$i]}"))"
    fi
done
dump_schema "$FULL_DB" "$WORK_DIR/reverted.sql"
diff -u "$WORK_DIR/empty.sql" "$WORK_DIR/reverted.sql" >"$WORK_DIR/diff.txt" \
    || die "$MIGRATIONS_DIR" "schema is not empty after reverting all migrations" "$WORK_DIR/diff.txt"

for up in "${UPS[@]}"; do
    run_file "$FULL_DB" "$up" || die "$up" "up migration failed when reapplied after a full revert"
done
dump_schema "$FULL_DB" "$WORK_DIR/second.sql"
diff -u "$WORK_DIR/first.sql" "$WORK_DIR/second.sql" >"$WORK_DIR/diff.txt" \
    || die "$MIGRATIONS_DIR" "schema after up, down, up differs from a clean up" "$WORK_DIR/diff.txt"

admin_sql "DROP DATABASE IF EXISTS \"$STEP_DB\""
admin_sql "DROP DATABASE IF EXISTS \"$FULL_DB\""

echo "✓ All ${#UPS[@]} migrations round-trip cleanly"
