# Backup Verification

Soroban Pulse runs automated backup verification nightly via `.github/workflows/backup-verify.yml`.

## What Is Tested

The workflow spins up the stack defined in the current `docker-compose.yml` (Postgres plus the application services), seeds a 10 000-row dataset, and then performs a full backup/restore round-trip:

| Check | Description |
|-------|-------------|
| Encrypted backup creation | Confirms `scripts/backup.sh` produces a `.dump.gpg` file |
| GPG encryption verification | Validates the backup file is genuinely GPG-encrypted |
| Database drop | Drops the database after the backup to prove the restore is self-sufficient |
| Full restore | Runs `scripts/restore.sh` against the emptied Postgres instance |
| Row count match | Compares `COUNT(*)` between the pre-backup and restored DB |
| Restore timing assertion | Fails the job if the restore of the 10 000-row dataset takes 60 seconds or more |
| Timing metrics | Records backup and restore durations for RTO tracking |

## Schedule

Runs nightly at 02:00 UTC (`cron: '0 2 * * *'`) and on `workflow_dispatch` for manual triggers.

## Failure Notifications

When a scheduled run fails, GitHub Actions sends a failure notification (email) to the team via the repository's default notifications. Ensure your GitHub notification settings are configured to receive workflow failure alerts for this repository.

## Manual Trigger

```bash
gh workflow run backup-verify.yml
```

## Local Verification

```bash
# Set up two local Postgres instances, then:
DATABASE_URL="postgres://..." BACKUP_ENCRYPTION_KEY="..." BACKUP_DEST=/tmp/bk bash scripts/backup.sh
DATABASE_URL="postgres://..." BACKUP_ENCRYPTION_KEY="..." bash scripts/restore.sh /tmp/bk/<file>.dump.gpg
```

## Interpreting Failures

| Symptom | Likely cause |
|---------|-------------|
| No `.dump.gpg` file | `pg_dump` failed or GPG not installed |
| GPG verification fails | Wrong `BACKUP_ENCRYPTION_KEY` or corrupt file |
| Row count mismatch | Restore truncated; check `pg_restore` output |
| Restore timing assertion failed | Restore took 60s or more for the 10 000-row dataset; check DB resources |
| Checksum mismatch | Data corruption during encrypt/decrypt cycle |

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `BACKUP_ENCRYPTION_KEY` | Passphrase for GPG symmetric encryption |
| `BACKUP_DEST` | Directory where backup files are written |
| `DATABASE_URL` | Source database connection string |
