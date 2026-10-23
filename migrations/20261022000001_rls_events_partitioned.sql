-- Issue #1344: Close row-level security gaps on the events table.
--
-- Gaps found while adding real-database isolation tests:
--
-- 1. Policy lost on the live table. 20260527000001_rls_events.sql enabled
--    RLS on the original `events` table, but 20260727000102 renamed that
--    table to `events_legacy` and promoted the partitioned table to
--    `events`. The policy moved with the old table, so the live
--    (partitioned) `events` table had no RLS at all.
-- 2. NULL vs '' setting. `current_setting('app.current_tenant_id', TRUE)`
--    returns NULL when the GUC was never set in the session, but '' once it
--    has been set and reset. The old policy only treated '' as "unscoped",
--    so the same unscoped session saw different rows depending on history.
--    Both are now normalised with COALESCE.
-- 3. No WITH CHECK clause. A tenant-scoped session could INSERT or UPDATE
--    rows stamped with another tenant's ID (or NULL, which is visible to
--    every tenant). Writes are now restricted to the session's own tenant.
-- 4. Direct partition access. RLS on a partitioned parent is not applied
--    when a partition (e.g. events_2026_01) is queried directly, so every
--    existing partition gets the same policy. New partitions should be
--    passed to apply_events_partition_rls().
--
-- Superusers and roles with BYPASSRLS are unaffected, so admin operations
-- (backups, migrations, support queries) still see every row. The table
-- owner also bypasses RLS unless FORCE ROW LEVEL SECURITY is set; see
-- docs/multi-tenancy.md for the recommended role layout.

CREATE OR REPLACE FUNCTION apply_events_partition_rls(target regclass)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', target);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %s', target);
    EXECUTE format($policy$
        CREATE POLICY tenant_isolation ON %s
            USING (
                tenant_id IS NULL
                OR COALESCE(current_setting('app.current_tenant_id', TRUE), '') = ''
                OR tenant_id = current_setting('app.current_tenant_id', TRUE)
            )
            WITH CHECK (
                COALESCE(current_setting('app.current_tenant_id', TRUE), '') = ''
                OR tenant_id = current_setting('app.current_tenant_id', TRUE)
            )
    $policy$, target);
END;
$$;

COMMENT ON FUNCTION apply_events_partition_rls(regclass) IS
    'Issue #1344: enable RLS and the tenant_isolation policy on an events table or partition. Call for every newly created events partition.';

-- Parent (partitioned) table: governs every query issued against `events`.
SELECT apply_events_partition_rls('events'::regclass);

-- Existing partitions: governs direct partition access.
DO $$
DECLARE
    part regclass;
BEGIN
    FOR part IN
        SELECT inhrelid::regclass
          FROM pg_inherits
         WHERE inhparent = 'events'::regclass
    LOOP
        PERFORM apply_events_partition_rls(part);
    END LOOP;
END;
$$;

-- Legacy table: keep it consistent with the live table.
DO $$
BEGIN
    IF to_regclass('events_legacy') IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM information_schema.columns
            WHERE table_name = 'events_legacy' AND column_name = 'tenant_id'
       )
    THEN
        PERFORM apply_events_partition_rls('events_legacy'::regclass);
    END IF;
END;
$$;

-- Future partitions: auto_create_partitions() (20260729000101) now applies the
-- policy to each partition it creates.
CREATE OR REPLACE FUNCTION auto_create_partitions(months_ahead INT DEFAULT 3)
RETURNS TABLE(partition_name TEXT, action TEXT) AS $$
DECLARE
    cur_date   DATE;
    target_dt  DATE;
    yr         INT;
    mo         INT;
    pname      TEXT;
    start_dt   DATE;
    end_dt     DATE;
BEGIN
    cur_date  := DATE_TRUNC('month', NOW())::DATE;
    target_dt := cur_date + (months_ahead || ' months')::INTERVAL;

    WHILE cur_date <= target_dt LOOP
        yr    := EXTRACT(YEAR  FROM cur_date)::INT;
        mo    := EXTRACT(MONTH FROM cur_date)::INT;
        pname := 'events_' || LPAD(yr::TEXT, 4, '0') || '_' || LPAD(mo::TEXT, 2, '0');
        start_dt := cur_date;
        end_dt   := cur_date + INTERVAL '1 month';

        -- Create partition (no-op if already exists)
        BEGIN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
                pname, start_dt, end_dt
            );
            -- Issue #1344: new partitions inherit the tenant_isolation policy.
            PERFORM apply_events_partition_rls(pname::regclass);
            RETURN QUERY SELECT pname::TEXT, 'created'::TEXT;
        EXCEPTION WHEN duplicate_table THEN
            RETURN QUERY SELECT pname::TEXT, 'exists'::TEXT;
        END;

        cur_date := cur_date + INTERVAL '1 month';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
