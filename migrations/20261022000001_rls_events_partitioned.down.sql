-- Revert Issue #1344: restore the pre-#1344 RLS state.
DO $$
DECLARE
    part regclass;
BEGIN
    FOR part IN
        SELECT inhrelid::regclass
          FROM pg_inherits
         WHERE inhparent = 'events'::regclass
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %s', part);
        EXECUTE format('ALTER TABLE %s DISABLE ROW LEVEL SECURITY', part);
    END LOOP;
END;
$$;

DROP POLICY IF EXISTS tenant_isolation ON events;
ALTER TABLE events DISABLE ROW LEVEL SECURITY;

-- events_legacy goes back to the original 20260527000001 policy.
DO $$
BEGIN
    IF to_regclass('events_legacy') IS NOT NULL THEN
        DROP POLICY IF EXISTS tenant_isolation ON events_legacy;
        CREATE POLICY tenant_isolation ON events_legacy
            USING (
                tenant_id IS NULL
                OR current_setting('app.current_tenant_id', TRUE) = ''
                OR tenant_id = current_setting('app.current_tenant_id', TRUE)
            );
    END IF;
END;
$$;

-- Restore auto_create_partitions() from 20260729000101 (without the RLS hook)
-- before dropping the helper it calls.
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
            RETURN QUERY SELECT pname::TEXT, 'created'::TEXT;
        EXCEPTION WHEN duplicate_table THEN
            RETURN QUERY SELECT pname::TEXT, 'exists'::TEXT;
        END;

        cur_date := cur_date + INTERVAL '1 month';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS apply_events_partition_rls(regclass);
