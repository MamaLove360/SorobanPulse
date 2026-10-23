-- Migration to add partition pruning capability
-- Implements automated dropping of old monthly partitions based on data retention policy

-- Create function to drop old partitions
-- This function is designed to be called on a schedule (e.g., daily) by src/pruner.rs
CREATE OR REPLACE FUNCTION drop_old_partitions(
    retain_months INT DEFAULT 3,
    dry_run BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    partition_name TEXT,
    dropped BOOLEAN,
    reason TEXT
) AS $$
DECLARE
    partition_record RECORD;
    cutoff_date TIMESTAMP;
    partition_count_dropped INT := 0;
    partition_count_checked INT := 0;
BEGIN
    -- Calculate cutoff date: today minus (retain_months * 30) days
    cutoff_date := CURRENT_DATE - (retain_months * INTERVAL '30 days');
    
    -- Iterate over all partitions of the events table
    FOR partition_record IN
        SELECT
            schemaname,
            tablename,
            -- Extract the partition date from the table name (e.g., events_2026_01 -> 2026-01-01)
            TO_DATE(
                RIGHT(tablename, 7) || '_01',
                'YYYY_MM_DD'
            ) AS partition_date
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE 'events_%'
          AND tablename != 'events' -- exclude the parent table
        ORDER BY tablename DESC
    LOOP
        partition_count_checked := partition_count_checked + 1;
        
        -- Check if this partition is older than the cutoff date
        IF partition_record.partition_date < cutoff_date THEN
            IF NOT dry_run THEN
                -- Execute the drop command
                EXECUTE format(
                    'DROP TABLE IF EXISTS %I.%I',
                    partition_record.schemaname,
                    partition_record.tablename
                );
                partition_count_dropped := partition_count_dropped + 1;
                RETURN QUERY SELECT
                    partition_record.tablename,
                    TRUE,
                    'Dropped (older than ' || retain_months || ' months)';
            ELSE
                -- In dry-run mode, just report what would be dropped
                RETURN QUERY SELECT
                    partition_record.tablename,
                    FALSE,
                    '[DRY RUN] Would drop (older than ' || retain_months || ' months)';
            END IF;
        ELSE
            RETURN QUERY SELECT
                partition_record.tablename,
                FALSE,
                'Retained (within ' || retain_months || ' month retention window)';
        END IF;
    END LOOP;
    
    -- Log summary to notice
    IF NOT dry_run THEN
        RAISE NOTICE 'Partition pruning completed: % partitions checked, % partitions dropped, cutoff date: %',
            partition_count_checked, partition_count_dropped, cutoff_date;
    ELSE
        RAISE NOTICE '[DRY RUN] Partition pruning simulation: % partitions checked, % would be dropped, cutoff date: %',
            partition_count_checked, partition_count_dropped, cutoff_date;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create metric tracking table for partition drops
CREATE TABLE IF NOT EXISTS partition_drops_log (
    id BIGSERIAL PRIMARY KEY,
    dropped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    partition_name TEXT NOT NULL,
    retained_months INT NOT NULL,
    cutoff_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_partition_drops_dropped_at (dropped_at DESC)
);

-- Verify that events table has monthly partitions (should already exist from migration 20260727000102)
-- If not present, this migration will fail
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'events_%'
    ) THEN
        RAISE EXCEPTION 'No monthly event partitions found. Run migration 20260727000102_partition_events_by_month.sql first.';
    END IF;
END
$$;
