-- Downgrade: remove partition pruning function and log table

DROP FUNCTION IF EXISTS drop_old_partitions(INT, BOOLEAN);
DROP TABLE IF EXISTS partition_drops_log;
