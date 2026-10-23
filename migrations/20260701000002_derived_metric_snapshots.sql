-- Derived metrics framework: stores historical snapshots of computed metric values.
-- See .kiro/specs/derived-metrics-framework/requirements.md (#1262).

CREATE TABLE IF NOT EXISTS derived_metric_snapshots (
    id BIGSERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    value DOUBLE PRECISION NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookups of the most recent snapshots for a given metric.
CREATE INDEX IF NOT EXISTS idx_derived_metric_snapshots_name_evaluated_at
    ON derived_metric_snapshots (metric_name, evaluated_at DESC);

-- Supports label-filtered queries (e.g. per-address or per-contract breakdowns).
CREATE INDEX IF NOT EXISTS idx_derived_metric_snapshots_labels
    ON derived_metric_snapshots USING GIN (labels);