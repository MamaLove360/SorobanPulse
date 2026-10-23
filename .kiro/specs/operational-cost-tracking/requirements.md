# Requirements Document

## Implementation Notes

- **Cost model:** `src/costs/calculator.rs` converts recorded metrics (RPC calls, DB queries, notification deliveries) into monetary costs using configurable per-unit rates; unset rates default to zero with a warning.
- **Snapshot cadence:** hourly aggregation persisted to the `cost_snapshots` table; retention of at least 90 days.
- **Reporting:** `GET /v1/admin/costs/report` returns a breakdown by component (indexer, webhook delivery, email) and by Resource_Type.
- **Forecasting:** `src/costs/forecast.rs` applies a linear model over the most recent 14 days of snapshots; forecasts must be within 20% of actuals on a 24-hour window.

## Introduction

This feature adds comprehensive operational cost tracking to the Soroban Pulse system. The goal is to measure, aggregate, and report on the costs of running infrastructure components including database operations, compute resources, RPC calls, and other services. This enables infrastructure cost optimization, budget forecasting, and per-component cost attribution.

## Glossary

- **Cost_Tracker**: The subsystem responsible for recording and aggregating operational costs
- **Database_Operations**: PostgreSQL read/write queries, connection pool usage, and storage costs
- **Compute_Resources**: CPU time, memory usage, and processing costs for indexing and API operations
- **RPC_Calls**: Stellar Soroban RPC endpoint requests made by the indexer
- **Cost_Period**: A time window (hourly, daily, monthly) for cost aggregation
- **Resource_Type**: A category of infrastructure resource (database, compute, network, storage)
- **Cost_Metric**: A specific measurable cost dimension (query_count, bytes_transferred, cpu_seconds)

## Requirements

### Requirement 1: Database Cost Tracking

**User Story:** As a platform operator, I want to track database operational costs, so that I can understand and optimize database spending.

#### Acceptance Criteria

1. WHEN a database query is executed, THE Cost_Tracker SHALL record the query type, execution time, and rows affected
2. WHEN a database connection is acquired from the pool, THE Cost_Tracker SHALL record the connection duration
3. THE Cost_Tracker SHALL calculate storage costs based on table sizes and index sizes
4. WHEN database metrics are aggregated for a Cost_Period, THE Cost_Tracker SHALL include read operations, write operations, and storage bytes
5. THE Cost_Tracker SHALL track costs separately for the primary database pool and the read replica pool

### Requirement 2: Compute Cost Tracking

**User Story:** As a platform operator, I want to track compute resource costs, so that I can identify expensive operations and optimize resource allocation.

#### Acceptance Criteria

1. WHEN the indexer processes a ledger, THE Cost_Tracker SHALL record the CPU time and memory delta
2. WHEN an HTTP request is handled, THE Cost_Tracker SHALL record the request processing time and response size
3. THE Cost_Tracker SHALL track memory usage changes during event parsing and serialization
4. THE Cost_Tracker SHALL aggregate compute costs by component (indexer, API handlers, background tasks)
5. WHERE compression is enabled, THE Cost_Tracker SHALL record compression CPU time separately

### Requirement 3: RPC Call Cost Tracking

**User Story:** As a platform operator, I want to track RPC call costs, so that I can understand the cost of polling Stellar Soroban RPC.

#### Acceptance Criteria

1. WHEN an RPC call is made to the Stellar Soroban RPC endpoint, THE Cost_Tracker SHALL record the call type, response size, and latency
2. THE Cost_Tracker SHALL count successful RPC calls and failed RPC calls separately
3. THE Cost_Tracker SHALL calculate bandwidth costs based on request and response payload sizes
4. WHEN RPC rate limiting or retries occur, THE Cost_Tracker SHALL attribute the additional cost appropriately
5. THE Cost_Tracker SHALL track RPC costs per Cost_Period

### Requirement 4: Cost Breakdown by Resource

**User Story:** As a platform operator, I want to see cost breakdowns by resource type, so that I can identify the most expensive components.

#### Acceptance Criteria

1. THE Cost_Tracker SHALL provide cost totals grouped by Resource_Type
2. THE Cost_Tracker SHALL provide cost totals grouped by component (indexer, API server, webhooks)
3. WHEN a cost report is generated, THE Cost_Tracker SHALL include per-resource cost percentages
4. THE Cost_Tracker SHALL support filtering cost data by time range
5. THE Cost_Tracker SHALL expose cost metrics in the Prometheus `/metrics` endpoint

### Requirement 5: Cost Report Generation

**User Story:** As a platform operator, I want to generate cost reports, so that I can analyze spending trends and budget accurately.

#### Acceptance Criteria

1. THE Cost_Tracker SHALL provide an HTTP endpoint to retrieve cost summaries for a specified Cost_Period
2. WHEN a cost report is requested, THE Cost_Tracker SHALL return costs in JSON format with Resource_Type breakdown
3. THE Cost_Tracker SHALL support querying cost data for hourly, daily, and monthly periods
4. THE Cost_Tracker SHALL include cumulative costs and period-over-period comparison in reports
5. WHERE cost data is unavailable for a requested period, THE Cost_Tracker SHALL return an empty report with appropriate metadata

### Requirement 6: Cost Forecast Functionality

**User Story:** As a platform operator, I want to forecast future costs, so that I can plan infrastructure budgets.

#### Acceptance Criteria

1. THE Cost_Tracker SHALL calculate a 7-day rolling average cost per Resource_Type
2. WHEN a forecast is requested, THE Cost_Tracker SHALL project costs for the next 30 days based on historical trends
3. THE Cost_Tracker SHALL use linear regression on the most recent 14 days of cost data for forecasting
4. THE Cost_Tracker SHALL include confidence intervals or variance indicators in forecast data
5. IF insufficient historical data exists (less than 7 days), THEN THE Cost_Tracker SHALL return an error indicating insufficient data for forecasting

### Requirement 7: Cost Data Persistence

**User Story:** As a platform operator, I want cost data to be persisted, so that I can query historical cost information.

#### Acceptance Criteria

1. THE Cost_Tracker SHALL store cost metrics in PostgreSQL with timestamp, Resource_Type, and cost value
2. THE Cost_Tracker SHALL aggregate and persist cost data at hourly intervals
3. THE Cost_Tracker SHALL maintain cost history for at least 90 days
4. WHEN the 90-day retention period is exceeded, THE Cost_Tracker SHALL archive or delete old cost records
5. THE Cost_Tracker SHALL use a separate database table optimized for time-series queries

### Requirement 8: Cost Configuration

**User Story:** As a platform operator, I want to configure cost rates, so that cost calculations reflect actual infrastructure pricing.

#### Acceptance Criteria

1. THE Cost_Tracker SHALL read cost rates from environment variables or configuration file
2. THE Cost_Tracker SHALL support configuring cost per database query, cost per compute second, and cost per RPC call
3. WHERE a cost rate is not configured, THE Cost_Tracker SHALL use a default value of zero and log a warning
4. THE Cost_Tracker SHALL allow cost rates to be updated without restarting the service
5. WHEN cost rates are changed, THE Cost_Tracker SHALL apply new rates to newly recorded metrics only (not retroactively)

### Requirement 9: Cost Metrics API

**User Story:** As a platform operator, I want to query cost metrics via API, so that I can integrate cost data with external monitoring and billing systems.

#### Acceptance Criteria

1. THE Cost_Tracker SHALL expose a `GET /v1/costs` endpoint returning cost summary for a time range
2. THE Cost_Tracker SHALL expose a `GET /v1/costs/forecast` endpoint returning projected costs
3. WHEN an invalid time range is provided, THE Cost_Tracker SHALL return HTTP 400 with a descriptive error message
4. THE Cost_Tracker SHALL support optional query parameters for filtering by Resource_Type
5. THE Cost_Tracker SHALL require API key authentication for cost endpoints when API_KEY is configured
