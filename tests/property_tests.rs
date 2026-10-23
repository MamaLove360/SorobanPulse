/// Property-based tests using proptest for edge case discovery.
/// These tests verify invariants and properties of core functions under random inputs.

use proptest::prelude::*;
use chrono::{DateTime, Utc, Duration};

// Import types from the main crate
// Note: These would be imported from the main library once integrated
// For now, we define them locally for testing purposes

/// Pagination parameters with validation logic
#[derive(Debug, Clone)]
struct PaginationParams {
    pub page: Option<i64>,
    pub limit: Option<i64>,
}

impl PaginationParams {
    pub fn offset(&self) -> i64 {
        let page = self.page.unwrap_or(1).max(1);
        (page - 1).saturating_mul(self.limit())
    }

    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }

    pub fn page(&self) -> i64 {
        self.page.unwrap_or(1).max(1)
    }
}

/// Timestamp validation function
fn validate_timestamp(ts: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(ts)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| format!("invalid timestamp format: {}", ts))
}

// ============================================================================
// PROPERTY STRATEGIES
// ============================================================================

/// Strategy for generating valid page numbers
fn page_strategy() -> impl Strategy<Value = Option<i64>> {
    prop_oneof![
        Just(None),
        Just(Some(0)),  // Edge case: 0
        Just(Some(1)),  // Boundary: 1
        (1i64..=1000).prop_map(Some),
        (1001i64..=i64::MAX).prop_map(Some),
    ]
}

/// Strategy for generating valid limit numbers
fn limit_strategy() -> impl Strategy<Value = Option<i64>> {
    prop_oneof![
        Just(None),
        Just(Some(-1)),  // Negative edge case
        Just(Some(0)),   // Zero edge case
        Just(Some(1)),   // Boundary: min valid
        Just(Some(100)), // Boundary: max valid
        Just(Some(101)), // Edge case: exceeds max
        (1i64..=100).prop_map(Some),
        (101i64..=i64::MAX).prop_map(Some),
    ]
}

/// Strategy for generating valid RFC3339 timestamps
fn timestamp_strategy() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps between 2020 and 2030
    (1577836800i64..=1893456000i64)
        .prop_map(|secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap())
}

/// Strategy for ISO 8601 timestamp strings
fn iso8601_timestamp_strategy() -> impl Strategy<Value = String> {
    timestamp_strategy().prop_map(|dt| dt.to_rfc3339())
}

/// Strategy for invalid timestamp strings
fn invalid_timestamp_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("2024-13-45T25:61:61Z".to_string()),  // Invalid month/day/hour
        Just("not-a-timestamp".to_string()),
        Just("2024/01/01 12:00:00".to_string()),   // Wrong format
        Just("2024-01-01".to_string()),             // Missing time
        (0u32..1000).prop_map(|n| format!("garbage_{}", n)),
    ]
}

/// Strategy for contract IDs (hex strings)
fn contract_id_strategy() -> impl Strategy<Value = String> {
    "[0-9a-f]{56}".prop_map(|s| s)
}

// ============================================================================
// PAGINATION PROPERTY TESTS
// ============================================================================

proptest! {
    #[test]
    fn prop_pagination_offset_is_nonnegative(
        page in page_strategy(),
        limit in limit_strategy()
    ) {
        let params = PaginationParams { page, limit };
        // Offset should never be negative
        assert!(params.offset() >= 0,
            "offset should be non-negative, got {} for page={:?}, limit={:?}",
            params.offset(), page, limit
        );
    }

    #[test]
    fn prop_pagination_limit_within_bounds(
        limit in limit_strategy()
    ) {
        let params = PaginationParams { page: None, limit };
        let resolved_limit = params.limit();
        // Limit should always be between 1 and 100
        assert!(resolved_limit >= 1 && resolved_limit <= 100,
            "limit should be between 1 and 100, got {} for {:?}",
            resolved_limit, limit
        );
    }

    #[test]
    fn prop_pagination_page_minimum_is_one(
        page in page_strategy()
    ) {
        let params = PaginationParams { page, limit: None };
        // Page should never be less than 1 after resolution
        assert!(params.page() >= 1,
            "page should be at least 1, got {} for {:?}",
            params.page(), page
        );
    }

    #[test]
    fn prop_pagination_none_defaults_are_consistent(
        other_page in page_strategy(),
        other_limit in limit_strategy()
    ) {
        let none_params = PaginationParams { page: None, limit: None };
        let defaults_page = none_params.page();
        let defaults_limit = none_params.limit();
        let defaults_offset = none_params.offset();

        // Default page is 1
        prop_assert_eq!(defaults_page, 1);
        // Default limit is 20
        prop_assert_eq!(defaults_limit, 20);
        // Offset with page=1, limit=20 should be 0
        prop_assert_eq!(defaults_offset, 0);
    }

    #[test]
    fn prop_pagination_offset_calculation_is_correct(
        page in 1i64..=1000,
        limit in 1i64..=100
    ) {
        let params = PaginationParams {
            page: Some(page),
            limit: Some(limit)
        };

        let expected_offset = (page - 1) * limit;
        let actual_offset = params.offset();

        prop_assert_eq!(actual_offset, expected_offset,
            "offset calculation incorrect for page={}, limit={}",
            page, limit
        );
    }

    #[test]
    fn prop_pagination_zero_page_treated_as_one(
        limit in limit_strategy()
    ) {
        let params_zero = PaginationParams { page: Some(0), limit };
        let params_one = PaginationParams { page: Some(1), limit };

        // Page 0 and Page 1 should behave identically
        prop_assert_eq!(params_zero.page(), params_one.page());
        prop_assert_eq!(params_zero.offset(), params_one.offset());
    }

    #[test]
    fn prop_pagination_negative_page_becomes_one(
        negative_page in -1000i64..=-1,
        limit in limit_strategy()
    ) {
        let params = PaginationParams { page: Some(negative_page), limit };
        // Negative pages should be clamped to 1
        prop_assert_eq!(params.page(), 1);
    }

    #[test]
    fn prop_pagination_huge_page_offset_saturates(
        page in (i64::MAX / 100)..=i64::MAX,
        limit in limit_strategy()
    ) {
        // Boundary found by fuzz_pagination_params: (page - 1) * limit used to
        // overflow and panic for very large page values.
        let params = PaginationParams { page: Some(page), limit };
        let expected = (page - 1).checked_mul(params.limit()).unwrap_or(i64::MAX);
        prop_assert_eq!(params.offset(), expected);
        prop_assert!(params.offset() >= 0);
    }

    #[test]
    fn prop_pagination_negative_limit_clamped_to_one(
        negative_limit in -1000i64..=-1
    ) {
        let params = PaginationParams { page: None, limit: Some(negative_limit) };
        // Negative limits should be clamped to minimum of 1
        prop_assert_eq!(params.limit(), 1);
    }
}

// ============================================================================
// TIMESTAMP PROPERTY TESTS
// ============================================================================

proptest! {
    #[test]
    fn prop_valid_timestamp_strings_parse(ts in iso8601_timestamp_strategy()) {
        let result = validate_timestamp(&ts);
        // Valid ISO 8601 strings should always parse
        prop_assert!(result.is_ok(),
            "valid timestamp should parse: {} -> {:?}",
            ts, result
        );
    }

    #[test]
    fn prop_invalid_timestamp_strings_reject(ts in invalid_timestamp_strategy()) {
        let result = validate_timestamp(&ts);
        // Invalid strings should all fail
        prop_assert!(result.is_err(),
            "invalid timestamp should not parse: {}",
            ts
        );
    }

    #[test]
    fn prop_timestamp_roundtrip_preserves_value(ts_str in iso8601_timestamp_strategy()) {
        let parsed = validate_timestamp(&ts_str).unwrap();
        let serialized = parsed.to_rfc3339();
        let reparsed = validate_timestamp(&serialized).unwrap();

        // Roundtrip should preserve the timestamp (within nanosecond precision)
        prop_assert_eq!(parsed, reparsed,
            "timestamp roundtrip should preserve value: {} -> {} -> {}",
            ts_str, serialized, reparsed
        );
    }

    #[test]
    fn prop_timestamp_ordering_preserved(
        ts1 in timestamp_strategy(),
        ts2 in timestamp_strategy()
    ) {
        let str1 = ts1.to_rfc3339();
        let str2 = ts2.to_rfc3339();
        let parsed1 = validate_timestamp(&str1).unwrap();
        let parsed2 = validate_timestamp(&str2).unwrap();

        // If ts1 < ts2, then parsed1 < parsed2
        if ts1 < ts2 {
            prop_assert!(parsed1 < parsed2);
        } else if ts1 > ts2 {
            prop_assert!(parsed1 > parsed2);
        } else {
            prop_assert_eq!(parsed1, parsed2);
        }
    }

    #[test]
    fn prop_timestamp_duration_arithmetic_consistent(
        ts in timestamp_strategy(),
        days in 0i64..=365
    ) {
        let later = ts + Duration::days(days);

        // Duration arithmetic should be consistent
        let diff = later.signed_duration_since(ts);
        prop_assert_eq!(diff.num_days(), days,
            "duration arithmetic should be consistent for {} days",
            days
        );
    }
}

// ============================================================================
// FILTER VALIDATION PROPERTY TESTS
// ============================================================================

proptest! {
    #[test]
    fn prop_contract_id_format_validation(
        contract_id in ".*",  // Any string
    ) {
        let is_valid = is_valid_contract_id(&contract_id);

        if contract_id.len() == 56 && contract_id.chars().all(|c| c.is_ascii_hexdigit()) {
            prop_assert!(is_valid,
                "56-char hex string should be valid: {}",
                contract_id
            );
        } else {
            prop_assert!(!is_valid,
                "non-56-char-hex should be invalid: {}",
                contract_id
            );
        }
    }

    #[test]
    fn prop_ledger_range_validation(
        from_ledger in 0i64..=1_000_000,
        to_ledger in 0i64..=1_000_000
    ) {
        // If from > to, it should be invalid
        if from_ledger > to_ledger {
            prop_assert!(!is_valid_ledger_range(from_ledger, to_ledger),
                "from_ledger > to_ledger should be invalid: {} > {}",
                from_ledger, to_ledger
            );
        } else {
            prop_assert!(is_valid_ledger_range(from_ledger, to_ledger),
                "from_ledger <= to_ledger should be valid: {} <= {}",
                from_ledger, to_ledger
            );
        }
    }

    #[test]
    fn prop_timestamp_range_validation(
        ts1 in timestamp_strategy(),
        ts2 in timestamp_strategy()
    ) {
        let (from, to) = if ts1 < ts2 { (ts1, ts2) } else { (ts2, ts1) };

        // Valid range: from <= to
        prop_assert!(is_valid_timestamp_range(from, to),
            "from_ts <= to_ts should be valid"
        );

        // Invalid range: from > to
        prop_assert!(!is_valid_timestamp_range(to, from),
            "from_ts > to_ts should be invalid"
        );
    }
}

// ============================================================================
// HELPER FUNCTIONS FOR VALIDATION TESTS
// ============================================================================

fn is_valid_contract_id(id: &str) -> bool {
    id.len() == 56 && id.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_valid_ledger_range(from: i64, to: i64) -> bool {
    from <= to
}

fn is_valid_timestamp_range(from: DateTime<Utc>, to: DateTime<Utc>) -> bool {
    from <= to
}

// ============================================================================
// SHRINKING STRATEGY TESTS
// ============================================================================

proptest! {
    #[test]
    fn prop_pagination_shrink_to_minimal_valid_state(
        page in page_strategy(),
        limit in limit_strategy()
    ) {
        let params = PaginationParams { page, limit };

        // After resolution, parameters should reach a stable minimal valid state
        let page_resolved = params.page();
        let limit_resolved = params.limit();
        let offset_resolved = params.offset();

        // Repeatedly resolving should be idempotent
        let params2 = PaginationParams {
            page: Some(page_resolved),
            limit: Some(limit_resolved)
        };

        prop_assert_eq!(params2.page(), page_resolved);
        prop_assert_eq!(params2.limit(), limit_resolved);
        prop_assert_eq!(params2.offset(), offset_resolved);
    }
}
