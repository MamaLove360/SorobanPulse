# Property-Based Testing Guide (Issue #554)

> This guide merges the former `property-testing.md` (Issue #1170).

## Overview

Property-based testing is a testing methodology that uses randomly generated test data to verify that code properties hold true across a wide range of inputs. Unlike traditional unit tests that verify specific cases, property-based tests specify invariants that should always be true.

## Why Property-Based Testing?

Property-based testing helps catch edge cases and corner cases that might be missed by hand-written unit tests. It's particularly useful for:

- **Finding edge cases**: Automatically generates extreme values, boundary conditions, and unusual combinations
- **Reducing test code**: One property test replaces many specific unit tests
- **Documenting assumptions**: Properties serve as executable specifications
- **Building confidence**: Tests thousands of generated inputs automatically

## Testing Framework: Proptest

We use the [proptest](https://docs.rs/proptest/) framework, a property-based testing library for Rust. Proptest provides:

- **Random value generation** (strategies)
- **Shrinking** (reducing failing cases to minimal examples)
- **Clear error messages** (showing exactly what caused failures)

## Running Property-Based Tests

### Run all property-based tests:
```bash
cargo test --test property_based_tests
```

### Run specific test category:
```bash
cargo test --test property_based_tests pagination_tests::
cargo test --test property_based_tests filter_tests::
cargo test --test property_based_tests timestamp_tests::
```

### Run with detailed output:
```bash
PROPTEST_VERBOSE=1 cargo test --test property_based_tests
```

### Run with custom seed (for reproducibility):
```bash
PROPTEST_RNG_SEED=12345 cargo test --test property_based_tests
```

## Test Categories

### 1. Pagination Tests

**Properties verified**:
- Page numbers are always positive integers
- Limits are within valid bounds (1-10,000)
- Offset calculation: `(page - 1) * limit` never overflows
- First page always has zero offset
- Offsets increase monotonically with page numbers

**Test cases**:
- `prop_pagination_page_positive`: Valid pages are > 0
- `prop_pagination_limit_within_bounds`: Limits between 1-10,000
- `prop_pagination_offset_calculation`: Offset math is correct
- `prop_pagination_no_overflow`: i64 arithmetic is safe
- `prop_pagination_cursor_monotonic`: Cursor iteration is valid
- `prop_pagination_no_panic_any_combination`: Never panic on any input

**Example failure scenario**:
```rust
// This would be caught by proptest:
let page = -1;  // Negative page
let limit = 100;
let offset = (page - 1) * limit;  // Invalid calculation
```

### 2. Filter Tests

**Properties verified**:
- Contract IDs match Stellar address format (56 chars, A-Z0-9)
- Contract ID prefixes are 4-56 characters
- Ledger ranges are valid (from ≤ to)
- Topic filters are valid JSON arrays
- Search queries are SQL-injection safe
- Multiple contract IDs don't exceed limit (20)
- Schema versions are in valid range (0-20)
- Filter combinations are independent

**Test cases**:
- `prop_filter_contract_id_format`: Valid contract ID patterns
- `prop_filter_contract_id_prefix_length`: Prefix size constraints
- `prop_filter_ledger_range_valid`: Ledger ordering is enforced
- `prop_filter_ledger_no_underflow`: No negative ledger numbers
- `prop_filter_topic_json_valid`: Topic arrays parse as JSON
- `prop_filter_search_query_safe`: No SQL injection patterns
- `prop_filter_contract_ids_count_limit`: Max 20 contract IDs
- `prop_filter_schema_version_valid`: Version range enforcement
- `prop_filter_combinations_independent`: Filters work together

**Example failure scenario**:
```rust
// This would be caught:
let contract_id = "abc";  // Too short
let filter = ContractIdFilter(contract_id);  // Invalid format
```

### 3. Timestamp Tests

**Properties verified**:
- ISO 8601 timestamps parse correctly
- Timestamp ranges are valid (from ≤ to)
- Arithmetic operations don't overflow
- Timestamp granularity (milliseconds) is preserved
- Timestamp comparisons are consistent and transitive
- Duration calculations are accurate (within 2 seconds)
- Sequences of timestamps maintain order

**Test cases**:
- `prop_timestamp_iso8601_parsing`: Valid RFC3339 format
- `prop_timestamp_range_ordering`: Range validation
- `prop_timestamp_no_overflow`: Duration arithmetic safety
- `prop_timestamp_granularity`: Precision is maintained
- `prop_timestamp_comparison_consistency`: Comparison logic
- `prop_timestamp_difference_accurate`: Duration calculations
- `prop_timestamp_sequence_ordering`: Collection ordering

**Example failure scenario**:
```rust
// This would be caught:
let from = Utc::now() + Duration::days(10);
let to = Utc::now();  // from > to (invalid range)
```

### 4. Shrinking Strategy Tests

**Properties verified**:
- Shrinking reduces test values to minimal failing cases
- Invariants are maintained during shrinking
- Minimal cases are found reliably

**Test cases**:
- `prop_shrinking_page_reduces`: Values shrink to lower bound
- `prop_shrinking_maintains_invariants`: Invariants hold after shrinking
- `prop_shrinking_finds_minimal_case`: Minimal case discovery

**How shrinking works**:
```rust
// If test fails with input: page=5432, limit=8901
// Proptest shrinks to: page=100, limit=1
// Until it finds the minimal case that still fails
```

### 5. Edge Case Tests

**Properties verified**:
- Zero and maximum values are handled appropriately
- Boundary values don't cause panics
- Empty strings are rejected or handled gracefully
- Special characters in strings are handled safely

**Test cases**:
- `prop_edge_case_zero_values`: Zero value handling
- `prop_edge_case_max_values`: Maximum value handling
- `prop_edge_case_empty_strings`: Empty string behavior
- `prop_edge_case_special_chars`: Special character safety

## Test Coverage

### Current Coverage

The property-based test suite covers:
- **Pagination**: 6 properties, ~10,000+ generated test cases per run
- **Filters**: 9 properties, ~10,000+ generated test cases per run
- **Timestamps**: 7 properties, ~10,000+ generated test cases per run
- **Shrinking**: 3 properties
- **Edge Cases**: 4 properties

**Total**: 29 property tests generating 50,000+ synthetic test cases per run

### Coverage Goals

- ✅ Pagination logic (100%)
- ✅ Filter validation (95%)
- ✅ Timestamp operations (100%)
- 🎯 Webhook payload validation (planned)
- 🎯 Notification formatting (planned)
- 🎯 Rate limiting logic (planned)

## Writing New Property Tests

### Basic Template

```rust
#[test]
fn prop_feature_invariant_holds() {
    proptest!(|(
        input in strategy
    )| {
        // Arrange
        let result = function_under_test(input);

        // Assert: Property should always hold
        assert!(
            property(result),
            "Expected property to hold"
        );
    });
}
```

### Strategies (Input Generators)

```rust
// Simple numeric ranges
proptest!(|(n in 0i64..100)| { /* n is 0-99 */ });

// Multiple inputs
proptest!(|(
    a in 0i64..100,
    b in 0i64..100
)| { /* a and b are independent */ });

// Collections
proptest!(|(
    items in prop::collection::vec("[a-z]+", 1..10)
)| { /* items is Vec<String> with 1-10 elements */ });

// Filtered strategies
proptest!(|(
    n in (0i64..1000).prop_filter("exclude zero", |n| *n != 0)
)| { /* n is never 0 */ });

// Custom strategies
proptest!(|(
    timestamp in valid_iso8601_timestamp()
)| { /* timestamp is valid ISO 8601 */ });
```

### Common Assertions

```rust
// Property holds always
assert!(predicate);

// Property produces valid output within bounds
assert!(result >= min && result <= max);

// Relationship between inputs and outputs
assert_eq!(expected, actual);

// Operations are idempotent
assert_eq!(f(x), f(f(x)));

// Operations are commutative
assert_eq!(f(a, b), f(b, a));
```

## Debugging Failed Properties

When a property test fails, proptest shows:

1. **The generated input** that caused failure
2. **The assertion that failed**
3. **The shrunk minimal case** that still fails

### Example failure output:
```
Test failed at /path/to/test:line 123
thread 'test_name' panicked at 'assertion failed'
Caused by:
    page: 42
    limit: 7
    offset calculated: 287

This is the shrunk minimal case that reproduces the failure
```

### Debugging steps:

1. **Run with verbose output**: `PROPTEST_VERBOSE=1 cargo test`
2. **Use the seed from failure**: `PROPTEST_RNG_SEED=<seed> cargo test`
3. **Add print statements**: `println!("Debug: {:?}", input);`
4. **Check edge cases**: Focus on boundary values in shrunk case
5. **Review assumption**: May reveal incorrect understanding of feature

## Integration with CI/CD

### GitHub Actions Configuration

```yaml
- name: Run property-based tests
  run: cargo test --test property_based_tests --verbose
  env:
    PROPTEST_CASES: 10000  # Run 10k cases per property
    PROPTEST_MAX_SHRINK_ITERS: 100000
```

### Build Performance

- Property tests add ~30-60 seconds to test suite
- Recommended: Run in separate CI job if needed
- Developers can run locally with fewer cases: `PROPTEST_CASES=100 cargo test`

## Best Practices

### ✅ Do's

- **Focus on invariants**: What must always be true?
- **Use meaningful property names**: Describe what's being tested
- **Test interaction**: How do multiple inputs combine?
- **Document assumptions**: Comment why properties matter
- **Use shrinking feedback**: Let proptest find edge cases

### ❌ Don'ts

- **Don't test implementation details**: Test behavior instead
- **Don't ignore shrunk cases**: They reveal the root issue
- **Don't make strategies too restrictive**: Let proptest explore
- **Don't write non-deterministic tests**: Reproducibility matters
- **Don't test unrelated properties**: Keep tests focused

## Strategies, Shrinking and Troubleshooting

### Strategies: Controlling Input Generation

A **strategy** defines the space of inputs that proptest generates. The project uses several strategies:

#### Basic Strategies
```rust
// Ranges
(1i64..=100).prop_map(Some)  // 1 to 100, wrapped in Option

// Alternatives (OR logic)
prop_oneof![
    Just(None),           // Always generate None
    Just(Some(0)),        // Always generate Some(0)
    (1..100).prop_map(Some),  // Random 1-100
]

// Mapped/transformed values
timestamp_strategy()
    .prop_map(|ts| ts.to_rfc3339())  // Convert to string
```

#### Custom Strategies
Define strategies for domain-specific values:

```rust
fn contract_id_strategy() -> impl Strategy<Value = String> {
    "[0-9a-f]{56}".prop_map(|s| s)  // Regex-based generation
}
```

#### Combining Strategies
```rust
proptest! {
    #[test]
    fn test_with_multiple_inputs(
        page in page_strategy(),
        limit in limit_strategy(),
        contract_id in contract_id_strategy()
    ) {
        // This test receives 3 random inputs per case
    }
}
```

### Shrinking

When a test fails, **proptest automatically shrinks** the failing input to a minimal reproducible example:

```
thread 'prop_pagination_limit_within_bounds' panicked at 'limit should be between 1 and 100, got -999999 for Some(-999999)'

Shrunk to: limit = Some(-1)
Shrunk to: limit = Some(0)
```

This makes debugging much easier — you get the smallest input that triggers the bug.

### Common Patterns

#### Testing bounds validation
```rust
proptest! {
    #[test]
    fn prop_value_is_clamped(value in i64::MIN..=i64::MAX) {
        let clamped = clamp(value, 0, 100);
        assert!(clamped >= 0 && clamped <= 100);
    }
}
```

#### Testing invariants
```rust
proptest! {
    #[test]
    fn prop_set_size_is_consistent(items in vec(any::<i32>(), 0..100)) {
        let mut set = MySet::new();
        for item in items {
            set.insert(item);
        }
        assert_eq!(set.len(), items.iter().unique().count());
    }
}
```

#### Testing ordering
```rust
proptest! {
    #[test]
    fn prop_sort_produces_ordered_output(
        mut items in vec(0i32..100, 0..100)
    ) {
        items.sort();
        for i in 0..items.len()-1 {
            assert!(items[i] <= items[i+1]);
        }
    }
}
```

### Performance Considerations

Property tests generate 256 test cases by default (`PROPTEST_CASES=256`). On large datasets:

- **Limit test size**: Use `vec(strategy, 0..100)` instead of unbounded
- **Cache expensive setup**: Use `proptest::prelude::proptest!` block scope
- **Consider CI intensity**: Use lower PROPTEST_CASES for local tests, higher for CI

### Troubleshooting

#### Test takes too long
Reduce the strategy scope:
```rust
// Bad: unbounded
(i64::MIN..=i64::MAX)

// Good: reasonable bounds
(0i64..=1_000_000)
```

#### Test fails with "state is empty after shrinking"
Your assertions may be too strict. Ensure they check actual invariants, not specific values.

#### "failed to find a failing input"
The invariant might actually be false. Review the logic carefully.

## Further Reading

- [Proptest Documentation](https://docs.rs/proptest/)
- [Property-Based Testing in Rust](https://matklad.github.io/2021/05/26/rustified-property-based-testing.html)
- [Why Property-Based Testing?](https://hypothesis.works/articles/what-is-property-based-testing/)
- [Quickcheck comparison](https://github.com/AltSysRq/proptest/wiki/FAQ#how-do-i-compare-proptest-and-quickcheck)
- [Strategy Guide](https://docs.rs/proptest/latest/proptest/arbitrary/index.html)

## Resources

- **Test file**: `tests/property_based_tests.rs`
- **Dependency**: `proptest = "1"` in `Cargo.toml`
- **Issue tracker**: #554
- **Contact**: @Xoulomon
