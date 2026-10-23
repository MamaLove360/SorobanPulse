//! Security test suite entry point.
//!
//! Run all security tests:
//!   cargo test --test security
//!
//! Run a specific sub-module:
//!   cargo test --test security owasp_tests
//!   cargo test --test security auth_bypass_tests
//!   cargo test --test security crypto_tests
//!   cargo test --test security regression_tests
//!   cargo test --test security graphql_limit_tests
//!   cargo test --test security problem_detail_tests

mod security;
