//! `GET /v1/meta` capabilities endpoint — Issue #1259.
//!
//! Lets clients discover which optional features (GraphQL, encryption,
//! multi-chain, …) this build supports without reading deployment config.
//! The feature list and build SHA are baked in at compile time by `build.rs`.

use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Current major version of the REST API (the `/v1` prefix).
pub const API_VERSION: &str = "v1";

/// Response body for `GET /v1/meta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MetaResponse {
    /// Cargo features this binary was compiled with, sorted alphabetically.
    #[schema(example = json!(["classical-crypto", "dashboard", "default"]))]
    pub features: Vec<String>,
    /// Crate version (`CARGO_PKG_VERSION`).
    #[schema(example = "0.1.0")]
    pub version: String,
    /// Git commit the binary was built from, or `"unknown"`.
    #[schema(example = "076772f1a2b3")]
    pub build_sha: String,
    /// REST API major version.
    #[schema(example = "v1")]
    pub api_version: String,
}

/// Cargo features compiled into this binary (set by `build.rs`).
pub fn compiled_features() -> Vec<String> {
    env!("SOROBAN_PULSE_FEATURES")
        .split(',')
        .filter(|f| !f.is_empty())
        .map(str::to_owned)
        .collect()
}

impl MetaResponse {
    pub fn current() -> Self {
        Self {
            features: compiled_features(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            build_sha: env!("SOROBAN_PULSE_BUILD_SHA").to_owned(),
            api_version: API_VERSION.to_owned(),
        }
    }
}

/// Build metadata and enabled features.
///
/// Public: no authentication and no rate limiting.
#[utoipa::path(
    get,
    path = "/v1/meta",
    tag = "system",
    responses(
        (status = 200, description = "Build metadata and compiled features", body = MetaResponse)
    )
)]
pub async fn get_meta() -> Json<MetaResponse> {
    Json(MetaResponse::current())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn features_match_compiled_cfg() {
        let features = compiled_features();
        assert_eq!(features.contains(&"dashboard".to_owned()), cfg!(feature = "dashboard"));
        assert_eq!(
            features.contains(&"classical-crypto".to_owned()),
            cfg!(feature = "classical-crypto")
        );
        assert_eq!(features.contains(&"kafka".to_owned()), cfg!(feature = "kafka"));
        assert_eq!(
            features.contains(&"quantum-crypto".to_owned()),
            cfg!(feature = "quantum-crypto")
        );
    }

    #[test]
    fn features_are_sorted_and_non_empty_strings() {
        let features = compiled_features();
        let mut sorted = features.clone();
        sorted.sort();
        assert_eq!(features, sorted);
        assert!(features.iter().all(|f| !f.is_empty()));
    }

    #[test]
    fn current_reports_version_and_api_version() {
        let meta = MetaResponse::current();
        assert_eq!(meta.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(meta.api_version, "v1");
        assert!(!meta.build_sha.is_empty());
    }
}
