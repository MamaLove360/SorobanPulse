//! Typestate builder for [`Config`] (issue #1247).
//!
//! The database and RPC settings are mandatory. The builder tracks whether
//! each has been supplied in its type parameters, and [`ConfigBuilder::build`]
//! only exists once both are [`Set`]. Forgetting one is a compile error:
//!
//! ```compile_fail
//! use soroban_pulse::config::{ConfigBuilder, DatabaseSettings};
//! // No `.with_rpc(...)`, so `build` does not exist for this builder.
//! let _ = ConfigBuilder::new()
//!     .with_database(DatabaseSettings::new("postgres://localhost/soroban_pulse"))
//!     .build();
//! ```
//!
//! ```
//! use soroban_pulse::config::{ConfigBuilder, DatabaseSettings, RpcSettings};
//! let config = ConfigBuilder::new()
//!     .with_database(DatabaseSettings::new("postgres://localhost/soroban_pulse"))
//!     .with_rpc(RpcSettings::new("https://soroban-testnet.stellar.org"))
//!     .build()
//!     .expect("valid configuration");
//! assert_eq!(config.port, 3000);
//! ```
//!
//! [`ConfigBuilder::build`] is the single place runtime validation happens.
//! It reports parse errors collected while reading the environment together
//! with every semantic check in [`crate::config_validation`], all in one
//! [`ConfigError`].

use std::marker::PhantomData;

use super::{
    validate_rpc_fallback_urls, validate_rpc_url_checked, Config, Environment, Role,
};

/// Typestate marker: a required section has not been provided yet.
#[derive(Debug, Clone, Copy)]
pub struct Missing;

/// Typestate marker: a required section has been provided.
#[derive(Debug, Clone, Copy)]
pub struct Set;

/// Database connection settings (`DATABASE_URL`, `DB_*`).
#[derive(Debug, Clone)]
pub struct DatabaseSettings {
    pub url: String,
    pub replica_url: Option<String>,
    pub max_connections: u32,
    pub min_connections: u32,
    pub statement_timeout_ms: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
    pub test_before_acquire: bool,
}

impl DatabaseSettings {
    /// Settings for `url` with the same defaults as the environment loader.
    pub fn new(url: impl Into<String>) -> Self {
        let defaults = Config::default();
        Self {
            url: url.into(),
            replica_url: None,
            max_connections: defaults.db_max_connections,
            min_connections: defaults.db_min_connections,
            statement_timeout_ms: defaults.db_statement_timeout_ms,
            idle_timeout_secs: defaults.db_idle_timeout_secs,
            max_lifetime_secs: defaults.db_max_lifetime_secs,
            test_before_acquire: defaults.db_test_before_acquire,
        }
    }
}

/// Stellar RPC settings (`STELLAR_RPC_*`, `RPC_*_TIMEOUT_SECS`).
#[derive(Debug, Clone)]
pub struct RpcSettings {
    pub url: String,
    pub fallback_urls: Vec<String>,
    pub headers: Vec<(String, String)>,
    pub connect_timeout_secs: u64,
    pub request_timeout_secs: u64,
}

impl RpcSettings {
    /// Settings for `url` with the same defaults as the environment loader.
    pub fn new(url: impl Into<String>) -> Self {
        let defaults = Config::default();
        Self {
            url: url.into(),
            fallback_urls: Vec::new(),
            headers: Vec::new(),
            connect_timeout_secs: defaults.rpc_connect_timeout_secs,
            request_timeout_secs: defaults.rpc_request_timeout_secs,
        }
    }
}

/// HTTP server settings. Optional: unset fields keep their defaults.
#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub allowed_origins: Vec<String>,
    pub behind_proxy: bool,
    pub environment: Environment,
    pub role: Role,
}

impl Default for ServerSettings {
    fn default() -> Self {
        let defaults = Config::default();
        Self {
            port: defaults.port,
            allowed_origins: defaults.allowed_origins,
            behind_proxy: defaults.behind_proxy,
            environment: defaults.environment,
            role: defaults.role,
        }
    }
}

/// Every problem found while building a [`Config`], reported together so
/// operators can fix them in one pass.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Configuration errors ({} found):\n{}", .errors.len(), .errors.join("\n"))]
pub struct ConfigError {
    errors: Vec<String>,
}

impl ConfigError {
    fn new(errors: Vec<String>) -> Self {
        let errors = errors
            .into_iter()
            .map(|e| format!("  {}", e.trim()))
            .collect();
        Self { errors }
    }

    /// The individual problems, one per entry.
    pub fn errors(&self) -> &[String] {
        &self.errors
    }
}

/// Builder for [`Config`]; see the [module docs](self).
#[derive(Debug, Clone)]
pub struct ConfigBuilder<Db = Missing, Rpc = Missing> {
    config: Config,
    /// Parse errors collected by [`ConfigBuilder::from_env`], reported by `build`.
    errors: Vec<String>,
    _state: PhantomData<(Db, Rpc)>,
}

impl ConfigBuilder<Missing, Missing> {
    /// Start from the built-in defaults.
    pub fn new() -> Self {
        Self {
            config: Config::default(),
            errors: Vec::new(),
            _state: PhantomData,
        }
    }
}

impl Default for ConfigBuilder<Missing, Missing> {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigBuilder<Set, Set> {
    /// Load every setting from the environment and `CONFIG_FILE`.
    ///
    /// Parse errors (malformed integers, bad hex keys, ...) are deferred to
    /// [`ConfigBuilder::build`] so they are reported alongside every semantic
    /// check. Individual settings can still be overridden before building.
    pub fn from_env() -> Self {
        let (config, errors) = Config::read_env();
        Self {
            config,
            errors,
            _state: PhantomData,
        }
    }

    /// Validate the configuration and produce a [`Config`].
    ///
    /// Advisory warnings from [`crate::config_validation`] are logged; any
    /// error (parse or semantic) fails the build.
    pub fn build(self) -> Result<Config, ConfigError> {
        let Self {
            mut config,
            mut errors,
            ..
        } = self;

        let report = crate::config_validation::validate(&config);
        for warning in &report.warnings {
            tracing::warn!(config_warning = warning.as_str(), "Configuration warning");
        }
        errors.extend(report.errors);

        if !errors.is_empty() {
            return Err(ConfigError::new(errors));
        }

        // The URLs passed validation above; normalise them (strip any
        // embedded credentials) for use and logging.
        let mut scratch = Vec::new();
        if let Some(safe) = validate_rpc_url_checked(&config.stellar_rpc_url, &mut scratch) {
            config.stellar_rpc_url = safe;
        }
        config.stellar_rpc_fallback_urls =
            validate_rpc_fallback_urls(&config.stellar_rpc_fallback_urls, &mut scratch);

        Ok(config)
    }
}

impl<Rpc> ConfigBuilder<Missing, Rpc> {
    /// Provide the (required) database settings.
    pub fn with_database(self, db: DatabaseSettings) -> ConfigBuilder<Set, Rpc> {
        let mut config = self.config;
        config.database_url = db.url;
        config.database_replica_url = db.replica_url;
        config.db_max_connections = db.max_connections;
        config.db_min_connections = db.min_connections;
        config.db_statement_timeout_ms = db.statement_timeout_ms;
        config.db_idle_timeout_secs = db.idle_timeout_secs;
        config.db_max_lifetime_secs = db.max_lifetime_secs;
        config.db_test_before_acquire = db.test_before_acquire;
        ConfigBuilder {
            config,
            errors: self.errors,
            _state: PhantomData,
        }
    }
}

impl<Db> ConfigBuilder<Db, Missing> {
    /// Provide the (required) Stellar RPC settings.
    pub fn with_rpc(self, rpc: RpcSettings) -> ConfigBuilder<Db, Set> {
        let mut config = self.config;
        config.stellar_rpc_url = rpc.url;
        config.stellar_rpc_fallback_urls = rpc.fallback_urls;
        config.rpc_headers = rpc.headers;
        config.rpc_connect_timeout_secs = rpc.connect_timeout_secs;
        config.rpc_request_timeout_secs = rpc.request_timeout_secs;
        ConfigBuilder {
            config,
            errors: self.errors,
            _state: PhantomData,
        }
    }
}

impl<Db, Rpc> ConfigBuilder<Db, Rpc> {
    /// Override the HTTP server settings.
    #[must_use]
    pub fn with_server(mut self, server: ServerSettings) -> Self {
        self.config.port = server.port;
        self.config.allowed_origins = server.allowed_origins;
        self.config.behind_proxy = server.behind_proxy;
        self.config.environment = server.environment;
        self.config.role = server.role;
        self
    }

    /// Adjust any other field. Changes are still validated by `build`.
    #[must_use]
    pub fn configure(mut self, f: impl FnOnce(&mut Config)) -> Self {
        f(&mut self.config);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> ConfigBuilder<Set, Set> {
        ConfigBuilder::new()
            .with_database(DatabaseSettings::new("postgres://localhost/soroban_pulse"))
            .with_rpc(RpcSettings::new("https://soroban-testnet.stellar.org"))
    }

    #[test]
    fn builds_with_required_sections() {
        let config = valid().build().unwrap();
        assert_eq!(config.database_url, "postgres://localhost/soroban_pulse");
        assert_eq!(config.stellar_rpc_url, "https://soroban-testnet.stellar.org/");
    }

    #[test]
    fn sections_can_be_supplied_in_either_order() {
        let config = ConfigBuilder::new()
            .with_rpc(RpcSettings::new("https://soroban-testnet.stellar.org"))
            .with_database(DatabaseSettings {
                max_connections: 25,
                ..DatabaseSettings::new("postgres://db/pulse")
            })
            .build()
            .unwrap();
        assert_eq!(config.db_max_connections, 25);
    }

    #[test]
    fn strips_rpc_credentials() {
        let config = valid()
            .configure(|c| c.stellar_rpc_url = "https://user:pass@rpc.example.com/".to_string())
            .build()
            .unwrap();
        assert_eq!(config.stellar_rpc_url, "https://rpc.example.com/");
    }

    #[test]
    fn reports_every_error_at_once() {
        let err = ConfigBuilder::new()
            .with_database(DatabaseSettings {
                min_connections: 50,
                max_connections: 5,
                ..DatabaseSettings::new("mysql://wrong")
            })
            .with_rpc(RpcSettings::new("ftp://example.com"))
            .build()
            .unwrap_err();
        let joined = err.errors().join("\n");
        assert!(joined.contains("DATABASE_URL"), "{joined}");
        assert!(joined.contains("DB_MIN_CONNECTIONS"), "{joined}");
        assert!(joined.contains("STELLAR_RPC_URL"), "{joined}");
        assert!(err.to_string().starts_with("Configuration errors (3 found)"), "{err}");
    }

    #[test]
    fn wildcard_origin_rejected_in_production() {
        let err = valid()
            .with_server(ServerSettings {
                environment: Environment::Production,
                allowed_origins: vec!["*".to_string()],
                ..ServerSettings::default()
            })
            .configure(|c| c.integration_encryption_key = Some([7u8; 32]))
            .build()
            .unwrap_err();
        assert!(err.errors().iter().any(|e| e.contains("ALLOWED_ORIGINS")), "{err}");
    }

    #[test]
    fn invalid_fallback_url_is_reported() {
        let err = valid()
            .configure(|c| c.stellar_rpc_fallback_urls = vec!["not a url".to_string()])
            .build()
            .unwrap_err();
        assert!(err.errors().iter().any(|e| e.contains("STELLAR_RPC_FALLBACK_URLS")), "{err}");
    }

    #[test]
    fn from_env_defers_parse_errors_to_build() {
        std::env::set_var("DATABASE_URL", "postgres://localhost/soroban_pulse");
        std::env::set_var("DB_MAX_CONNECTIONS", "not-a-number");
        let result = ConfigBuilder::from_env().build();
        std::env::remove_var("DB_MAX_CONNECTIONS");
        let err = result.unwrap_err();
        assert!(err.errors().iter().any(|e| e.contains("DB_MAX_CONNECTIONS")), "{err}");
    }

    #[test]
    fn parse_errors_from_env_are_reported_by_build() {
        let builder = ConfigBuilder {
            errors: vec!["  PORT=\"abc\" is not a valid integer.".to_string()],
            ..valid()
        };
        let err = builder.build().unwrap_err();
        assert!(err.errors().iter().any(|e| e.contains("PORT")), "{err}");
    }
}
