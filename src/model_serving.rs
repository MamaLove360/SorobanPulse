//! ML model serving infrastructure for event prediction and classification.
//!
//! This module provides the building blocks needed to serve trained ML models
//! against live indexer events: feature extraction, model version management,
//! a predictions API, and monitoring/performance tracking for served models.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Supported model task types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelTask {
    /// Predict a continuous or categorical future event property.
    EventPrediction,
    /// Classify an event into one of a fixed set of labels.
    EventClassification,
}

/// Metadata describing a single deployable model version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    pub model_name: String,
    pub version: String,
    pub task: ModelTask,
    pub created_at_unix: u64,
    /// Whether this version is actively serving traffic.
    pub active: bool,
    /// Arbitrary training/serving metadata (hyperparameters, dataset hash, etc).
    pub metadata: HashMap<String, String>,
}

impl ModelVersion {
    pub fn new(model_name: impl Into<String>, version: impl Into<String>, task: ModelTask) -> Self {
        Self {
            model_name: model_name.into(),
            version: version.into(),
            task,
            created_at_unix: now_unix(),
            active: false,
            metadata: HashMap::new(),
        }
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

/// A single extracted feature vector, ready to be fed to a model.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureVector {
    pub numeric: HashMap<String, f64>,
    pub categorical: HashMap<String, String>,
}

impl FeatureVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_numeric(mut self, key: impl Into<String>, value: f64) -> Self {
        self.numeric.insert(key.into(), value);
        self
    }

    pub fn with_categorical(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.categorical.insert(key.into(), value.into());
        self
    }
}

/// Minimal shape of an indexed contract event used for feature extraction.
/// Kept decoupled from the concrete `Event` model so this module has no
/// hard dependency on the DB layer.
#[derive(Debug, Clone, Default)]
pub struct EventFeatureInput {
    pub contract_id: String,
    pub topic0: Option<String>,
    pub ledger_sequence: u64,
    pub data_size_bytes: usize,
    pub successful: bool,
}

/// Extracts model-ready features from raw event input.
pub trait FeatureExtractor: Send + Sync {
    fn extract(&self, input: &EventFeatureInput) -> FeatureVector;
    fn name(&self) -> &str;
}

/// Default feature extractor producing a compact, generally-useful feature set.
pub struct DefaultFeatureExtractor;

impl FeatureExtractor for DefaultFeatureExtractor {
    fn extract(&self, input: &EventFeatureInput) -> FeatureVector {
        let mut fv = FeatureVector::new()
            .with_numeric("ledger_sequence", input.ledger_sequence as f64)
            .with_numeric("data_size_bytes", input.data_size_bytes as f64)
            .with_numeric("successful", if input.successful { 1.0 } else { 0.0 })
            .with_categorical("contract_id", input.contract_id.clone());
        if let Some(topic) = &input.topic0 {
            fv = fv.with_categorical("topic0", topic.clone());
        }
        fv
    }

    fn name(&self) -> &str {
        "default_feature_extractor"
    }
}

/// Result of running inference against a served model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prediction {
    pub model_name: String,
    pub model_version: String,
    pub label: String,
    pub score: f64,
    pub latency_ms: f64,
}

/// A servable model. Implementations wrap the actual inference backend
/// (ONNX runtime, remote inference service, rule-based fallback, etc).
pub trait ServableModel: Send + Sync {
    fn predict(&self, features: &FeatureVector) -> Result<(String, f64), ModelServingError>;
}

/// Trivial baseline model useful for tests and as a safe default before a
/// real model is deployed for a given task.
pub struct BaselineModel {
    pub default_label: String,
}

impl ServableModel for BaselineModel {
    fn predict(&self, _features: &FeatureVector) -> Result<(String, f64), ModelServingError> {
        Ok((self.default_label.clone(), 0.5))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ModelServingError {
    #[error("model not found: {0}")]
    ModelNotFound(String),
    #[error("no active version for model: {0}")]
    NoActiveVersion(String),
    #[error("inference failed: {0}")]
    InferenceFailed(String),
}

/// Per-model, per-version performance counters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelPerformanceStats {
    pub prediction_count: u64,
    pub error_count: u64,
    pub total_latency_ms: f64,
    pub score_sum: f64,
}

impl ModelPerformanceStats {
    pub fn record_success(&mut self, latency_ms: f64, score: f64) {
        self.prediction_count += 1;
        self.total_latency_ms += latency_ms;
        self.score_sum += score;
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
    }

    pub fn avg_latency_ms(&self) -> f64 {
        if self.prediction_count == 0 {
            0.0
        } else {
            self.total_latency_ms / self.prediction_count as f64
        }
    }

    pub fn avg_score(&self) -> f64 {
        if self.prediction_count == 0 {
            0.0
        } else {
            self.score_sum / self.prediction_count as f64
        }
    }

    pub fn error_rate(&self) -> f64 {
        let total = self.prediction_count + self.error_count;
        if total == 0 {
            0.0
        } else {
            self.error_count as f64 / total as f64
        }
    }
}

/// Central registry that owns model versions, served backends, and monitors
/// their performance. Acts as the entry point for the predictions API.
pub struct ModelRegistry {
    versions: RwLock<HashMap<String, Vec<ModelVersion>>>,
    backends: RwLock<HashMap<String, Arc<dyn ServableModel>>>,
    extractor: Arc<dyn FeatureExtractor>,
    stats: RwLock<HashMap<String, ModelPerformanceStats>>,
}

impl ModelRegistry {
    pub fn new(extractor: Arc<dyn FeatureExtractor>) -> Self {
        Self {
            versions: RwLock::new(HashMap::new()),
            backends: RwLock::new(HashMap::new()),
            extractor,
            stats: RwLock::new(HashMap::new()),
        }
    }

    fn version_key(model_name: &str, version: &str) -> String {
        format!("{model_name}::{version}")
    }

    /// Registers a new model version and its backend, without activating it.
    pub fn register_version(
        &self,
        version: ModelVersion,
        backend: Arc<dyn ServableModel>,
    ) {
        let key = Self::version_key(&version.model_name, &version.version);
        self.backends.write().unwrap_or_else(std::sync::PoisonError::into_inner).insert(key.clone(), backend);
        self.stats
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(key)
            .or_insert_with(ModelPerformanceStats::default);
        self.versions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(version.model_name.clone())
            .or_default()
            .push(version);
    }

    /// Activates a specific version, deactivating all other versions of the
    /// same model. This is the model version management surface used for
    /// rollouts and rollbacks.
    pub fn activate_version(&self, model_name: &str, version: &str) -> Result<(), ModelServingError> {
        let mut versions = self.versions.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = versions
            .get_mut(model_name)
            .ok_or_else(|| ModelServingError::ModelNotFound(model_name.to_string()))?;
        let mut found = false;
        for v in entries.iter_mut() {
            v.active = v.version == version;
            found |= v.active;
        }
        if !found {
            return Err(ModelServingError::ModelNotFound(format!(
                "{model_name}@{version}"
            )));
        }
        Ok(())
    }

    fn active_version(&self, model_name: &str) -> Result<String, ModelServingError> {
        let versions = self.versions.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = versions
            .get(model_name)
            .ok_or_else(|| ModelServingError::ModelNotFound(model_name.to_string()))?;
        entries
            .iter()
            .find(|v| v.active)
            .map(|v| v.version.clone())
            .ok_or_else(|| ModelServingError::NoActiveVersion(model_name.to_string()))
    }

    /// Predictions API entry point: extracts features from raw event input
    /// and runs inference against the currently active version of a model.
    pub fn predict(
        &self,
        model_name: &str,
        input: &EventFeatureInput,
    ) -> Result<Prediction, ModelServingError> {
        let version = self.active_version(model_name)?;
        let key = Self::version_key(model_name, &version);
        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .cloned()
            .ok_or_else(|| ModelServingError::ModelNotFound(key.clone()))?;

        let features = self.extractor.extract(input);
        let start = SystemTime::now();
        let result = backend.predict(&features);
        let latency_ms = start.elapsed().unwrap_or(Duration::ZERO).as_secs_f64() * 1000.0;

        let mut stats = self.stats.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = stats.entry(key).or_default();
        match result {
            Ok((label, score)) => {
                entry.record_success(latency_ms, score);
                Ok(Prediction {
                    model_name: model_name.to_string(),
                    model_version: version,
                    label,
                    score,
                    latency_ms,
                })
            }
            Err(e) => {
                entry.record_error();
                Err(e)
            }
        }
    }

    /// Returns performance stats for a given model version, used by the
    /// monitoring/tracking surface below.
    pub fn performance(&self, model_name: &str, version: &str) -> Option<ModelPerformanceStats> {
        let key = Self::version_key(model_name, version);
        self.stats.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(&key).cloned()
    }

    pub fn list_versions(&self, model_name: &str) -> Vec<ModelVersion> {
        self.versions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(model_name)
            .cloned()
            .unwrap_or_default()
    }
}

/// Alert thresholds used by the model monitoring loop.
#[derive(Debug, Clone)]
pub struct MonitoringThresholds {
    pub max_error_rate: f64,
    pub max_avg_latency_ms: f64,
}

impl Default for MonitoringThresholds {
    fn default() -> Self {
        Self {
            max_error_rate: 0.05,
            max_avg_latency_ms: 250.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MonitoringAlert {
    HighErrorRate { model: String, version: String, rate: f64 },
    HighLatency { model: String, version: String, avg_ms: f64 },
}

/// Evaluates a model's current stats against thresholds, producing alerts
/// for anything that has drifted out of acceptable bounds. This is the
/// core of "model monitoring" — intended to be polled periodically by a
/// background task and piped into the alerting subsystem.
pub fn evaluate_monitoring(
    model_name: &str,
    version: &str,
    stats: &ModelPerformanceStats,
    thresholds: &MonitoringThresholds,
) -> Vec<MonitoringAlert> {
    let mut alerts = Vec::new();
    if stats.error_rate() > thresholds.max_error_rate {
        alerts.push(MonitoringAlert::HighErrorRate {
            model: model_name.to_string(),
            version: version.to_string(),
            rate: stats.error_rate(),
        });
    }
    if stats.avg_latency_ms() > thresholds.max_avg_latency_ms {
        alerts.push(MonitoringAlert::HighLatency {
            model: model_name.to_string(),
            version: version.to_string(),
            avg_ms: stats.avg_latency_ms(),
        });
    }
    alerts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> EventFeatureInput {
        EventFeatureInput {
            contract_id: "C123".into(),
            topic0: Some("transfer".into()),
            ledger_sequence: 42,
            data_size_bytes: 128,
            successful: true,
        }
    }

    #[test]
    fn extracts_expected_features() {
        let extractor = DefaultFeatureExtractor;
        let fv = extractor.extract(&sample_input());
        assert_eq!(fv.numeric.get("ledger_sequence"), Some(&42.0));
        assert_eq!(fv.categorical.get("contract_id"), Some(&"C123".to_string()));
        assert_eq!(fv.categorical.get("topic0"), Some(&"transfer".to_string()));
    }

    #[test]
    fn registry_requires_active_version_before_predicting() {
        let registry = ModelRegistry::new(Arc::new(DefaultFeatureExtractor));
        let version = ModelVersion::new("churn", "v1", ModelTask::EventClassification);
        registry.register_version(
            version,
            Arc::new(BaselineModel { default_label: "active".into() }),
        );

        let err = registry.predict("churn", &sample_input()).unwrap_err();
        assert!(matches!(err, ModelServingError::NoActiveVersion(_)));

        registry.activate_version("churn", "v1").unwrap();
        let prediction = registry.predict("churn", &sample_input()).unwrap();
        assert_eq!(prediction.label, "active");
        assert_eq!(prediction.model_version, "v1");
    }

    #[test]
    fn activating_unknown_model_errors() {
        let registry = ModelRegistry::new(Arc::new(DefaultFeatureExtractor));
        let err = registry.activate_version("missing", "v1").unwrap_err();
        assert!(matches!(err, ModelServingError::ModelNotFound(_)));
    }

    #[test]
    fn version_management_switches_active_flag() {
        let registry = ModelRegistry::new(Arc::new(DefaultFeatureExtractor));
        registry.register_version(
            ModelVersion::new("fraud", "v1", ModelTask::EventPrediction),
            Arc::new(BaselineModel { default_label: "low_risk".into() }),
        );
        registry.register_version(
            ModelVersion::new("fraud", "v2", ModelTask::EventPrediction),
            Arc::new(BaselineModel { default_label: "high_risk".into() }),
        );
        registry.activate_version("fraud", "v1").unwrap();
        registry.activate_version("fraud", "v2").unwrap();

        let versions = registry.list_versions("fraud");
        let v1 = versions.iter().find(|v| v.version == "v1").unwrap();
        let v2 = versions.iter().find(|v| v.version == "v2").unwrap();
        assert!(!v1.active);
        assert!(v2.active);
    }

    #[test]
    fn performance_tracking_accumulates_stats() {
        let registry = ModelRegistry::new(Arc::new(DefaultFeatureExtractor));
        registry.register_version(
            ModelVersion::new("risk", "v1", ModelTask::EventClassification),
            Arc::new(BaselineModel { default_label: "ok".into() }),
        );
        registry.activate_version("risk", "v1").unwrap();

        for _ in 0..5 {
            registry.predict("risk", &sample_input()).unwrap();
        }

        let stats = registry.performance("risk", "v1").unwrap();
        assert_eq!(stats.prediction_count, 5);
        assert_eq!(stats.error_count, 0);
        assert!(stats.avg_score() > 0.0);
    }

    #[test]
    fn monitoring_flags_high_error_rate() {
        let mut stats = ModelPerformanceStats::default();
        for _ in 0..10 {
            stats.record_error();
        }
        let thresholds = MonitoringThresholds::default();
        let alerts = evaluate_monitoring("m", "v1", &stats, &thresholds);
        assert!(alerts.iter().any(|a| matches!(a, MonitoringAlert::HighErrorRate { .. })));
    }

    #[test]
    fn monitoring_flags_high_latency() {
        let mut stats = ModelPerformanceStats::default();
        stats.record_success(9999.0, 0.9);
        let thresholds = MonitoringThresholds::default();
        let alerts = evaluate_monitoring("m", "v1", &stats, &thresholds);
        assert!(alerts.iter().any(|a| matches!(a, MonitoringAlert::HighLatency { .. })));
    }

    #[test]
    fn monitoring_clean_stats_produce_no_alerts() {
        let mut stats = ModelPerformanceStats::default();
        stats.record_success(10.0, 0.9);
        let thresholds = MonitoringThresholds::default();
        let alerts = evaluate_monitoring("m", "v1", &stats, &thresholds);
        assert!(alerts.is_empty());
    }
}
