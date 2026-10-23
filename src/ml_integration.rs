//! AI/ML Integration & Intelligence Features (Issue #842)
//!
//! This module provides intelligent features including:
//! - ML model integration framework
//! - Pattern recognition and prediction
//! - Intelligent event filtering
//! - Advanced anomaly detection with ML
//! - Automated model training and updates

use chrono::{DateTime, Utc, Duration};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;
use tracing::{info, warn, error};
use std::collections::{HashMap, VecDeque};

/// ML model types supported
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MLModelType {
    AnomalyDetection,
    PatternRecognition,
    EventClassification,
    PredictiveForecasting,
    IntelligentFiltering,
}

/// ML model metadata
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MLModel {
    pub id: Uuid,
    pub name: String,
    pub model_type: String,
    pub version: String,
    pub accuracy: Option<f64>,
    pub precision: Option<f64>,
    pub recall: Option<f64>,
    pub f1_score: Option<f64>,
    pub training_samples: i64,
    pub last_trained_at: DateTime<Utc>,
    pub is_active: bool,
    pub hyperparameters: serde_json::Value,
    pub feature_importance: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Pattern detected by ML models
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedPattern {
    pub pattern_id: Uuid,
    pub pattern_type: String,
    pub confidence: f64,
    pub description: String,
    pub affected_contracts: Vec<String>,
    pub frequency: i64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Prediction result from ML model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prediction {
    pub prediction_id: Uuid,
    pub model_id: Uuid,
    pub prediction_type: String,
    pub predicted_value: f64,
    pub confidence_interval: (f64, f64),
    pub confidence_score: f64,
    pub feature_values: HashMap<String, f64>,
    pub prediction_time: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}

/// Intelligent filter rule learned by ML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelligentFilter {
    pub filter_id: Uuid,
    pub name: String,
    pub description: String,
    pub conditions: Vec<FilterCondition>,
    pub action: FilterAction,
    pub confidence: f64,
    pub auto_learned: bool,
    pub performance_metrics: FilterPerformance,
    pub created_at: DateTime<Utc>,
}

/// Filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterCondition {
    pub field: String,
    pub operator: String,
    pub value: serde_json::Value,
    pub weight: f64,
}

/// Filter action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterAction {
    Route(String),
    Tag(Vec<String>),
    Priority(String),
    Alert,
    Suppress,
}

/// Filter performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterPerformance {
    pub true_positives: i64,
    pub false_positives: i64,
    pub true_negatives: i64,
    pub false_negatives: i64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,
}

/// Time series forecasting using exponential smoothing with ML enhancements
#[derive(Debug, Clone)]
pub struct MLEnhancedForecaster {
    pub alpha: f64,          // Level smoothing
    pub beta: f64,           // Trend smoothing
    pub gamma: f64,          // Seasonality smoothing
    pub level: f64,
    pub trend: f64,
    pub seasonal: Vec<f64>,
    pub season_length: usize,
    pub history: VecDeque<f64>,
    pub max_history: usize,
    pub anomaly_threshold: f64,
}

impl MLEnhancedForecaster {
    /// Create a new ML-enhanced forecaster with Holt-Winters parameters
    pub fn new(season_length: usize, anomaly_threshold: f64) -> Self {
        Self {
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.2,
            level: 0.0,
            trend: 0.0,
            seasonal: vec![1.0; season_length],
            season_length,
            history: VecDeque::with_capacity(100),
            max_history: 100,
            anomaly_threshold,
        }
    }

    /// Update the forecaster with a new observation
    pub fn update(&mut self, value: f64) {
        if self.history.len() >= self.max_history {
            self.history.pop_front();
        }
        self.history.push_back(value);

        if self.history.len() < self.season_length {
            return;
        }

        let seasonal_idx = (self.history.len() - 1) % self.season_length;
        let old_level = self.level;

        // Holt-Winters triple exponential smoothing
        self.level = self.alpha * (value / self.seasonal[seasonal_idx])
            + (1.0 - self.alpha) * (old_level + self.trend);
        
        self.trend = self.beta * (self.level - old_level)
            + (1.0 - self.beta) * self.trend;
        
        self.seasonal[seasonal_idx] = self.gamma * (value / self.level)
            + (1.0 - self.gamma) * self.seasonal[seasonal_idx];
    }

    /// Forecast future values
    pub fn forecast(&self, steps: usize) -> Vec<f64> {
        let mut forecasts = Vec::with_capacity(steps);
        
        for i in 1..=steps {
            let seasonal_idx = (self.history.len() - 1 + i) % self.season_length;
            let forecast = (self.level + i as f64 * self.trend) * self.seasonal[seasonal_idx];
            forecasts.push(forecast);
        }
        
        forecasts
    }

    /// Detect if a value is anomalous using prediction interval
    pub fn is_anomalous(&self, value: f64) -> bool {
        if self.history.len() < 3 {
            return false;
        }

        let forecast = self.forecast(1)[0];
        let std_dev = self.calculate_std_dev();
        let margin = self.anomaly_threshold * std_dev;

        value < (forecast - margin) || value > (forecast + margin)
    }

    /// Calculate standard deviation of residuals
    fn calculate_std_dev(&self) -> f64 {
        if self.history.len() < 2 {
            return 0.0;
        }

        let mean = self.history.iter().sum::<f64>() / self.history.len() as f64;
        let variance = self.history.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / (self.history.len() - 1) as f64;
        
        variance.sqrt()
    }
}

/// Pattern recognition engine
pub struct PatternRecognitionEngine {
    patterns: HashMap<String, Vec<f64>>,
    min_confidence: f64,
}

impl PatternRecognitionEngine {
    pub fn new(min_confidence: f64) -> Self {
        Self {
            patterns: HashMap::new(),
            min_confidence,
        }
    }

    /// Learn a pattern from event sequences
    pub fn learn_pattern(&mut self, pattern_name: String, sequence: Vec<f64>) {
        self.patterns.insert(pattern_name, sequence);
    }

    /// Detect patterns in a sequence
    pub fn detect_patterns(&self, sequence: &[f64]) -> Vec<(String, f64)> {
        let mut detected = Vec::new();

        for (name, pattern) in &self.patterns {
            let confidence = self.calculate_similarity(sequence, pattern);
            if confidence >= self.min_confidence {
                detected.push((name.clone(), confidence));
            }
        }

        detected.sort_by(|a, b| b.1.total_cmp(&a.1));
        detected
    }

    /// Calculate similarity between two sequences using Dynamic Time Warping
    fn calculate_similarity(&self, seq1: &[f64], seq2: &[f64]) -> f64 {
        if seq1.is_empty() || seq2.is_empty() {
            return 0.0;
        }

        let n = seq1.len();
        let m = seq2.len();
        let mut dtw = vec![vec![f64::INFINITY; m + 1]; n + 1];
        dtw[0][0] = 0.0;

        for i in 1..=n {
            for j in 1..=m {
                let cost = (seq1[i - 1] - seq2[j - 1]).abs();
                dtw[i][j] = cost + dtw[i - 1][j].min(dtw[i][j - 1]).min(dtw[i - 1][j - 1]);
            }
        }

        let max_distance = n.max(m) as f64 * 10.0; // Normalize
        let normalized_distance = dtw[n][m] / max_distance;
        
        (1.0 - normalized_distance).max(0.0).min(1.0)
    }
}

/// Event classifier using simple feature-based classification
pub struct EventClassifier {
    classes: HashMap<String, ClassFeatures>,
}

#[derive(Debug, Clone)]
struct ClassFeatures {
    feature_means: HashMap<String, f64>,
    feature_std_devs: HashMap<String, f64>,
    sample_count: usize,
}

impl EventClassifier {
    pub fn new() -> Self {
        Self {
            classes: HashMap::new(),
        }
    }

    /// Train the classifier with labeled examples
    pub fn train(&mut self, class: String, features: HashMap<String, f64>) {
        let class_features = self.classes.entry(class).or_insert_with(|| ClassFeatures {
            feature_means: HashMap::new(),
            feature_std_devs: HashMap::new(),
            sample_count: 0,
        });

        // Update running statistics
        for (feature_name, value) in features {
            let mean = class_features.feature_means.entry(feature_name.clone()).or_insert(0.0);
            let old_mean = *mean;
            let n = class_features.sample_count as f64;
            *mean = (old_mean * n + value) / (n + 1.0);

            // Update std dev using Welford's algorithm
            let std_dev = class_features.feature_std_devs.entry(feature_name).or_insert(0.0);
            if class_features.sample_count > 0 {
                *std_dev = ((*std_dev * n + (value - old_mean) * (value - *mean)) / (n + 1.0)).sqrt();
            }
        }

        class_features.sample_count += 1;
    }

    /// Classify new features
    pub fn classify(&self, features: &HashMap<String, f64>) -> Option<(String, f64)> {
        let mut best_class = None;
        let mut best_score = f64::NEG_INFINITY;

        for (class_name, class_features) in &self.classes {
            let score = self.calculate_likelihood(features, class_features);
            if score > best_score {
                best_score = score;
                best_class = Some(class_name.clone());
            }
        }

        best_class.map(|c| (c, best_score.exp())) // Convert log-likelihood to probability
    }

    /// Calculate log-likelihood using Gaussian Naive Bayes
    fn calculate_likelihood(&self, features: &HashMap<String, f64>, class_features: &ClassFeatures) -> f64 {
        let mut log_likelihood = 0.0;

        for (feature_name, value) in features {
            if let Some(mean) = class_features.feature_means.get(feature_name) {
                let std_dev = class_features.feature_std_devs.get(feature_name).unwrap_or(&1.0);
                let variance = std_dev.powi(2).max(1e-10); // Prevent division by zero
                
                // Gaussian probability density
                let exponent = -((value - mean).powi(2)) / (2.0 * variance);
                let coefficient = 1.0 / (2.0 * std::f64::consts::PI * variance).sqrt();
                log_likelihood += coefficient.ln() + exponent;
            }
        }

        log_likelihood
    }
}

/// Train an anomaly detection model
pub async fn train_anomaly_model(
    pool: &PgPool,
    contract_id: &str,
    lookback_days: i32,
) -> Result<MLModel, String> {
    let cutoff = Utc::now() - Duration::days(lookback_days as i64);
    
    // Fetch training data
    let training_data: Vec<(DateTime<Utc>, f64)> = sqlx::query_as(
        "SELECT created_at, value FROM events WHERE contract_id = $1 AND created_at >= $2 ORDER BY created_at"
    )
    .bind(contract_id)
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to fetch training data: {}", e))?;
    
    if training_data.len() < 100 {
        return Err("Insufficient training data (minimum 100 samples required)".to_string());
    }
    
    // Train forecaster
    let mut forecaster = MLEnhancedForecaster::new(24, 3.0); // 24-hour seasonality
    for (_, value) in &training_data {
        forecaster.update(*value);
    }
    
    // Calculate model metrics
    let mut correct_predictions = 0;
    let mut total_predictions = 0;
    
    for i in 50..training_data.len() - 1 {
        let actual = training_data[i + 1].1;
        let predicted = forecaster.forecast(1)[0];
        let error = (actual - predicted).abs();
        let std_dev = forecaster.calculate_std_dev();
        
        if error <= 2.0 * std_dev {
            correct_predictions += 1;
        }
        total_predictions += 1;
    }
    
    let accuracy = correct_predictions as f64 / total_predictions as f64;
    
    // Save model
    let model_id = Uuid::new_v4();
    let hyperparameters = serde_json::json!({
        "alpha": forecaster.alpha,
        "beta": forecaster.beta,
        "gamma": forecaster.gamma,
        "season_length": forecaster.season_length,
        "anomaly_threshold": forecaster.anomaly_threshold
    });
    
    sqlx::query(
        r#"
        INSERT INTO ml_models (
            id, name, model_type, version, accuracy, training_samples,
            last_trained_at, is_active, hyperparameters, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#
    )
    .bind(model_id)
    .bind(format!("anomaly_model_{}", contract_id))
    .bind("anomaly_detection")
    .bind("1.0.0")
    .bind(accuracy)
    .bind(training_data.len() as i64)
    .bind(Utc::now())
    .bind(true)
    .bind(hyperparameters.clone())
    .bind(Utc::now())
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to save model: {}", e))?;
    
    info!(
        model_id = %model_id,
        contract_id = %contract_id,
        accuracy = accuracy,
        samples = training_data.len(),
        "Anomaly detection model trained"
    );
    
    Ok(MLModel {
        id: model_id,
        name: format!("anomaly_model_{}", contract_id),
        model_type: "anomaly_detection".to_string(),
        version: "1.0.0".to_string(),
        accuracy: Some(accuracy),
        precision: None,
        recall: None,
        f1_score: None,
        training_samples: training_data.len() as i64,
        last_trained_at: Utc::now(),
        is_active: true,
        hyperparameters,
        feature_importance: serde_json::json!({}),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    })
}

/// Detect patterns in event sequences
pub async fn detect_event_patterns(
    pool: &PgPool,
    contract_id: &str,
    lookback_hours: i32,
) -> Result<Vec<DetectedPattern>, String> {
    let cutoff = Utc::now() - Duration::hours(lookback_hours as i64);
    
    // Fetch event sequences
    let events: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT event_type, created_at FROM events WHERE contract_id = $1 AND created_at >= $2 ORDER BY created_at"
    )
    .bind(contract_id)
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to fetch events: {}", e))?;
    
    // Group events into sequences
    let mut patterns = HashMap::new();
    for window in events.windows(3) {
        let pattern = format!("{}->{}->{}", window[0].0, window[1].0, window[2].0);
        *patterns.entry(pattern).or_insert(0) += 1;
    }
    
    // Filter significant patterns (occurring more than 3 times)
    let detected_patterns: Vec<DetectedPattern> = patterns
        .into_iter()
        .filter(|(_, count)| *count > 3)
        .map(|(pattern, frequency)| {
            let confidence = (frequency as f64 / events.len() as f64).min(1.0);
            DetectedPattern {
                pattern_id: Uuid::new_v4(),
                pattern_type: "sequence".to_string(),
                confidence,
                description: format!("Recurring event sequence: {}", pattern),
                affected_contracts: vec![contract_id.to_string()],
                frequency,
                first_seen: cutoff,
                last_seen: Utc::now(),
                metadata: HashMap::new(),
            }
        })
        .collect();
    
    info!(
        contract_id = %contract_id,
        patterns_found = detected_patterns.len(),
        "Event patterns detected"
    );
    
    Ok(detected_patterns)
}

/// Create intelligent filter based on learned patterns
pub async fn create_intelligent_filter(
    pool: &PgPool,
    filter_name: String,
    conditions: Vec<FilterCondition>,
    action: FilterAction,
) -> Result<IntelligentFilter, String> {
    let filter_id = Uuid::new_v4();
    
    let filter = IntelligentFilter {
        filter_id,
        name: filter_name,
        description: "Auto-learned intelligent filter".to_string(),
        conditions,
        action,
        confidence: 0.8,
        auto_learned: true,
        performance_metrics: FilterPerformance {
            true_positives: 0,
            false_positives: 0,
            true_negatives: 0,
            false_negatives: 0,
            precision: 0.0,
            recall: 0.0,
            f1_score: 0.0,
        },
        created_at: Utc::now(),
    };
    
    info!(
        filter_id = %filter_id,
        filter_name = %filter.name,
        "Intelligent filter created"
    );
    
    Ok(filter)
}

/// Get ML model recommendations for optimizing subscriptions
pub async fn get_optimization_recommendations(
    pool: &PgPool,
    tenant_id: &str,
) -> Result<Vec<String>, String> {
    let mut recommendations = Vec::new();
    
    // Analyze subscription patterns
    let subscription_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM subscriptions WHERE tenant_id = $1"
    )
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    
    if subscription_count > 50 {
        recommendations.push(
            "Consider consolidating subscriptions with similar filters to reduce overhead".to_string()
        );
    }
    
    // Analyze event volume
    let recent_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE tenant_id = $1 AND created_at >= NOW() - INTERVAL '1 day'"
    )
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    
    if recent_events > 100000 {
        recommendations.push(
            "High event volume detected. Consider using intelligent filters to reduce noise".to_string()
        );
    }
    
    if recommendations.is_empty() {
        recommendations.push("Your setup is optimized. No recommendations at this time.".to_string());
    }
    
    Ok(recommendations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ml_forecaster() {
        let mut forecaster = MLEnhancedForecaster::new(7, 3.0);
        
        // Train with sample data
        for i in 0..50 {
            forecaster.update((i as f64).sin() * 10.0 + 50.0);
        }
        
        assert!(forecaster.history.len() > 0);
        let forecast = forecaster.forecast(3);
        assert_eq!(forecast.len(), 3);
    }

    #[test]
    fn test_pattern_recognition() {
        let mut engine = PatternRecognitionEngine::new(0.7);
        
        engine.learn_pattern("uptrend".to_string(), vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        engine.learn_pattern("downtrend".to_string(), vec![5.0, 4.0, 3.0, 2.0, 1.0]);
        
        let detected = engine.detect_patterns(&[1.1, 2.1, 3.0, 4.2, 5.1]);
        assert!(!detected.is_empty());
        assert_eq!(detected[0].0, "uptrend");
    }

    #[test]
    fn test_event_classifier() {
        let mut classifier = EventClassifier::new();
        
        // Train with examples
        let mut features1 = HashMap::new();
        features1.insert("value".to_string(), 100.0);
        features1.insert("latency".to_string(), 50.0);
        classifier.train("normal".to_string(), features1);
        
        let mut features2 = HashMap::new();
        features2.insert("value".to_string(), 1000.0);
        features2.insert("latency".to_string(), 500.0);
        classifier.train("anomalous".to_string(), features2);
        
        // Classify new event
        let mut test_features = HashMap::new();
        test_features.insert("value".to_string(), 110.0);
        test_features.insert("latency".to_string(), 55.0);
        
        let result = classifier.classify(&test_features);
        assert!(result.is_some());
    }
}
