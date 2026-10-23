//! Log analysis tool for troubleshooting.
//!
//! Parses structured/plain log lines, aggregates errors, builds a rough
//! timeline, detects likely-correlated entries (reusing the correlation IDs
//! from `distributed_tracing.rs` when present), and generates a simple
//! human-readable troubleshooting report. This is an offline/CLI-friendly
//! analysis tool, not a request-path component — see
//! `docs/log-analysis-tool.md` for usage.

use std::collections::HashMap;

/// A single parsed log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedLogLine {
    pub level: LogLevel,
    pub message: String,
    pub correlation_id: Option<String>,
    pub timestamp_ms: Option<u128>,
    pub raw: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Unknown,
}

impl LogLevel {
    fn from_str(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "TRACE" => LogLevel::Trace,
            "DEBUG" => LogLevel::Debug,
            "INFO" => LogLevel::Info,
            "WARN" | "WARNING" => LogLevel::Warn,
            "ERROR" => LogLevel::Error,
            _ => LogLevel::Unknown,
        }
    }
}

/// Parse a single log line. Recognizes the common
/// `TIMESTAMP LEVEL [correlation_id=ID] message` shape produced by
/// `tracing`'s JSON/text formatters, but degrades gracefully to a raw
/// `Unknown`-level entry for anything else so no line is dropped silently.
pub fn parse_line(line: &str) -> ParsedLogLine {
    let level = ["TRACE", "DEBUG", "INFO", "WARN", "WARNING", "ERROR"]
        .iter()
        .find(|lvl| line.to_ascii_uppercase().contains(*lvl))
        .map(|lvl| LogLevel::from_str(lvl))
        .unwrap_or(LogLevel::Unknown);

    let correlation_id = extract_field(line, "correlation_id")
        .or_else(|| extract_field(line, "correlation-id"))
        .or_else(|| extract_field(line, "x-correlation-id"));

    let timestamp_ms = extract_field(line, "timestamp_ms").and_then(|s| s.parse().ok());

    ParsedLogLine {
        level,
        message: line.trim().to_string(),
        correlation_id,
        timestamp_ms,
        raw: line.to_string(),
    }
}

/// Naive `key=value` field extractor used for parsing structured log lines
/// without pulling in a full log-format dependency.
fn extract_field(line: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    let value = if let Some(stripped) = rest.strip_prefix('"') {
        stripped.split('"').next()?
    } else {
        rest.split(|c: char| c.is_whitespace() || c == ',').next()?
    };
    Some(value.to_string())
}

/// Parse a full multi-line log dump into structured entries.
pub fn parse_log(text: &str) -> Vec<ParsedLogLine> {
    text.lines().filter(|l| !l.trim().is_empty()).map(parse_line).collect()
}

/// Aggregate errors by a normalized message signature, counting occurrences.
/// Normalization strips obvious variable data (numbers, hex, quoted values)
/// so that repeated errors differing only by an ID collapse into one bucket.
pub fn aggregate_errors(entries: &[ParsedLogLine]) -> Vec<(String, usize)> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for entry in entries.iter().filter(|e| e.level == LogLevel::Error) {
        let signature = normalize_signature(&entry.message);
        *counts.entry(signature).or_insert(0) += 1;
    }
    let mut result: Vec<(String, usize)> = counts.into_iter().collect();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    result
}

fn normalize_signature(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut prev_was_digit = false;
    for ch in message.chars() {
        if ch.is_ascii_digit() {
            if !prev_was_digit {
                out.push('#');
            }
            prev_was_digit = true;
        } else {
            out.push(ch);
            prev_was_digit = false;
        }
    }
    out
}

/// A point in the reconstructed timeline.
#[derive(Clone, Debug)]
pub struct TimelineEvent {
    pub timestamp_ms: u128,
    pub level: LogLevel,
    pub message: String,
}

/// Build a chronological timeline from parsed entries that carry a
/// timestamp, for "timeline visualization" (as text/JSON here; a UI layer
/// can render this as a chart).
pub fn build_timeline(entries: &[ParsedLogLine]) -> Vec<TimelineEvent> {
    let mut timeline: Vec<TimelineEvent> = entries
        .iter()
        .filter_map(|e| {
            e.timestamp_ms.map(|ts| TimelineEvent {
                timestamp_ms: ts,
                level: e.level,
                message: e.message.clone(),
            })
        })
        .collect();
    timeline.sort_by_key(|e| e.timestamp_ms);
    timeline
}

/// Group entries by correlation ID, implementing "correlation detection":
/// entries sharing a correlation ID are almost certainly part of the same
/// logical request/operation and should be investigated together.
pub fn detect_correlated_groups(entries: &[ParsedLogLine]) -> HashMap<String, Vec<ParsedLogLine>> {
    let mut groups: HashMap<String, Vec<ParsedLogLine>> = HashMap::new();
    for entry in entries {
        if let Some(id) = &entry.correlation_id {
            groups.entry(id.clone()).or_default().push(entry.clone());
        }
    }
    groups
}

/// A heuristic root-cause suggestion for a group of correlated log entries.
#[derive(Clone, Debug)]
pub struct RootCauseSuggestion {
    pub correlation_id: String,
    pub suggestion: String,
    pub confidence: f32,
}

/// Very lightweight root-cause heuristics: look at the sequence of levels
/// within a correlated group and flag common patterns (e.g. a warning
/// immediately preceding an error often indicates the warning was the
/// precipitating condition).
pub fn suggest_root_causes(
    groups: &HashMap<String, Vec<ParsedLogLine>>,
) -> Vec<RootCauseSuggestion> {
    let mut suggestions = Vec::new();
    for (correlation_id, entries) in groups {
        let has_error = entries.iter().any(|e| e.level == LogLevel::Error);
        if !has_error {
            continue;
        }
        let warn_before_error = entries
            .windows(2)
            .any(|w| w[0].level == LogLevel::Warn && w[1].level == LogLevel::Error);

        let (suggestion, confidence) = if warn_before_error {
            (
                "A warning immediately preceded the error; investigate the warning's \
                 condition as the likely root cause."
                    .to_string(),
                0.6,
            )
        } else if entries.iter().filter(|e| e.level == LogLevel::Error).count() > 1 {
            (
                "Multiple errors in this correlation group; look for a single \
                 upstream failure causing cascading errors."
                    .to_string(),
                0.5,
            )
        } else {
            (
                "Isolated error with no preceding warning; check the error message \
                 and surrounding service logs directly."
                    .to_string(),
                0.3,
            )
        };

        suggestions.push(RootCauseSuggestion {
            correlation_id: correlation_id.clone(),
            suggestion,
            confidence,
        });
    }
    suggestions.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    suggestions
}

/// A generated troubleshooting report combining all analysis steps.
#[derive(Clone, Debug)]
pub struct AnalysisReport {
    pub total_lines: usize,
    pub error_count: usize,
    pub top_errors: Vec<(String, usize)>,
    pub correlated_group_count: usize,
    pub root_cause_suggestions: Vec<RootCauseSuggestion>,
}

/// Run the full analysis pipeline over a raw log dump and generate a report.
pub fn generate_report(text: &str) -> AnalysisReport {
    let entries = parse_log(text);
    let error_count = entries.iter().filter(|e| e.level == LogLevel::Error).count();
    let top_errors = aggregate_errors(&entries);
    let groups = detect_correlated_groups(&entries);
    let root_cause_suggestions = suggest_root_causes(&groups);

    AnalysisReport {
        total_lines: entries.len(),
        error_count,
        top_errors,
        correlated_group_count: groups.len(),
        root_cause_suggestions,
    }
}

impl AnalysisReport {
    /// Render the report as human-readable text for CLI/troubleshooting use.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Log Analysis Report\n====================\nTotal lines: {}\nErrors: {}\nCorrelated groups: {}\n\n",
            self.total_lines, self.error_count, self.correlated_group_count
        ));

        out.push_str("Top error signatures:\n");
        for (signature, count) in self.top_errors.iter().take(10) {
            out.push_str(&format!("  [{count}x] {signature}\n"));
        }

        out.push_str("\nRoot cause suggestions:\n");
        for s in &self.root_cause_suggestions {
            out.push_str(&format!(
                "  correlation_id={} confidence={:.1}: {}\n",
                s.correlation_id, s.confidence, s.suggestion
            ));
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_line_extracts_level_and_correlation_id() {
        let line = r#"2026-08-30T00:00:00Z ERROR correlation_id="abc123" db connection failed"#;
        let parsed = parse_line(line);
        assert_eq!(parsed.level, LogLevel::Error);
        assert_eq!(parsed.correlation_id.as_deref(), Some("abc123"));
    }

    #[test]
    fn parse_line_defaults_to_unknown_level() {
        let parsed = parse_line("just some plain text");
        assert_eq!(parsed.level, LogLevel::Unknown);
    }

    #[test]
    fn aggregate_errors_collapses_similar_messages() {
        let entries = vec![
            parse_line("ERROR failed to connect to host 10.0.0.1"),
            parse_line("ERROR failed to connect to host 10.0.0.2"),
            parse_line("ERROR something else entirely"),
        ];
        let aggregated = aggregate_errors(&entries);
        assert_eq!(aggregated[0].1, 2);
    }

    #[test]
    fn build_timeline_sorts_by_timestamp() {
        let entries = vec![
            parse_line("timestamp_ms=200 ERROR second"),
            parse_line("timestamp_ms=100 INFO first"),
        ];
        let timeline = build_timeline(&entries);
        assert_eq!(timeline[0].message.contains("first"), true);
        assert_eq!(timeline[1].message.contains("second"), true);
    }

    #[test]
    fn detect_correlated_groups_buckets_by_id() {
        let entries = vec![
            parse_line(r#"correlation_id="a" INFO start"#),
            parse_line(r#"correlation_id="a" ERROR failed"#),
            parse_line(r#"correlation_id="b" INFO other"#),
        ];
        let groups = detect_correlated_groups(&entries);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.get("a").unwrap().len(), 2);
    }

    #[test]
    fn suggest_root_causes_flags_warn_before_error() {
        let entries = vec![
            parse_line(r#"correlation_id="a" WARN slow response"#),
            parse_line(r#"correlation_id="a" ERROR timeout"#),
        ];
        let groups = detect_correlated_groups(&entries);
        let suggestions = suggest_root_causes(&groups);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].suggestion.contains("warning"));
    }

    #[test]
    fn generate_report_produces_summary_and_text() {
        let log = r#"
correlation_id="x" INFO request start
correlation_id="x" WARN cache miss
correlation_id="x" ERROR upstream timeout
"#;
        let report = generate_report(log);
        assert_eq!(report.error_count, 1);
        assert_eq!(report.correlated_group_count, 1);
        let text = report.to_text();
        assert!(text.contains("Log Analysis Report"));
    }
}
