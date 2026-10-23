//! Timezone and Locale Handling Module (Issue #594)
//!
//! Provides utilities for timezone conversion, locale-specific formatting,
//! daylight saving time transition detection, and timestamp parsing/validation.

use anyhow::{anyhow, Result};
use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDateTime, Offset, TimeZone, Timelike, Utc,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

// ---------------------------------------------------------------------------
// Supported timezones (offset-based, no external tz crate needed)
// ---------------------------------------------------------------------------

/// A named timezone with its UTC offset in seconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimezoneConfig {
    pub name: String,
    /// Standard offset from UTC in seconds (e.g. -18000 for EST UTC-5)
    pub offset_seconds: i32,
    /// DST offset in seconds added on top of standard offset (0 if no DST)
    pub dst_offset_seconds: i32,
    /// Month (1-12) DST begins (None if no DST)
    pub dst_start_month: Option<u32>,
    /// Month (1-12) DST ends (None if no DST)
    pub dst_end_month: Option<u32>,
}

impl TimezoneConfig {
    /// UTC — no offset, no DST.
    pub fn utc() -> Self {
        Self {
            name: "UTC".to_string(),
            offset_seconds: 0,
            dst_offset_seconds: 0,
            dst_start_month: None,
            dst_end_month: None,
        }
    }

    /// US Eastern (EST/EDT): UTC-5 standard, UTC-4 DST (Mar–Nov).
    pub fn us_eastern() -> Self {
        Self {
            name: "America/New_York".to_string(),
            offset_seconds: -5 * 3600,
            dst_offset_seconds: 3600,
            dst_start_month: Some(3),
            dst_end_month: Some(11),
        }
    }

    /// US Pacific (PST/PDT): UTC-8 standard, UTC-7 DST (Mar–Nov).
    pub fn us_pacific() -> Self {
        Self {
            name: "America/Los_Angeles".to_string(),
            offset_seconds: -8 * 3600,
            dst_offset_seconds: 3600,
            dst_start_month: Some(3),
            dst_end_month: Some(11),
        }
    }

    /// Central European (CET/CEST): UTC+1 standard, UTC+2 DST (Mar–Oct).
    pub fn central_european() -> Self {
        Self {
            name: "Europe/Berlin".to_string(),
            offset_seconds: 3600,
            dst_offset_seconds: 3600,
            dst_start_month: Some(3),
            dst_end_month: Some(10),
        }
    }

    /// Japan Standard Time: UTC+9, no DST.
    pub fn japan() -> Self {
        Self {
            name: "Asia/Tokyo".to_string(),
            offset_seconds: 9 * 3600,
            dst_offset_seconds: 0,
            dst_start_month: None,
            dst_end_month: None,
        }
    }

    /// Nigeria / West Africa Time: UTC+1, no DST.
    pub fn west_africa() -> Self {
        Self {
            name: "Africa/Lagos".to_string(),
            offset_seconds: 3600,
            dst_offset_seconds: 0,
          dst_start_month: None,
            dst_end_month: None,
        }
    }

    /// Returns true if DST is in effect for the given UTC month.
    pub fn is_dst_active(&self, utc_month: u32) -> bool {
        match (self.dst_start_month, self.dst_end_month) {
            (Some(start), Some(end)) => {
                if start < end {
                    utc_month >= start && utc_month < end
                } else {
                    // Southern hemisphere: DST wraps across year boundary
                    utc_month >= start || utc_month < end
                }
            }
            _ => false,
        }
    }

    /// Effective offset in seconds, accounting for DST.
    pub fn effective_offset(&self, utc_month: u32) -> i32 {
        if self.is_dst_active(utc_month) {
            self.offset_seconds + self.dst_offset_seconds
        } else {
            self.offset_seconds
        }
    }

    /// Convert a UTC `DateTime` to this timezone, returning a `DateTime<FixedOffset>`.
    pub fn convert_from_utc(&self, utc: &DateTime<Utc>) -> DateTime<FixedOffset> {
        let offset_secs = self.effective_offset(utc.month());
        let fixed = FixedOffset::east_opt(offset_secs)
            .unwrap_or_else(|| Utc.fix());
        utc.with_timezone(&fixed)
    }

    /// Parse an ISO 8601 string and convert it to UTC.
    pub fn parse_to_utc(&self, s: &str) -> Result<DateTime<Utc>> {
        // Try with timezone info first
        if let Ok(dt) = s.parse::<DateTime<Utc>>() {
            return Ok(dt);
        }
        if let Ok(dt) = s.parse::<DateTime<FixedOffset>>() {
            return Ok(dt.with_timezone(&Utc));
        }
        // Try as naive datetime, interpret as this timezone
        if let Ok(naive) = s.parse::<NaiveDateTime>() {
            let offset_secs = self.effective_offset(naive.month());
            let fixed = FixedOffset::east_opt(offset_secs)
                .unwrap_or_else(|| Utc.fix());
            let local = fixed
                .from_local_datetime(&naive)
                .single()
                .ok_or_else(|| anyhow!("ambiguous local time (DST gap): {s}"))?;
            return Ok(local.with_timezone(&Utc));
        }
        Err(anyhow!("cannot parse timestamp: {s}"))
    }
}

// ---------------------------------------------------------------------------
// Locale-specific formatting (Task 2)
// ---------------------------------------------------------------------------

/// Supported locale identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Locale {
    EnUs,   // English (US)   — MM/DD/YYYY
    EnGb,   // English (UK)   — DD/MM/YYYY
    De,     // German         — DD.MM.YYYY
    Ja,     // Japanese       — YYYY年MM月DD日
    Fr,     // French         — DD/MM/YYYY
    Ng,     // Nigerian (en)  — DD/MM/YYYY
}

impl Locale {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "en-us" | "en_us" => Some(Self::EnUs),
            "en-gb" | "en_gb" => Some(Self::EnGb),
            "de" | "de-de" => Some(Self::De),
            "ja" | "ja-jp" => Some(Self::Ja),
            "fr" | "fr-fr" => Some(Self::Fr),
            "ng" | "en-ng" => Some(Self::Ng),
            _ => None,
        }
    }

    /// Decimal separator used in this locale.
    pub fn decimal_separator(&self) -> char {
        match self {
            Self::De | Self::Fr => ',',
            _ => '.',
        }
    }

    /// Thousands separator used in this locale.
    pub fn thousands_separator(&self) -> char {
        match self {
            Self::De => '.',
            Self::Fr => ' ',
            _ => ',',
        }
    }
}

/// Formats a `DateTime<Utc>` for display in the given locale.
pub fn format_datetime_for_locale(dt: &DateTime<Utc>, locale: &Locale) -> String {
    let (y, m, d) = (dt.year(), dt.month(), dt.day());
    let (h, min, s) = (dt.hour(), dt.minute(), dt.second());
    match locale {
        Locale::EnUs => format!("{m:02}/{d:02}/{y} {h:02}:{min:02}:{s:02}"),
        Locale::EnGb | Locale::Ng | Locale::Fr => {
            format!("{d:02}/{m:02}/{y} {h:02}:{min:02}:{s:02}")
        }
        Locale::De => format!("{d:02}.{m:02}.{y} {h:02}:{min:02}:{s:02}"),
        Locale::Ja => format!("{y}年{m:02}月{d:02}日 {h:02}:{min:02}:{s:02}"),
    }
}

/// Formats a number according to locale conventions.
pub fn format_number_for_locale(value: f64, decimal_places: usize, locale: &Locale) -> String {
    let factor = 10_f64.powi(decimal_places as i32);
    let rounded = (value * factor).round() / factor;
    let integer_part = rounded.abs().trunc() as u64;
    let frac = rounded.abs().fract();

    // Build thousands-separated integer part
    let int_str = integer_part.to_string();
    let sep = locale.thousands_separator();
    let grouped: String = int_str
        .chars()
        .rev()
        .enumerate()
        .flat_map(|(i, c)| {
            if i > 0 && i % 3 == 0 {
                vec![sep, c]
            } else {
                vec![c]
            }
        })
        .collect::<String>()
  .chars()
        .rev()
        .collect();

    let dec_sep = locale.decimal_separator();
    let sign = if value < 0.0 { "-" } else { "" };

    if decimal_places == 0 {
        format!("{sign}{grouped}")
    } else {
        let frac_str = format!("{:.prec$}", frac, prec = decimal_places);
        let frac_digits = &frac_str[2..]; // strip "0."
        format!("{sign}{grouped}{dec_sep}{frac_digits}")
    }
}

// ---------------------------------------------------------------------------
// Timezone conversion utilities (Task 3)
// ---------------------------------------------------------------------------

/// Converts a UTC timestamp to multiple timezones at once.
pub fn convert_to_all_timezones(
    utc: &DateTime<Utc>,
    timezones: &[TimezoneConfig],
) -> HashMap<String, DateTime<FixedOffset>> {
    timezones
        .iter()
        .map(|tz| (tz.name.clone(), tz.convert_from_utc(utc)))
        .collect()
}

/// Validates that a from/to timestamp pair is logically ordered.
pub fn validate_timestamp_range(
    from: &DateTime<Utc>,
    to: &DateTime<Utc>,
) -> Result<()> {
    if from > to {
        return Err(anyhow!(
            "from_timestamp ({}) must not be after to_timestamp ({})",
            from,
            to
        ));
    }
    Ok(())
}

/// Parses a timestamp string tolerantly: tries UTC, FixedOffset, then naive.
pub fn parse_timestamp_lenient(s: &str) -> Result<DateTime<Utc>> {
    TimezoneConfig::utc().parse_to_utc(s)
}

// ---------------------------------------------------------------------------
// DST transition detection (Task 4)
// ---------------------------------------------------------------------------

/// A DST transition event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DstTransitionKind {
    /// Clocks spring forward — local times in the gap don't exist.
    SpringForward,
    /// Clocks fall back — local times in the overlap are ambiguous.
    FallBack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DstTransition {
    pub timezone: String,
    pub kind: DstTransitionKind,
    pub utc_at: DateTime<Utc>,
    pub offset_before_seconds: i32,
    pub offset_after_seconds: i32,
}

/// Checks whether two consecutive UTC timestamps straddle a DST boundary.
pub fn detect_dst_transition(
    tz: &TimezoneConfig,
    before: &DateTime<Utc>,
    after: &DateTime<Utc>,
) -> Option<DstTransition> {
    let offset_before = tz.effective_offset(before.month());
    let offset_after = tz.effective_offset(after.month());

    if offset_before == offset_after {
        return None;
    }

    let kind = if offset_after > offset_before {
        DstTransitionKind::SpringForward
    } else {
        DstTransitionKind::FallBack
    };

    warn!(
        timezone = %tz.name,
        ?kind,
        "DST transition detected between {} and {}",
        before,
        after
    );

    Some(DstTransition {
        timezone: tz.name.clone(),
        kind,
        utc_at: *after,
        offset_before_seconds: offset_before,
        offset_after_seconds: offset_after,
    })
}

/// Scans a sorted slice of UTC timestamps and returns all DST transitions found.
pub fn scan_for_dst_transitions(
    tz: &TimezoneConfig,
    timestamps: &[DateTime<Utc>],
) -> Vec<DstTransition> {
    timestamps
        .windows(2)
        .filter_map(|w| detect_dst_transition(tz, &w[0], &w[1]))
        .collect()
}

// ---------------------------------------------------------------------------
// Timezone handling documentation (Task 5) — exposed as a public struct
// ---------------------------------------------------------------------------

/// Documents the timezone handling strategy used by SorobanPulse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimezoneHandlingDoc {
    pub storage_format: &'static str,
    pub api_input_format: &'static str,
    pub api_output_format: &'static str,
    pub dst_strategy: &'static str,
    pub supported_timezones: Vec<&'static str>,
    pub locale_formatting: &'static str,
}

impl TimezoneHandlingDoc {
    pub fn generate() -> Self {
        Self {
            storage_format: "All timestamps stored as UTC (DateTime<Utc>) in PostgreSQL TIMESTAMPTZ columns.",
            api_input_format: "ISO 8601 / RFC 3339 strings accepted; bare naive datetimes interpreted as UTC.",
            api_output_format: "UTC ISO 8601 by default; locale-formatted strings available via Accept-Language header.",
            dst_strategy: "DST transitions detected by comparing effective offsets across month boundaries. Ambiguous times flagged as warnings; gaps rejected with 400.",
            supported_timezones: vec![
                "UTC",
                "America/New_York (EST/EDT)",
                "America/Los_Angeles (PST/PDT)",
                "Europe/Berlin (CET/CEST)",
                "Asia/Tokyo (JST)",
                "Africa/Lagos (WAT)",
            ],
            locale_formatting: "Date/number formatting driven by Locale enum; covers en-US, en-GB, de, ja, fr, en-NG.",
        }
    }
}
