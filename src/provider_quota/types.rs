//! Unified quota data model — direct port of axonhub's
//! `biz/provider_quota/types.go`. JSON shapes match axonhub exactly so stored
//! `quota_data` stays compatible.

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::storage::Channel;
use super::credentials::ChannelCredentials;

pub const WARNING_THRESHOLD_RATIO: f64 = 0.8;

// Well-known limit window identifiers (axonhub QuotaWindow*).
pub const WINDOW_5H: &str = "5h";
pub const WINDOW_7D: &str = "7d";
pub const WINDOW_30D: &str = "30d";
pub const WINDOW_DAILY: &str = "daily";
pub const WINDOW_WEEKLY: &str = "weekly";
pub const WINDOW_MONTHLY: &str = "monthly";
pub const WINDOW_PRIMARY: &str = "primary";
pub const WINDOW_SECONDARY: &str = "secondary";
pub const WINDOW_PAY_AS_YOU_GO: &str = "pay_as_you_go";
pub const WINDOW_CREDITS: &str = "credits";
pub const WINDOW_CYCLE: &str = "cycle";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaLimitType {
    Image,
    Token,
    SubscriptionCycle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaLimitStatus {
    #[serde(rename = "type")]
    pub kind: QuotaLimitType,
    pub status: String,
    pub usage_ratio: f64,
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_reset_at: Option<DateTime<Utc>>,
    /// Alternative capacity sources aggregated with OR semantics for routing.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub availability_group: String,
    /// Window label ("5h", "7d", "weekly", ..., or a model name).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub window: String,
    /// Display label for the credential a limit belongs to (never a secret).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub account: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_start: Option<DateTime<Utc>>,
    /// Cost accumulated in [period_start, now) per our usage logs. Filled by
    /// the quota service, not by checkers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_quota: Option<f64>,
}

impl QuotaLimitStatus {
    pub fn new(kind: QuotaLimitType, status: &str, usage_ratio: f64, next_reset_at: Option<DateTime<Utc>>) -> Self {
        Self {
            kind,
            status: status.to_string(),
            usage_ratio,
            ready: is_ready_status(status),
            next_reset_at,
            availability_group: String::new(),
            window: String::new(),
            account: String::new(),
            period_start: None,
            period_cost: None,
            period_quota: None,
        }
    }

    pub fn token(status: &str, usage_ratio: f64, next_reset_at: Option<DateTime<Utc>>) -> Self {
        Self::new(QuotaLimitType::Token, status, usage_ratio, next_reset_at)
    }

    /// Labels the limit and derives period_start from next_reset_at - window.
    /// A zero duration only sets the label.
    pub fn with_window(mut self, name: &str, window: Duration) -> Self {
        self.window = name.to_string();
        self.period_start = period_start_from_reset(self.next_reset_at.as_ref(), window);
        self
    }

    /// Recomputes period_quota from period_cost and usage_ratio.
    pub fn fill_period_quota(&mut self) {
        self.period_quota = None;
        if let Some(cost) = self.period_cost {
            if let Some(q) = estimate_period_quota(cost, self.usage_ratio) {
                self.period_quota = Some(q);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaData {
    /// available | warning | exhausted | unknown
    pub status: String,
    pub provider_type: String,
    #[serde(default)]
    pub raw_data: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_reset_at: Option<DateTime<Utc>>,
    pub ready: bool,
    #[serde(default)]
    pub limits: Vec<QuotaLimitStatus>,
}

impl QuotaData {
    pub fn new(provider_type: &str, status: &str) -> Self {
        Self {
            status: status.to_string(),
            provider_type: provider_type.to_string(),
            raw_data: Map::new(),
            next_reset_at: None,
            ready: is_ready_status(status),
            limits: Vec::new(),
        }
    }

    pub fn fill_period_quotas(&mut self) {
        for l in &mut self.limits {
            l.fill_period_quota();
        }
    }
}

/// Checker failure. InvalidCredentials means cached quota data must not be
/// trusted (401/403-class), matching axonhub's ErrInvalidCredentials.
#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
}

#[async_trait]
pub trait QuotaChecker: Send + Sync {
    fn provider_type(&self) -> &'static str;
    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError>;

    /// Optional quota-reset capability (axonhub `Resetter`). Checkers that
    /// support listing/consuming provider-managed resets override this.
    fn as_resetter(&self) -> Option<&dyn QuotaResetter> {
        None
    }
}

pub fn is_ready_status(status: &str) -> bool {
    status == "available" || status == "warning"
}

/// Rank for comparisons: exhausted > warning > available > unknown.
pub fn status_rank(status: &str) -> u8 {
    match status {
        "exhausted" => 3,
        "warning" => 2,
        "available" => 1,
        _ => 0,
    }
}

pub fn normalize_quota_status(status: &str) -> &str {
    match status {
        "available" | "warning" | "exhausted" | "unknown" => status,
        _ => "unknown",
    }
}

/// Ratio -> status using the standard 0.8 / 1.0 thresholds.
pub fn status_from_ratio(ratio: f64) -> &'static str {
    if ratio >= 1.0 {
        "exhausted"
    } else if ratio >= WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    }
}

/// Maps a window length onto a well-known label, "" when unmatched.
pub fn normalize_window_label(window: Duration) -> &'static str {
    if window == Duration::hours(5) {
        WINDOW_5H
    } else if window == Duration::hours(24) {
        WINDOW_DAILY
    } else if window == Duration::hours(7 * 24) {
        WINDOW_7D
    } else if window == Duration::hours(30 * 24) {
        WINDOW_30D
    } else {
        ""
    }
}

pub fn period_start_from_reset(next_reset_at: Option<&DateTime<Utc>>, window: Duration) -> Option<DateTime<Utc>> {
    match next_reset_at {
        Some(t) if window > Duration::zero() => Some(*t - window),
        _ => None,
    }
}

/// Start of the month-long window ending at next_reset_at; the reset day is
/// clamped to the previous month's last day (Go AddDate semantics).
pub fn period_start_from_monthly_reset(next_reset_at: Option<&DateTime<Utc>>) -> Option<DateTime<Utc>> {
    let t = next_reset_at?;
    let (year, month, day) = (t.year(), t.month(), t.day());
    // Day 0 of `month` = last day of the previous month (chrono normalizes).
    let last_day_prev = chrono::NaiveDate::from_ymd_opt(year, month, 1)?
        .pred_opt()?
        .day();
    let day = day.min(last_day_prev);
    let (py, pm) = if month == 1 { (year - 1, 12) } else { (year, month - 1) };
    let date = chrono::NaiveDate::from_ymd_opt(py, pm, day)?;
    let time = chrono::NaiveTime::from_hms_nano_opt(t.hour(), t.minute(), t.second(), t.nanosecond())?;
    Some(DateTime::from_naive_utc_and_offset(date.and_time(time), Utc))
}

/// Derives total money quota of a period: period_cost / usage_ratio.
pub fn estimate_period_quota(period_cost: f64, usage_ratio: f64) -> Option<f64> {
    if period_cost <= 0.0 || !period_cost.is_finite() || usage_ratio <= 0.0 || !usage_ratio.is_finite() {
        return None;
    }
    let total = period_cost / usage_ratio;
    total.is_finite().then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_labels() {
        assert_eq!(normalize_window_label(Duration::hours(5)), "5h");
        assert_eq!(normalize_window_label(Duration::hours(24)), "daily");
        assert_eq!(normalize_window_label(Duration::days(7)), "7d");
        assert_eq!(normalize_window_label(Duration::days(30)), "30d");
        assert_eq!(normalize_window_label(Duration::hours(6)), "");
    }

    #[test]
    fn monthly_reset_clamps_day() {
        // Reset on Mar 31 -> period start Feb 28 (clamped), not Mar 3.
        let reset = "2025-03-31T10:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let start = period_start_from_monthly_reset(Some(&reset)).unwrap();
        assert_eq!(start, "2025-02-28T10:00:00Z".parse::<DateTime<Utc>>().unwrap());
    }

    #[test]
    fn status_thresholds() {
        assert_eq!(status_from_ratio(0.79), "available");
        assert_eq!(status_from_ratio(0.8), "warning");
        assert_eq!(status_from_ratio(1.0), "exhausted");
        assert_eq!(status_rank("exhausted") > status_rank("warning"), true);
    }
}

/// A provider-agnostic quota reset that can be consumed (axonhub `Reset`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reset {
    pub id: String,
    pub status: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(default, rename = "grantedAt", skip_serializing_if = "Option::is_none")]
    pub granted_at: Option<DateTime<Utc>>,
    #[serde(default, rename = "expiresAt", skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// A provider's optional reset capability and current resets (axonhub `ResetList`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetList {
    pub supported: bool,
    #[serde(default)]
    pub resets: Vec<Reset>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Optional capability implemented by quota providers that can list and
/// consume provider-managed quota resets (axonhub `Resetter`).
#[async_trait]
pub trait QuotaResetter: Send + Sync {
    async fn list_resets(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<ResetList, QuotaError>;

    async fn reset(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<(), QuotaError>;
}
