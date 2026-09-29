//! NanoGPT quota checker — port of axonhub's `nanogpt_checker.go`.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, QuotaLimitType,
    WINDOW_DAILY, WINDOW_WEEKLY, WARNING_THRESHOLD_RATIO,
};
use crate::storage::Channel;

const NANO_GPT_DEFAULT_QUOTA_URL: &str = "https://nano-gpt.com/api/subscription/v1/usage";

pub struct NanogptChecker;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsageResponse {
    active: Option<bool>,
    provider: Option<String>,
    provider_status: Option<String>,
    #[serde(default)]
    limits: Option<Limits>,
    allow_overage: Option<bool>,
    period: Option<Period>,
    daily_images: Option<QuotaWindow>,
    daily_input_tokens: Option<QuotaWindow>,
    weekly_input_tokens: Option<QuotaWindow>,
    state: Option<String>,
    grace_until: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Limits {
    weekly_input_tokens: Option<i64>,
    daily_input_tokens: Option<i64>,
    daily_images: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Period {
    current_period_end: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuotaWindow {
    used: Option<i64>,
    remaining: Option<i64>,
    percent_used: Option<f64>,
    reset_at: Option<i64>,
}

#[async_trait]
impl QuotaChecker for NanogptChecker {
    fn provider_type(&self) -> &'static str {
        "nanogpt"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = creds
            .all_api_keys()
            .first()
            .map(|k| k.trim().to_string())
            .unwrap_or_default();
        if api_key.is_empty() {
            return Err(QuotaError::Parse("channel has no API key".into()));
        }

        let url = build_nanogpt_quota_url(&channel.base_url);
        let resp = http
            .get(&url)
            .bearer_auth(&api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("quota request failed: {e}")))?;

        // Faithful to the Go checker: HTTP status is not inspected; the body
        // is parsed directly and a parse failure surfaces as the error.
        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("quota response read failed: {e}")))?;
        parse_response(&body)
    }
}

fn build_nanogpt_quota_url(base_url: &str) -> String {
    let base_url = base_url.trim();
    if base_url.is_empty() {
        return NANO_GPT_DEFAULT_QUOTA_URL.to_string();
    }
    let Some((scheme, rest)) = base_url.split_once("://") else {
        return NANO_GPT_DEFAULT_QUOTA_URL.to_string();
    };
    let host = rest.split(['/']).next().unwrap_or(rest);
    if scheme.is_empty() && host.is_empty() {
        return NANO_GPT_DEFAULT_QUOTA_URL.to_string();
    }
    let scheme = if scheme == "http" { "https" } else { scheme };
    format!("{scheme}://{host}/api/subscription/v1/usage")
}

fn parse_response(body: &str) -> Result<QuotaData, QuotaError> {
    let response: UsageResponse = serde_json::from_str(body)
        .map_err(|e| QuotaError::Parse(format!("failed to parse nanogpt usage response: {e}")))?;

    let mut normalized_status = "unknown".to_string();
    if let Some(state) = &response.state {
        normalized_status = match state.as_str() {
            "active" => "available".to_string(),
            "grace" => "warning".to_string(),
            "inactive" => "exhausted".to_string(),
            _ => "unknown".to_string(),
        };
    }

    let limits = build_limit_statuses(
        response.daily_images.as_ref(),
        response.daily_input_tokens.as_ref(),
        response.weekly_input_tokens.as_ref(),
    );

    // Escalate to the worst per-limit status; only from available/warning
    // bases so state-driven statuses are never overridden.
    if normalized_status == "available" || normalized_status == "warning" {
        for limit in &limits {
            if status_rank(&limit.status) > status_rank(&normalized_status) {
                normalized_status = limit.status.clone();
            }
        }
    }

    let mut next_reset_at = find_earliest_reset_at(&[
        response.daily_images.as_ref(),
        response.daily_input_tokens.as_ref(),
        response.weekly_input_tokens.as_ref(),
    ]);
    if next_reset_at.is_none() {
        if let Some(grace_until) = &response.grace_until {
            if let Ok(t) = DateTime::parse_from_rfc3339(grace_until) {
                next_reset_at = Some(t.with_timezone(&Utc));
            }
        }
    }

    let mut raw_data = Map::new();
    if let Some(active) = response.active {
        raw_data.insert("active".into(), json!(active));
    }
    if let Some(provider) = &response.provider {
        raw_data.insert("provider".into(), json!(provider));
    }
    if let Some(provider_status) = &response.provider_status {
        raw_data.insert("providerStatus".into(), json!(provider_status));
    }
    if let Some(state) = &response.state {
        raw_data.insert("state".into(), json!(state));
    }
    if let Some(allow_overage) = response.allow_overage {
        raw_data.insert("allowOverage".into(), json!(allow_overage));
    }
    if let Some(limits) = &response.limits {
        let mut m = Map::new();
        if let Some(v) = limits.weekly_input_tokens {
            m.insert("weeklyInputTokens".into(), json!(v));
        }
        if let Some(v) = limits.daily_input_tokens {
            m.insert("dailyInputTokens".into(), json!(v));
        }
        if let Some(v) = limits.daily_images {
            m.insert("dailyImages".into(), json!(v));
        }
        raw_data.insert("limits".into(), Value::Object(m));
    }
    if let Some(period) = &response.period {
        let mut m = Map::new();
        if let Some(v) = &period.current_period_end {
            m.insert("currentPeriodEnd".into(), json!(v));
        }
        raw_data.insert("period".into(), Value::Object(m));
    }
    let mut windows = Map::new();
    if let Some(w) = &response.daily_images {
        windows.insert("dailyImages".into(), window_to_json(w));
    }
    if let Some(w) = &response.daily_input_tokens {
        windows.insert("dailyInputTokens".into(), window_to_json(w));
    }
    if let Some(w) = &response.weekly_input_tokens {
        windows.insert("weeklyInputTokens".into(), window_to_json(w));
    }
    if !windows.is_empty() {
        raw_data.insert("windows".into(), Value::Object(windows));
    }
    if let Some(grace_until) = &response.grace_until {
        raw_data.insert("graceUntil".into(), json!(grace_until));
    }

    let ready = is_ready_status(&normalized_status);
    let mut data = QuotaData::new("nanogpt", &normalized_status);
    data.raw_data = raw_data;
    data.next_reset_at = next_reset_at;
    data.ready = ready;
    data.limits = limits;
    Ok(data)
}

fn build_limit_statuses(
    image_window: Option<&QuotaWindow>,
    daily_token_window: Option<&QuotaWindow>,
    weekly_token_window: Option<&QuotaWindow>,
) -> Vec<QuotaLimitStatus> {
    let typed = [
        (
            image_window,
            QuotaLimitType::Image,
            WINDOW_DAILY,
            Duration::hours(24),
        ),
        (
            daily_token_window,
            QuotaLimitType::Token,
            WINDOW_DAILY,
            Duration::hours(24),
        ),
        (
            weekly_token_window,
            QuotaLimitType::Token,
            WINDOW_WEEKLY,
            Duration::hours(7 * 24),
        ),
    ];

    let mut limits = Vec::new();
    for (window, limit_type, window_label, window_len) in typed {
        let Some(window) = window else { continue };

        let mut status = "available";
        let mut usage_ratio = 0.0;
        if let Some(percent_used) = window.percent_used {
            // percentUsed is a 0-1 fraction.
            usage_ratio = percent_used;
        }

        if window.remaining.map(|r| r <= 0).unwrap_or(false) {
            status = "exhausted";
            usage_ratio = 1.0;
        } else if usage_ratio >= 1.0 {
            status = "exhausted";
        } else if usage_ratio >= WARNING_THRESHOLD_RATIO {
            status = "warning";
        }

        let reset_at = window
            .reset_at
            .filter(|r| *r > 0)
            .and_then(DateTime::from_timestamp_millis);

        limits.push(
            QuotaLimitStatus::new(limit_type, status, usage_ratio, reset_at)
                .with_window(window_label, window_len),
        );
    }
    limits
}

fn find_earliest_reset_at(windows: &[Option<&QuotaWindow>]) -> Option<DateTime<Utc>> {
    let mut earliest: Option<DateTime<Utc>> = None;
    for w in windows.iter().flatten() {
        let Some(reset) = w.reset_at.filter(|r| *r > 0) else {
            continue;
        };
        let Some(t) = DateTime::from_timestamp_millis(reset) else {
            continue;
        };
        if earliest.map(|e| t < e).unwrap_or(true) {
            earliest = Some(t);
        }
    }
    earliest
}

fn window_to_json(window: &QuotaWindow) -> Value {
    let mut m = Map::new();
    if let Some(v) = window.used {
        m.insert("used".into(), json!(v));
    }
    if let Some(v) = window.remaining {
        m.insert("remaining".into(), json!(v));
    }
    if let Some(v) = window.percent_used {
        m.insert("percentUsed".into(), json!(v));
    }
    if let Some(v) = window.reset_at {
        m.insert("resetAt".into(), json!(v));
    }
    Value::Object(m)
}

fn status_rank(status: &str) -> i32 {
    match status {
        "exhausted" => 2,
        "warning" => 1,
        "available" => 0,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_quota_url_and_upgrades_http() {
        assert_eq!(build_nanogpt_quota_url(""), NANO_GPT_DEFAULT_QUOTA_URL);
        assert_eq!(
            build_nanogpt_quota_url("http://nano-gpt.com"),
            "https://nano-gpt.com/api/subscription/v1/usage"
        );
        assert_eq!(
            build_nanogpt_quota_url("https://proxy.example.com/some/path"),
            "https://proxy.example.com/api/subscription/v1/usage"
        );
        assert_eq!(build_nanogpt_quota_url("junk"), NANO_GPT_DEFAULT_QUOTA_URL);
    }

    #[test]
    fn active_state_with_windows() {
        let body = r#"{
            "active": true,
            "provider": "nano",
            "state": "active",
            "limits": {"weeklyInputTokens": 500000, "dailyInputTokens": 50000, "dailyImages": 30},
            "period": {"currentPeriodEnd": "2099-01-01T00:00:00Z"},
            "dailyImages": {"used": 3, "remaining": 27, "percentUsed": 0.1, "resetAt": 1760000000000},
            "dailyInputTokens": {"used": 1000, "remaining": 49000, "percentUsed": 0.02, "resetAt": 1759900000000},
            "weeklyInputTokens": {"used": 10000, "remaining": 490000, "percentUsed": 0.02, "resetAt": 1760600000000}
        }"#;
        let quota = parse_response(body).unwrap();
        assert_eq!(quota.status, "available");
        assert!(quota.ready);
        assert_eq!(quota.provider_type, "nanogpt");
        assert_eq!(quota.limits.len(), 3);
        assert_eq!(quota.limits[0].kind, QuotaLimitType::Image);
        assert_eq!(quota.limits[0].window, "daily");
        assert_eq!(quota.limits[2].window, "weekly");
        assert!((quota.limits[0].usage_ratio - 0.1).abs() < 1e-9);
        // Earliest reset: 1759900000000 (dailyInputTokens).
        assert_eq!(quota.next_reset_at, DateTime::from_timestamp_millis(1759900000000));
        assert_eq!(quota.raw_data["state"], "active");
        assert_eq!(quota.raw_data["limits"]["dailyImages"], 30);
        assert_eq!(quota.raw_data["period"]["currentPeriodEnd"], "2099-01-01T00:00:00Z");
        assert_eq!(quota.raw_data["windows"]["dailyImages"]["percentUsed"], 0.1);
    }

    #[test]
    fn percent_used_scale_is_fraction() {
        // percentUsed 0.9 (fraction, not 90) -> warning.
        let body = r#"{
            "state": "active",
            "dailyInputTokens": {"used": 90, "remaining": 10, "percentUsed": 0.9, "resetAt": 1760000000000}
        }"#;
        let quota = parse_response(body).unwrap();
        assert_eq!(quota.limits[0].status, "warning");
        assert_eq!(quota.status, "warning");
    }

    #[test]
    fn remaining_zero_forces_exhausted() {
        let body = r#"{
            "state": "active",
            "weeklyInputTokens": {"used": 100, "remaining": 0, "percentUsed": 0.5, "resetAt": 1760000000000}
        }"#;
        let quota = parse_response(body).unwrap();
        assert_eq!(quota.limits[0].status, "exhausted");
        assert!((quota.limits[0].usage_ratio - 1.0).abs() < 1e-9);
        assert_eq!(quota.status, "exhausted");
        assert!(!quota.ready);
    }

    #[test]
    fn grace_state_maps_to_warning_and_uses_grace_until() {
        let body = r#"{
            "state": "grace",
            "graceUntil": "2099-06-01T12:00:00Z"
        }"#;
        let quota = parse_response(body).unwrap();
        assert_eq!(quota.status, "warning");
        assert!(quota.ready);
        assert_eq!(
            quota.next_reset_at,
            Some(
                DateTime::parse_from_rfc3339("2099-06-01T12:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }

    #[test]
    fn inactive_state_not_overridden_by_limit_statuses() {
        // Window says available, state says inactive -> stays exhausted.
        let body = r#"{
            "state": "inactive",
            "dailyInputTokens": {"used": 1, "remaining": 99, "percentUsed": 0.01, "resetAt": 1760000000000}
        }"#;
        let quota = parse_response(body).unwrap();
        assert_eq!(quota.status, "exhausted");
        assert_eq!(quota.limits[0].status, "available");
    }

    #[test]
    fn unknown_state_stays_unknown() {
        let body = r#"{"state":"paused"}"#;
        let quota = parse_response(body).unwrap();
        assert_eq!(quota.status, "unknown");
        assert!(!quota.ready);
    }

    #[test]
    fn empty_body_is_error() {
        assert!(parse_response("not json").is_err());
    }
}
