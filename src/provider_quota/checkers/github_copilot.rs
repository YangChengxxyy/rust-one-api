//! Port of axonhub `github_copilot_checker.go`. Reads
//! `https://api.github.com/copilot_internal/user` with the LiteLLM-style
//! editor headers from copilot/outbound.go SetCopilotHeaders.

use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use serde_json::{Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, WARNING_THRESHOLD_RATIO,
};
use crate::storage::Channel;

const USER_URL: &str = "https://api.github.com/copilot_internal/user";

pub struct GithubCopilotChecker;

/// getAccessToken: oauth token first, else the api key itself.
fn get_access_token(creds: &ChannelCredentials) -> Result<String, QuotaError> {
    if let Some(token) = creds.oauth_access_token().filter(|t| !t.is_empty()) {
        return Ok(token);
    }
    let token = creds.api_key.as_deref().map(str::trim).unwrap_or("");
    if token.is_empty() {
        return Err(QuotaError::InvalidCredentials("GitHub access token is missing".into()));
    }
    Ok(token.to_string())
}

/// mapPlanType.
pub fn map_plan_type(sku: &str) -> &'static str {
    match sku {
        "copilot_free" | "free_limited_copilot" => "Free",
        "copilot_pro" => "Pro",
        "copilot_pro_plus" => "Pro+",
        "copilot_business" => "Business",
        "copilot_enterprise" => "Enterprise",
        "free_educational_quota" => "Edu",
        _ => "",
    }
}

/// getNumber: JSON numbers only.
fn get_number(val: &Value) -> Option<f64> {
    val.as_f64()
}

fn is_finite_number(value: f64) -> bool {
    value.is_finite()
}

fn copilot_limit(window: &str, ratio: f64, reset_at: Option<DateTime<Utc>>) -> QuotaLimitStatus {
    let status = if ratio >= 1.0 {
        "exhausted"
    } else if ratio >= WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    };
    QuotaLimitStatus::token(status, ratio, reset_at).with_window(window, Duration::zero())
}

fn build_limits(payload: &Value, reset_at: Option<DateTime<Utc>>) -> Vec<QuotaLimitStatus> {
    let mut limits = Vec::new();
    let empty = Map::new();

    let limited = payload
        .get("limited_user_quotas")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let monthly = payload
        .get("monthly_quotas")
        .and_then(Value::as_object)
        .unwrap_or(&empty);

    for (key, remaining_val) in limited {
        let Some(remaining) = get_number(remaining_val) else { continue };
        if !is_finite_number(remaining) || remaining < 0.0 {
            continue;
        }
        let mut total = remaining;
        if let Some(t) = monthly.get(key).and_then(get_number) {
            if is_finite_number(t) && t >= 0.0 {
                total = t;
            }
        }
        if total == 0.0 && remaining != 0.0 {
            continue;
        }
        let ratio = if total > 0.0 { (total - remaining) / total } else { 0.0 };
        limits.push(copilot_limit(key, ratio, reset_at));
    }

    let snapshots = payload
        .get("quota_snapshots")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    for (key, snapshot) in snapshots {
        let Some(s) = snapshot.as_object() else { continue };
        if s.get("unlimited") == Some(&Value::Bool(true)) {
            continue;
        }
        let Some(remaining) = s.get("percent_remaining").and_then(get_number) else { continue };
        if !is_finite_number(remaining) || !(0.0..=100.0).contains(&remaining) {
            continue;
        }
        limits.push(copilot_limit(key, 1.0 - remaining / 100.0, reset_at));
    }

    limits
}

/// calculateStatus: lowest remaining percentage -> status.
pub fn calculate_status(payload: &Value) -> String {
    let mut lowest_percentage = 100.0f64;
    let empty = Map::new();

    let limited = payload
        .get("limited_user_quotas")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let monthly = payload
        .get("monthly_quotas")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    for (key, remaining_val) in limited {
        if let Some(remaining) = get_number(remaining_val) {
            let mut total = remaining;
            if let Some(t) = monthly.get(key).and_then(get_number) {
                if t > 0.0 {
                    total = t;
                }
            }
            if total > 0.0 {
                let pct = (remaining / total) * 100.0;
                if pct < lowest_percentage {
                    lowest_percentage = pct;
                }
            }
        }
    }

    if let Some(snapshots) = payload.get("quota_snapshots").and_then(Value::as_object) {
        for snapshot in snapshots.values() {
            let Some(s) = snapshot.as_object() else { continue };
            if s.get("unlimited") == Some(&Value::Bool(true)) {
                continue;
            }
            if let Some(pct) = s.get("percent_remaining").and_then(get_number) {
                if pct < lowest_percentage {
                    lowest_percentage = pct;
                }
            }
        }
    }

    if lowest_percentage <= 0.0 {
        "exhausted".into()
    } else if lowest_percentage < 20.0 {
        "warning".into()
    } else {
        "available".into()
    }
}

/// parseResetDate: quota_reset_date_utc (RFC3339), then quota_reset_date,
/// then limited_user_reset_date (both YYYY-MM-DD).
pub fn parse_reset_date(payload: &Value) -> Option<DateTime<Utc>> {
    if let Some(v) = payload.get("quota_reset_date_utc").and_then(Value::as_str) {
        if !v.is_empty() {
            if let Ok(t) = DateTime::parse_from_rfc3339(v) {
                return Some(t.with_timezone(&Utc));
            }
        }
    }
    for field in ["quota_reset_date", "limited_user_reset_date"] {
        if let Some(v) = payload.get(field).and_then(Value::as_str) {
            if v.is_empty() {
                continue;
            }
            if let Ok(d) = NaiveDate::parse_from_str(v, "%Y-%m-%d") {
                let dt = d.and_hms_opt(0, 0, 0)?;
                return Some(Utc.from_utc_datetime(&dt));
            }
        }
    }
    None
}

fn prepare_raw_data(payload: &Value) -> Map<String, Value> {
    let mut raw = Map::new();
    raw.insert(
        "copilot_plan".into(),
        payload.get("copilot_plan").cloned().unwrap_or(Value::Null),
    );
    if let Some(sku) = payload.get("access_type_sku").and_then(Value::as_str) {
        raw.insert("access_type_sku".into(), sku.into());
        let plan = map_plan_type(sku);
        if !plan.is_empty() {
            raw.insert("plan_type".into(), plan.into());
        }
    }
    for (field, out) in [
        ("quota_reset_date_utc", "quota_reset_date_utc"),
        ("limited_user_reset_date", "limited_user_reset_date"),
        ("quota_reset_date", "quota_reset_date"),
    ] {
        if let Some(v) = payload.get(field) {
            if !v.is_null() {
                raw.insert(out.into(), v.clone());
            }
        }
    }
    if let Some(v) = payload.get("limited_user_quotas") {
        if !v.is_null() {
            raw.insert("limited_user_quotas".into(), v.clone());
        }
    }
    if let Some(v) = payload.get("monthly_quotas") {
        if !v.is_null() {
            raw.insert("total_quotas".into(), v.clone());
        }
    }
    if let Some(v) = payload.get("quota_snapshots") {
        if !v.is_null() {
            raw.insert("quota_snapshots".into(), v.clone());
        }
    }
    raw
}

/// Payload JSON -> QuotaData (pure, unit-testable).
pub fn parse_response(payload: &Value) -> Result<QuotaData, QuotaError> {
    let status = calculate_status(payload);
    let reset_at = parse_reset_date(payload);
    let limits = build_limits(payload, reset_at);

    let mut data = QuotaData::new("github_copilot", &status);
    data.raw_data = prepare_raw_data(payload);
    data.next_reset_at = reset_at;
    data.ready = is_ready_status(&status);
    data.limits = limits;
    Ok(data)
}

#[async_trait]
impl QuotaChecker for GithubCopilotChecker {
    fn provider_type(&self) -> &'static str {
        "github_copilot"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        _channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let access_token = get_access_token(creds)?;

        let response = http
            .get(USER_URL)
            .header("Authorization", format!("token {access_token}"))
            .header("Accept", "application/json")
            // SetCopilotHeaders (copilot/outbound.go)
            .header("Editor-Version", "vscode/1.95.0")
            .header("Editor-Plugin-Version", "copilot-chat/0.26.7")
            .header("User-Agent", "GitHubCopilotChat/0.26.7")
            .header("Copilot-Integration-Id", "vscode-chat")
            .header("Openai-Intent", "conversation-edits")
            .header("X-Github-Api-Version", "2025-04-01")
            .header("X-Vscode-User-Agent-Library-Version", "electron-fetch")
            .timeout(StdDuration::from_secs(30))
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("failed to fetch copilot user info: {e}")))?;

        let status = response.status().as_u16();
        if status == 401 || status == 403 {
            return Err(QuotaError::InvalidCredentials(format!("HTTP {status}")));
        }
        if !(200..300).contains(&status) {
            return Err(QuotaError::Http(format!(
                "failed to fetch copilot user info, status: {status}"
            )));
        }

        let text = response
            .text()
            .await
            .map_err(|e| QuotaError::Parse(format!("failed to read copilot user response: {e}")))?;
        let payload: Value = serde_json::from_str(&text)
            .map_err(|e| QuotaError::Parse(format!("failed to parse copilot user response: {e}")))?;
        parse_response(&payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn status_from_limited_quotas_percent() {
        let payload = serde_json::json!({
            "limited_user_quotas": {"chat": 90, "completions": 900},
            "monthly_quotas": {"chat": 300, "completions": 3000}
        });
        assert_eq!(calculate_status(&payload), "available"); // 30% remaining, above the 20% warning line
    }

    #[test]
    fn status_warning_below_20_percent() {
        let payload = serde_json::json!({
            "limited_user_quotas": {"chat": 50},
            "monthly_quotas": {"chat": 300}
        });
        assert_eq!(calculate_status(&payload), "warning");
    }

    #[test]
    fn status_exhausted_at_zero() {
        let payload = serde_json::json!({
            "limited_user_quotas": {"chat": 0},
            "monthly_quotas": {"chat": 300}
        });
        assert_eq!(calculate_status(&payload), "exhausted");
    }

    #[test]
    fn status_from_snapshots_ignores_unlimited() {
        let payload = serde_json::json!({
            "quota_snapshots": {
                "premium_requests": {"percent_remaining": 5.0},
                "chat": {"percent_remaining": 90.0, "unlimited": true}
            }
        });
        assert_eq!(calculate_status(&payload), "warning");
    }

    #[test]
    fn limits_from_limited_quotas_and_snapshots() {
        let payload = serde_json::json!({
            "quota_reset_date_utc": "2026-10-01T00:00:00Z",
            "limited_user_quotas": {"chat": 30, "completions": 300},
            "monthly_quotas": {"chat": 300, "completions": 3000},
            "quota_snapshots": {
                "premium_requests": {"percent_remaining": 25.0},
                "premium": {"percent_remaining": 100.0, "unlimited": true},
                "broken": {"percent_remaining": 150.0}
            }
        });
        let data = parse_response(&payload).unwrap();
        let by_window: std::collections::HashMap<&str, &QuotaLimitStatus> =
            data.limits.iter().map(|l| (l.window.as_str(), l)).collect();
        assert_eq!(data.limits.len(), 3); // chat + completions quotas, premium_requests snapshot
        assert!((by_window["chat"].usage_ratio - 0.9).abs() < 1e-9);
        assert!((by_window["completions"].usage_ratio - 0.9).abs() < 1e-9);
        assert!((by_window["premium_requests"].usage_ratio - 0.75).abs() < 1e-9);
        assert_eq!(data.next_reset_at, DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z").ok().map(|t| t.with_timezone(&Utc)));
        assert_eq!(data.status, "warning");
        assert_eq!(data.raw_data["total_quotas"]["chat"], 300);
        assert!(data.raw_data.get("plan_type").is_none());
    }

    #[test]
    fn limit_ratio_zero_when_no_total() {
        let payload = serde_json::json!({
            "limited_user_quotas": {"chat": 10}
        });
        let data = parse_response(&payload).unwrap();
        assert_eq!(data.limits.len(), 1);
        assert_eq!(data.limits[0].usage_ratio, 0.0);
        assert_eq!(data.limits[0].status, "available");
    }

    #[test]
    fn reset_date_precedence() {
        let payload = serde_json::json!({
            "quota_reset_date_utc": "2026-10-01T12:00:00Z",
            "quota_reset_date": "2026-11-01",
            "limited_user_reset_date": "2026-12-01"
        });
        assert_eq!(
            parse_reset_date(&payload),
            DateTime::parse_from_rfc3339("2026-10-01T12:00:00Z").ok().map(|t| t.with_timezone(&Utc))
        );

        let payload = serde_json::json!({
            "quota_reset_date": "2026-11-01",
            "limited_user_reset_date": "2026-12-01"
        });
        assert_eq!(
            parse_reset_date(&payload),
            NaiveDate::parse_from_str("2026-11-01", "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|dt| Utc.from_utc_datetime(&dt))
        );

        let payload = serde_json::json!({"limited_user_reset_date": "2026-12-01"});
        assert_eq!(
            parse_reset_date(&payload).map(|t| t.date_naive()),
            NaiveDate::parse_from_str("2026-12-01", "%Y-%m-%d").ok()
        );
    }

    #[test]
    fn invalid_utc_date_falls_back() {
        let payload = serde_json::json!({
            "quota_reset_date_utc": "not-a-date",
            "quota_reset_date": "2026-11-01"
        });
        assert!(parse_reset_date(&payload).is_some());
    }

    #[test]
    fn map_plan_type_values() {
        assert_eq!(map_plan_type("copilot_free"), "Free");
        assert_eq!(map_plan_type("free_limited_copilot"), "Free");
        assert_eq!(map_plan_type("copilot_pro_plus"), "Pro+");
        assert_eq!(map_plan_type("free_educational_quota"), "Edu");
        assert_eq!(map_plan_type("whatever"), "");
    }

    #[test]
    fn raw_data_plan_type_mapped() {
        let payload = serde_json::json!({
            "copilot_plan": "",
            "access_type_sku": "copilot_business"
        });
        let data = parse_response(&payload).unwrap();
        assert_eq!(data.raw_data["plan_type"], "Business");
        assert_eq!(data.raw_data["access_type_sku"], "copilot_business");
        assert_eq!(data.status, "available");
        assert!(data.limits.is_empty());
    }

    #[test]
    fn access_token_fallback_to_api_key() {
        let creds = ChannelCredentials {
            api_key: Some(" ghu_abc ".into()),
            ..Default::default()
        };
        assert_eq!(get_access_token(&creds).unwrap(), "ghu_abc");

        let empty = ChannelCredentials::default();
        assert!(matches!(get_access_token(&empty), Err(QuotaError::InvalidCredentials(_))));
    }
}
