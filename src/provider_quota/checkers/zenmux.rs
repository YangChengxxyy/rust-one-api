//! ZenMux quota checker — port of axonhub's `zenmux_checker.go`.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, status_from_ratio, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus,
    WINDOW_5H, WINDOW_7D,
};
use crate::storage::Channel;

const ZENMUX_DEFAULT_BASE_URL: &str = "https://zenmux.ai";
const SUBSCRIPTION_DETAIL_PATH: &str = "/api/v1/management/subscription/detail";

pub struct ZenmuxChecker;

#[derive(Debug, Deserialize)]
struct SubscriptionResponse {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    message: String,
    data: Option<SubscriptionData>,
}

#[derive(Debug, Deserialize)]
struct SubscriptionData {
    #[serde(default)]
    plan: Value,
    #[serde(default)]
    account_status: String,
    #[serde(default)]
    quota_5_hour: QuotaWindow,
    #[serde(default)]
    quota_7_day: QuotaWindow,
    #[serde(default)]
    quota_monthly: Value,
}

#[derive(Debug, Default, Deserialize)]
struct QuotaWindow {
    #[serde(default)]
    usage_percentage: f64,
    resets_at: Option<String>,
}

#[async_trait]
impl QuotaChecker for ZenmuxChecker {
    fn provider_type(&self) -> &'static str {
        "zenmux"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        _channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let management_key = creds
            .management_api_key
            .as_deref()
            .map(str::trim)
            .unwrap_or("");
        if management_key.is_empty() {
            return Err(QuotaError::InvalidCredentials(
                "channel has no management API key".into(),
            ));
        }

        let url = format!("{ZENMUX_DEFAULT_BASE_URL}{SUBSCRIPTION_DETAIL_PATH}");
        let resp = http
            .get(&url)
            .bearer_auth(management_key)
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("zenmux quota request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let code = status.as_u16();
            let body = resp.text().await.unwrap_or_default();
            if code == 401 || code == 403 {
                return Err(QuotaError::InvalidCredentials(format!("HTTP {code}")));
            }
            return Err(QuotaError::Http(format!(
                "zenmux quota request returned HTTP {code}: {}",
                body.chars().take(200).collect::<String>()
            )));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("zenmux quota read failed: {e}")))?;
        parse_zenmux_quota_response(&body)
    }
}

fn parse_zenmux_quota_response(body: &str) -> Result<QuotaData, QuotaError> {
    let response: SubscriptionResponse = serde_json::from_str(body)
        .map_err(|e| QuotaError::Parse(format!("failed to parse zenmux quota response: {e}")))?;

    if !response.success {
        if !response.message.is_empty() {
            return Err(QuotaError::Parse(format!("zenmux API error: {}", response.message)));
        }
        return Err(QuotaError::Parse("zenmux API returned success=false".into()));
    }
    let data = response
        .data
        .ok_or_else(|| QuotaError::Parse("zenmux quota response contains no data".into()))?;

    let five_hour_reset = parse_reset_at(data.quota_5_hour.resets_at.as_deref())
        .map_err(|e| QuotaError::Parse(format!("parse zenmux 5-hour reset: {e}")))?;
    let seven_day_reset = parse_reset_at(data.quota_7_day.resets_at.as_deref())
        .map_err(|e| QuotaError::Parse(format!("parse zenmux 7-day reset: {e}")))?;

    let five_hour_status = status_from_ratio(data.quota_5_hour.usage_percentage);
    let seven_day_status = status_from_ratio(data.quota_7_day.usage_percentage);
    let overall_status = worst_zenmux_status(&[
        account_status_map(&data.account_status),
        five_hour_status,
        seven_day_status,
    ]);

    let next_reset_at = match (five_hour_reset, seven_day_reset) {
        (Some(f), Some(s)) => Some(f.min(s)),
        (Some(f), None) => Some(f),
        (None, s) => s,
    };

    let limits = vec![
        QuotaLimitStatus::token(five_hour_status, data.quota_5_hour.usage_percentage, five_hour_reset)
            .with_window(WINDOW_5H, Duration::hours(5)),
        QuotaLimitStatus::token(seven_day_status, data.quota_7_day.usage_percentage, seven_day_reset)
            .with_window(WINDOW_7D, Duration::hours(7 * 24)),
    ];

    let mut raw_data = Map::new();
    raw_data.insert("plan".into(), data.plan);
    raw_data.insert("quota_monthly".into(), data.quota_monthly);
    raw_data.insert("account_status".into(), Value::String(data.account_status));

    let ready = is_ready_status(&overall_status);
    let mut out = QuotaData::new("zenmux", &overall_status);
    out.raw_data = raw_data;
    out.next_reset_at = next_reset_at;
    out.ready = ready;
    out.limits = limits;
    Ok(out)
}

fn parse_reset_at(value: Option<&str>) -> Result<Option<DateTime<Utc>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    DateTime::parse_from_rfc3339(value)
        .map(|t| Some(t.with_timezone(&Utc)))
        .map_err(|e| format!("invalid reset timestamp {value:?}: {e}"))
}

fn account_status_map(status: &str) -> &'static str {
    match status.trim().to_lowercase().as_str() {
        "healthy" | "monitored" => "available",
        "abusive" => "warning",
        "suspended" | "banned" => "exhausted",
        _ => "unknown",
    }
}

fn worst_zenmux_status(statuses: &[&str]) -> String {
    let rank = |s: &str| match s {
        "available" => 0,
        "warning" => 1,
        "exhausted" => 2,
        _ => 3,
    };
    let mut worst = "available";
    for status in statuses {
        if rank(status) > rank(worst) {
            worst = status;
        }
    }
    worst.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(account_status: &str, five_hour_usage: f64, five_hour_reset: &str) -> String {
        format!(
            r#"{{
                "success": true,
                "data": {{
                    "plan": {{"tier":"pro","amount_usd":200,"interval":"month","expires_at":"2099-10-01T00:00:00Z"}},
                    "account_status": "{}",
                    "quota_5_hour": {{
                        "usage_percentage": {},
                        "resets_at": {},
                        "max_flows": 1000,
                        "used_flows": 400,
                        "remaining_flows": 600,
                        "used_value_usd": 40,
                        "max_value_usd": 100
                    }},
                    "quota_7_day": {{
                        "usage_percentage": 0.6,
                        "resets_at": "2099-09-10T10:00:00Z",
                        "max_flows": 10000,
                        "used_flows": 6000,
                        "remaining_flows": 4000,
                        "used_value_usd": 120,
                        "max_value_usd": 200
                    }},
                    "quota_monthly": {{"max_flows":50000,"max_value_usd":500}}
                }}
            }}"#,
            account_status, five_hour_usage, five_hour_reset
        )
    }

    #[test]
    fn healthy_mid_usage() {
        let quota = parse_zenmux_quota_response(&fixture("healthy", 0.4, r#""2099-09-03T15:00:00Z""#)).unwrap();
        assert_eq!(quota.status, "available");
        assert!(quota.ready);
        assert_eq!(quota.provider_type, "zenmux");
        assert_eq!(quota.limits.len(), 2);
        assert_eq!(quota.limits[0].window, "5h");
        assert_eq!(quota.limits[0].status, "available");
        assert!(quota.limits[0].next_reset_at.is_some());
        assert!(quota.limits[0].period_start.is_some());
        assert_eq!(quota.limits[1].window, "7d");
        // Earliest reset: 5h window (Sep 3 < Sep 10).
        assert_eq!(
            quota.next_reset_at,
            DateTime::parse_from_rfc3339("2099-09-03T15:00:00Z").ok().map(|t| t.with_timezone(&Utc))
        );
        assert!(quota.raw_data.contains_key("plan"));
        assert!(quota.raw_data.contains_key("quota_monthly"));
        assert_eq!(quota.raw_data["account_status"], "healthy");
    }

    #[test]
    fn five_hour_exhausted() {
        let quota = parse_zenmux_quota_response(&fixture("healthy", 1.0, r#""2099-09-03T15:00:00Z""#)).unwrap();
        assert_eq!(quota.status, "exhausted");
        assert!(!quota.ready);
        assert_eq!(quota.limits[0].status, "exhausted");
    }

    #[test]
    fn null_reset_keeps_limit_without_period_start() {
        let quota = parse_zenmux_quota_response(&fixture("healthy", 0.2, "null")).unwrap();
        assert_eq!(quota.status, "available");
        assert_eq!(quota.limits[0].status, "available");
        assert!(quota.limits[0].next_reset_at.is_none());
        assert!(quota.limits[0].period_start.is_none());
        // 7-day reset still present and becomes the overall next reset.
        assert!(quota.next_reset_at.is_some());
    }

    #[test]
    fn suspended_account_is_exhausted() {
        let quota = parse_zenmux_quota_response(&fixture("suspended", 0.2, r#""2099-09-03T15:00:00Z""#)).unwrap();
        assert_eq!(quota.status, "exhausted");
        assert!(!quota.ready);
        assert_eq!(quota.limits[0].status, "available");
        assert_eq!(quota.raw_data["account_status"], "suspended");
    }

    #[test]
    fn account_status_mapping() {
        assert_eq!(account_status_map("healthy"), "available");
        assert_eq!(account_status_map("Monitored "), "available");
        assert_eq!(account_status_map("abusive"), "warning");
        assert_eq!(account_status_map("banned"), "exhausted");
        assert_eq!(account_status_map("weird"), "unknown");
        // Unknown ranks worst in the overall combination.
        let quota = parse_zenmux_quota_response(&fixture("weird", 0.1, "null")).unwrap();
        assert_eq!(quota.status, "unknown");
        assert!(!quota.ready);
    }

    #[test]
    fn unsuccessful_response_is_error() {
        let err = parse_zenmux_quota_response(r#"{"success":false,"message":"invalid management key"}"#).unwrap_err();
        assert!(err.to_string().contains("invalid management key"));
        assert!(parse_zenmux_quota_response(r#"{"success":false}"#).is_err());
    }

    #[test]
    fn bad_reset_timestamp_is_error() {
        let err = parse_zenmux_quota_response(&fixture("healthy", 0.2, r#""Sep 3 2099""#)).unwrap_err();
        assert!(err.to_string().contains("5-hour reset"));
        let err = parse_zenmux_quota_response(
            &fixture("healthy", 0.2, r#""2099-09-03T15:00:00Z""#)
                .replace("2099-09-10T10:00:00Z", "not-a-date"),
        ).unwrap_err();
        assert!(err.to_string().contains("7-day reset"));
    }

    #[test]
    fn missing_data_is_error() {
        assert!(parse_zenmux_quota_response(r#"{"success":true}"#).is_err());
        assert!(parse_zenmux_quota_response("not json").is_err());
    }
}
