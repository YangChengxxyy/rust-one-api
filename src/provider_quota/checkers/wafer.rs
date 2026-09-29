//! Wafer quota checker — port of axonhub `wafer_checker.go`.
//!
//! GET {scheme}://{host}/v1/inference/quota with a Bearer API key.
//! `current_period_used_percent` drives status (< 80 available, else
//! warning; null -> unknown) unless `remaining_included_requests` <= 0,
//! which forces exhausted. Window boundaries come from the response.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, QuotaLimitType,
    WINDOW_CYCLE,
};
use crate::storage::Channel;

pub struct WaferChecker;

const WAFER_DEFAULT_BASE_URL: &str = "https://pass.wafer.ai";
const WARNING_USED_PERCENT: f64 = 80.0;

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn usage_ratio(percent: Option<f64>) -> f64 {
    percent.map(|p| p / 100.0).unwrap_or(0.0)
}

fn parse_response(body: &str) -> Result<QuotaData, QuotaError> {
    let value: Value = serde_json::from_str(body).map_err(|e| QuotaError::Parse(e.to_string()))?;

    let used_percent = value
        .get("current_period_used_percent")
        .and_then(|v| v.as_f64());
    let remaining_included = value
        .get("remaining_included_requests")
        .and_then(|v| v.as_i64());

    let mut status = "unknown".to_string();
    if let Some(pct) = used_percent {
        if pct < WARNING_USED_PERCENT {
            status = "available".into();
        } else {
            status = "warning".into();
        }
    }
    if remaining_included.map(|r| r <= 0).unwrap_or(false) {
        status = "exhausted".into();
    }

    let next_reset_at = value
        .get("window_end")
        .and_then(|v| v.as_str())
        .and_then(parse_rfc3339);
    let period_start = value
        .get("window_start")
        .and_then(|v| v.as_str())
        .and_then(parse_rfc3339);

    // Pass all reported fields through to raw_data.
    let mut raw = Map::new();
    let passthrough_str = [
        "endpoint",
        "billing_model",
        "plan_tier",
        "window_start",
        "window_end",
    ];
    let passthrough_i64 = [
        "request_count",
        "included_request_limit",
        "included_request_count",
        "remaining_included_requests",
        "overage_request_count",
        "input_tokens",
        "output_tokens",
        "total_tokens",
    ];
    let passthrough_f64 = ["current_period_used_percent"];
    if let Some(obj) = value.as_object() {
        for key in passthrough_str {
            if let Some(v) = obj.get(key).and_then(|v| v.as_str()) {
                raw.insert(key.into(), Value::String(v.to_string()));
            }
        }
        for key in passthrough_i64 {
            if let Some(v) = obj.get(key).and_then(|v| v.as_i64()) {
                raw.insert(key.into(), v.into());
            }
        }
        for key in passthrough_f64 {
            if let Some(v) = obj.get(key).and_then(|v| v.as_f64()) {
                raw.insert(key.into(), v.into());
            }
        }
    }

    let ratio = if status == "exhausted" && used_percent.is_none() {
        1.0
    } else {
        usage_ratio(used_percent)
    };

    let mut limit = QuotaLimitStatus::new(QuotaLimitType::Token, &status, ratio, next_reset_at);
    limit.ready = is_ready_status(&status);
    limit.window = WINDOW_CYCLE.into();
    // Wafer reports both ends of the billing window outright.
    limit.period_start = period_start;

    let mut quota = QuotaData::new("wafer", &status);
    quota.ready = is_ready_status(&status);
    quota.raw_data = raw;
    quota.next_reset_at = next_reset_at;
    quota.limits = vec![limit];
    Ok(quota)
}

/// Rebuilds `{scheme}://{host}/v1/inference/quota`, upgrading http to https.
fn build_wafer_quota_url(base_url: &str) -> String {
    let base = base_url.trim();
    let (scheme, rest) = match base.split_once("://") {
        Some((s, r)) => (s.to_string(), r),
        None => ("https".to_string(), base),
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return format!("{WAFER_DEFAULT_BASE_URL}/v1/inference/quota");
    }
    let scheme = if scheme == "http" { "https" } else { &scheme };
    format!("{scheme}://{host}/v1/inference/quota")
}

#[async_trait]
impl QuotaChecker for WaferChecker {
    fn provider_type(&self) -> &'static str {
        "wafer"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = creds
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .map(str::to_string)
            .or_else(|| creds.api_keys.first().cloned())
            .ok_or_else(|| QuotaError::InvalidCredentials("channel has no API key".into()))?;

        let quota_url = build_wafer_quota_url(&channel.base_url);

        let resp = http
            .get(&quota_url)
            .bearer_auth(&api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("quota request failed: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(QuotaError::InvalidCredentials(format!(
                "HTTP {}",
                status.as_u16()
            )));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("reading body: {e}")))?;
        if !status.is_success() {
            let snippet: String = body.chars().take(200).collect();
            return Err(QuotaError::Http(format!(
                "HTTP {}: {}",
                status.as_u16(),
                snippet
            )));
        }

        parse_response(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HAPPY: &str = r#"{
        "endpoint": "/v1/inference",
        "billing_model": "included",
        "plan_tier": "pro",
        "window_start": "2026-09-01T00:00:00Z",
        "window_end": "2026-10-01T00:00:00Z",
        "request_count": 120,
        "included_request_limit": 1000,
        "included_request_count": 120,
        "remaining_included_requests": 880,
        "overage_request_count": 0,
        "current_period_used_percent": 12.0,
        "input_tokens": 1000,
        "output_tokens": 2000,
        "total_tokens": 3000
    }"#;

    #[test]
    fn happy_path_available() {
        let q = parse_response(HAPPY).unwrap();
        assert_eq!(q.status, "available");
        assert!(q.ready);
        assert_eq!(q.provider_type, "wafer");
        assert!((q.limits[0].usage_ratio - 0.12).abs() < 1e-9);
        assert_eq!(q.limits[0].window, "cycle");
        assert_eq!(
            q.limits[0].period_start.unwrap().to_rfc3339(),
            "2026-09-01T00:00:00+00:00"
        );
        assert_eq!(
            q.next_reset_at.unwrap().to_rfc3339(),
            "2026-10-01T00:00:00+00:00"
        );
        assert_eq!(q.raw_data["plan_tier"], "pro");
        assert_eq!(q.raw_data["remaining_included_requests"], serde_json::json!(880));
    }

    #[test]
    fn warning_at_80_percent() {
        let q = parse_response(
            r#"{"current_period_used_percent": 80.0, "remaining_included_requests": 200}"#,
        )
        .unwrap();
        assert_eq!(q.status, "warning");
        assert!(q.ready);
        assert!((q.limits[0].usage_ratio - 0.8).abs() < 1e-9);
    }

    #[test]
    fn exhausted_when_no_remaining_requests() {
        let q = parse_response(
            r#"{"current_period_used_percent": 95.0, "remaining_included_requests": 0}"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
        assert!(!q.ready);
        // used_percent present, so ratio still derives from it
        assert!((q.limits[0].usage_ratio - 0.95).abs() < 1e-9);
    }

    #[test]
    fn exhausted_without_percent_ratio_is_one() {
        let q = parse_response(r#"{"remaining_included_requests": 0}"#).unwrap();
        assert_eq!(q.status, "exhausted");
        assert_eq!(q.limits[0].usage_ratio, 1.0);
    }

    #[test]
    fn null_percent_unknown() {
        let q = parse_response(
            r#"{"current_period_used_percent": null, "remaining_included_requests": 100}"#,
        )
        .unwrap();
        assert_eq!(q.status, "unknown");
        assert!(!q.ready);
        assert_eq!(q.limits[0].usage_ratio, 0.0);
    }

    #[test]
    fn empty_body_unknown() {
        let q = parse_response("{}").unwrap();
        assert_eq!(q.status, "unknown");
        assert!(q.raw_data.is_empty());
    }

    #[test]
    fn malformed_json() {
        assert!(parse_response("{invalid").is_err());
    }

    #[test]
    fn url_building() {
        assert_eq!(
            build_wafer_quota_url(""),
            "https://pass.wafer.ai/v1/inference/quota"
        );
        assert_eq!(
            build_wafer_quota_url("https://pass.wafer.ai/v1"),
            "https://pass.wafer.ai/v1/inference/quota"
        );
        assert_eq!(
            build_wafer_quota_url("http://pass.wafer.ai"),
            "https://pass.wafer.ai/v1/inference/quota"
        );
    }
}
