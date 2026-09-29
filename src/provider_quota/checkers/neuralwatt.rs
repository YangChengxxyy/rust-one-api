//! NeuralWatt quota checker — port of axonhub `neuralwatt_checker.go`.
//!
//! GET {scheme}://{host}/v1/quota with a Bearer API key. Status from the
//! `subscription` block: in_overage or kwh_remaining <= 0 -> exhausted;
//! remaining < 20% of included -> warning; otherwise available (unknown when
//! the subscription is absent or lacks kwh_remaining).

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus,
};
use crate::storage::Channel;

pub struct NeuralwattChecker;

const NEURALWATT_DEFAULT_BASE_URL: &str = "https://api.neuralwatt.com";
const WARNING_REMAINING_FRACTION: f64 = 0.2;

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn is_low_remaining(remaining: f64, included: f64) -> bool {
    included > 0.0 && remaining < included * WARNING_REMAINING_FRACTION
}

fn parse_response(body: &str) -> Result<QuotaData, QuotaError> {
    let value: Value = serde_json::from_str(body).map_err(|e| QuotaError::Parse(e.to_string()))?;

    let sub = value.get("subscription").and_then(|s| s.as_object());

    let mut status = "unknown".to_string();
    if let Some(sub) = sub {
        let in_overage = sub.get("in_overage").and_then(|v| v.as_bool());
        let kwh_remaining = sub.get("kwh_remaining").and_then(|v| v.as_f64());
        let kwh_included = sub.get("kwh_included").and_then(|v| v.as_f64());

        if in_overage == Some(true) {
            status = "exhausted".into();
        } else if let Some(remaining) = kwh_remaining {
            if remaining <= 0.0 {
                status = "exhausted".into();
            } else if let Some(included) = kwh_included {
                if is_low_remaining(remaining, included) {
                    status = "warning".into();
                } else {
                    status = "available".into();
                }
            } else {
                status = "available".into();
            }
        }
    }

    let next_reset_at = sub
        .and_then(|s| s.get("kwh_reset_date"))
        .and_then(|v| v.as_str())
        .and_then(parse_rfc3339);

    let mut raw = Map::new();
    if let Some(balance) = value.get("balance").and_then(|b| b.as_object()) {
        let mut m = Map::new();
        for (key, json_key) in [
            ("credits_remaining_usd", "credits_remaining_usd"),
            ("total_credits_usd", "total_credits_usd"),
        ] {
            if let Some(v) = balance.get(key).and_then(|v| v.as_f64()) {
                m.insert(json_key.into(), json!(v));
            }
        }
        if let Some(v) = balance.get("accounting_method").and_then(|v| v.as_str()) {
            m.insert("accounting_method".into(), json!(v));
        }
        raw.insert("balance".into(), Value::Object(m));
    }
    if let Some(sub) = sub {
        let mut m = Map::new();
        for key in [
            "plan",
            "status",
            "kwh_reset_date",
        ] {
            if let Some(v) = sub.get(key).and_then(|v| v.as_str()) {
                m.insert(key.into(), json!(v));
            }
        }
        for key in ["kwh_included", "kwh_used", "kwh_remaining"] {
            if let Some(v) = sub.get(key).and_then(|v| v.as_f64()) {
                m.insert(key.into(), json!(v));
            }
        }
        if let Some(v) = sub.get("in_overage").and_then(|v| v.as_bool()) {
            m.insert("in_overage".into(), json!(v));
        }
        raw.insert("subscription".into(), Value::Object(m));
    }

    let mut usage_ratio = 0.0;
    if let Some(sub) = sub {
        if let Some(included) = sub.get("kwh_included").and_then(|v| v.as_f64()) {
            if included > 0.0 {
                if let Some(used) = sub.get("kwh_used").and_then(|v| v.as_f64()) {
                    usage_ratio = used / included;
                } else if let Some(remaining) = sub.get("kwh_remaining").and_then(|v| v.as_f64()) {
                    usage_ratio = 1.0 - (remaining / included);
                }
            }
        }
    }

    let mut quota = QuotaData::new("neuralwatt", &status);
    quota.ready = is_ready_status(&status);
    quota.raw_data = raw;
    quota.next_reset_at = next_reset_at;
    quota.limits = vec![QuotaLimitStatus::token(&status, usage_ratio, next_reset_at)
        .with_window("kwh", Duration::zero())];
    Ok(quota)
}

/// Rebuilds `{scheme}://{host}/v1/quota`, upgrading http to https.
fn build_neuralwatt_quota_url(base_url: &str) -> String {
    let base = base_url.trim();
    let (scheme, rest) = match base.split_once("://") {
        Some((s, r)) => (s.to_string(), r),
        None => ("https".to_string(), base),
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return format!("{NEURALWATT_DEFAULT_BASE_URL}/v1/quota");
    }
    let scheme = if scheme == "http" { "https" } else { &scheme };
    format!("{scheme}://{host}/v1/quota")
}

#[async_trait]
impl QuotaChecker for NeuralwattChecker {
    fn provider_type(&self) -> &'static str {
        "neuralwatt"
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

        let quota_url = build_neuralwatt_quota_url(&channel.base_url);

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
        "balance": {"credits_remaining_usd": 42.5, "total_credits_usd": 50.0, "accounting_method": "prepaid"},
        "subscription": {
            "plan": "starter", "status": "active",
            "kwh_included": 100.0, "kwh_used": 25.0, "kwh_remaining": 75.0,
            "in_overage": false,
            "kwh_reset_date": "2026-10-01T00:00:00Z"
        }
    }"#;

    #[test]
    fn happy_path_available() {
        let q = parse_response(HAPPY).unwrap();
        assert_eq!(q.status, "available");
        assert!(q.ready);
        assert_eq!(q.provider_type, "neuralwatt");
        assert!((q.limits[0].usage_ratio - 0.25).abs() < 1e-9);
        assert_eq!(q.limits[0].window, "kwh");
        assert_eq!(
            q.next_reset_at.unwrap().to_rfc3339(),
            "2026-10-01T00:00:00+00:00"
        );
        assert_eq!(q.raw_data["balance"]["accounting_method"], json!("prepaid"));
        assert_eq!(q.raw_data["subscription"]["plan"], json!("starter"));
    }

    #[test]
    fn warning_when_below_20_percent() {
        let q = parse_response(
            r#"{"subscription": {"kwh_included": 100.0, "kwh_used": 85.0, "kwh_remaining": 15.0, "in_overage": false}}"#,
        )
        .unwrap();
        assert_eq!(q.status, "warning");
        assert!(q.ready);
        assert!((q.limits[0].usage_ratio - 0.85).abs() < 1e-9);
    }

    #[test]
    fn exhausted_when_in_overage() {
        let q = parse_response(
            r#"{"subscription": {"kwh_included": 100.0, "kwh_used": 100.0, "kwh_remaining": 0.0, "in_overage": true}}"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
        assert!(!q.ready);
    }

    #[test]
    fn exhausted_when_remaining_zero_without_overage() {
        let q = parse_response(
            r#"{"subscription": {"kwh_included": 100.0, "kwh_used": 100.0, "kwh_remaining": 0.0, "in_overage": false}}"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
    }

    #[test]
    fn exhausted_overrides_low_remaining_warning() {
        let q = parse_response(
            r#"{"subscription": {"kwh_included": 100.0, "kwh_remaining": -5.0, "in_overage": false}}"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
    }

    #[test]
    fn missing_subscription_unknown() {
        let q = parse_response(r#"{"balance": {}}"#).unwrap();
        assert_eq!(q.status, "unknown");
        assert!(!q.ready);
        assert_eq!(q.limits[0].usage_ratio, 0.0);
    }

    #[test]
    fn usage_ratio_falls_back_to_remaining() {
        let q = parse_response(
            r#"{"subscription": {"kwh_included": 200.0, "kwh_remaining": 50.0}}"#,
        )
        .unwrap();
        assert!((q.limits[0].usage_ratio - 0.75).abs() < 1e-9);
    }

    #[test]
    fn malformed_json() {
        assert!(parse_response("{invalid").is_err());
    }

    #[test]
    fn url_building() {
        assert_eq!(
            build_neuralwatt_quota_url(""),
            "https://api.neuralwatt.com/v1/quota"
        );
        assert_eq!(
            build_neuralwatt_quota_url("https://api.neuralwatt.com/v1"),
            "https://api.neuralwatt.com/v1/quota"
        );
        assert_eq!(
            build_neuralwatt_quota_url("http://api.example.com"),
            "https://api.example.com/v1/quota"
        );
    }
}
