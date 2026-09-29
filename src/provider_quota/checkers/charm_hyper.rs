//! Charm Hyper quota checker — port of axonhub `charm_hyper_checker.go`.
//!
//! GET {base}/v1/credits with a Bearer API key; the `balance` field is
//! compared against a 100-credit baseline: 0 -> exhausted, <= 20 -> warning,
//! otherwise available. Usage ratio = max(0, 1 - balance/100).

use async_trait::async_trait;
use chrono::Duration;
use serde_json::{json, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, WINDOW_CREDITS,
};
use crate::storage::Channel;

pub struct CharmHyperChecker;

const CHARM_HYPER_DEFAULT_BASE_URL: &str = "https://hyper.charm.land";
const CHARM_HYPER_BASELINE: f64 = 100.0;
const CHARM_HYPER_WARNING_THRESHOLD: f64 = 20.0;

fn compute_status(balance: f64) -> (String, bool, f64) {
    let mut usage_ratio = 1.0 - balance / CHARM_HYPER_BASELINE;
    if usage_ratio < 0.0 {
        usage_ratio = 0.0;
    }

    if balance == 0.0 {
        return ("exhausted".into(), false, 1.0);
    }
    if balance <= CHARM_HYPER_WARNING_THRESHOLD {
        return ("warning".into(), true, usage_ratio);
    }
    ("available".into(), true, usage_ratio)
}

fn parse_response(body: &str) -> Result<QuotaData, QuotaError> {
    let value: Value = serde_json::from_str(body).map_err(|e| QuotaError::Parse(e.to_string()))?;
    let balance = value
        .get("balance")
        .and_then(|b| b.as_f64())
        .ok_or_else(|| QuotaError::Parse("missing balance field".into()))?;

    let (status, ready, usage_ratio) = compute_status(balance);

    let mut quota = QuotaData::new("charm_hyper", &status);
    quota.ready = ready;
    quota.raw_data.insert("balance".into(), json!(balance));
    quota.limits = vec![QuotaLimitStatus::token(&status, usage_ratio, None)
        .with_window(WINDOW_CREDITS, Duration::zero())];
    Ok(quota)
}

/// Rebuilds `{scheme}://{host}/v1/credits`, dropping any path/query/fragment.
fn build_charm_hyper_quota_url(base_url: &str) -> String {
    let base = base_url.trim();
    let (scheme, rest) = match base.split_once("://") {
        Some((s, r)) => (s, r),
        None => ("https", base),
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return format!("{CHARM_HYPER_DEFAULT_BASE_URL}/v1/credits");
    }
    format!("{scheme}://{host}/v1/credits")
}

fn extract_api_key(creds: &ChannelCredentials) -> Option<String> {
    if let Some(key) = creds.api_key.as_deref().map(str::trim).filter(|k| !k.is_empty()) {
        return Some(key.to_string());
    }
    creds
        .api_keys
        .first()
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty())
}

#[async_trait]
impl QuotaChecker for CharmHyperChecker {
    fn provider_type(&self) -> &'static str {
        "charm_hyper"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = extract_api_key(creds)
            .ok_or_else(|| QuotaError::InvalidCredentials("missing API key for Charm Hyper channel".into()))?;

        let base_url = if channel.base_url.trim().is_empty() {
            CHARM_HYPER_DEFAULT_BASE_URL.to_string()
        } else {
            channel.base_url.trim().to_string()
        };
        let quota_url = build_charm_hyper_quota_url(&base_url);

        let resp = http
            .get(&quota_url)
            .bearer_auth(&api_key)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("executing Charm Hyper quota request: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(QuotaError::InvalidCredentials(format!(
                "Charm Hyper quota API returned status {}",
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

    #[test]
    fn happy_path_available() {
        let q = parse_response(r#"{"balance": 85.0}"#).unwrap();
        assert_eq!(q.status, "available");
        assert!(q.ready);
        assert_eq!(q.provider_type, "charm_hyper");
        assert!((q.limits[0].usage_ratio - 0.15).abs() < 1e-9);
        assert_eq!(q.limits[0].window, "credits");
        assert_eq!(q.raw_data["balance"], json!(85.0));
    }

    #[test]
    fn warning_state() {
        let q = parse_response(r#"{"balance": 15.0}"#).unwrap();
        assert_eq!(q.status, "warning");
        assert!(q.ready);
        assert!((q.limits[0].usage_ratio - 0.85).abs() < 1e-9);
    }

    #[test]
    fn warning_boundary_at_20() {
        let q = parse_response(r#"{"balance": 20.0}"#).unwrap();
        assert_eq!(q.status, "warning");
        assert!((q.limits[0].usage_ratio - 0.8).abs() < 1e-9);
    }

    #[test]
    fn just_above_warning_boundary() {
        let q = parse_response(r#"{"balance": 20.5}"#).unwrap();
        assert_eq!(q.status, "available");
    }

    #[test]
    fn exhausted_at_zero() {
        let q = parse_response(r#"{"balance": 0}"#).unwrap();
        assert_eq!(q.status, "exhausted");
        assert!(!q.ready);
        assert_eq!(q.limits[0].usage_ratio, 1.0);
    }

    #[test]
    fn balance_above_baseline_clamps_ratio_to_zero() {
        let q = parse_response(r#"{"balance": 150.0}"#).unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits[0].usage_ratio, 0.0);
    }

    #[test]
    fn full_balance_ratio_zero() {
        let q = parse_response(r#"{"balance": 100.0}"#).unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits[0].usage_ratio, 0.0);
    }

    #[test]
    fn missing_balance_field() {
        assert!(parse_response("{}").is_err());
    }

    #[test]
    fn null_balance_field() {
        assert!(parse_response(r#"{"balance": null}"#).is_err());
    }

    #[test]
    fn malformed_json() {
        assert!(parse_response("{invalid").is_err());
    }

    #[test]
    fn url_building() {
        assert_eq!(
            build_charm_hyper_quota_url(""),
            "https://hyper.charm.land/v1/credits"
        );
        assert_eq!(
            build_charm_hyper_quota_url("https://custom.charm.land/api"),
            "https://custom.charm.land/v1/credits"
        );
        assert_eq!(
            build_charm_hyper_quota_url("https://custom.charm.land/v1/credits?x=1"),
            "https://custom.charm.land/v1/credits"
        );
    }
}
