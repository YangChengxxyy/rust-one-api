//! Apertis quota checker — port of axonhub `apertis_checker.go`.
//!
//! GET {scheme}://{host}/v1/dashboard/billing/credits with a Bearer API key.
//! Status is the more lenient of the subscription path and the PAYG path
//! (PAYG participates only for non-subscribers, when fallback is enabled,
//! when the subscription is suspended/cancelled, or when the cycle is used
//! up). A missing API key yields an `unknown` QuotaData without an error.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, QuotaLimitType,
    WARNING_THRESHOLD_RATIO, WINDOW_CREDITS, WINDOW_CYCLE, WINDOW_PAY_AS_YOU_GO,
};
use crate::storage::Channel;

pub struct ApertisChecker;

const APERTIS_DEFAULT_BASE_URL: &str = "https://api.apertis.ai";
const APERTIS_AVAILABILITY_GROUP: &str = "apertis_capacity";

#[derive(Default, Debug, Clone)]
struct ApertisPayg {
    account_credits: f64,
    token_used: f64,
    token_total: Value,
    token_remaining: Value,
    token_is_unlimited: bool,
    token_monthly_limit_usd: Option<f64>,
    token_monthly_used_usd: Option<f64>,
    monthly_reset_day: Option<i64>,
}

#[derive(Default, Debug, Clone)]
struct ApertisSubscription {
    plan_type: String,
    status: String,
    cycle_quota_limit: i64,
    cycle_quota_used: i64,
    cycle_quota_remaining: i64,
    cycle_start: String,
    cycle_end: String,
    payg_fallback_enabled: bool,
}

#[derive(Default, Debug, Clone)]
struct ApertisResponse {
    is_subscriber: bool,
    payg: Option<ApertisPayg>,
    subscription: Option<ApertisSubscription>,
}

/// Go `toFloat64`: numbers, numeric strings, "unlimited" -> None.
fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => {
            if s == "unlimited" {
                None
            } else {
                s.parse::<f64>().ok()
            }
        }
        _ => None,
    }
}

fn field_f64(map: &Map<String, Value>, key: &str) -> f64 {
    map.get(key).and_then(to_f64).unwrap_or(0.0)
}

fn field_i64(map: &Map<String, Value>, key: &str) -> i64 {
    map.get(key).and_then(|v| v.as_i64()).unwrap_or(0)
}

fn field_string(map: &Map<String, Value>, key: &str) -> String {
    map.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn field_bool(map: &Map<String, Value>, key: &str) -> bool {
    map.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

fn parse_apertis_response(v: &Value) -> ApertisResponse {
    let root = match v.as_object() {
        Some(m) => m,
        None => return ApertisResponse::default(),
    };
    let payg = root
        .get("payg")
        .and_then(|p| p.as_object())
        .map(|p| ApertisPayg {
            account_credits: field_f64(p, "account_credits"),
            token_used: field_f64(p, "token_used"),
            token_total: p.get("token_total").cloned().unwrap_or(Value::Null),
            token_remaining: p.get("token_remaining").cloned().unwrap_or(Value::Null),
            token_is_unlimited: field_bool(p, "token_is_unlimited"),
            token_monthly_limit_usd: p.get("token_monthly_limit_usd").and_then(to_f64),
            token_monthly_used_usd: p.get("token_monthly_used_usd").and_then(to_f64),
            monthly_reset_day: p.get("monthly_reset_day").and_then(|x| x.as_i64()),
        });
    let subscription = root
        .get("subscription")
        .and_then(|s| s.as_object())
        .map(|s| ApertisSubscription {
            plan_type: field_string(s, "plan_type"),
            status: field_string(s, "status"),
            cycle_quota_limit: field_i64(s, "cycle_quota_limit"),
            cycle_quota_used: field_i64(s, "cycle_quota_used"),
            cycle_quota_remaining: field_i64(s, "cycle_quota_remaining"),
            cycle_start: field_string(s, "cycle_start"),
            cycle_end: field_string(s, "cycle_end"),
            payg_fallback_enabled: field_bool(s, "payg_fallback_enabled"),
        });
    ApertisResponse {
        is_subscriber: field_bool(root, "is_subscriber"),
        payg,
        subscription,
    }
}

/// Rank for the lenient-merge: available > warning > exhausted > unknown.
fn better_status_rank(status: &str) -> i64 {
    match status {
        "available" => 3,
        "warning" => 2,
        "exhausted" => 1,
        _ => 0,
    }
}

fn better_status(a: &str, b: &str) -> String {
    if better_status_rank(b) > better_status_rank(a) {
        b.to_string()
    } else {
        a.to_string()
    }
}

fn is_suspended_or_cancelled(status: &str) -> bool {
    let s = status.to_ascii_lowercase();
    s == "suspended" || s == "cancelled"
}

fn determine_subscription_status(sub: &ApertisSubscription) -> String {
    if is_suspended_or_cancelled(&sub.status) {
        return "exhausted".into();
    }
    if sub.cycle_quota_limit <= 0 {
        return "unknown".into();
    }
    let usage_ratio = sub.cycle_quota_used as f64 / sub.cycle_quota_limit as f64;
    if sub.cycle_quota_remaining <= 0 {
        return "exhausted".into();
    }
    if usage_ratio >= WARNING_THRESHOLD_RATIO {
        return "warning".into();
    }
    "available".into()
}

fn determine_payg_status(payg: &ApertisPayg) -> String {
    // Unlimited token is always available regardless of account_credits.
    if payg.token_is_unlimited {
        return "available".into();
    }

    if payg.account_credits > 0.0 {
        // A positive account balance keeps PAYG available even when its token
        // limit is exhausted. A non-exhausted token limit can still warn.
        if let Some(total) = to_f64(&payg.token_total) {
            if total > 0.0 {
                let usage_ratio = payg.token_used / total;
                if usage_ratio >= 1.0 {
                    return "available".into();
                }
                if usage_ratio >= WARNING_THRESHOLD_RATIO {
                    return "warning".into();
                }
            }
        }
        return "available".into();
    }

    match to_f64(&payg.token_total) {
        Some(total) if total > 0.0 => "exhausted".into(),
        _ => "unknown".into(),
    }
}

fn determine_apertis_status(resp: &ApertisResponse) -> String {
    let mut best = "unknown".to_string();

    // --- Subscription path ---
    if let Some(sub) = &resp.subscription {
        best = better_status(&best, &determine_subscription_status(sub));
    }

    // --- PAYG path: fallback whenever subscription quota is unavailable. ---
    if let Some(payg) = &resp.payg {
        let mut should_check_payg = !resp.is_subscriber;
        if let Some(sub) = &resp.subscription {
            if sub.payg_fallback_enabled {
                should_check_payg = true;
            }
            if is_suspended_or_cancelled(&sub.status) {
                should_check_payg = true;
            }
            if sub.cycle_quota_remaining <= 0 {
                should_check_payg = true;
            }
        }
        if should_check_payg {
            best = better_status(&best, &determine_payg_status(payg));
        }
    }

    best
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn build_apertis_limits(
    resp: &ApertisResponse,
    next_reset_at: Option<DateTime<Utc>>,
) -> Vec<QuotaLimitStatus> {
    let mut limits = Vec::new();

    if let Some(payg) = &resp.payg {
        // Skip PAYG only when an active subscription has remaining quota and
        // no fallback is needed — same conditions as determine_apertis_status.
        let should_skip_payg = resp.is_subscriber
            && matches!(&resp.subscription, Some(sub)
                if !sub.payg_fallback_enabled
                    && sub.status == "active"
                    && sub.cycle_quota_remaining > 0);
        if !should_skip_payg {
            let token_status;
            let usage_ratio;
            if payg.token_is_unlimited {
                token_status = "available".to_string();
                usage_ratio = 0.0;
            } else {
                match to_f64(&payg.token_total) {
                    Some(total) if total > 0.0 => {
                        usage_ratio = payg.token_used / total;
                        token_status = if payg.token_used >= total {
                            "exhausted".to_string()
                        } else if usage_ratio >= WARNING_THRESHOLD_RATIO {
                            "warning".to_string()
                        } else {
                            "available".to_string()
                        };
                    }
                    _ => {
                        // Can't determine usage ratio
                        token_status = "unknown".to_string();
                        usage_ratio = 0.0;
                    }
                }
            }
            let mut limit = QuotaLimitStatus::new(
                QuotaLimitType::Token,
                &token_status,
                usage_ratio,
                next_reset_at,
            );
            limit.ready = is_ready_status(&token_status);
            limit.availability_group = APERTIS_AVAILABILITY_GROUP.into();
            limit.window = WINDOW_PAY_AS_YOU_GO.into();
            limits.push(limit);

            if payg.account_credits > 0.0 {
                let mut limit =
                    QuotaLimitStatus::new(QuotaLimitType::Token, "available", 0.0, None);
                limit.ready = true;
                limit.availability_group = APERTIS_AVAILABILITY_GROUP.into();
                limit.window = WINDOW_CREDITS.into();
                limits.push(limit);
            }
        }
    }

    if resp.is_subscriber {
        if let Some(sub) = &resp.subscription {
            let sub_status;
            let mut usage_ratio = 0.0;
            if is_suspended_or_cancelled(&sub.status) {
                sub_status = "exhausted".to_string();
            } else if sub.cycle_quota_limit > 0 {
                usage_ratio = sub.cycle_quota_used as f64 / sub.cycle_quota_limit as f64;
                sub_status = if sub.cycle_quota_remaining <= 0 {
                    "exhausted".to_string()
                } else if usage_ratio >= WARNING_THRESHOLD_RATIO {
                    "warning".to_string()
                } else {
                    "available".to_string()
                };
            } else {
                sub_status = "unknown".to_string();
            }

            let mut limit = QuotaLimitStatus::new(
                QuotaLimitType::SubscriptionCycle,
                &sub_status,
                usage_ratio,
                next_reset_at,
            );
            limit.ready = is_ready_status(&sub_status);
            limit.availability_group = APERTIS_AVAILABILITY_GROUP.into();
            limit.window = WINDOW_CYCLE.into();
            // Apertis reports the cycle boundaries outright.
            limit.period_start = parse_rfc3339(&sub.cycle_start);
            limits.push(limit);
        }
    }

    limits
}

fn convert_apertis_response_to_map(resp: &ApertisResponse) -> Map<String, Value> {
    let mut raw = Map::new();
    raw.insert("is_subscriber".into(), Value::Bool(resp.is_subscriber));

    if let Some(payg) = &resp.payg {
        let mut m = Map::new();
        m.insert("account_credits".into(), json!(payg.account_credits));
        m.insert("token_used".into(), json!(payg.token_used));
        m.insert("token_total".into(), payg.token_total.clone());
        m.insert("token_remaining".into(), payg.token_remaining.clone());
        m.insert(
            "token_is_unlimited".into(),
            Value::Bool(payg.token_is_unlimited),
        );
        if let Some(v) = payg.token_monthly_limit_usd {
            m.insert("token_monthly_limit_usd".into(), json!(v));
        }
        if let Some(v) = payg.token_monthly_used_usd {
            m.insert("token_monthly_used_usd".into(), json!(v));
        }
        if let Some(v) = payg.monthly_reset_day {
            m.insert("monthly_reset_day".into(), json!(v));
        }
        raw.insert("payg".into(), Value::Object(m));
    }

    if let Some(sub) = &resp.subscription {
        let mut m = Map::new();
        m.insert("plan_type".into(), json!(sub.plan_type));
        m.insert("status".into(), json!(sub.status));
        m.insert("cycle_quota_limit".into(), json!(sub.cycle_quota_limit));
        m.insert("cycle_quota_used".into(), json!(sub.cycle_quota_used));
        m.insert(
            "cycle_quota_remaining".into(),
            json!(sub.cycle_quota_remaining),
        );
        m.insert("cycle_start".into(), json!(sub.cycle_start));
        m.insert("cycle_end".into(), json!(sub.cycle_end));
        m.insert(
            "payg_fallback_enabled".into(),
            Value::Bool(sub.payg_fallback_enabled),
        );
        raw.insert("subscription".into(), Value::Object(m));
    }

    raw
}

/// Builds `{scheme}://{host}/v1/dashboard/billing/credits` from a base URL.
fn build_apertis_quota_url(base_url: &str) -> String {
    let scheme_host = base_url.trim();
    let scheme_host = if scheme_host.is_empty() {
        APERTIS_DEFAULT_BASE_URL
    } else {
        scheme_host
    };

    let (scheme, rest) = match scheme_host.split_once("://") {
        Some((s, r)) => (s, r),
        None => ("https", scheme_host),
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return format!("{APERTIS_DEFAULT_BASE_URL}/v1/dashboard/billing/credits");
    }
    format!("{scheme}://{host}/v1/dashboard/billing/credits")
}

fn parse_response(body: &str) -> Result<QuotaData, QuotaError> {
    let value: Value = serde_json::from_str(body).map_err(|e| QuotaError::Parse(e.to_string()))?;
    let resp = parse_apertis_response(&value);

    let status = determine_apertis_status(&resp);
    let mut quota = QuotaData::new("apertis", &status);
    quota.ready = is_ready_status(&status);
    quota.raw_data = convert_apertis_response_to_map(&resp);

    let mut next_reset_at = None;
    if resp.is_subscriber {
        if let Some(sub) = &resp.subscription {
            if !sub.cycle_end.is_empty() {
                if let Some(t) = parse_rfc3339(&sub.cycle_end) {
                    next_reset_at = Some(t);
                }
            }
        }
    }
    quota.next_reset_at = next_reset_at;
    quota.limits = build_apertis_limits(&resp, next_reset_at);
    Ok(quota)
}

#[async_trait]
impl QuotaChecker for ApertisChecker {
    fn provider_type(&self) -> &'static str {
        "apertis"
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
            .or_else(|| creds.api_keys.first().cloned());

        let api_key = match api_key {
            Some(k) => k,
            None => {
                // Unique Apertis behavior: missing key -> unknown, no error.
                let mut q = QuotaData::new("apertis", "unknown");
                q.ready = false;
                q.raw_data.insert("error".into(), json!("missing API key"));
                return Ok(q);
            }
        };

        let quota_url = build_apertis_quota_url(&channel.base_url);

        let resp = http
            .get(&quota_url)
            .bearer_auth(&api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("request failed: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(QuotaError::InvalidCredentials(format!(
                "apertis API returned status {}",
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

    fn limit<'a>(q: &'a QuotaData, window: &str) -> &'a QuotaLimitStatus {
        q.limits.iter().find(|l| l.window == window).unwrap()
    }

    #[test]
    fn payg_only_happy_path() {
        let q = parse_response(
            r#"{
                "object": "billing_credits",
                "is_subscriber": false,
                "payg": {
                    "account_credits": 5.0,
                    "token_used": 2.0,
                    "token_total": 7.0,
                    "token_remaining": 5.0,
                    "token_is_unlimited": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert!(q.ready);
        assert_eq!(q.limits.len(), 2);
        assert_eq!(limit(&q, WINDOW_PAY_AS_YOU_GO).status, "available");
        assert_eq!(limit(&q, WINDOW_CREDITS).status, "available");
    }

    #[test]
    fn payg_warning_state() {
        let q = parse_response(
            r#"{
                "is_subscriber": false,
                "payg": {"account_credits": 50.0, "token_used": 9.0, "token_total": 10.0, "token_remaining": 1.0}
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "warning");
        assert!((q.limits[0].usage_ratio - 0.9).abs() < 1e-9);
    }

    #[test]
    fn payg_exhausted_state() {
        let q = parse_response(
            r#"{
                "is_subscriber": false,
                "payg": {"account_credits": 0, "token_used": 10.0, "token_total": 10.0, "token_remaining": 0}
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
        assert!(!q.ready);
    }

    #[test]
    fn empty_sources_unknown() {
        let q = parse_response(r#"{"is_subscriber": false}"#).unwrap();
        assert_eq!(q.status, "unknown");
        assert!(q.limits.is_empty());
    }

    #[test]
    fn empty_source_objects_unknown() {
        let q = parse_response(
            r#"{"is_subscriber": true, "payg": {}, "subscription": {"status": "active"}}"#,
        )
        .unwrap();
        assert_eq!(q.status, "unknown");
        assert!(!q.ready);
    }

    #[test]
    fn payg_credits_keep_available_when_token_limit_exhausted() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 3.0, "token_used": 10.0, "token_total": 10.0, "token_remaining": 0.0},
                "subscription": {
                    "status": "active", "cycle_quota_limit": 5000, "cycle_quota_used": 5000,
                    "cycle_quota_remaining": 0, "payg_fallback_enabled": true
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(limit(&q, WINDOW_PAY_AS_YOU_GO).status, "exhausted");
        assert_eq!(limit(&q, WINDOW_CREDITS).status, "available");
        assert_eq!(
            limit(&q, WINDOW_PAY_AS_YOU_GO).availability_group,
            "apertis_capacity"
        );
    }

    #[test]
    fn subscription_active_skips_payg_limit() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 9.98, "token_used": 0.05, "token_total": 1.0, "token_remaining": 0.95},
                "subscription": {
                    "plan_type": "lite", "status": "active", "cycle_quota_limit": 600,
                    "cycle_quota_used": 10, "cycle_quota_remaining": 590,
                    "cycle_start": "2099-03-16T10:02:35Z", "cycle_end": "2099-04-16T10:02:35Z",
                    "payg_fallback_enabled": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits.len(), 1);
        assert_eq!(q.limits[0].kind, QuotaLimitType::SubscriptionCycle);
        assert_eq!(
            q.next_reset_at.unwrap().to_rfc3339(),
            "2099-04-16T10:02:35+00:00"
        );
    }

    #[test]
    fn subscription_warning_state() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 100.0, "token_used": 0.5, "token_total": 3.5, "token_remaining": 3.0},
                "subscription": {
                    "status": "active", "cycle_quota_limit": 1000, "cycle_quota_used": 850,
                    "cycle_quota_remaining": 150, "payg_fallback_enabled": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "warning");
    }

    #[test]
    fn subscription_suspended_with_payg_credits() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 100.0, "token_used": 0.5, "token_total": 3.5, "token_remaining": 3.0},
                "subscription": {
                    "status": "suspended", "cycle_quota_limit": 1000, "cycle_quota_used": 500,
                    "cycle_quota_remaining": 500, "payg_fallback_enabled": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits.len(), 3);
        assert_eq!(limit(&q, WINDOW_CYCLE).status, "exhausted");
        assert_eq!(limit(&q, WINDOW_PAY_AS_YOU_GO).status, "available");
        assert_eq!(limit(&q, WINDOW_CREDITS).status, "available");
    }

    #[test]
    fn subscription_suspended_no_payg_credits() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 0, "token_used": 10.0, "token_total": 10.0, "token_remaining": 0},
                "subscription": {
                    "status": "suspended", "cycle_quota_limit": 1000, "cycle_quota_used": 500,
                    "cycle_quota_remaining": 500, "payg_fallback_enabled": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
        assert!(!q.ready);
        // credits limit suppressed (account_credits == 0)
        assert_eq!(q.limits.len(), 2);
        assert_eq!(limit(&q, WINDOW_CYCLE).status, "exhausted");
    }

    #[test]
    fn cycle_exhausted_with_payg_fallback() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 3.0, "token_used": 0.5, "token_total": 3.5, "token_remaining": 3.0},
                "subscription": {
                    "status": "active", "cycle_quota_limit": 5000, "cycle_quota_used": 5000,
                    "cycle_quota_remaining": 0, "cycle_start": "2026-03-01T00:00:00Z",
                    "cycle_end": "2026-04-01T00:00:00Z", "payg_fallback_enabled": true
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits.len(), 3);
        assert_eq!(limit(&q, WINDOW_CYCLE).status, "exhausted");
        assert_eq!(
            limit(&q, WINDOW_CYCLE).period_start.unwrap().to_rfc3339(),
            "2026-03-01T00:00:00+00:00"
        );
    }

    #[test]
    fn cycle_exhausted_no_payg_fallback() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 0, "token_used": 10.0, "token_total": 10.0, "token_remaining": 0},
                "subscription": {
                    "status": "active", "cycle_quota_limit": 600, "cycle_quota_used": 600,
                    "cycle_quota_remaining": 0, "payg_fallback_enabled": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "exhausted");
        assert!(!q.ready);
    }

    #[test]
    fn unlimited_payg_token() {
        let q = parse_response(
            r#"{
                "is_subscriber": false,
                "payg": {
                    "account_credits": 500.0, "token_used": 87.61,
                    "token_total": "unlimited", "token_remaining": "unlimited",
                    "token_is_unlimited": true
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits[0].status, "available");
        assert_eq!(q.limits[0].usage_ratio, 0.0);
    }

    #[test]
    fn subscriber_with_unlimited_payg() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {"account_credits": 0, "token_used": 0, "token_total": "unlimited", "token_is_unlimited": true},
                "subscription": {
                    "status": "active", "cycle_quota_limit": 600, "cycle_quota_used": 183,
                    "cycle_quota_remaining": 417, "cycle_start": "2099-05-20T23:28:04Z",
                    "cycle_end": "2099-06-20T23:28:04Z", "payg_fallback_enabled": false
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.status, "available");
        assert_eq!(q.limits.len(), 1);
        assert!((q.limits[0].usage_ratio - 0.305).abs() < 0.001);
        assert!(q.next_reset_at.is_some());
        assert!(q.raw_data.contains_key("payg"));
    }

    #[test]
    fn malformed_json_is_parse_error() {
        assert!(parse_response("{invalid json").is_err());
    }

    #[test]
    fn raw_data_contains_all_fields() {
        let q = parse_response(
            r#"{
                "is_subscriber": true,
                "payg": {
                    "account_credits": 9.98, "token_used": 0.05, "token_total": 1.0,
                    "token_remaining": 0.95, "token_is_unlimited": false,
                    "token_monthly_limit_usd": 100.0, "token_monthly_used_usd": 12.5,
                    "monthly_reset_day": 1
                },
                "subscription": {
                    "plan_type": "max", "status": "active", "cycle_quota_limit": 5000,
                    "cycle_quota_used": 5000, "cycle_quota_remaining": 0,
                    "cycle_start": "2026-03-01T00:00:00Z", "cycle_end": "2026-04-01T00:00:00Z",
                    "payg_fallback_enabled": true, "payg_spent_usd": 2.5, "payg_limit_usd": 10.0
                }
            }"#,
        )
        .unwrap();
        assert_eq!(q.raw_data["is_subscriber"], json!(true));
        assert_eq!(q.raw_data["payg"]["account_credits"], json!(9.98));
        assert_eq!(q.raw_data["payg"]["monthly_reset_day"], json!(1));
        assert_eq!(q.raw_data["payg"]["token_total"], json!(1.0));
        assert_eq!(q.raw_data["subscription"]["status"], json!("active"));
        assert_eq!(q.raw_data["subscription"]["plan_type"], json!("max"));
        assert_eq!(
            q.raw_data["subscription"]["cycle_start"],
            json!("2026-03-01T00:00:00Z")
        );
    }

    #[test]
    fn url_building() {
        assert_eq!(
            build_apertis_quota_url(""),
            "https://api.apertis.ai/v1/dashboard/billing/credits"
        );
        assert_eq!(
            build_apertis_quota_url("https://custom.apertis.ai"),
            "https://custom.apertis.ai/v1/dashboard/billing/credits"
        );
        assert_eq!(
            build_apertis_quota_url("https://custom.apertis.ai/v1/chat"),
            "https://custom.apertis.ai/v1/dashboard/billing/credits"
        );
    }

    #[test]
    fn better_status_prefers_lenient() {
        assert_eq!(better_status("exhausted", "available"), "available");
        assert_eq!(better_status("warning", "exhausted"), "warning");
        assert_eq!(better_status("unknown", "exhausted"), "exhausted");
        assert_eq!(better_status("available", "warning"), "available");
    }
}
