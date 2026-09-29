//! Synthetic (synthetic.new) quota checker — port of axonhub's
//! `synthetic_checker.go`.
//!
//! GET {scheme}://{host}/v2/quotas with a Bearer token. The rolling 5h
//! `tickPercent` is already a 0-1 usage ratio and is used as-is; the weekly
//! `percentRemaining` is 0-100 and converted.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, WARNING_THRESHOLD_RATIO,
    WINDOW_5H, WINDOW_WEEKLY,
};
use crate::storage::Channel;

const SYNTHETIC_DEFAULT_QUOTA_URL: &str = "https://api.synthetic.new/v2/quotas";

pub struct SyntheticChecker;

#[async_trait]
impl QuotaChecker for SyntheticChecker {
    fn provider_type(&self) -> &'static str {
        "synthetic"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let mut api_key = creds.api_key.as_deref().unwrap_or("").trim().to_string();
        if api_key.is_empty() && !creds.api_keys.is_empty() {
            api_key = creds.api_keys[0].clone();
        }
        if api_key.is_empty() {
            return Err(QuotaError::InvalidCredentials("channel has no API key".into()));
        }

        let quota_url = build_quota_url(&channel.base_url);

        let resp = http
            .get(&quota_url)
            .bearer_auth(&api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("quota request failed: {e}")))?;

        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("quota response read failed: {e}")))?;

        if status != 200 {
            let detail: String = body.chars().take(200).collect();
            if status == 401 || status == 403 {
                return Err(QuotaError::InvalidCredentials(format!(
                    "HTTP {status}: {detail}"
                )));
            }
            return Err(QuotaError::Http(format!("HTTP {status}: {detail}")));
        }

        parse_response(&body)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyntheticUsageResponse {
    subscription: Option<SyntheticSubscription>,
    search: Option<SyntheticSearch>,
    weekly_token_limit: Option<SyntheticWeeklyTokenLimit>,
    rolling_five_hour_limit: Option<SyntheticRollingFiveHourLimit>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyntheticSubscription {
    limit: Option<i64>,
    requests: Option<i64>,
    renews_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyntheticSearch {
    hourly: Option<SyntheticSearchHourly>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyntheticSearchHourly {
    limit: Option<i64>,
    requests: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyntheticWeeklyTokenLimit {
    next_regen_at: Option<String>,
    percent_remaining: Option<f64>,
    max_credits: Option<String>,
    remaining_credits: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyntheticRollingFiveHourLimit {
    next_tick_at: Option<String>,
    tick_percent: Option<f64>,
    remaining: Option<f64>,
    max: Option<f64>,
    limited: Option<bool>,
}

pub fn parse_response(body: &str) -> Result<QuotaData, QuotaError> {
    let response: SyntheticUsageResponse = serde_json::from_str(body)
        .map_err(|e| QuotaError::Parse(format!("failed to parse synthetic usage response: {e}")))?;

    let mut normalized_status = "unknown";
    let limits = build_limit_statuses(response.weekly_token_limit.as_ref(), response.rolling_five_hour_limit.as_ref());

    if !limits.is_empty() {
        normalized_status = "available";
        for limit in &limits {
            if limit.status == "exhausted" {
                normalized_status = "exhausted";
                break;
            }
            if limit.status == "warning" {
                normalized_status = "warning";
            }
        }
    }

    let next_reset_at = find_earliest_reset_at(
        response.subscription.as_ref(),
        response.weekly_token_limit.as_ref(),
        response.rolling_five_hour_limit.as_ref(),
    );

    let mut raw_data = Map::new();
    if let Some(sub) = &response.subscription {
        raw_data.insert("subscription".into(), subscription_map(sub));
    }
    if let Some(search) = &response.search {
        let mut m = Map::new();
        if let Some(hourly) = &search.hourly {
            m.insert("hourly".into(), hourly_map(hourly));
        }
        raw_data.insert("search".into(), Value::Object(m));
    }
    if let Some(wtl) = &response.weekly_token_limit {
        raw_data.insert("weeklyTokenLimit".into(), weekly_map(wtl));
    }
    if let Some(rfhl) = &response.rolling_five_hour_limit {
        raw_data.insert("rollingFiveHourLimit".into(), rolling_map(rfhl));
    }

    let mut data = QuotaData::new("synthetic", normalized_status);
    data.raw_data = raw_data;
    data.next_reset_at = next_reset_at;
    data.ready = is_ready_status(normalized_status);
    data.limits = limits;
    Ok(data)
}

/// Maps a channel base URL onto the quota endpoint: scheme upgraded to https,
/// host preserved, path replaced. Empty/unparseable bases fall back to the
/// public API host.
pub fn build_quota_url(base_url: &str) -> String {
    let base_url = base_url.trim();
    if base_url.is_empty() {
        return SYNTHETIC_DEFAULT_QUOTA_URL.to_string();
    }

    // Mirror Go's url.Parse: without a scheme the input is a path, not a host.
    let Some((scheme_raw, rest)) = base_url.split_once("://") else {
        return SYNTHETIC_DEFAULT_QUOTA_URL.to_string();
    };
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return SYNTHETIC_DEFAULT_QUOTA_URL.to_string();
    }

    let scheme = if scheme_raw == "http" { "https" } else { scheme_raw };
    format!("{scheme}://{host}/v2/quotas")
}

fn build_limit_statuses(
    weekly: Option<&SyntheticWeeklyTokenLimit>,
    five_hour: Option<&SyntheticRollingFiveHourLimit>,
) -> Vec<QuotaLimitStatus> {
    let mut limits = Vec::new();

    if let Some(five_hour) = five_hour {
        let mut status = "available";
        let mut usage_ratio = 0.0;

        if five_hour.limited == Some(true) {
            status = "exhausted";
            usage_ratio = 1.0;
        } else if let Some(tick) = five_hour.tick_percent {
            // tickPercent is already a 0-1 ratio; used as-is.
            usage_ratio = tick;
            if usage_ratio >= WARNING_THRESHOLD_RATIO {
                status = "warning";
            }
        }

        // NextTickAt is a regeneration tick, not a fixed window boundary, so
        // no period start can be derived from it.
        let mut limit = QuotaLimitStatus::token(status, usage_ratio, parse_rfc3339(five_hour.next_tick_at.as_deref()));
        limit.window = WINDOW_5H.to_string();
        limits.push(limit);
    }

    if let Some(weekly) = weekly {
        let mut status = "available";
        let mut usage_ratio = 0.0;

        if let Some(pct) = weekly.percent_remaining {
            usage_ratio = 1.0 - (pct / 100.0);
            if usage_ratio >= 1.0 {
                status = "exhausted";
            } else if usage_ratio >= WARNING_THRESHOLD_RATIO {
                status = "warning";
            }
        }

        // NextRegenAt marks an incremental regeneration, not a window boundary.
        let mut limit = QuotaLimitStatus::token(status, usage_ratio, parse_rfc3339(weekly.next_regen_at.as_deref()));
        limit.window = WINDOW_WEEKLY.to_string();
        limits.push(limit);
    }

    limits
}

fn find_earliest_reset_at(
    subscription: Option<&SyntheticSubscription>,
    weekly: Option<&SyntheticWeeklyTokenLimit>,
    five_hour: Option<&SyntheticRollingFiveHourLimit>,
) -> Option<DateTime<Utc>> {
    let mut earliest: Option<DateTime<Utc>> = None;

    let mut consider = |t: Option<DateTime<Utc>>| {
        if let Some(t) = t {
            if earliest.is_none_or(|e| t < e) {
                earliest = Some(t);
            }
        }
    };

    consider(subscription.and_then(|s| parse_rfc3339(s.renews_at.as_deref())));
    consider(weekly.and_then(|w| parse_rfc3339(w.next_regen_at.as_deref())));
    consider(five_hour.and_then(|f| parse_rfc3339(f.next_tick_at.as_deref())));

    earliest
}

fn parse_rfc3339(s: Option<&str>) -> Option<DateTime<Utc>> {
    s.and_then(|s| DateTime::parse_from_rfc3339(s).ok().map(|t| t.with_timezone(&Utc)))
}

fn put_opt(m: &mut Map<String, Value>, key: &str, v: Option<impl Into<Value>>) {
    if let Some(v) = v {
        m.insert(key.into(), v.into());
    }
}

fn subscription_map(sub: &SyntheticSubscription) -> Value {
    let mut m = Map::new();
    put_opt(&mut m, "limit", sub.limit);
    put_opt(&mut m, "requests", sub.requests);
    put_opt(&mut m, "renewsAt", sub.renews_at.clone());
    Value::Object(m)
}

fn hourly_map(hourly: &SyntheticSearchHourly) -> Value {
    let mut m = Map::new();
    put_opt(&mut m, "limit", hourly.limit);
    put_opt(&mut m, "requests", hourly.requests);
    Value::Object(m)
}

fn weekly_map(wtl: &SyntheticWeeklyTokenLimit) -> Value {
    let mut m = Map::new();
    put_opt(&mut m, "nextRegenAt", wtl.next_regen_at.clone());
    put_opt(&mut m, "percentRemaining", wtl.percent_remaining);
    put_opt(&mut m, "maxCredits", wtl.max_credits.clone());
    put_opt(&mut m, "remainingCredits", wtl.remaining_credits.clone());
    Value::Object(m)
}

fn rolling_map(rfhl: &SyntheticRollingFiveHourLimit) -> Value {
    let mut m = Map::new();
    put_opt(&mut m, "nextTickAt", rfhl.next_tick_at.clone());
    put_opt(&mut m, "tickPercent", rfhl.tick_percent);
    put_opt(&mut m, "remaining", rfhl.remaining);
    put_opt(&mut m, "max", rfhl.max);
    put_opt(&mut m, "limited", rfhl.limited);
    Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_percent_passes_through_as_ratio() {
        let body = r#"{
            "subscription": {"limit": 500, "requests": 42, "renewsAt": "2026-09-30T00:00:00Z"},
            "search": {"hourly": {"limit": 100, "requests": 3}},
            "weeklyTokenLimit": {"nextRegenAt": "2026-09-29T12:00:00Z", "percentRemaining": 75.0,
                                  "maxCredits": "500", "remainingCredits": "375"},
            "rollingFiveHourLimit": {"nextTickAt": "2026-09-29T11:00:00Z", "tickPercent": 0.42,
                                      "remaining": 58.0, "max": 100.0, "limited": false}
        }"#;
        let data = parse_response(body).unwrap();
        assert_eq!(data.status, "available");
        assert_eq!(data.limits.len(), 2);
        let five_hour = &data.limits[0];
        assert_eq!(five_hour.window, "5h");
        // tickPercent used AS-IS as the ratio (0.42, not 0.0042 or 42).
        assert!((five_hour.usage_ratio - 0.42).abs() < 1e-9);
        assert_eq!(five_hour.status, "available");
        assert_eq!(five_hour.period_start, None);
        let weekly = &data.limits[1];
        assert_eq!(weekly.window, "weekly");
        assert!((weekly.usage_ratio - 0.25).abs() < 1e-9);
        // Earliest of renewsAt/nextRegenAt/nextTickAt.
        assert_eq!(data.next_reset_at, parse_rfc3339(Some("2026-09-29T11:00:00Z")));
        assert_eq!(data.raw_data["subscription"]["requests"], serde_json::json!(42));
        assert_eq!(data.raw_data["rollingFiveHourLimit"]["tickPercent"], serde_json::json!(0.42));
    }

    #[test]
    fn limited_rolling_window_is_exhausted() {
        let body = r#"{
            "rollingFiveHourLimit": {"limited": true},
            "weeklyTokenLimit": {"percentRemaining": 90.0}
        }"#;
        let data = parse_response(body).unwrap();
        assert_eq!(data.status, "exhausted");
        assert!(!data.ready);
        assert!((data.limits[0].usage_ratio - 1.0).abs() < 1e-9);
    }

    #[test]
    fn warning_weekly_beats_available() {
        let body = r#"{"weeklyTokenLimit": {"percentRemaining": 5.0}}"#;
        let data = parse_response(body).unwrap();
        assert_eq!(data.status, "warning");
        assert!((data.limits[0].usage_ratio - 0.95).abs() < 1e-9);
    }

    #[test]
    fn no_limits_leaves_status_unknown() {
        let body = r#"{"subscription": {"renewsAt": "2026-09-30T00:00:00Z"}}"#;
        let data = parse_response(body).unwrap();
        assert_eq!(data.status, "unknown");
        assert!(data.limits.is_empty());
        assert_eq!(data.next_reset_at, parse_rfc3339(Some("2026-09-30T00:00:00Z")));
    }

    #[test]
    fn quota_url_building() {
        assert_eq!(build_quota_url(""), SYNTHETIC_DEFAULT_QUOTA_URL);
        assert_eq!(build_quota_url("   "), SYNTHETIC_DEFAULT_QUOTA_URL);
        assert_eq!(build_quota_url("relative/path"), SYNTHETIC_DEFAULT_QUOTA_URL);
        assert_eq!(build_quota_url("api.synthetic.new"), SYNTHETIC_DEFAULT_QUOTA_URL);
        assert_eq!(build_quota_url("http://proxy.local:8080/x"), "https://proxy.local:8080/v2/quotas");
        assert_eq!(build_quota_url("https://api.synthetic.new/v1"), "https://api.synthetic.new/v2/quotas");
        assert_eq!(build_quota_url("https://custom.example.com"), "https://custom.example.com/v2/quotas");
    }

    #[test]
    fn invalid_json_is_parse_error() {
        assert!(matches!(parse_response("{{{"), Err(QuotaError::Parse(_))));
    }
}
