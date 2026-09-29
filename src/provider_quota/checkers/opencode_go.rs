//! OpenCode Go plan quota checker — port of axonhub's
//! `opencode_go_checker.go`.
//!
//! GET https://opencode.ai/zen/go/v1/usage with `Authorization: Bearer <key>`.
//! The Go checker does not inspect the HTTP status; the body is parsed
//! directly (JSON parse failure is the only transport-level error).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, period_start_from_monthly_reset, period_start_from_reset, status_rank,
    QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, WARNING_THRESHOLD_RATIO,
    WINDOW_5H, WINDOW_MONTHLY, WINDOW_WEEKLY,
};
use crate::storage::Channel;

const OPENCODE_GO_USAGE_URL: &str = "https://opencode.ai/zen/go/v1/usage";

pub struct OpencodeGoChecker;

#[async_trait]
impl QuotaChecker for OpencodeGoChecker {
    fn provider_type(&self) -> &'static str {
        "opencode_go"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        _channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = creds
            .all_api_keys()
            .into_iter()
            .map(str::trim)
            .find(|k| !k.is_empty())
            .ok_or_else(|| QuotaError::InvalidCredentials("channel has no API key".into()))?;

        let resp = http
            .get(OPENCODE_GO_USAGE_URL)
            .bearer_auth(api_key)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("opencode go usage request failed: {e}")))?;

        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("opencode go usage read failed: {e}")))?;

        parse_response(&body, Utc::now())
    }
}

struct ParsedWindow {
    usage_percent: f64,
    reset_in_sec: f64,
    reset_at: DateTime<Utc>,
    api_sub_status: String,
}

/// Parses the usage payload into QuotaData. Public-ish for unit tests.
pub fn parse_response(body: &str, now: DateTime<Utc>) -> Result<QuotaData, QuotaError> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|e| QuotaError::Parse(format!("parse OpenCode Go usage response: {e}")))?;

    let usage = parsed.get("usage").cloned().unwrap_or(Value::Null);

    let mut windows: Vec<(&str, ParsedWindow)> = Vec::with_capacity(3);
    for key in ["rolling", "weekly", "monthly"] {
        let Some(window) = usage.get(key).filter(|w| !w.is_null()) else {
            continue;
        };
        let Some(reset_at) = parse_resets_at(window.get("resetsAt")) else {
            // Window with an unparseable resetsAt is dropped entirely.
            continue;
        };
        let mut reset_in_sec = (reset_at - now).num_milliseconds() as f64 / 1000.0;
        if reset_in_sec < 0.0 {
            reset_in_sec = 0.0;
        }
        windows.push((
            key,
            ParsedWindow {
                usage_percent: window.get("percent").and_then(Value::as_f64).unwrap_or(0.0),
                reset_in_sec,
                reset_at,
                api_sub_status: window
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
        ));
    }

    if windows.is_empty() {
        return Err(QuotaError::Parse("could not parse OpenCode Go usage windows".into()));
    }

    let mut raw_windows = Map::new();
    let mut limits = Vec::with_capacity(windows.len());
    let mut normalized_status = "available";
    let mut next_reset_at: Option<DateTime<Utc>> = None;

    for (key, window) in windows {
        let usage_ratio = window.usage_percent / 100.0;
        let status = normalize_window_status(usage_ratio);
        if status_rank(status) > status_rank(normalized_status) {
            normalized_status = status;
        }

        let reset_at = window.reset_at;
        if next_reset_at.is_none_or(|t| reset_at < t) {
            next_reset_at = Some(reset_at);
        }

        let mut raw = json!({
            "usage_percent": window.usage_percent,
            "reset_in_seconds": window.reset_in_sec,
            "reset_time": reset_at.to_rfc3339(),
            "status": status,
            "percent_remaining": 100.0 - window.usage_percent,
        });
        if !window.api_sub_status.is_empty() {
            raw["api_status"] = Value::String(window.api_sub_status.clone());
        }
        raw_windows.insert(key.to_string(), raw);

        let mut limit = QuotaLimitStatus::token(status, usage_ratio, Some(reset_at));
        limit.period_start = period_start(key, reset_at);
        limit.window = window_label(key).to_string();
        limits.push(limit);
    }

    let mut data = QuotaData::new("opencode_go", normalized_status);
    data.raw_data
        .insert("plan_type".into(), Value::String("go".into()));
    data.raw_data.insert("windows".into(), Value::Object(raw_windows));
    data.next_reset_at = next_reset_at;
    data.ready = is_ready_status(normalized_status);
    data.limits = limits;
    Ok(data)
}

/// Parses the resetsAt value: RFC3339 (Nano) string, or unix seconds /
/// milliseconds as a number. Bounds: rejects negative or > 1e15 values;
/// >= 1e12 is treated as epoch milliseconds.
pub fn parse_resets_at(raw: Option<&Value>) -> Option<DateTime<Utc>> {
    let raw = raw?;
    match raw {
        Value::String(s) => DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.with_timezone(&Utc)),
        Value::Number(n) => {
            let as_seconds = n.as_f64()?;
            if !(0.0..=1e15).contains(&as_seconds) {
                return None;
            }
            if as_seconds >= 1e12 {
                DateTime::from_timestamp_millis(as_seconds as i64).map(|t| t.with_timezone(&Utc))
            } else {
                DateTime::from_timestamp(as_seconds as i64, 0).map(|t| t.with_timezone(&Utc))
            }
        }
        _ => None,
    }
}

fn window_label<'a>(key: &'a str) -> &'a str {
    match key {
        "rolling" => WINDOW_5H,
        "weekly" => WINDOW_WEEKLY,
        "monthly" => WINDOW_MONTHLY,
        other => other,
    }
}

fn period_start(key: &str, reset_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match key {
        "rolling" => period_start_from_reset(Some(&reset_at), chrono::Duration::hours(5)),
        "weekly" => period_start_from_reset(Some(&reset_at), chrono::Duration::hours(7 * 24)),
        "monthly" => period_start_from_monthly_reset(Some(&reset_at)),
        _ => None,
    }
}

fn normalize_window_status(usage_ratio: f64) -> &'static str {
    if usage_ratio >= 1.0 {
        "exhausted"
    } else if usage_ratio >= WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-12T10:00:00Z").unwrap().with_timezone(&Utc)
    }

    #[test]
    fn parses_all_windows_and_derives_status() {
        let body = r#"{
            "usage": {
                "rolling": {"percent": 85.0, "resetsAt": "2026-08-12T11:24:29.905Z", "status": "ok"},
                "weekly":  {"percent": 40.0, "resetsAt": "2026-08-15T11:24:29.905Z", "status": "ok"},
                "monthly": {"percent": 10.0, "resetsAt": "2026-08-31T00:00:00Z", "status": "ok"}
            }
        }"#;
        let data = parse_response(body, now()).unwrap();
        assert_eq!(data.status, "warning");
        assert_eq!(data.limits.len(), 3);
        assert_eq!(data.limits[0].window, "5h");
        assert_eq!(data.limits[0].status, "warning");
        assert!((data.limits[0].usage_ratio - 0.85).abs() < 1e-9);
        // rolling period start = reset - 5h
        let rolling_reset = DateTime::parse_from_rfc3339("2026-08-12T11:24:29.905Z").unwrap().with_timezone(&Utc);
        assert_eq!(data.limits[0].period_start, Some(rolling_reset - Duration::hours(5)));
        assert_eq!(data.limits[1].window, "weekly");
        assert_eq!(data.limits[2].window, "monthly");
        assert_eq!(data.limits[2].period_start, Some(DateTime::parse_from_rfc3339("2026-07-31T00:00:00Z").unwrap().with_timezone(&Utc)));
        assert_eq!(data.next_reset_at, Some(rolling_reset));
        let windows = data.raw_data.get("windows").unwrap();
        assert_eq!(windows["rolling"]["api_status"], json!("ok"));
        assert!((windows["rolling"]["reset_in_seconds"].as_f64().unwrap() - 5069.905).abs() < 0.01);
        assert!((windows["rolling"]["percent_remaining"].as_f64().unwrap() - 15.0).abs() < 1e-9);
    }

    #[test]
    fn resets_at_unix_seconds_and_millis_fallback() {
        let body = r#"{
            "usage": {
                "rolling": {"percent": 10, "resetsAt": 1786531600, "status": "ok"},
                "weekly":  {"percent": 20, "resetsAt": 1786531600123, "status": "ok"}
            }
        }"#;
        let data = parse_response(body, now()).unwrap();
        assert_eq!(
            data.limits[0].next_reset_at,
            DateTime::from_timestamp(1786531600, 0)
        );
        assert_eq!(
            data.limits[1].next_reset_at,
            DateTime::from_timestamp_millis(1786531600123)
        );
    }

    #[test]
    fn drops_window_with_unparseable_resets_at() {
        let body = r#"{
            "usage": {
                "rolling": {"percent": 10, "resetsAt": "not-a-timestamp", "status": "ok"},
                "weekly":  {"percent": 20, "resetsAt": "2026-08-15T11:24:29.905Z", "status": "ok"}
            }
        }"#;
        let data = parse_response(body, now()).unwrap();
        assert_eq!(data.limits.len(), 1);
        assert_eq!(data.limits[0].window, "weekly");
    }

    #[test]
    fn all_windows_dropped_is_parse_error() {
        let body = r#"{"usage": {"rolling": {"percent": 10, "resetsAt": null}}}"#;
        let err = parse_response(body, now()).unwrap_err();
        assert!(matches!(err, QuotaError::Parse(_)));
    }

    #[test]
    fn exhausted_window_drives_channel_status() {
        let body = r#"{
            "usage": {
                "rolling": {"percent": 100, "resetsAt": "2026-08-12T11:24:29Z", "status": "ok"},
                "weekly":  {"percent": 5, "resetsAt": "2026-08-15T11:24:29Z", "status": "ok"}
            }
        }"#;
        let data = parse_response(body, now()).unwrap();
        assert_eq!(data.status, "exhausted");
        assert!(!data.ready);
    }

    #[test]
    fn invalid_json_is_parse_error() {
        assert!(matches!(parse_response("nope{", now()), Err(QuotaError::Parse(_))));
    }
}
