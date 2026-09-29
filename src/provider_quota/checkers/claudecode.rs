//! Port of axonhub `claudecode_checker.go`. The Anthropic rate-limit
//! information lives entirely in response headers of a minimal
//! /v1/messages probe request.
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use reqwest::header::HeaderMap;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus,
    WARNING_THRESHOLD_RATIO, WINDOW_5H, WINDOW_7D,
};
use crate::storage::Channel;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_BETA: &str = "claude-code-20250219,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,effort-2025-11-24";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const QUOTA_CHECK_MODEL: &str = "claude-haiku-4-5";

pub struct ClaudeCodeChecker;

fn header_get(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn parse_unix_timestamp(s: &str) -> i64 {
    s.trim().parse().unwrap_or(0)
}

fn parse_float(s: &str) -> f64 {
    s.trim().parse().unwrap_or(0.0)
}

fn unix_time(ts: i64) -> Option<DateTime<Utc>> {
    if ts > 0 {
        Utc.timestamp_opt(ts, 0).single()
    } else {
        None
    }
}

/// getEndpointURL: base URL reassembly with the /v1 suffix rule.
pub fn endpoint_url(base_url: &str) -> String {
    if base_url.is_empty() {
        return format!("{DEFAULT_BASE_URL}/v1/messages");
    }
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/v1") {
        format!("{base}/messages")
    } else {
        format!("{base}/v1/messages")
    }
}

fn window_headers(window_key: &str) -> (&'static str, &'static str, &'static str) {
    match window_key {
        WINDOW_7D => (
            "anthropic-ratelimit-unified-7d-status",
            "anthropic-ratelimit-unified-7d-reset",
            "anthropic-ratelimit-unified-7d-utilization",
        ),
        _ => (
            "anthropic-ratelimit-unified-5h-status",
            "anthropic-ratelimit-unified-5h-reset",
            "anthropic-ratelimit-unified-5h-utilization",
        ),
    }
}

fn build_token_limit(window_key: &str, headers: &HeaderMap) -> Option<QuotaLimitStatus> {
    let (status_key, reset_key, utilization_key) = window_headers(window_key);
    let window = if window_key == WINDOW_7D {
        Duration::hours(7 * 24)
    } else {
        Duration::hours(5)
    };

    let status_h = header_get(headers, status_key);
    let reset_h = header_get(headers, reset_key);
    let utilization_h = header_get(headers, utilization_key);
    if status_h.is_empty() && reset_h.is_empty() && utilization_h.is_empty() {
        return None;
    }

    let utilization = parse_float(&utilization_h);
    let reset_ts = parse_unix_timestamp(&reset_h);

    let status = if utilization >= 1.0 {
        "exhausted"
    } else if utilization >= WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    };

    let next_reset = unix_time(reset_ts);
    Some(
        QuotaLimitStatus::token(status, utilization, next_reset)
            .with_window(window_key, window),
    )
}

/// parseResponse: header map -> QuotaData (headers only, no body).
pub fn parse_response(headers: &HeaderMap) -> Result<QuotaData, QuotaError> {
    let unified_status = header_get(headers, "anthropic-ratelimit-unified-status");
    if unified_status.is_empty() {
        return Err(QuotaError::Parse("missing quota headers".into()));
    }
    let representative_claim = header_get(headers, "anthropic-ratelimit-unified-representative-claim");

    let mut windows = Map::new();
    for key in ["5h", "7d", "overage"] {
        let prefix = format!("anthropic-ratelimit-unified-{}-", key.to_uppercase());
        windows.insert(
            key.into(),
            json!({
                "status": header_get(headers, &format!("{prefix}status")),
                "reset": parse_unix_timestamp(&header_get(headers, &format!("{prefix}reset"))),
                "utilization": parse_float(&header_get(headers, &format!("{prefix}utilization"))),
            }),
        );
    }

    let raw_data = json!({
        "unified_status": unified_status,
        "windows": windows,
        "representative_claim": representative_claim,
        "fallback": header_get(headers, "anthropic-ratelimit-unified-fallback"),
        "fallback_percentage": parse_float(&header_get(headers, "anthropic-ratelimit-unified-fallback-percentage")),
        "reset": parse_unix_timestamp(&header_get(headers, "anthropic-ratelimit-unified-reset")),
    });

    let mut normalized_status = match unified_status.as_str() {
        "allowed" => "available",
        "throttled" | "rejected" => "exhausted",
        _ => "unknown",
    };

    if normalized_status == "available" {
        let five_hour_utilization = parse_float(&header_get(headers, "anthropic-ratelimit-unified-5h-utilization"));
        let seven_day_utilization = parse_float(&header_get(headers, "anthropic-ratelimit-unified-7d-utilization"));
        if five_hour_utilization >= WARNING_THRESHOLD_RATIO
            || seven_day_utilization >= WARNING_THRESHOLD_RATIO
        {
            normalized_status = "warning";
        }
    }

    let window_key = match representative_claim.as_str() {
        "five_hour" => WINDOW_5H,
        "seven_day" => WINDOW_7D,
        other => other,
    };
    let next_reset_at = windows
        .get(window_key)
        .and_then(|w| w.get("reset"))
        .and_then(Value::as_i64)
        .and_then(unix_time);

    let mut limits = Vec::with_capacity(2);
    for key in [WINDOW_5H, WINDOW_7D] {
        if let Some(limit) = build_token_limit(key, headers) {
            limits.push(limit);
        }
    }

    let mut data = QuotaData::new("claudecode", normalized_status);
    if let Value::Object(map) = raw_data {
        data.raw_data = map;
    }
    data.next_reset_at = next_reset_at;
    data.ready = is_ready_status(normalized_status);
    data.limits = limits;
    Ok(data)
}

#[async_trait]
impl QuotaChecker for ClaudeCodeChecker {
    fn provider_type(&self) -> &'static str {
        "claudecode"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let access_token = creds
            .oauth_access_token()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| QuotaError::InvalidCredentials("channel credentials missing access token".into()))?;

        let body = json!({
            "model": QUOTA_CHECK_MODEL,
            "messages": [{"role": "user", "content": "limit"}],
            "max_tokens": 1,
        });

        let response = http
            .post(endpoint_url(&channel.base_url))
            .bearer_auth(&access_token)
            .header("anthropic-beta", ANTHROPIC_BETA)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("anthropic-dangerous-direct-browser-access", "true")
            .header("x-app", "cli")
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap_or_default())
            .timeout(StdDuration::from_secs(30))
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("HTTP request failed: {e}")))?;

        let status = response.status().as_u16();
        if status == 401 || status == 403 {
            return Err(QuotaError::InvalidCredentials(format!("HTTP {status}")));
        }
        if status != 200 {
            let text = response.text().await.unwrap_or_default();
            let snippet: String = text.chars().take(200).collect();
            return Err(QuotaError::Http(format!("HTTP {status}: {snippet}")));
        }
        parse_response(response.headers())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderName;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                HeaderName::from_lowercase(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        map
    }

    #[test]
    fn endpoint_url_rules() {
        assert_eq!(endpoint_url(""), "https://api.anthropic.com/v1/messages");
        assert_eq!(endpoint_url("https://x.com/"), "https://x.com/v1/messages");
        assert_eq!(endpoint_url("https://x.com/v1"), "https://x.com/v1/messages");
        assert_eq!(endpoint_url("https://x.com/v1/"), "https://x.com/v1/messages");
    }

    #[test]
    fn parse_allowed() {
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "allowed"),
            ("anthropic-ratelimit-unified-representative-claim", "five_hour"),
            ("anthropic-ratelimit-unified-5h-status", "allowed"),
            ("anthropic-ratelimit-unified-5h-reset", "1800000000"),
            ("anthropic-ratelimit-unified-5h-utilization", "0.4"),
            ("anthropic-ratelimit-unified-7d-status", "allowed"),
            ("anthropic-ratelimit-unified-7d-reset", "1801000000"),
            ("anthropic-ratelimit-unified-7d-utilization", "0.1"),
        ]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.status, "available");
        assert_eq!(data.limits.len(), 2);
        assert_eq!(data.limits[0].window, "5h");
        assert_eq!(data.limits[0].status, "available");
        assert_eq!(data.limits[0].usage_ratio, 0.4);
        assert_eq!(data.next_reset_at, unix_time(1800000000));
        assert_eq!(data.raw_data["unified_status"], "allowed");
        assert_eq!(data.raw_data["representative_claim"], "five_hour");
    }

    #[test]
    fn parse_warning_from_utilization() {
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "allowed"),
            ("anthropic-ratelimit-unified-5h-utilization", "0.9"),
            ("anthropic-ratelimit-unified-5h-reset", "1800000000"),
            ("anthropic-ratelimit-unified-7d-utilization", "0.2"),
        ]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.status, "warning");
        assert_eq!(data.limits[0].status, "warning");
        assert_eq!(data.limits[0].usage_ratio, 0.9);
    }

    #[test]
    fn parse_exhausted_and_representative_claim_7d() {
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "throttled"),
            ("anthropic-ratelimit-unified-representative-claim", "seven_day"),
            ("anthropic-ratelimit-unified-7d-reset", "1802000000"),
            ("anthropic-ratelimit-unified-7d-utilization", "1"),
        ]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.status, "exhausted");
        assert_eq!(data.next_reset_at, unix_time(1802000000));
        assert_eq!(data.limits.len(), 1); // 5h window fully absent
        assert_eq!(data.limits[0].window, "7d");
        assert_eq!(data.limits[0].status, "exhausted");
    }

    #[test]
    fn parse_rejected_maps_to_exhausted() {
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "rejected"),
            ("anthropic-ratelimit-unified-5h-status", "blocked"),
        ]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.status, "exhausted");
        assert!(!data.ready);
    }

    #[test]
    fn parse_unknown_status_value() {
        let h = headers(&[("anthropic-ratelimit-unified-status", "something-else")]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.status, "unknown");
        assert!(!data.ready);
    }

    #[test]
    fn missing_unified_status_is_parse_error() {
        let h = headers(&[("anthropic-ratelimit-unified-5h-utilization", "0.5")]);
        let err = parse_response(&h).unwrap_err();
        assert!(matches!(err, QuotaError::Parse(_)));
    }

    #[test]
    fn unknown_representative_claim_uses_raw_key() {
        let h = headers(&[
            ("anthropic-ratelimit-unified-status", "allowed"),
            ("anthropic-ratelimit-unified-representative-claim", "overage"),
            ("anthropic-ratelimit-unified-overage-reset", "1800000000"),
            ("anthropic-ratelimit-unified-overage-utilization", "0.5"),
        ]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.next_reset_at, unix_time(1800000000));
    }

    #[test]
    fn parse_partial_headers_no_limits() {
        let h = headers(&[("anthropic-ratelimit-unified-status", "allowed")]);
        let data = parse_response(&h).unwrap();
        assert_eq!(data.status, "available");
        assert!(data.limits.is_empty());
        assert!(data.next_reset_at.is_none());
    }
}
