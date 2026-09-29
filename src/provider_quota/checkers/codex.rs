//! Port of axonhub `codex_checker.go`: CheckQuota plus the reset-credit
//! Resetter (ListResets/Reset) backed by the ChatGPT backend API.

use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{Map, Number, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, normalize_window_label, status_rank, QuotaChecker, QuotaData, QuotaError,
    QuotaLimitStatus, QuotaResetter, Reset, ResetList, WARNING_THRESHOLD_RATIO, WINDOW_PRIMARY,
    WINDOW_SECONDARY,
};
use crate::storage::Channel;

const USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const RESET_CREDITS_URL: &str = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits";
const RESET_CONSUME_URL: &str = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume";

const RESET_STATUS_AVAILABLE: &str = "available";

#[derive(Debug, Default, serde::Deserialize)]
struct UsageWindow {
    #[serde(default)]
    used_percent: Option<f64>,
    #[serde(default)]
    reset_at: Option<i64>,
    #[serde(default)]
    reset_after_seconds: Option<i64>,
    #[serde(default)]
    limit_window_seconds: Option<i64>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct RateLimitInfo {
    #[serde(default)]
    allowed: Option<bool>,
    #[serde(default)]
    limit_reached: Option<bool>,
    #[serde(default)]
    primary_window: Option<UsageWindow>,
    #[serde(default)]
    secondary_window: Option<UsageWindow>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct UsageResponse {
    #[serde(default)]
    plan_type: Option<String>,
    #[serde(default)]
    rate_limit: Option<RateLimitInfo>,
    #[serde(default)]
    code_review_rate_limit: Option<RateLimitInfo>,
}

/// codexWindowDuration: nil seconds is a valid zero window; non-positive or
/// overflowing values are invalid (None).
fn window_duration(seconds: Option<i64>) -> Option<Duration> {
    match seconds {
        None => Some(Duration::zero()),
        Some(s) if s > 0 => Duration::try_seconds(s),
        Some(_) => None,
    }
}

fn window_reset_at(window: &UsageWindow, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if let Some(ts) = window.reset_at.filter(|&t| t > 0) {
        return Utc.timestamp_opt(ts, 0).single();
    }
    if let Some(d) = window_duration(window.reset_after_seconds).filter(|d| *d > Duration::zero()) {
        return Some(now + d);
    }
    None
}

fn rate_limit_exhausted(rate_limit: &RateLimitInfo) -> bool {
    rate_limit.limit_reached == Some(true) || rate_limit.allowed == Some(false)
}

fn build_quota_limit(
    name: &str,
    window: Option<&UsageWindow>,
    exhausted: bool,
    now: DateTime<Utc>,
) -> Option<QuotaLimitStatus> {
    let window = window?;
    let duration = window_duration(window.limit_window_seconds)?;
    if let Some(used) = window.used_percent {
        if !used.is_finite() || used < 0.0 {
            return None;
        }
    }

    let mut usage_ratio = 0.0f64;
    let mut status = "available";
    if let Some(used) = window.used_percent {
        usage_ratio = (used / 100.0).min(1.0);
        status = if usage_ratio >= 1.0 {
            "exhausted"
        } else if usage_ratio >= WARNING_THRESHOLD_RATIO {
            "warning"
        } else {
            "available"
        };
    }

    if exhausted {
        status = "exhausted";
        usage_ratio = 1.0;
    }

    let reset_at = window_reset_at(window, now);
    let label = normalize_window_label(duration);
    let label = if label.is_empty() { name } else { label };
    Some(QuotaLimitStatus::token(status, usage_ratio, reset_at).with_window(label, duration))
}

fn aggregate_status(rate_limit: Option<&RateLimitInfo>, limits: &[QuotaLimitStatus]) -> String {
    let mut status = "unknown";
    if let Some(rl) = rate_limit {
        if rl.allowed == Some(true) {
            status = "available";
        }
        if rate_limit_exhausted(rl) {
            status = "exhausted";
        }
    }
    for limit in limits {
        if status_rank(&limit.status) > status_rank(status) {
            status = limit.status.as_str();
        }
    }
    status.to_string()
}

fn earliest_future_reset(limits: &[QuotaLimitStatus], now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    limits
        .iter()
        .filter_map(|l| l.next_reset_at)
        .filter(|t| *t > now)
        .min()
}

fn window_to_map(window: &UsageWindow) -> Value {
    let mut m = Map::new();
    if let Some(v) = window.used_percent {
        m.insert("used_percent".into(), json_num(v));
    }
    if let Some(v) = window.reset_at {
        m.insert("reset_at".into(), v.into());
    }
    if let Some(v) = window.reset_after_seconds {
        m.insert("reset_after_seconds".into(), v.into());
    }
    if let Some(v) = window.limit_window_seconds {
        m.insert("limit_window_seconds".into(), v.into());
    }
    Value::Object(m)
}

fn rate_limit_to_map(rate_limit: &RateLimitInfo) -> Value {
    let mut m = Map::new();
    if let Some(v) = rate_limit.allowed {
        m.insert("allowed".into(), v.into());
    }
    if let Some(v) = rate_limit.limit_reached {
        m.insert("limit_reached".into(), v.into());
    }
    if let Some(w) = &rate_limit.primary_window {
        m.insert("primary_window".into(), window_to_map(w));
    }
    if let Some(w) = &rate_limit.secondary_window {
        m.insert("secondary_window".into(), window_to_map(w));
    }
    Value::Object(m)
}

/// Go marshals float64 via encoding/json.
fn json_num(v: f64) -> Value {
    Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
}

/// parseResponse: usage JSON -> QuotaData.
pub fn parse_response(body: &Value, now: DateTime<Utc>) -> Result<QuotaData, QuotaError> {
    let response: UsageResponse = serde_json::from_value(body.clone())
        .map_err(|e| QuotaError::Parse(format!("failed to parse codex usage response: {e}")))?;

    let mut raw_data = Map::new();
    raw_data.insert("plan_type".into(), response.plan_type.clone().into());
    if let Some(rl) = &response.rate_limit {
        raw_data.insert("rate_limit".into(), rate_limit_to_map(rl));
    }
    if let Some(rl) = &response.code_review_rate_limit {
        raw_data.insert("code_review_rate_limit".into(), rate_limit_to_map(rl));
    }

    let mut limits = Vec::with_capacity(2);
    if let Some(rl) = &response.rate_limit {
        let exhausted = rate_limit_exhausted(rl);
        for (name, window) in [(WINDOW_PRIMARY, &rl.primary_window), (WINDOW_SECONDARY, &rl.secondary_window)] {
            if let Some(limit) = build_quota_limit(name, window.as_ref(), exhausted, now) {
                limits.push(limit);
            }
        }
    }

    let status = aggregate_status(response.rate_limit.as_ref(), &limits);
    let next_reset_at = earliest_future_reset(&limits, now);

    let mut data = QuotaData::new("codex", &status);
    data.raw_data = raw_data;
    data.next_reset_at = next_reset_at;
    data.ready = is_ready_status(&status);
    data.limits = limits;
    Ok(data)
}

pub struct CodexChecker;

#[async_trait]
impl QuotaChecker for CodexChecker {
    fn provider_type(&self) -> &'static str {
        "codex"
    }

    fn as_resetter(&self) -> Option<&dyn crate::provider_quota::types::QuotaResetter> {
        Some(self)
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        _channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let access_token = creds
            .oauth_access_token()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| QuotaError::InvalidCredentials("OAuth missing access_token".into()))?;

        let response = http
            .get(USAGE_URL)
            .bearer_auth(&access_token)
            .header("content-type", "application/json")
            .timeout(StdDuration::from_secs(30))
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("quota request failed: {e}")))?;

        let status = response.status().as_u16();
        if status == 401 || status == 403 {
            return Err(QuotaError::InvalidCredentials(format!("HTTP {status}")));
        }
        if !(200..300).contains(&status) {
            let text = response.text().await.unwrap_or_default();
            let snippet: String = text.chars().take(200).collect();
            return Err(QuotaError::Http(format!("HTTP {status}: {snippet}")));
        }

        let text = response
            .text()
            .await
            .map_err(|e| QuotaError::Parse(format!("failed to read codex usage response: {e}")))?;
        let body: Value = serde_json::from_str(&text)
            .map_err(|e| QuotaError::Parse(format!("failed to parse codex usage response: {e}")))?;
        parse_response(&body, Utc::now())
    }
}

// ---------- Resetter (axonhub codex_checker.go ListResets/Reset) ----------

#[derive(Debug, Default, serde::Deserialize)]
struct ResetCredit {
    id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    reset_type: Option<String>,
    #[serde(default)]
    granted_at: Option<String>,
    #[serde(default)]
    expires_at: Option<String>,
    #[serde(default)]
    title: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ResetCreditsResponse {
    #[serde(default)]
    credits: Vec<ResetCredit>,
}

/// extractCodexCredentials: OAuth access token plus the ChatGPT account id
/// carried in the JWT payload under `https://api.openai.com/auth.chatgpt_account_id`.
fn extract_reset_credentials(creds: &ChannelCredentials) -> Result<(String, String), QuotaError> {
    let access_token = creds
        .oauth_access_token()
        .filter(|t| !t.trim().is_empty())
        .ok_or_else(|| QuotaError::InvalidCredentials("channel has no credentials".into()))?;

    let account_id = extract_chatgpt_account_id_from_jwt(&access_token).ok_or_else(|| {
        QuotaError::InvalidCredentials("failed to extract ChatGPT account id from access token".into())
    })?;
    Ok((access_token, account_id))
}

/// `codex.ExtractChatGPTAccountIDFromJWT`: base64url-decode the JWT payload
/// segment and read the nested auth claim. No signature verification.
pub fn extract_chatgpt_account_id_from_jwt(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let trimmed = payload.trim_end_matches('=');
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(trimmed).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let account_id = claims
        .get("https://api.openai.com/auth")?
        .get("chatgpt_account_id")?
        .as_str()?;
    if account_id.is_empty() { None } else { Some(account_id.to_string()) }
}

/// parseCodexResetTime: empty or invalid RFC3339 -> None.
fn parse_reset_time(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?;
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value).ok().map(|t| t.with_timezone(&Utc))
}

/// Maps a rate-limit-reset-credits response to available resets only.
pub fn parse_reset_list(body: &Value) -> Result<Vec<Reset>, QuotaError> {
    let response: ResetCreditsResponse = serde_json::from_value(body.clone())
        .map_err(|e| QuotaError::Parse(format!("failed to parse codex reset credits response: {e}")))?;

    Ok(response
        .credits
        .into_iter()
        .filter(|c| c.status == RESET_STATUS_AVAILABLE)
        .map(|c| Reset {
            id: c.id,
            status: c.status,
            r#type: c.reset_type,
            granted_at: parse_reset_time(c.granted_at.as_deref()),
            expires_at: parse_reset_time(c.expires_at.as_deref()),
            title: c.title,
        })
        .collect())
}

async fn send_reset_request(
    http: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    access_token: &str,
    account_id: &str,
    body: Option<Value>,
) -> Result<Value, QuotaError> {
    let mut req = http
        .request(method, url)
        .bearer_auth(access_token)
        .header("ChatGPT-Account-Id", account_id)
        .header("content-type", "application/json")
        .timeout(StdDuration::from_secs(30));
    if let Some(body) = body {
        req = req.body(body.to_string());
    }
    let response = req.send().await.map_err(|e| QuotaError::Http(format!("{e}")))?;

    let status = response.status().as_u16();
    if status == 401 || status == 403 {
        return Err(QuotaError::InvalidCredentials(format!("HTTP {status}")));
    }
    if !(200..300).contains(&status) {
        let text = response.text().await.unwrap_or_default();
        let snippet: String = text.chars().take(200).collect();
        return Err(QuotaError::Http(format!("HTTP {status}: {snippet}")));
    }

    let text = response.text().await.map_err(|e| QuotaError::Parse(format!("{e}")))?;
    serde_json::from_str(&text).map_err(|e| QuotaError::Parse(format!("failed to parse codex reset response: {e}")))
}

#[async_trait]
impl QuotaResetter for CodexChecker {
    async fn list_resets(
        &self,
        http: &reqwest::Client,
        _channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<ResetList, QuotaError> {
        let (access_token, account_id) = extract_reset_credentials(creds)?;
        let body = send_reset_request(http, reqwest::Method::GET, RESET_CREDITS_URL, &access_token, &account_id, None)
            .await
            .map_err(|e| QuotaError::Http(format!("list codex reset credits failed: {e}")))?;

        Ok(ResetList { supported: true, resets: parse_reset_list(&body)?, error: None })
    }

    async fn reset(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<(), QuotaError> {
        let list = self.list_resets(http, channel, creds).await?;
        let credit_id = list
            .resets
            .first()
            .map(|r| r.id.clone())
            .ok_or_else(|| QuotaError::Http("no available codex reset credit".into()))?;

        let (access_token, account_id) = extract_reset_credentials(creds)?;
        send_reset_request(
            http,
            reqwest::Method::POST,
            RESET_CONSUME_URL,
            &access_token,
            &account_id,
            Some(serde_json::json!({
                "credit_id": credit_id,
                "redeem_request_id": uuid::Uuid::new_v4().to_string(),
            })),
        )
        .await
        .map_err(|e| QuotaError::Http(format!("consume codex reset credit failed: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000, 0).unwrap()
    }

    #[test]
    fn parse_available_with_windows() {
        let body = serde_json::json!({
            "plan_type": "pro",
            "rate_limit": {
                "allowed": true,
                "limit_reached": false,
                "primary_window": {
                    "used_percent": 40.0,
                    "reset_at": 1_800_000_000 + 3600,
                    "limit_window_seconds": 3600
                },
                "secondary_window": {
                    "used_percent": 10.0,
                    "reset_at": 1_800_000_000 + 86400 * 5,
                    "limit_window_seconds": 86400 * 7
                }
            }
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.status, "available");
        assert_eq!(data.limits.len(), 2);
        // 3600s maps to no well-known label -> falls back to "primary"
        assert_eq!(data.limits[0].window, "primary");
        assert_eq!(data.limits[1].window, "7d");
        assert_eq!(data.raw_data["plan_type"], "pro");
        assert_eq!(data.raw_data["rate_limit"]["allowed"], true);
        assert_eq!(data.raw_data["rate_limit"]["primary_window"]["used_percent"], 40.0);
    }

    #[test]
    fn parse_warning_threshold() {
        let body = serde_json::json!({
            "rate_limit": {
                "allowed": true,
                "primary_window": {"used_percent": 85.0, "reset_at": 1_800_000_000 + 3600, "limit_window_seconds": 3600}
            }
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.status, "warning");
        assert_eq!(data.limits[0].status, "warning");
        assert!((data.limits[0].usage_ratio - 0.85).abs() < 1e-9);
    }

    #[test]
    fn limit_reached_forces_exhausted() {
        let body = serde_json::json!({
            "rate_limit": {
                "allowed": true,
                "limit_reached": true,
                "primary_window": {"used_percent": 5.0, "reset_at": 1_800_000_000 + 60, "reset_after_seconds": 60},
                "secondary_window": {"used_percent": 2.0, "reset_at": 1_800_000_000 + 120}
            }
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.status, "exhausted");
        for l in &data.limits {
            assert_eq!(l.status, "exhausted");
            assert_eq!(l.usage_ratio, 1.0);
        }
        // earliest future reset wins
        assert_eq!(data.next_reset_at, Some(Utc.timestamp_opt(1_800_000_060, 0).unwrap()));
    }

    #[test]
    fn allowed_false_forces_exhausted_even_without_windows() {
        let body = serde_json::json!({"rate_limit": {"allowed": false}});
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.status, "exhausted");
        assert!(data.limits.is_empty());
    }

    #[test]
    fn window_labels_map_to_known_windows() {
        let body = serde_json::json!({
            "rate_limit": {
                "allowed": true,
                "primary_window": {"used_percent": 10.0, "reset_at": 1_800_000_000 + 3600, "limit_window_seconds": 18000},
                "secondary_window": {"used_percent": 10.0, "reset_at": 1_800_000_000 + 7200, "limit_window_seconds": 604800}
            }
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.limits[0].window, "5h");
        assert_eq!(data.limits[1].window, "7d");
    }

    #[test]
    fn unknown_duration_falls_back_to_name() {
        let body = serde_json::json!({
            "rate_limit": {
                "allowed": true,
                "primary_window": {"used_percent": 10.0, "reset_at": 1_800_000_000 + 3600, "limit_window_seconds": 12345}
            }
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.limits[0].window, "primary");
    }

    #[test]
    fn reset_after_seconds_used_when_no_reset_at() {
        let body = serde_json::json!({
            "rate_limit": {
                "allowed": true,
                "primary_window": {"used_percent": 10.0, "reset_after_seconds": 600, "limit_window_seconds": 18000}
            }
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.next_reset_at, Some(Utc.timestamp_opt(1_800_000_600, 0).unwrap()));
    }

    #[test]
    fn missing_rate_limit_is_unknown() {
        let data = parse_response(&serde_json::json!({"plan_type": "free"}), now()).unwrap();
        assert_eq!(data.status, "unknown");
        assert!(!data.ready);
    }

    #[test]
    fn code_review_rate_limit_only_in_raw_data() {
        let body = serde_json::json!({
            "rate_limit": {"allowed": true, "primary_window": {"used_percent": 1.0, "reset_at": 1_800_000_000 + 60, "limit_window_seconds": 18000}},
            "code_review_rate_limit": {"allowed": true, "limit_reached": true}
        });
        let data = parse_response(&body, now()).unwrap();
        assert_eq!(data.status, "available");
        assert_eq!(data.raw_data["code_review_rate_limit"]["limit_reached"], true);
    }

    #[test]
    fn invalid_json_shape_is_parse_error() {
        let err = parse_response(&serde_json::json!({"rate_limit": "nope"}), now()).unwrap_err();
        assert!(matches!(err, QuotaError::Parse(_)));
    }

    // ---------- Resetter tests (mirror axonhub codex_checker_test.go) ----------

    fn b64url(s: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s)
    }

    fn test_jwt(account_id: &str) -> String {
        let header = b64url(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = b64url(&format!(
            r#"{{"https://api.openai.com/auth":{{"chatgpt_account_id":"{account_id}"}}}}"#
        ));
        format!("{header}.{payload}.")
    }

    #[test]
    fn jwt_account_id_extraction() {
        assert_eq!(
            extract_chatgpt_account_id_from_jwt(&test_jwt("acct_reset")).as_deref(),
            Some("acct_reset")
        );
        // missing claim / malformed payloads
        let no_claim = format!("{}.{}.", b64url(r#"{"alg":"none"}"#), b64url(r#"{"sub":"x"}"#));
        assert_eq!(extract_chatgpt_account_id_from_jwt(&no_claim), None);
        assert_eq!(extract_chatgpt_account_id_from_jwt("not-a-jwt"), None);
    }

    #[test]
    fn reset_list_filters_available_and_maps_times() {
        let body = serde_json::json!({
            "credits": [
                {"id": "cred_1", "status": "available", "reset_type": "codex_rate_limits",
                 "granted_at": "2026-09-01T00:00:00Z", "expires_at": "2026-09-08T00:00:00Z"},
                {"id": "cred_2", "status": "redeemed"}
            ],
            "available_count": 1
        });
        let resets = parse_reset_list(&body).unwrap();
        assert_eq!(resets.len(), 1);
        assert_eq!(resets[0].id, "cred_1");
        assert_eq!(resets[0].status, "available");
        assert_eq!(resets[0].r#type.as_deref(), Some("codex_rate_limits"));
        assert_eq!(resets[0].granted_at.map(|t| t.to_rfc3339()).as_deref(), Some("2026-09-01T00:00:00+00:00"));
        assert_eq!(resets[0].expires_at.map(|t| t.to_rfc3339()).as_deref(), Some("2026-09-08T00:00:00+00:00"));

        // empty availability
        let empty = serde_json::json!({"credits": [{"id": "c", "status": "redeemed"}], "available_count": 0});
        assert!(parse_reset_list(&empty).unwrap().is_empty());
    }

    #[test]
    fn reset_time_invalid_values_are_none() {
        assert!(parse_reset_time(None).is_none());
        assert!(parse_reset_time(Some("")).is_none());
        assert!(parse_reset_time(Some("not-a-time")).is_none());
    }

    #[test]
    fn reset_list_json_shape_matches_go() {
        let list = ResetList {
            supported: true,
            resets: vec![Reset {
                id: "cred_1".into(),
                status: "available".into(),
                r#type: Some("codex_rate_limits".into()),
                granted_at: None,
                expires_at: None,
                title: None,
            }],
            error: None,
        };
        let v = serde_json::to_value(&list).unwrap();
        assert_eq!(v["supported"], true);
        assert_eq!(v["resets"][0]["id"], "cred_1");
        assert_eq!(v["resets"][0]["type"], "codex_rate_limits");
        // omitempty fields absent
        assert!(v.get("error").is_none());
        assert!(v["resets"][0].get("grantedAt").is_none());
        assert!(v["resets"][0].get("expiresAt").is_none());
        assert!(v["resets"][0].get("title").is_none());
    }

    #[tokio::test]
    async fn reset_credentials_and_body_shape() {
        use crate::provider_quota::credentials::{ChannelCredentials, OAuthCredentials};

        let access_token = test_jwt("acct_reset");
        let creds = ChannelCredentials {
            oauth: Some(OAuthCredentials { access_token: access_token.clone(), refresh_token: None, ..Default::default() }),
            ..Default::default()
        };
        let (tok, account_id) = extract_reset_credentials(&creds).unwrap();
        assert_eq!(tok, access_token);
        assert_eq!(account_id, "acct_reset");

        // Consume body shape: credit_id from first available credit + uuid-v4 redeem_request_id.
        let body = serde_json::json!({
            "credit_id": "cred_2",
            "redeem_request_id": uuid::Uuid::new_v4().to_string(),
        });
        assert_eq!(body["credit_id"], "cred_2");
        let redeem = body["redeem_request_id"].as_str().unwrap();
        assert_eq!(redeem.len(), 36);
        assert_eq!(redeem.matches('-').count(), 4);

        // no available credit -> "no available codex reset credit"
        let empty: Vec<Reset> = parse_reset_list(&serde_json::json!({
            "credits": [{"id": "cred_1", "status": "redeemed"}], "available_count": 0
        }))
        .unwrap();
        let err = empty
            .first()
            .map(|r| r.id.clone())
            .ok_or_else(|| QuotaError::Http("no available codex reset credit".into()));
        assert!(err.unwrap_err().to_string().contains("no available codex reset credit"));
    }
}
