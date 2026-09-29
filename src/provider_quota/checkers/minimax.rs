//! MiniMax quota checker — port of axonhub's `minimax_checker.go`.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, WINDOW_5H,
    WARNING_THRESHOLD_RATIO, WINDOW_WEEKLY,
};
use crate::storage::Channel;

const MINIMAX_DEFAULT_BASE_URL: &str = "https://www.minimaxi.com";

pub struct MinimaxChecker;

#[derive(Debug, Deserialize)]
struct MinimaxResponse {
    #[serde(default)]
    model_remains: Vec<ModelRemain>,
    #[serde(default)]
    base_resp: BaseResp,
}

#[derive(Debug, Default, Deserialize)]
struct BaseResp {
    #[serde(default)]
    status_code: i64,
    #[serde(default)]
    status_msg: String,
}

#[derive(Debug, Default, Deserialize)]
struct ModelRemain {
    #[serde(default)]
    model_name: String,
    #[serde(default)]
    start_time: i64,
    #[serde(default)]
    end_time: i64,
    #[serde(default)]
    current_interval_status: i64,
    #[serde(default)]
    current_interval_remaining_percent: i64,
    #[serde(default)]
    current_interval_boost_permille: i64,
    #[serde(default)]
    weekly_start_time: i64,
    #[serde(default)]
    weekly_end_time: i64,
    #[serde(default)]
    current_weekly_status: i64,
    #[serde(default)]
    current_weekly_remaining_percent: i64,
    #[serde(default)]
    weekly_boost_permille: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelRow {
    model_name: String,
    interval_used_percent: f64,
    interval_total_percent: f64,
    interval_percent: f64,
    interval_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_reset_at: Option<String>,
    weekly_used_percent: f64,
    weekly_total_percent: f64,
    weekly_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    weekly_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    weekly_reset_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    weekly_boost_permille: Option<i64>,
}

#[async_trait]
impl QuotaChecker for MinimaxChecker {
    fn provider_type(&self) -> &'static str {
        "minimax"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = creds
            .all_api_keys()
            .first()
            .map(|k| k.trim().to_string())
            .unwrap_or_default();
        if api_key.is_empty() {
            return Err(QuotaError::Parse("channel has no API key".into()));
        }

        let url = build_minimax_quota_url(&channel.base_url);
        let resp = http
            .get(&url)
            .bearer_auth(&api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("minimax quota request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let code = status.as_u16();
            let body = resp.text().await.unwrap_or_default();
            if code == 401 || code == 403 {
                return Err(QuotaError::InvalidCredentials(format!("HTTP {code}")));
            }
            return Err(QuotaError::Http(format!(
                "HTTP {code}: {}",
                body.chars().take(200).collect::<String>()
            )));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("minimax quota read failed: {e}")))?;
        parse_minimax_response(&body)
    }
}

fn build_minimax_quota_url(base_url: &str) -> String {
    let base_url = base_url.trim();
    if base_url.is_empty() {
        return format!("{MINIMAX_DEFAULT_BASE_URL}/v1/token_plan/remains");
    }
    let Some((scheme, rest)) = base_url.split_once("://") else {
        return format!("{MINIMAX_DEFAULT_BASE_URL}/v1/token_plan/remains");
    };
    let host = rest.split(['/']).next().unwrap_or(rest);
    if scheme.is_empty() || host.is_empty() {
        return format!("{MINIMAX_DEFAULT_BASE_URL}/v1/token_plan/remains");
    }
    format!("{scheme}://{host}/v1/token_plan/remains")
}

/// Boost permille -> total percent (1500 -> 150.0; 0/absent -> 100.0).
fn total_percent(boost_permille: i64) -> f64 {
    if boost_permille > 0 {
        boost_permille as f64 / 10.0
    } else {
        100.0
    }
}

fn status_for_ratio(api_status: i64, ratio: f64) -> &'static str {
    if api_status == 3 {
        return "exhausted";
    }
    if ratio >= 1.0 {
        return "exhausted";
    }
    if ratio >= WARNING_THRESHOLD_RATIO {
        return "warning";
    }
    if api_status == 1 {
        return "available";
    }
    "unknown"
}

fn period_start_millis(millis: i64) -> Option<DateTime<Utc>> {
    if millis <= 0 {
        return None;
    }
    DateTime::from_timestamp_millis(millis)
}

fn parse_minimax_response(body: &str) -> Result<QuotaData, QuotaError> {
    let response: MinimaxResponse = serde_json::from_str(body)
        .map_err(|e| QuotaError::Parse(format!("failed to parse minimax quota response: {e}")))?;

    if response.base_resp.status_code != 0 {
        return Err(QuotaError::Http(format!(
            "minimax API error: {} (code {})",
            response.base_resp.status_msg, response.base_resp.status_code
        )));
    }
    if response.model_remains.is_empty() {
        return Err(QuotaError::Parse(
            "minimax quota response contains no model remains".into(),
        ));
    }

    let model = response
        .model_remains
        .iter()
        .find(|m| m.model_name == "general")
        .ok_or_else(|| QuotaError::Parse("minimax quota response contains no general model".into()))?;

    let mut overall_status = "available".to_string();
    let mut limits: Vec<QuotaLimitStatus> = Vec::new();
    let mut next_reset_at: Option<DateTime<Utc>> = None;

    // Interval (5h) window.
    let interval_total_percent = total_percent(model.current_interval_boost_permille);
    let interval_used_percent = 100.0 - model.current_interval_remaining_percent as f64;
    let interval_bar_percent = interval_used_percent / interval_total_percent * 100.0;
    let interval_ratio = interval_bar_percent / 100.0;
    let interval_status = status_for_ratio(model.current_interval_status, interval_ratio);

    let mut interval_reset_at: Option<DateTime<Utc>> = None;
    if model.end_time > 0 {
        if let Some(t) = DateTime::from_timestamp_millis(model.end_time) {
            interval_reset_at = Some(t);
            next_reset_at = Some(next_reset_at.map_or(t, |e: DateTime<Utc>| e.min(t)));
        }
    }

    let mut interval_limit = QuotaLimitStatus::token(interval_status, interval_ratio, interval_reset_at)
        .with_window(WINDOW_5H, Duration::hours(5));
    interval_limit.period_start = period_start_millis(model.start_time);
    limits.push(interval_limit);
    overall_status = worse_status(&overall_status, interval_status);

    // Weekly window: only when current_weekly_status == 1.
    let mut weekly_total_percent = 0.0;
    let mut weekly_used_percent = 0.0;
    let mut weekly_bar_percent = 0.0;
    let mut weekly_status: Option<String> = None;
    let mut weekly_reset_at: Option<DateTime<Utc>> = None;
    let mut weekly_reset_at_str: Option<String> = None;

    if model.current_weekly_status == 1 {
        weekly_total_percent = total_percent(model.weekly_boost_permille);
        weekly_used_percent = 100.0 - model.current_weekly_remaining_percent as f64;
        weekly_bar_percent = weekly_used_percent / weekly_total_percent * 100.0;
        let weekly_ratio = weekly_bar_percent / 100.0;
        let status = status_for_ratio(model.current_weekly_status, weekly_ratio);
        weekly_status = Some(status.to_string());

        if model.weekly_end_time > 0 {
            if let Some(t) = DateTime::from_timestamp_millis(model.weekly_end_time) {
                weekly_reset_at = Some(t);
                next_reset_at = Some(next_reset_at.map_or(t, |e: DateTime<Utc>| e.min(t)));
            }
        }

        let mut weekly_limit = QuotaLimitStatus::token(status, weekly_ratio, weekly_reset_at)
            .with_window(WINDOW_WEEKLY, Duration::hours(7 * 24));
        weekly_limit.period_start = period_start_millis(model.weekly_start_time);
        limits.push(weekly_limit);
        overall_status = worse_status(&overall_status, status);

        weekly_reset_at_str = weekly_reset_at.map(|t| t.to_rfc3339());
    }

    let interval_reset_at_str = interval_reset_at.map(|t| t.to_rfc3339());
    let row = ModelRow {
        model_name: model.model_name.clone(),
        interval_used_percent,
        interval_total_percent,
        interval_percent: interval_bar_percent,
        interval_status: interval_status.to_string(),
        interval_reset_at: interval_reset_at_str,
        weekly_used_percent,
        weekly_total_percent,
        weekly_percent: weekly_bar_percent,
        weekly_status,
        weekly_reset_at: weekly_reset_at_str,
        weekly_boost_permille: if model.weekly_boost_permille != 0 { Some(model.weekly_boost_permille) } else { None },
    };

    let mut raw_data = Map::new();
    raw_data.insert("rows".into(), json!([row]));

    let ready = is_ready_status(&overall_status);
    let mut data = QuotaData::new("minimax", &overall_status);
    data.raw_data = raw_data;
    data.next_reset_at = next_reset_at;
    data.ready = ready;
    data.limits = limits;
    Ok(data)
}

fn worse_status(a: &str, b: &str) -> String {
    let rank = |s: &str| match s {
        "available" => 0,
        "warning" => 1,
        "exhausted" => 2,
        _ => 0,
    };
    if rank(b) > rank(a) {
        b.to_string()
    } else {
        a.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_quota_url() {
        assert_eq!(
            build_minimax_quota_url(""),
            "https://www.minimaxi.com/v1/token_plan/remains"
        );
        assert_eq!(
            build_minimax_quota_url("https://api.example.com/whatever/path"),
            "https://api.example.com/v1/token_plan/remains"
        );
        assert_eq!(
            build_minimax_quota_url("no scheme"),
            "https://www.minimaxi.com/v1/token_plan/remains"
        );
    }

    #[test]
    fn happy_path() {
        let body = r#"{
            "model_remains": [
                {
                    "model_name": "general",
                    "start_time": 1784304000000,
                    "end_time": 1784322000000,
                    "current_interval_status": 1,
                    "current_interval_remaining_percent": 98,
                    "current_interval_boost_permille": 0,
                    "weekly_start_time": 1783872000000,
                    "weekly_end_time": 1784476800000,
                    "current_weekly_status": 1,
                    "current_weekly_remaining_percent": 95,
                    "weekly_boost_permille": 0
                },
                {
                    "model_name": "video",
                    "current_interval_status": 1,
                    "current_interval_remaining_percent": 50,
                    "current_weekly_status": 1,
                    "current_weekly_remaining_percent": 50
                }
            ],
            "base_resp": {"status_code": 0, "status_msg": "success"}
        }"#;
        let quota = parse_minimax_response(body).unwrap();
        assert_eq!(quota.status, "available");
        assert!(quota.ready);
        assert_eq!(quota.provider_type, "minimax");
        assert_eq!(quota.limits.len(), 2);
        assert!((quota.limits[0].usage_ratio - 0.02).abs() < 1e-9);
        assert_eq!(quota.limits[0].status, "available");
        assert!((quota.limits[1].usage_ratio - 0.05).abs() < 1e-9);
        assert_eq!(quota.limits[1].status, "available");

        // Epoch-millis reset times.
        assert_eq!(quota.next_reset_at, DateTime::from_timestamp_millis(1784322000000));
        assert_eq!(
            quota.limits[0].period_start,
            DateTime::from_timestamp_millis(1784304000000)
        );
        assert_eq!(quota.limits[0].window, "5h");
        assert_eq!(quota.limits[1].window, "weekly");
        assert_eq!(
            quota.limits[1].period_start,
            DateTime::from_timestamp_millis(1783872000000)
        );

        let rows = quota.raw_data["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["modelName"], "general");
        assert_eq!(rows[0]["intervalStatus"], "available");
    }

    #[test]
    fn weekly_boost_permille_math() {
        let body = r#"{
            "model_remains": [{
                "model_name": "general",
                "start_time": 4087929600000,
                "end_time": 4087947600000,
                "current_interval_status": 1,
                "current_interval_remaining_percent": 100,
                "current_interval_boost_permille": 0,
                "weekly_start_time": 4087324800000,
                "weekly_end_time": 4088534400000,
                "current_weekly_status": 1,
                "current_weekly_remaining_percent": 97,
                "weekly_boost_permille": 1500
            }],
            "base_resp": {"status_code": 0}
        }"#;
        let quota = parse_minimax_response(body).unwrap();
        assert_eq!(quota.status, "available");
        assert_eq!(quota.limits.len(), 2);
        // 5h: remaining=100, no boost -> ratio 0.
        assert!((quota.limits[0].usage_ratio - 0.0).abs() < 1e-9);
        // Weekly: total=150, used=3, bar=3/150 -> ratio 0.02.
        assert!((quota.limits[1].usage_ratio - 0.02).abs() < 1e-9);

        let rows = quota.raw_data["rows"].as_array().unwrap();
        assert!((rows[0]["weeklyUsedPercent"].as_f64().unwrap() - 3.0).abs() < 1e-9);
        assert!((rows[0]["weeklyTotalPercent"].as_f64().unwrap() - 150.0).abs() < 1e-9);
        assert!((rows[0]["weeklyPercent"].as_f64().unwrap() - 2.0).abs() < 1e-9);
        assert_eq!(rows[0]["weeklyBoostPermille"], 1500);
    }

    #[test]
    fn interval_boost_permille_math() {
        let body = r#"{
            "model_remains": [{
                "model_name": "general",
                "current_interval_status": 1,
                "current_interval_remaining_percent": 50,
                "current_interval_boost_permille": 2000,
                "current_weekly_status": 3,
                "current_weekly_remaining_percent": 100,
                "weekly_boost_permille": 0
            }],
            "base_resp": {"status_code": 0}
        }"#;
        let quota = parse_minimax_response(body).unwrap();
        assert_eq!(quota.limits.len(), 1); // weekly skipped (status=3)
        // total=200, used=50, bar=50/200=25% -> ratio 0.25.
        assert!((quota.limits[0].usage_ratio - 0.25).abs() < 1e-9);
        assert_eq!(quota.limits[0].status, "available");

        let rows = quota.raw_data["rows"].as_array().unwrap();
        assert!((rows[0]["intervalTotalPercent"].as_f64().unwrap() - 200.0).abs() < 1e-9);
        assert!((rows[0]["intervalPercent"].as_f64().unwrap() - 25.0).abs() < 1e-9);
        assert!(rows[0].get("weeklyStatus").is_none());
    }

    #[test]
    fn warning_threshold() {
        let body = r#"{
            "model_remains": [{
                "model_name": "general",
                "current_interval_status": 1,
                "current_interval_remaining_percent": 10,
                "current_weekly_status": 1,
                "current_weekly_remaining_percent": 90
            }],
            "base_resp": {"status_code": 0}
        }"#;
        let quota = parse_minimax_response(body).unwrap();
        assert_eq!(quota.status, "warning");
        assert!(quota.ready);
    }

    #[test]
    fn interval_status_three_is_exhausted() {
        let body = r#"{
            "model_remains": [{
                "model_name": "general",
                "current_interval_status": 3,
                "current_interval_remaining_percent": 100,
                "current_weekly_status": 1,
                "current_weekly_remaining_percent": 100
            }],
            "base_resp": {"status_code": 0}
        }"#;
        let quota = parse_minimax_response(body).unwrap();
        assert_eq!(quota.status, "exhausted");
        assert!(!quota.ready);
    }

    #[test]
    fn base_resp_error() {
        let body = r#"{"model_remains":[],"base_resp":{"status_code":1004,"status_msg":"invalid api key"}}"#;
        let err = parse_minimax_response(body).unwrap_err();
        assert!(err.to_string().contains("invalid api key"));
    }

    #[test]
    fn missing_general_model_is_error() {
        let body = r#"{"model_remains":[{"model_name":"video"}],"base_resp":{"status_code":0}}"#;
        assert!(parse_minimax_response(body).is_err());
        assert!(parse_minimax_response(r#"{"base_resp":{"status_code":0}}"#).is_err());
        assert!(parse_minimax_response("not json").is_err());
    }

    #[test]
    fn unknown_interval_status_maps_to_unknown() {
        let body = r#"{
            "model_remains": [{
                "model_name": "general",
                "current_interval_status": 2,
                "current_interval_remaining_percent": 100,
                "current_weekly_status": 2
            }],
            "base_resp": {"status_code": 0}
        }"#;
        let quota = parse_minimax_response(body).unwrap();
        assert_eq!(quota.limits[0].status, "unknown");
        // Go worseStatus treats unknown as rank 0, so overall stays available.
        assert_eq!(quota.status, "available");
    }
}
