//! Kimi Code (Moonshot Coding) quota checker — port of axonhub's
//! `kimi_code_checker.go`.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    normalize_window_label, QuotaChecker, QuotaData, QuotaError,
    QuotaLimitStatus, WARNING_THRESHOLD_RATIO,
};
use crate::storage::Channel;

const KIMI_CODE_DEFAULT_BASE_URL: &str = "https://api.kimi.com/coding/v1";

pub struct KimiCodeChecker;

#[derive(Debug, Clone, Serialize)]
struct UsageRow {
    label: String,
    used: i64,
    limit: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    reset_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reset_after_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BoosterWallet {
    balance_cents: i64,
    total_cents: i64,
    monthly_charge_limit_enabled: bool,
    monthly_charge_limit_cents: i64,
    monthly_used_cents: i64,
    currency: String,
}

#[async_trait]
impl QuotaChecker for KimiCodeChecker {
    fn provider_type(&self) -> &'static str {
        "kimi_code"
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

        let url = build_kimi_code_usage_url(&channel.base_url);
        let resp = http
            .get(&url)
            .bearer_auth(&api_key)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("kimi code usage request failed: {e}")))?;

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
            .map_err(|e| QuotaError::Http(format!("kimi code usage read failed: {e}")))?;
        parse_kimi_code_usage_response(&body)
    }
}

fn build_kimi_code_usage_url(base_url: &str) -> String {
    let base_url = base_url.trim();
    if base_url.is_empty() {
        return format!("{KIMI_CODE_DEFAULT_BASE_URL}/usages");
    }
    let Some((scheme, rest)) = base_url.split_once("://") else {
        return format!("{KIMI_CODE_DEFAULT_BASE_URL}/usages");
    };
    if scheme.is_empty() {
        return format!("{KIMI_CODE_DEFAULT_BASE_URL}/usages");
    }
    // Strip query and fragment.
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let (host, path) = match rest.split_once('/') {
        Some((h, p)) => (h, format!("/{p}")),
        None => (rest, String::new()),
    };
    if host.is_empty() {
        return format!("{KIMI_CODE_DEFAULT_BASE_URL}/usages");
    }
    let path = path.trim_end_matches('/');
    let path = if path.ends_with("/v1") {
        path.to_string()
    } else {
        format!("{path}/v1")
    };
    format!("{scheme}://{host}{path}/usages")
}

fn parse_kimi_code_usage_response(body: &str) -> Result<QuotaData, QuotaError> {
    let response: Value =
        serde_json::from_str(body).map_err(|e| QuotaError::Parse(format!("failed to parse kimi code usage response: {e}")))?;

    let usage = response.get("usage").and_then(Value::as_object);
    let limits_arr = response
        .get("limits")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut rows: Vec<UsageRow> = Vec::new();
    let mut windows: Vec<Duration> = Vec::new();

    if let Some(usage) = usage {
        if let Some(row) = parse_usage_row(usage, "Weekly limit") {
            rows.push(row);
            windows.push(Duration::hours(7 * 24));
        }
    }
    for (i, item) in limits_arr.iter().enumerate() {
        let item = item.as_object().cloned().unwrap_or_default();
        let detail = item
            .get("detail")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| item.clone());
        let label = limit_label(&item, &detail, i);
        if let Some(row) = parse_usage_row(&detail, &label) {
            rows.push(row);
            windows.push(window_length(&item, &detail));
        }
    }
    if rows.is_empty() {
        return Err(QuotaError::Parse(
            "kimi code usage response contains no quota limits".into(),
        ));
    }

    let mut status = "available".to_string();
    let mut limit_statuses: Vec<QuotaLimitStatus> = Vec::with_capacity(rows.len());
    let mut next_reset_at: Option<DateTime<Utc>> = None;

    for (i, row) in rows.iter().enumerate() {
        let ratio = if row.limit > 0 {
            row.used as f64 / row.limit as f64
        } else {
            0.0
        };
        let row_status = status_for_usage_ratio(ratio);
        status = worse_status(&status, row_status);

        let mut reset_at: Option<DateTime<Utc>> = None;
        if let Some(reset_str) = &row.reset_at {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(reset_str) {
                let parsed = parsed.with_timezone(&Utc);
                if next_reset_at.map(|t| parsed < t).unwrap_or(true) {
                    next_reset_at = Some(parsed);
                }
                reset_at = Some(parsed);
            }
        } else if let Some(secs) = row.reset_after_seconds.filter(|s| *s > 0) {
            let parsed = Utc::now() + Duration::seconds(secs);
            if next_reset_at.map(|t| parsed < t).unwrap_or(true) {
                next_reset_at = Some(parsed);
            }
            reset_at = Some(parsed);
        }

        let window = windows[i];
        let window_label = normalize_window_label(window);
        let window_label = if window_label.is_empty() {
            row.label.as_str()
        } else {
            window_label
        };

        limit_statuses.push(
            QuotaLimitStatus::token(row_status, ratio, reset_at).with_window(window_label, window),
        );
    }

    let mut raw_data = Map::new();
    raw_data.insert("rows".into(), json!(rows));
    if let Some(wallet) = response
        .get("boosterWallet")
        .and_then(Value::as_object)
        .and_then(parse_booster_wallet)
    {
        raw_data.insert("boosterWallet".into(), json!(wallet));
    }

    let mut data = QuotaData::new("kimi_code", &status);
    data.raw_data = raw_data;
    data.next_reset_at = next_reset_at;
    data.limits = limit_statuses;
    Ok(data)
}

fn parse_usage_row(raw: &Map<String, Value>, fallback_label: &str) -> Option<UsageRow> {
    if raw.is_empty() {
        return None;
    }
    let limit = kimi_int(raw.get("limit"));
    let mut used = kimi_int(raw.get("used"));
    if used.is_none() {
        if let (Some(limit), Some(remaining)) = (limit, kimi_int(raw.get("remaining"))) {
            used = Some(limit - remaining);
        }
    }
    if limit.is_none() && used.is_none() {
        return None;
    }

    let mut label = fallback_label.to_string();
    for key in ["name", "title"] {
        if let Some(value) = raw.get(key).and_then(Value::as_str) {
            if !value.is_empty() {
                label = value.to_string();
                break;
            }
        }
    }

    Some(UsageRow {
        label,
        used: used.unwrap_or(0),
        limit: limit.unwrap_or(0),
        reset_at: reset_at_string(raw),
        reset_after_seconds: reset_after_seconds(raw),
    })
}

/// Window length from a limit payload: duration + timeUnit (e.g. 300 MINUTE).
fn window_length(item: &Map<String, Value>, detail: &Map<String, Value>) -> Duration {
    let window = item.get("window").and_then(Value::as_object);

    let get = |key: &str| -> Vec<Option<&Value>> {
        match window {
            Some(w) => vec![w.get(key), item.get(key), detail.get(key)],
            None => vec![None, item.get(key), detail.get(key)],
        }
    };

    let duration = get("duration").into_iter().find_map(kimi_int_ref);
    let Some(duration) = duration.filter(|d| *d > 0) else {
        return Duration::zero();
    };

    let unit: String = get("timeUnit")
        .into_iter()
        .find_map(|v| v.and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default();

    if unit.contains("MINUTE") {
        Duration::minutes(duration)
    } else if unit.contains("HOUR") {
        Duration::hours(duration)
    } else if unit.contains("DAY") {
        Duration::hours(duration * 24)
    } else {
        Duration::seconds(duration)
    }
}

fn limit_label(item: &Map<String, Value>, detail: &Map<String, Value>, index: usize) -> String {
    for key in ["name", "title", "scope"] {
        for source in [item, detail] {
            if let Some(value) = source.get(key).and_then(Value::as_str) {
                if !value.is_empty() {
                    return value.to_string();
                }
            }
        }
    }

    let window = item.get("window").and_then(Value::as_object);
    let duration = [
        window.and_then(|w| w.get("duration")),
        item.get("duration"),
        detail.get("duration"),
    ]
    .into_iter()
    .find_map(kimi_int_ref);
    if let Some(duration) = duration {
        let unit: String = [
            window.and_then(|w| w.get("timeUnit")),
            item.get("timeUnit"),
            detail.get("timeUnit"),
        ]
        .into_iter()
        .find_map(|v| v.and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default();

        if unit.contains("MINUTE") {
            if duration >= 60 && duration % 60 == 0 {
                return format!("{}h limit", duration / 60);
            }
            return format!("{duration}m limit");
        } else if unit.contains("HOUR") {
            return format!("{duration}h limit");
        } else if unit.contains("DAY") {
            return format!("{duration}d limit");
        }
        return format!("{duration}s limit");
    }
    format!("Limit #{}", index + 1)
}

fn reset_at_string(raw: &Map<String, Value>) -> Option<String> {
    ["reset_at", "resetAt", "reset_time", "resetTime"]
        .iter()
        .find_map(|key| {
            raw.get(*key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
}

fn reset_after_seconds(raw: &Map<String, Value>) -> Option<i64> {
    ["reset_in", "resetIn", "ttl", "window"]
        .iter()
        .find_map(|key| kimi_int_ref(raw.get(*key)).filter(|v| *v > 0))
}

fn parse_booster_wallet(raw: &Map<String, Value>) -> Option<BoosterWallet> {
    let balance = raw.get("balance")?.as_object()?;
    if balance.get("type").and_then(Value::as_str) != Some("BOOSTER") {
        return None;
    }
    let amount = kimi_int(balance.get("amount")).filter(|a| *a > 0)?;
    let amount_left = kimi_int(balance.get("amountLeft")).unwrap_or(0);
    let (monthly_limit, limit_currency) = kimi_money(raw.get("monthlyChargeLimit"));
    let (monthly_used, used_currency) = kimi_money(raw.get("monthlyUsed"));
    let mut currency = limit_currency;
    if currency.is_empty() {
        currency = used_currency;
    }
    if currency.is_empty() {
        currency = "USD".to_string();
    }

    Some(BoosterWallet {
        balance_cents: fixed_point_to_cents(amount_left),
        total_cents: fixed_point_to_cents(amount),
        monthly_charge_limit_enabled: raw.get("monthlyChargeLimitEnabled") == Some(&Value::Bool(true)),
        monthly_charge_limit_cents: monthly_limit,
        monthly_used_cents: monthly_used,
        currency,
    })
}

fn kimi_money(raw: Option<&Value>) -> (i64, String) {
    let Some(record) = raw.and_then(Value::as_object) else {
        return (0, String::new());
    };
    let cents = kimi_int(record.get("priceInCents")).unwrap_or(0);
    let currency = record
        .get("currency")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    (cents, currency)
}

fn fixed_point_to_cents(value: i64) -> i64 {
    if value > 0 && value < 1_000_000 {
        return 1;
    }
    (value + 500_000) / 1_000_000
}

fn kimi_int(value: Option<&Value>) -> Option<i64> {
    kimi_int_ref(value)
}

fn kimi_int_ref(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(n)) => n.as_f64().map(|f| f as i64),
        Some(Value::String(s)) => s.parse::<f64>().ok().map(|f| f as i64),
        _ => None,
    }
}

fn status_for_usage_ratio(ratio: f64) -> &'static str {
    if ratio >= 1.0 {
        "exhausted"
    } else if ratio >= WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    }
}

fn worse_status(a: &str, b: &str) -> String {
    let rank = |s: &str| match s {
        "available" => 0,
        "warning" => 1,
        "exhausted" => 2,
        _ => -1,
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
    fn builds_usage_url() {
        assert_eq!(
            build_kimi_code_usage_url(""),
            "https://api.kimi.com/coding/v1/usages"
        );
        assert_eq!(
            build_kimi_code_usage_url("https://example.com/kimi"),
            "https://example.com/kimi/v1/usages"
        );
        assert_eq!(
            build_kimi_code_usage_url("https://example.com/kimi/v1/"),
            "https://example.com/kimi/v1/usages"
        );
        assert_eq!(
            build_kimi_code_usage_url("not a URL"),
            "https://api.kimi.com/coding/v1/usages"
        );
    }

    #[test]
    fn parses_full_response() {
        let body = r#"{
            "usage":{"name":"Weekly limit","used":80,"limit":100,"resetAt":"2099-07-20T00:00:00.123456Z"},
            "limits":[
                {"detail":{"remaining":"0","limit":"20"},"window":{"duration":300,"timeUnit":"MINUTE"}},
                {"detail":{"used":1,"limit":10,"title":"Daily limit"}}
            ],
            "boosterWallet":{
                "balance":{"type":"BOOSTER","amount":1250000000,"amountLeft":250000000},
                "monthlyChargeLimitEnabled":true,
                "monthlyChargeLimit":{"priceInCents":5000,"currency":"USD"},
                "monthlyUsed":{"priceInCents":1234,"currency":"USD"}
            }
        }"#;
        let quota = parse_kimi_code_usage_response(body).unwrap();
        assert_eq!(quota.status, "exhausted");
        assert!(!quota.ready);
        assert_eq!(quota.provider_type, "kimi_code");
        assert_eq!(quota.limits.len(), 3);
        assert!((quota.limits[0].usage_ratio - 0.8).abs() < 1e-9);
        assert!((quota.limits[1].usage_ratio - 1.0).abs() < 1e-9);
        assert_eq!(
            quota.next_reset_at.unwrap(),
            DateTime::parse_from_rfc3339("2099-07-20T00:00:00.123456Z")
                .unwrap()
                .with_timezone(&Utc)
        );

        let rows = quota.raw_data["rows"].as_array().unwrap();
        assert_eq!(rows[1]["label"], "5h limit");
        assert_eq!(rows[1]["used"], 20);
        let wallet = &quota.raw_data["boosterWallet"];
        assert_eq!(wallet["balanceCents"], 250);
        assert_eq!(wallet["totalCents"], 1250);
        assert_eq!(wallet["monthlyChargeLimitCents"], 5000);
        assert_eq!(wallet["monthlyUsedCents"], 1234);
        assert_eq!(wallet["currency"], "USD");
        assert_eq!(wallet["monthlyChargeLimitEnabled"], true);
    }

    #[test]
    fn reset_in_falls_back_to_now_plus_seconds() {
        let before = Utc::now();
        let body = r#"{
            "usage":{"used":5,"limit":100,"reset_in":120}
        }"#;
        let quota = parse_kimi_code_usage_response(body).unwrap();
        let reset = quota.next_reset_at.expect("reset from reset_in");
        assert!(reset > before + Duration::seconds(119));
        assert!(reset <= before + Duration::seconds(121));
        // Weekly row has a 7d window, so the well-known label wins over the row label.
        assert_eq!(quota.limits[0].window, "7d");
    }

    #[test]
    fn no_rows_is_error() {
        assert!(parse_kimi_code_usage_response(r#"{"usage":{},"limits":[]}"#).is_err());
        assert!(parse_kimi_code_usage_response("not json").is_err());
    }

    #[test]
    fn derives_used_from_remaining() {
        let body = r#"{"limits":[{"detail":{"remaining":"7","limit":"10"},"resetAt":"2099-01-01T00:00:00Z"}]}"#;
        let quota = parse_kimi_code_usage_response(body).unwrap();
        let rows = quota.raw_data["rows"].as_array().unwrap();
        assert_eq!(rows[0]["used"], 3);
        assert_eq!(quota.limits.len(), 1);
        assert_eq!(quota.status, "available");
    }

    #[test]
    fn wallet_requires_booster_type_and_positive_amount() {
        let raw: Map<String, Value> =
            serde_json::from_str(r#"{"balance":{"type":"REGULAR","amount":1000}}"#).unwrap();
        assert!(parse_booster_wallet(&raw).is_none());
        let raw: Map<String, Value> =
            serde_json::from_str(r#"{"balance":{"type":"BOOSTER","amount":0}}"#).unwrap();
        assert!(parse_booster_wallet(&raw).is_none());
    }
}
