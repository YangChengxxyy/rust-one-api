//! Command Code quota checker — port of axonhub's `commandcode_checker.go`.
//!
//! Dual auth: the account API key (`/alpha/billing/*`, preferred) or the
//! Studio session cookie from `channel.settings.provider_quota.commandcode.auth_cookie`
//! (`/internal/billing/*`, fallback). A key that only authenticates the chat
//! surface degrades to the cookie path on 401/403.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::{auth_cookie, ChannelCredentials};
use crate::provider_quota::types::{
    is_ready_status, period_start_from_reset, status_rank, QuotaChecker, QuotaData, QuotaError,
    QuotaLimitStatus, QuotaLimitType, WARNING_THRESHOLD_RATIO, WINDOW_5H, WINDOW_MONTHLY,
    WINDOW_WEEKLY,
};
use crate::storage::Channel;

pub struct CommandcodeChecker;

const ALPHA_CREDITS_URL: &str = "https://api.commandcode.ai/alpha/billing/credits";
const ALPHA_SUBSCRIPTIONS_URL: &str = "https://api.commandcode.ai/alpha/billing/subscriptions";
const INTERNAL_CREDITS_URL: &str = "https://api.commandcode.ai/internal/billing/credits";
const INTERNAL_SUBSCRIPTIONS_URL: &str = "https://api.commandcode.ai/internal/billing/subscriptions";
const QUOTA_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const MAX_RESET_EPOCH_MILLIS: f64 = 1e15;

/// The only cookie names forwarded to the billing endpoints. Everything else
/// (analytics, Stripe, better-auth, raw whole-header pastes) is dropped.
/// Lookups are case-insensitive.
const COOKIE_NAMES: [&str; 6] = [
    "__secure-commandcode_prod_.session_token",
    "__host-commandcode_prod_.session_token",
    "commandcode_prod_.session_token",
    "__secure-commandcode_prod_.session_data",
    "__host-commandcode_prod_.session_data",
    "commandcode_prod_.session_data",
];

/// Local monthly allowance table. A plan id can map to more than one row; the
/// wire 5h/weekly caps select the row and double as the price-drift check, so
/// a stale row only drops the monthly denominator. Pay-as-you-go plans report
/// no windows and are intentionally absent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanAllowance {
    pub monthly_usd: f64,
    pub five_hour_usd: f64,
    pub weekly_usd: f64,
}

fn plan_allowances(plan_id: &str) -> &'static [PlanAllowance] {
    match plan_id.trim() {
        "individual-go" => &[PlanAllowance { monthly_usd: 10.0, five_hour_usd: 3.0, weekly_usd: 6.0 }],
        "individual-goat" => &[PlanAllowance { monthly_usd: 70.0, five_hour_usd: 14.0, weekly_usd: 35.0 }],
        // Legacy Pro reports the $30 allowance the CLI still ships; its caps
        // are scaled from the re-issued Pro row.
        "individual-pro" => &[
            PlanAllowance { monthly_usd: 80.0, five_hour_usd: 16.0, weekly_usd: 40.0 },
            PlanAllowance { monthly_usd: 30.0, five_hour_usd: 6.0, weekly_usd: 15.0 },
        ],
        "individual-pro-v1" => &[PlanAllowance { monthly_usd: 80.0, five_hour_usd: 16.0, weekly_usd: 40.0 }],
        "individual-max" => &[PlanAllowance { monthly_usd: 150.0, five_hour_usd: 45.0, weekly_usd: 90.0 }],
        "individual-ultra" => &[PlanAllowance { monthly_usd: 300.0, five_hour_usd: 90.0, weekly_usd: 180.0 }],
        _ => &[],
    }
}

/// Team Pro has no stable plan id in the subscriptions payload; matched by label.
const TEAM_PRO_ALLOWANCE: PlanAllowance = PlanAllowance { monthly_usd: 40.0, five_hour_usd: 12.0, weekly_usd: 24.0 };

#[async_trait]
impl QuotaChecker for CommandcodeChecker {
    fn provider_type(&self) -> &'static str {
        "commandcode"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = commandcode_api_key(creds);

        let cookie_result = auth_cookie(&channel.settings, "commandcode").map(|raw| normalize_command_code_cookie(&raw));
        let cookie: Option<Result<String, String>> = cookie_result;

        let cookie_value = match &cookie {
            Some(Ok(c)) if !c.is_empty() => Some(c.clone()),
            _ => None,
        };

        if api_key.is_none() && cookie_value.is_none() {
            if let Some(Err(e)) = &cookie {
                return Err(QuotaError::InvalidCredentials(format!("invalid Command Code auth cookie: {}", e)));
            }
            return Err(QuotaError::InvalidCredentials(
                "channel has no Command Code API key or quota cookie".into(),
            ));
        }

        // Prefer the account API key: it does not expire the way the Studio
        // session cookie does.
        if let Some(key) = &api_key {
            match fetch_quota(http, ALPHA_CREDITS_URL, ALPHA_SUBSCRIPTIONS_URL, "", key).await {
                Ok(quota) => return Ok(quota),
                Err(err) => {
                    if cookie_value.is_none() || !matches!(err, QuotaError::InvalidCredentials(_)) {
                        return Err(err);
                    }
                    // Keys that only authenticate the chat surface still leave
                    // the cookie usable, so degrade to it instead of failing
                    // the whole check.
                }
            }
        }

        fetch_quota(
            http,
            INTERNAL_CREDITS_URL,
            INTERNAL_SUBSCRIPTIONS_URL,
            cookie_value.as_deref().unwrap_or(""),
            "",
        )
        .await
    }
}

fn commandcode_api_key(creds: &ChannelCredentials) -> Option<String> {
    creds
        .all_api_keys()
        .into_iter()
        .map(str::trim)
        .find(|k| !k.is_empty())
        .map(str::to_string)
}

async fn fetch_quota(
    http: &reqwest::Client,
    credits_url: &str,
    subscriptions_url: &str,
    cookie: &str,
    api_key: &str,
) -> Result<QuotaData, QuotaError> {
    let credits_body = commandcode_get(http, credits_url, cookie, api_key).await?;

    // The subscription endpoint is optional: a failure only drops plan info
    // (and with it the trusted monthly denominator), never the credits result.
    let subscriptions_body = commandcode_get(http, subscriptions_url, cookie, api_key).await.ok();

    parse_command_code_credits(&credits_body, subscriptions_body.as_deref())
}

/// Authenticates with the API key when one is supplied, otherwise with the
/// session cookie.
async fn commandcode_get(
    http: &reqwest::Client,
    url: &str,
    cookie: &str,
    api_key: &str,
) -> Result<Vec<u8>, QuotaError> {
    let mut req = http
        .get(url)
        .header("Accept", "application/json, text/plain, */*")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("User-Agent", QUOTA_UA)
        .header("Origin", "https://commandcode.ai")
        .header("Referer", "https://commandcode.ai/");
    if !api_key.is_empty() {
        req = req.bearer_auth(api_key);
    } else {
        req = req.header("Cookie", cookie);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| QuotaError::Http(format!("Command Code billing request failed: {}", e)))?;
    let status = resp.status().as_u16();
    let body = resp
        .bytes()
        .await
        .map_err(|e| QuotaError::Http(format!("Command Code billing request failed: {}", e)))?;

    if !(200..300).contains(&status) {
        return Err(status_error(status, &body, !api_key.is_empty()));
    }
    Ok(body.to_vec())
}

/// 401/403 mean the credential can no longer be trusted; every other status
/// keeps the generic message so the standard quota-error backoff applies.
fn status_error(status: u16, body: &[u8], with_api_key: bool) -> QuotaError {
    if status == 401 || status == 403 {
        let hint = if with_api_key { "invalid API key?" } else { "expired session cookie?" };
        return QuotaError::InvalidCredentials(format!("Command Code billing API returned {} ({})", status, hint));
    }
    let prefix = String::from_utf8_lossy(&body[..body.len().min(200)]);
    QuotaError::Http(format!("HTTP {}: {}", status, prefix))
}

/// Canonicalizes a raw browser cookie capture into the exact allowlisted
/// Cookie header. Only the six namespaced session_token/session_data cookies
/// survive; at least one session_token cookie must remain. Values must be
/// non-empty and free of control characters; names must be valid RFC 6265
/// tokens. Output preserves the original cookie order.
pub fn normalize_command_code_cookie(raw: &str) -> Result<String, String> {
    let mut cleaned = raw.trim();
    if cleaned.is_empty() {
        return Err("cookie is empty".into());
    }

    // Strip an optional leading "Cookie:" label (browser devtools paste).
    if let Some(idx) = cleaned.find(':') {
        if cleaned[..idx].trim().eq_ignore_ascii_case("cookie") {
            cleaned = cleaned[idx + 1..].trim();
        }
    }

    // Reject multi-line pastes (e.g. cURL text) outright.
    if cleaned.contains(['\r', '\n']) {
        return Err("cookie contains line breaks".into());
    }

    let mut kept: Vec<String> = Vec::new();
    let mut seen_token = false;

    for part in cleaned.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, value) = match part.split_once('=') {
            Some((n, v)) => (n.trim(), v),
            None => return Err("invalid cookie segment: expected name=value".into()),
        };
        if name.is_empty() {
            return Err("invalid cookie segment: expected name=value".into());
        }

        let lower = name.to_lowercase();
        if !COOKIE_NAMES.contains(&lower.as_str()) {
            continue;
        }

        // A name must be a valid RFC 6265 token: no separators, whitespace or
        // control characters.
        for r in name.chars() {
            if r <= '\u{20}' || r >= '\u{7f}' || "()<>@,;:\\\"/[]?={}".contains(r) {
                return Err("invalid cookie name".into());
            }
        }

        let value = value.trim();
        if value.is_empty() {
            return Err("cookie has an empty value".into());
        }
        for r in value.chars() {
            if r <= '\u{20}' || r == '\u{7f}' {
                return Err(format!("cookie {:?} has an invalid value", name));
            }
        }

        if lower.ends_with(".session_token") {
            seen_token = true;
        }
        kept.push(format!("{}={}", name, value));
    }

    if !seen_token {
        return Err("no Command Code session_token cookie found".into());
    }

    Ok(kept.join("; "))
}

#[derive(Default, Debug, Clone)]
struct Window {
    used_usd: f64,
    cap_usd: f64,
    usage_pct: f64, // stored as ratio
    reset_at: Option<DateTime<Utc>>,
    has_cap: bool,
    has_usage: bool,
}

impl Window {
    fn usage_ratio(&self) -> f64 {
        if self.usage_pct > 0.0 {
            return self.usage_pct;
        }
        if self.has_cap && self.cap_usd > 0.0 && self.has_usage {
            return self.used_usd / self.cap_usd;
        }
        0.0
    }

    fn raw(&self) -> Map<String, Value> {
        let mut m = json!({ "usage_percent": self.usage_ratio() * 100.0 }).as_object().unwrap().clone();
        if self.has_cap {
            m.insert("cap_usd".into(), json!(self.cap_usd));
        }
        m.insert("used_usd".into(), json!(self.used_usd));
        if let Some(t) = &self.reset_at {
            m.insert("reset_time".into(), json!(t.to_rfc3339()));
        }
        m
    }
}

/// Turns the credits body (and optional subscriptions body) into QuotaData.
/// The credits payload is consumed case-insensitively; windowLimits may be
/// nested at the root or under credits.
pub fn parse_command_code_credits(
    credits_body: &[u8],
    subscriptions_body: Option<&[u8]>,
) -> Result<QuotaData, QuotaError> {
    let root: Value = serde_json::from_slice(credits_body)
        .map_err(|e| QuotaError::Parse(format!("parse Command Code credits response: {}", e)))?;
    let root = root
        .as_object()
        .ok_or_else(|| QuotaError::Parse("parse Command Code credits response: not a JSON object".into()))?;

    // Credits-scoped map: prefer the nested credits object when present.
    let credits_obj = nested_fold(root, &["credits"]).unwrap_or(root);

    // Windows map may sit at the root or under the credits object.
    let windows_obj = nested_fold(root, &["windowLimits", "windows", "limits"])
        .or_else(|| nested_fold(credits_obj, &["windowLimits", "windows", "limits"]));

    let five_hour = parse_window(windows_obj, &["five_hour", "5h", "fiveHour"]);
    let weekly = parse_window(windows_obj, &["weekly"]);

    // Wire payload: monthlyCredits / purchasedCredits. Older snake_case names
    // stay accepted for tolerance.
    let monthly_remaining = num_fold(credits_obj, &["monthlyCredits", "monthlyRemainingUsd", "monthly_remaining_usd"]);
    let monthly_limit_wire = num_fold(credits_obj, &["monthlyLimitUsd", "monthly_limit_usd"]);
    let purchased = num_fold(credits_obj, &["purchasedCredits", "purchasedCreditsUsd", "purchased_credits_usd"]);

    // Optional subscription enrichments (plan identity for the monthly
    // denominator). Failures degrade silently to a balance-only result.
    let (mut plan_id, mut plan_label, mut sub_status, mut current_period_end) =
        (String::new(), String::new(), String::new(), String::new());
    if let Some(subs) = subscriptions_body.filter(|b| !b.is_empty()) {
        if let Ok(Value::Object(sub_root)) = serde_json::from_slice::<Value>(subs) {
            let sub_obj = nested_fold(&sub_root, &["data", "subscription"]).unwrap_or(&sub_root);
            plan_id = string_fold(sub_obj, &["planId", "plan_id"]);
            plan_label = string_fold(sub_obj, &["planLabel", "plan_label"]);
            sub_status = string_fold(sub_obj, &["status", "subscription_status"]);
            current_period_end = string_fold(sub_obj, &["currentPeriodEnd", "current_period_end"]);
        }
    }

    let mut raw_credits = Map::new();
    if let Some(v) = monthly_remaining {
        raw_credits.insert("monthly_remaining_usd".into(), json!(v));
    }
    if let Some(v) = purchased {
        raw_credits.insert("purchased_credits_usd".into(), json!(v));
    }

    let mut raw = json!({
        "plan_id": plan_id,
        "plan_label": plan_label,
        "subscription_status": sub_status,
    })
    .as_object()
    .unwrap()
    .clone();
    if !current_period_end.is_empty() {
        raw.insert("current_period_end".into(), json!(current_period_end));
    }
    if five_hour.is_some() || weekly.is_some() {
        let mut windows_raw = Map::new();
        if let Some(w) = &five_hour {
            windows_raw.insert("five_hour".into(), Value::Object(w.raw()));
        }
        if let Some(w) = &weekly {
            windows_raw.insert("weekly".into(), Value::Object(w.raw()));
        }
        raw.insert("windows".into(), Value::Object(windows_raw));
    }

    // Trusted monthly denominator: only when the wire 5h/weekly caps select
    // one of the plan's local allowance rows and the remaining balance fits
    // inside that row. Unknown plan / subscription failure / price drift never
    // guess a denominator.
    let allowance = match_plan(&plan_id, &plan_label, &five_hour, &weekly);
    let trusted_monthly = match (allowance, monthly_remaining) {
        (Some(a), Some(remaining)) => remaining <= a.monthly_usd,
        _ => false,
    };
    if trusted_monthly {
        let a = allowance.unwrap();
        if let Some(wire) = monthly_limit_wire {
            raw_credits.insert("monthly_limit_usd".into(), json!(wire));
        } else {
            // Production payload has no monthly limit field; the plan table
            // (matched by planId with wire caps verified above) is the
            // denominator source.
            raw_credits.insert("monthly_limit_usd".into(), json!(a.monthly_usd));
        }
        raw.insert("credits".into(), Value::Object(raw_credits));
    } else if !raw_credits.is_empty() {
        raw.insert("credits".into(), Value::Object(raw_credits));
    }

    let mut overall = "unknown";
    let next_reset_at = earliest_reset(five_hour.as_ref(), weekly.as_ref());
    let mut limits = Vec::with_capacity(3);

    fn add_window(
        window: &str,
        win_len: Duration,
        ratio: f64,
        reset_at: Option<DateTime<Utc>>,
        limits: &mut Vec<QuotaLimitStatus>,
        overall: &mut &'static str,
    ) {
        let status = if ratio >= 1.0 {
            "exhausted"
        } else if ratio >= WARNING_THRESHOLD_RATIO {
            "warning"
        } else {
            "available"
        };
        if status_rank(status) > status_rank(overall) {
            *overall = status;
        }
        let mut l = QuotaLimitStatus::new(QuotaLimitType::SubscriptionCycle, status, ratio, reset_at);
        l.window = window.to_string();
        if win_len > Duration::zero() {
            l.period_start = period_start_from_reset(l.next_reset_at.as_ref(), win_len);
        }
        limits.push(l);
    }

    if let (Some(fh), Some(wk)) = (&five_hour, &weekly) {
        add_window(WINDOW_5H, Duration::hours(5), fh.usage_ratio(), fh.reset_at, &mut limits, &mut overall);
        add_window(WINDOW_WEEKLY, Duration::days(7), wk.usage_ratio(), wk.reset_at, &mut limits, &mut overall);
        if trusted_monthly {
            let a = allowance.unwrap();
            let used = a.monthly_usd - monthly_remaining.unwrap();
            add_window(WINDOW_MONTHLY, Duration::zero(), used / a.monthly_usd, next_reset_at, &mut limits, &mut overall);
        }
    } else {
        // No windows: provider is on pay-as-you-go. Available only while any
        // balance remains; top-ups only ever show the numeric balance.
        let has_balance =
            monthly_remaining.map_or(false, |v| v > 0.0) || purchased.map_or(false, |v| v > 0.0);
        overall = if has_balance { "available" } else { "exhausted" };
        let mut l = QuotaLimitStatus::new(QuotaLimitType::SubscriptionCycle, overall, 0.0, None);
        l.window = WINDOW_MONTHLY.to_string();
        limits.push(l);
    }

    let mut data = QuotaData::new("commandcode", overall);
    data.ready = is_ready_status(overall);
    data.next_reset_at = next_reset_at;
    data.limits = limits;
    data.raw_data = raw;
    Ok(data)
}

fn parse_window(obj: Option<&Map<String, Value>>, keys: &[&str]) -> Option<Window> {
    let obj = obj?;
    let raw = find_map_fold(obj, keys)?;

    let mut w = Window::default();
    let used = num_fold(raw, &["used_usd", "usedUsd", "used"]);
    let cap = num_fold(raw, &["cap_usd", "capUsd", "cap", "limitUsd", "limit_usd"]);
    let pct = num_fold(raw, &["usage_percent", "usagePercent", "percent"]);
    w.has_usage = used.is_some() || pct.is_some();
    w.has_cap = cap.is_some();
    w.used_usd = used.unwrap_or(0.0);
    if let Some(p) = pct {
        w.usage_pct = p / 100.0;
    } else if let (Some(u), Some(c)) = (used, cap) {
        if c > 0.0 {
            w.usage_pct = u / c;
        }
    }
    if let Some(c) = cap {
        w.cap_usd = c;
    }

    if let Some(reset_raw) = find_fold_any(raw, &["reset_time", "resetTime", "resetAt", "reset", "reset_at"]) {
        w.reset_at = parse_reset(reset_raw);
    }

    Some(w)
}

fn earliest_reset(five_hour: Option<&Window>, weekly: Option<&Window>) -> Option<DateTime<Utc>> {
    let mut earliest: Option<DateTime<Utc>> = None;
    for w in [five_hour, weekly].into_iter().flatten() {
        if let Some(t) = w.reset_at {
            if earliest.map_or(true, |cur| t < cur) {
                earliest = Some(t);
            }
        }
    }
    earliest
}

/// Parses a window reset value: unix seconds, unix milliseconds, or RFC3339.
/// Values <= 0 or unparsable produce None (no reset).
fn parse_reset(v: &Value) -> Option<DateTime<Utc>> {
    match v {
        Value::Number(n) => n.as_f64().and_then(reset_from_epoch),
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            if let Ok(f) = trimmed.parse::<f64>() {
                return reset_from_epoch(f);
            }
            DateTime::parse_from_rfc3339(trimmed).ok().map(|t| t.with_timezone(&Utc))
        }
        _ => None,
    }
}

fn reset_from_epoch(epoch: f64) -> Option<DateTime<Utc>> {
    if epoch <= 0.0 || epoch > MAX_RESET_EPOCH_MILLIS {
        return None;
    }
    if epoch >= 1e12 {
        DateTime::from_timestamp_millis(epoch as i64)
    } else {
        DateTime::from_timestamp(epoch as i64, 0)
    }
}

/// Selects the local allowance row for a subscription. A plan id can have
/// several rows (legacy vs re-issued Pro), so the 5h/weekly caps reported by
/// the provider pick the row and double as the drift check.
fn match_plan(
    plan_id: &str,
    plan_label: &str,
    five_hour: &Option<Window>,
    weekly: &Option<Window>,
) -> Option<PlanAllowance> {
    let (five_hour, weekly) = (five_hour.as_ref()?, weekly.as_ref()?);

    let mut candidates: &[PlanAllowance] = plan_allowances(plan_id);
    if candidates.is_empty() && plan_label.trim().eq_ignore_ascii_case("team pro") {
        candidates = std::slice::from_ref(&TEAM_PRO_ALLOWANCE);
    }

    candidates
        .iter()
        .find(|c| c.five_hour_usd == five_hour.cap_usd && c.weekly_usd == weekly.cap_usd)
        .copied()
}

fn nested_fold<'a>(obj: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Map<String, Value>> {
    for key in keys {
        for (k, v) in obj {
            if k.eq_ignore_ascii_case(key) {
                if let Value::Object(m) = v {
                    return Some(m);
                }
            }
        }
    }
    None
}

fn find_map_fold<'a>(obj: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Map<String, Value>> {
    for key in keys {
        for (k, v) in obj {
            if k.eq_ignore_ascii_case(key) {
                let m = v.as_object()?;
                return Some(m);
            }
        }
    }
    None
}

fn find_fold_any<'a>(obj: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    for key in keys {
        for (k, v) in obj {
            if k.eq_ignore_ascii_case(key) {
                return Some(v);
            }
        }
    }
    None
}

fn num_fold(obj: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    let v = find_fold_any(obj, keys)?;
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn string_fold(obj: &Map<String, Value>, keys: &[&str]) -> String {
    match find_fold_any(obj, keys) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "tok-abcdefghijklmnopqrstuvwxyz0123456789";

    #[test]
    fn normalizes_cookie_keeplist() {
        let raw = format!(
            "Cookie: better-auth.session_token=x; __Secure-commandcode_prod_.session_token={}; _ga=GA1.1; __Host-commandcode_prod_.session_data=d",
            TOKEN
        );
        let out = normalize_command_code_cookie(&raw).unwrap();
        assert_eq!(
            out,
            format!("__Secure-commandcode_prod_.session_token={}; __Host-commandcode_prod_.session_data=d", TOKEN)
        );
    }

    #[test]
    fn cookie_requires_session_token() {
        assert!(normalize_command_code_cookie("commandcode_prod_.session_data=only").is_err());
        assert!(normalize_command_code_cookie("").is_err());
        assert!(normalize_command_code_cookie("a\r\nb").is_err());
        assert!(normalize_command_code_cookie("commandcode_prod_.session_token=").is_err());
        // A non-allowlisted segment without '=' errors (invalid segment).
        assert!(normalize_command_code_cookie("ga; commandcode_prod_.session_token=v").is_err());
    }

    #[test]
    fn parses_case_folded_windows_and_plan_trust() {
        let credits = r#"{
            "Credits": {
                "MonthlyCredits": 26.5,
                "PurchasedCredits": 5.0,
                "WindowLimits": {
                    "FiveHour": {"used_usd": 1.5, "cap_usd": 3, "reset_time": "2026-09-29T18:00:00Z"},
                    "Weekly": {"usage_percent": 50, "cap_usd": 6, "reset_time": 1759276800}
                }
            }
        }"#;
        let subs = r#"{"success":true,"data":{"planId":"individual-go","status":"active","currentPeriodEnd":"2026-10-01T00:00:00Z"}}"#;
        let data = parse_command_code_credits(credits.as_bytes(), Some(subs.as_bytes())).unwrap();

        // monthlyRemaining 26.5 > trusted allowance 10 -> denominator dropped.
        assert!(data.raw_data["credits"].get("monthly_limit_usd").is_none());
        assert_eq!(data.limits.len(), 2);

        let credits2 = r#"{"credits":{"monthlyCredits":4.0,"windowLimits":{
            "five_hour":{"used_usd":1.5,"cap_usd":3,"reset_time":1760000000000},
            "weekly":{"usage_percent":50,"cap_usd":6,"reset_time":"2026-10-02T00:00:00Z"}}}}"#;
        let data2 = parse_command_code_credits(credits2.as_bytes(), Some(subs.as_bytes())).unwrap();
        // caps 3/6 select individual-go (monthly 10); 4.0 <= 10 -> trusted
        assert_eq!(data2.raw_data["credits"]["monthly_limit_usd"], json!(10.0));
        assert_eq!(data2.limits.len(), 3);
        assert_eq!(data2.limits[0].window, "5h");
        assert_eq!(data2.limits[1].window, "weekly");
        assert_eq!(data2.limits[2].window, "monthly");
        assert!((data2.limits[2].usage_ratio - 0.6).abs() < 1e-9); // (10-4)/10
        // unix millis reset parsed on the 5h window
        assert!(data2.limits[0].next_reset_at.is_some());
        assert!(data2.next_reset_at.is_some());
        // 5h 1.5/3=0.5, weekly 0.5, monthly 0.6 -> all below 0.8
        assert_eq!(data2.status, "available");
    }

    #[test]
    fn plan_row_selected_by_wire_caps() {
        // Legacy Pro row (6/15) selected via caps, monthly 30.
        let credits = r#"{"credits":{"monthlyCredits":10.0,"windowLimits":{
            "five_hour":{"used_usd":1.0,"cap_usd":6},
            "weekly":{"used_usd":3.0,"cap_usd":15}}}}"#;
        let subs = r#"{"data":{"planId":"individual-pro"}}"#;
        let data = parse_command_code_credits(credits.as_bytes(), Some(subs.as_bytes())).unwrap();
        assert_eq!(data.raw_data["credits"]["monthly_limit_usd"], json!(30.0));
        assert!((data.limits[2].usage_ratio - (20.0 / 30.0)).abs() < 1e-9);
    }

    #[test]
    fn drift_caps_drop_monthly_denominator() {
        let credits = r#"{"credits":{"monthlyCredits":5.0,"windowLimits":{
            "five_hour":{"cap_usd":3},"weekly":{"cap_usd":99}}}}"#;
        let subs = r#"{"data":{"planId":"individual-go"}}"#;
        let data = parse_command_code_credits(credits.as_bytes(), Some(subs.as_bytes())).unwrap();
        assert!(data.raw_data["credits"].get("monthly_limit_usd").is_none());
        assert_eq!(data.limits.len(), 2);
    }

    #[test]
    fn team_pro_matched_by_label() {
        let credits = r#"{"credits":{"monthlyCredits":12.0,"windowLimits":{
            "five_hour":{"cap_usd":12},"weekly":{"cap_usd":24}}}}"#;
        let subs = r#"{"data":{"planLabel":"Team Pro"}}"#;
        let data = parse_command_code_credits(credits.as_bytes(), Some(subs.as_bytes())).unwrap();
        assert_eq!(data.raw_data["credits"]["monthly_limit_usd"], json!(40.0));
    }

    #[test]
    fn payg_no_windows_uses_balance() {
        let credits = r#"{"credits":{"monthlyCredits":0,"purchasedCredits":2.5}}"#;
        let data = parse_command_code_credits(credits.as_bytes(), None).unwrap();
        assert_eq!(data.status, "available");
        assert_eq!(data.limits.len(), 1);
        assert_eq!(data.limits[0].window, "monthly");
        assert_eq!(data.limits[0].status, "available");

        let empty = r#"{"credits":{"monthlyCredits":0}}"#;
        let data = parse_command_code_credits(empty.as_bytes(), None).unwrap();
        assert_eq!(data.status, "exhausted");
        assert!(!data.ready);
    }

    #[test]
    fn reset_value_shapes() {
        assert!(parse_reset(&json!("2026-09-29T18:00:00Z")).is_some());
        assert!(parse_reset(&json!(0)).is_none());
        assert!(parse_reset(&json!(-5)).is_none());
        assert!(parse_reset(&json!(2e18)).is_none());
        assert!(parse_reset(&json!("")).is_none());
        assert!(parse_reset(&json!(null)).is_none());
    }

    #[test]
    fn invalid_json_is_parse_error() {
        assert!(matches!(
            parse_command_code_credits(b"not json", None).unwrap_err(),
            QuotaError::Parse(_)
        ));
    }
}
