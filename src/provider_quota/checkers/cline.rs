//! Cline quota checker — port of axonhub's `cline_checker.go`.
//!
//! Reads the Cline account API (user identity, plans, balance, official
//! usage-limits windows) and the `/usages` cost ledger to build 5h/7d/30d
//! ClinePass windows. The official usage-limits response drives percentages;
//! the ledger only supplies the supplementary per-window cost breakdown, so a
//! ledger failure (e.g. HTTP 429) degrades to `cost_unavailable` instead of
//! failing the whole check.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::{
    is_ready_status, status_rank, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus,
    WARNING_THRESHOLD_RATIO, WINDOW_30D, WINDOW_5H, WINDOW_7D,
};
use crate::storage::Channel;

pub struct ClineChecker;

const PROVIDER_TYPE: &str = "cline";
const PASS_MODEL_PREFIX: &str = "cline-pass/";
const DEFAULT_BASE_URL: &str = "https://api.cline.bot";
/// Cline caps /usages at 200 items per page; requesting more silently returns
/// 200 items. Using the cap halves the requests needed to rebuild the ledger.
const USAGE_PAGE_LIMIT: u32 = 200;
const MAX_USAGE_PAGES: u32 = 100;
const COST_UNITS_PER_USD: i64 = 100_000_000;
const MAX_RESPONSE_BODY_SIZE: usize = 1 << 20;
const USAGE_LIMITS_PATH: &str = "/api/v1/users/me/plan/usage-limits";

const LIMIT_TYPE_FIVE_HOUR: &str = "five_hour";
const LIMIT_TYPE_WEEKLY: &str = "weekly";
const LIMIT_TYPE_MONTHLY: &str = "monthly";

const FETCH_COMPLETE: &str = "complete";
const FETCH_PARTIAL: &str = "partial";
const FETCH_UNUSABLE: &str = "unusable";
const FETCH_PASS_UNAVAILABLE: &str = "cline_pass_unavailable";

const SRC_OFFICIAL_USAGE_LIMITS: &str = "official_usage_limits";
const SRC_OFFICIAL_WINDOW_LEDGER: &str = "cline_pass_ledger_official_window";
const SRC_OFFICIAL_NO_ACTIVE_WINDOW: &str = "official_no_active_window";
const SRC_UNAVAILABLE: &str = "unavailable";

const RESET_ACTIVE: &str = "active";
const RESET_INACTIVE: &str = "inactive";
const RESET_UNAVAILABLE: &str = "unavailable";
const RESET_INVALID: &str = "invalid";

const STATE_ACTIVE: &str = "active";
const STATE_INACTIVE: &str = "inactive";
const STATE_UNAVAILABLE: &str = "unavailable";
const STATE_INVALID: &str = "invalid";

const WINDOW_BOUNDARY_TOLERANCE: Duration = Duration::milliseconds(2_000);

const LEDGER_ERROR_RATE_LIMITED: &str = "rate_limited";
const LEDGER_ERROR_TRANSPORT: &str = "transport";

/// Internal fetch failure classification (never carries the raw error, which
/// can embed credentials, user identifiers, or pagination cursors).
#[derive(Debug, Clone)]
enum FetchError {
    Status(u16),
    Transport,
    Parse,
}

impl FetchError {
    fn into_quota_error(self, ctx: &str) -> QuotaError {
        match self {
            FetchError::Status(code) if code == 401 || code == 403 => {
                QuotaError::InvalidCredentials(format!("{}: HTTP {}", ctx, code))
            }
            FetchError::Status(code) => QuotaError::Http(format!("{}: HTTP {}", ctx, code)),
            FetchError::Transport => QuotaError::Http(format!("{}: request failed during transport", ctx)),
            FetchError::Parse => QuotaError::Parse(format!("failed to parse {} response", ctx)),
        }
    }
}
#[derive(Deserialize, Default, Clone)]
struct PassEntitlement {
    #[serde(default)]
    enabled: bool,
    #[serde(default, rename = "inferenceCapThreshold")]
    inference_cap_threshold: Option<InferenceCapThreshold>,
}

#[derive(Deserialize, Default, Clone)]
struct InferenceCapThreshold {
    #[serde(default, rename = "last5HoursUsageCostUSDPerUser")]
    last5_hours: i64,
    #[serde(default, rename = "last7DaysUsageCostUSDPerUser")]
    last7_days: i64,
    #[serde(default, rename = "last30DaysUsageCostUSDPerUser")]
    last30_days: i64,
}

#[derive(Deserialize, Default, Clone)]
struct Entitlements {
    cline_pass: Option<PassEntitlement>,
}

#[derive(Deserialize, Default, Clone)]
struct Plan {
    #[serde(default, rename = "type")]
    type_: String,
    #[serde(default)]
    interval: String,
    #[serde(default, rename = "isActive")]
    is_active: bool,
    #[serde(default)]
    entitlements: Entitlements,
}

#[derive(Deserialize, Default, Clone)]
struct UsageItem {
    #[serde(default, rename = "createdAt")]
    created_at: String,
    #[serde(default, rename = "costUsd")]
    cost_usd: i64,
    #[serde(default, rename = "creditsUsed")]
    credits_used: i64,
    #[serde(default, rename = "aiModelTypeName")]
    ai_model_type_name: String,
}

#[derive(Default, Clone)]
struct UsageLimit {
    type_: String,
    percent_used: Option<f64>,
    resets_at: String,
    reset_field_state: &'static str,
}

#[derive(Default, Clone)]
struct OfficialWindowLimit {
    usage_ratio: Option<f64>,
    next_reset_at: Option<DateTime<Utc>>,
    reset_state: &'static str,
}

#[derive(Default, Clone)]
struct UsageLimitsFetchMeta {
    status: &'static str,
    entries_seen: usize,
    recognized_entries: usize,
    usable_windows: usize,
    usable_fields: usize,
}

#[derive(Default, Clone)]
struct UsageFetchMeta {
    pages: u32,
    items_seen: usize,
    cline_pass_items_seen: usize,
    direct_items_seen: usize,
    unclassified_items_seen: usize,
    invalid_timestamp_items: usize,
    truncated: bool,
    ledger_unavailable: bool,
    ledger_error_code: String,
}

struct Window {
    key: &'static str,
    limit_units: i64,
    used_units: i64,
    credits_used: i64,
    items_count: usize,
    usage_ratio: Option<f64>,
    cost_usage_ratio: Option<f64>,
    usage_source: &'static str,
    cost_source: &'static str,
    next_reset_at: Option<DateTime<Utc>>,
    window_start_at: Option<DateTime<Utc>>,
    cost_start_at: Option<DateTime<Utc>>,
    reset_source: &'static str,
    state: &'static str,
    active: bool,
    cost_available: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelScope {
    PassOnly,
    Mixed,
    Direct,
    Unknown,
}

impl ModelScope {
    fn as_str(self) -> &'static str {
        match self {
            ModelScope::PassOnly => "cline_pass_only",
            ModelScope::Mixed => "mixed",
            ModelScope::Direct => "direct_only",
            ModelScope::Unknown => "unknown",
        }
    }
}

#[async_trait]
impl QuotaChecker for ClineChecker {
    fn provider_type(&self) -> &'static str {
        PROVIDER_TYPE
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let api_key = cline_api_key(creds)
            .ok_or_else(|| QuotaError::InvalidCredentials("channel has no API key".into()))?;

        let base_url = &channel.base_url;

        let me: Value = get_json(http, base_url, "/api/v1/users/me", &[], &api_key)
            .await
            .map_err(|e| e.into_quota_error("failed to read Cline user identity"))?;
        let me_id = me
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if me_id.is_empty() {
            return Err(QuotaError::Parse("Cline user identity response missing id".into()));
        }

        let plans_value: Value = get_json(http, base_url, "/api/v1/plans", &[], &api_key)
            .await
            .map_err(|e| e.into_quota_error("failed to read Cline plans"))?;
        let plans: Vec<Plan> = parse_data_list(&plans_value);
        let (threshold, plan_summaries, has_cline_pass) = select_pass_threshold(&plans);

        let balance_path = format!("/api/v1/users/{}/balance", path_escape(&me_id));
        let balance_value: Value = get_json(http, base_url, &balance_path, &[], &api_key)
            .await
            .map_err(|e| e.into_quota_error("failed to read Cline balance"))?;
        let balance = balance_value
            .get("data")
            .and_then(|d| d.get("balance"))
            .and_then(Value::as_i64);

        let scope = classify_model_scope(channel);
        if scope == ModelScope::Direct {
            return Ok(build_direct_only_quota(balance, &plan_summaries));
        }
        if !has_cline_pass {
            return Err(QuotaError::Parse(
                "Cline plans response does not include an active ClinePass threshold".into(),
            ));
        }

        let (official_limits, official_meta) = match fetch_usage_limits(http, base_url, &api_key).await {
            Ok(v) => v,
            Err(FetchError::Status(404)) => (HashMap::new(), UsageLimitsFetchMeta { status: FETCH_PASS_UNAVAILABLE, ..Default::default() }),
            Err(e) => return Err(e.into_quota_error("failed to read Cline usage limits")),
        };
        if official_meta.status == FETCH_PASS_UNAVAILABLE {
            return Ok(build_pass_unavailable_quota(scope, &plan_summaries, balance, &official_meta));
        }
        if official_meta.status == FETCH_UNUSABLE {
            return Err(QuotaError::Parse(
                "failed to read Cline usage limits: response contains no usable window data".into(),
            ));
        }

        let (items, fetch_meta) = match fetch_usage_items(http, base_url, &me_id, &api_key).await {
            Ok(v) => v,
            Err(e) => {
                // The /usages ledger only supplies the supplementary
                // per-window cost breakdown; the official usage-limits
                // response already carries the authoritative percentages.
                // Cline rate limits rapid /usages pagination (HTTP 429), so
                // treat a ledger failure as "cost unavailable" instead of
                // failing the whole quota check.
                let mut meta = UsageFetchMeta::default();
                meta.ledger_unavailable = true;
                meta.ledger_error_code = ledger_error_code(&e);
                (Vec::new(), meta)
            }
        };

        Ok(build_quota_data(
            Utc::now(),
            scope,
            &threshold,
            &plan_summaries,
            balance,
            &items,
            &fetch_meta,
            &official_limits,
            &official_meta,
        ))
    }
}

fn cline_api_key(creds: &ChannelCredentials) -> Option<String> {
    if let Some(k) = &creds.api_key {
        let t = k.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    creds
        .api_keys
        .iter()
        .map(|k| k.trim())
        .find(|k| !k.is_empty())
        .map(str::to_string)
}

async fn get_json(
    http: &reqwest::Client,
    base_url: &str,
    path: &str,
    query: &[(&str, String)],
    api_key: &str,
) -> Result<Value, FetchError> {
    let url = build_quota_url(base_url, path, query);
    let resp = http
        .get(&url)
        .header("Accept", "application/json")
        .header("Authorization", format!("Bearer {}", api_key))
        .header("User-Agent", "axonhub/1.0")
        .send()
        .await
        .map_err(|_| FetchError::Transport)?;
    let status = resp.status().as_u16();
    let body = resp.bytes().await.map_err(|_| FetchError::Transport)?;
    let body = &body[..body.len().min(MAX_RESPONSE_BODY_SIZE)];
    if !(200..300).contains(&status) {
        let _ = body;
        return Err(FetchError::Status(status));
    }
    serde_json::from_slice(body).map_err(|_| FetchError::Parse)
}

/// Base default https://api.cline.bot forced https, scheme+host reassembly
/// (any base path is dropped).
fn build_quota_url(base_url: &str, path: &str, query: &[(&str, String)]) -> String {
    let mut base = base_url.trim();
    if base.is_empty() {
        base = DEFAULT_BASE_URL;
    }

    let parsed = reqwest::Url::parse(base).ok().filter(|u| u.host_str().is_some());
    let (mut scheme, mut host) = match &parsed {
        Some(u) => {
            let host = match u.port() {
                Some(p) => format!("{}:{}", u.host_str().unwrap_or(""), p),
                None => u.host_str().unwrap_or("").to_string(),
            };
            (u.scheme().to_string(), host)
        }
        None => (String::new(), String::new()),
    };

    if scheme.is_empty() || scheme == "http" {
        scheme = "https".to_string();
    }
    if host.is_empty() {
        scheme = "https".to_string();
        host = "api.cline.bot".to_string();
    }

    let mut url = reqwest::Url::parse(&format!("{}://{}{}", scheme, host, path))
        .unwrap_or_else(|_| reqwest::Url::parse(DEFAULT_BASE_URL).unwrap());
    url.set_query(None);
    if !query.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (k, v) in query {
            pairs.append_pair(k, v);
        }
        drop(pairs);
    }
    url.into()
}

fn path_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn parse_data_list<T: for<'de> Deserialize<'de>>(value: &Value) -> Vec<T> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(|v| serde_json::from_value(v.clone()).ok()).collect())
        .unwrap_or_default()
}

fn classify_model_scope(channel: &Channel) -> ModelScope {
    let models: Vec<String> = serde_json::from_str(&channel.supported_models).unwrap_or_default();
    if models.is_empty() {
        return ModelScope::Unknown;
    }

    let mut has_pass = false;
    let mut has_direct = false;
    for model in &models {
        let model = model.trim();
        if model.is_empty() {
            continue;
        }
        if model.starts_with(PASS_MODEL_PREFIX) {
            has_pass = true;
        } else {
            has_direct = true;
        }
    }

    match (has_pass, has_direct) {
        (true, true) => ModelScope::Mixed,
        (true, false) => ModelScope::PassOnly,
        (false, true) => ModelScope::Direct,
        (false, false) => ModelScope::Unknown,
    }
}

fn select_pass_threshold(plans: &[Plan]) -> (InferenceCapThreshold, Vec<Map<String, Value>>, bool) {
    let mut selected = InferenceCapThreshold::default();
    let mut summaries: Vec<Map<String, Value>> = Vec::new();
    let mut found = false;

    for plan in plans {
        let pass = match &plan.entitlements.cline_pass {
            Some(p) if p.enabled && p.inference_cap_threshold.is_some() && plan.is_active => p,
            _ => continue,
        };
        summaries.push(
            json!({ "type": plan.type_, "interval": plan.interval })
                .as_object()
                .unwrap()
                .clone(),
        );
        if !found {
            selected = pass.inference_cap_threshold.clone().unwrap();
            found = true;
        }
    }

    (selected, summaries, found)
}

async fn fetch_usage_limits(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
) -> Result<(HashMap<String, OfficialWindowLimit>, UsageLimitsFetchMeta), FetchError> {
    let response: Value = get_json(http, base_url, USAGE_LIMITS_PATH, &[], api_key).await?;
    let items: Vec<UsageLimit> = response
        .get("data")
        .and_then(|d| d.get("limits"))
        .and_then(Value::as_array)
        .map(|arr| arr.iter().map(parse_usage_limit).collect())
        .unwrap_or_default();
    let (limits, meta) = parse_usage_limits(items);
    if meta.status == FETCH_UNUSABLE {
        return Err(FetchError::Parse);
    }
    Ok((limits, meta))
}

async fn fetch_usage_items(
    http: &reqwest::Client,
    base_url: &str,
    user_id: &str,
    api_key: &str,
) -> Result<(Vec<UsageItem>, UsageFetchMeta), FetchError> {
    let items: Vec<UsageItem> = Vec::new();
    let mut meta = UsageFetchMeta::default();
    let mut cursor = String::new();
    let cutoff = Utc::now() - Duration::days(30);
    let path = format!("/api/v1/users/{}/usages", path_escape(user_id));

    for _ in 0..MAX_USAGE_PAGES {
        let mut query: Vec<(&str, String)> = vec![("limit", USAGE_PAGE_LIMIT.to_string())];
        if !cursor.is_empty() {
            query.push(("cursor", cursor.clone()));
        }

        let resp: Value = get_json(http, base_url, &path, &query, api_key).await?;
        let data = resp.get("data").cloned().unwrap_or(Value::Null);
        let page_items: Vec<UsageItem> = data
            .get("items")
            .and_then(Value::as_array)
            .map(|arr| arr.iter().filter_map(|v| serde_json::from_value(v.clone()).ok()).collect())
            .unwrap_or_default();
        let next_token = data
            .get("nextToken")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();

        meta.pages += 1;
        meta.items_seen += page_items.len();
        for item in &page_items {
            match item.ai_model_type_name.trim() {
                "cline-pass" => meta.cline_pass_items_seen += 1,
                "" => meta.unclassified_items_seen += 1,
                _ => meta.direct_items_seen += 1,
            }
            if parse_cline_time(&item.created_at).is_none() {
                meta.invalid_timestamp_items += 1;
            }
        }
        let oldest = oldest_usage_time(&page_items);

        if next_token.is_empty() || page_items.is_empty() || oldest.map_or(false, |t| t < cutoff) {
            return Ok((items, meta));
        }
        cursor = next_token;
    }

    meta.truncated = true;
    Ok((items, meta))
}

/// Classifies a /usages ledger failure without exposing the raw error.
fn ledger_error_code(err: &FetchError) -> String {
    match err {
        FetchError::Status(429) => LEDGER_ERROR_RATE_LIMITED.to_string(),
        FetchError::Status(code) => format!("http_{}", code),
        _ => LEDGER_ERROR_TRANSPORT.to_string(),
    }
}

fn oldest_usage_time(items: &[UsageItem]) -> Option<DateTime<Utc>> {
    let mut oldest: Option<DateTime<Utc>> = None;
    for item in items {
        if let Some(parsed) = parse_cline_time(&item.created_at) {
            if oldest.map_or(true, |o| parsed < o) {
                oldest = Some(parsed);
            }
        }
    }
    oldest
}

fn parse_cline_time(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Port of the Go custom `UnmarshalJSON`: distinguishes missing / null /
/// empty-string / unparsable / valid `resetsAt`.
fn parse_usage_limit(v: &Value) -> UsageLimit {
    let mut l = UsageLimit::default();
    let obj = match v.as_object() {
        Some(o) => o,
        None => return l,
    };

    if let Some(t) = obj.get("type") {
        l.type_ = t.as_str().unwrap_or("").to_string();
    }
    if let Some(p) = obj.get("percentUsed") {
        if let Some(n) = p.as_f64() {
            l.percent_used = Some(n);
        }
    }
    match obj.get("resetsAt") {
        None => l.reset_field_state = RESET_UNAVAILABLE,
        Some(Value::Null) => l.reset_field_state = RESET_INACTIVE,
        Some(Value::String(s)) => {
            l.resets_at = s.clone();
            l.reset_field_state = if s.trim().is_empty() { RESET_INACTIVE } else { RESET_ACTIVE };
        }
        Some(_) => l.reset_field_state = RESET_INVALID,
    }
    l
}

fn usage_limit_window_key(value: &str) -> Option<&'static str> {
    match value.trim() {
        LIMIT_TYPE_FIVE_HOUR => Some("last5h"),
        LIMIT_TYPE_WEEKLY => Some("last7d"),
        LIMIT_TYPE_MONTHLY => Some("last30d"),
        _ => None,
    }
}

fn parse_usage_limits(items: Vec<UsageLimit>) -> (HashMap<String, OfficialWindowLimit>, UsageLimitsFetchMeta) {
    let mut limits: HashMap<String, OfficialWindowLimit> = HashMap::with_capacity(3);
    let mut meta = UsageLimitsFetchMeta { status: FETCH_UNUSABLE, entries_seen: items.len(), ..Default::default() };

    for item in items {
        let key = match usage_limit_window_key(&item.type_) {
            Some(k) => k,
            None => continue,
        };
        meta.recognized_entries += 1;

        let mut limit = limits.get(key).cloned().unwrap_or_default();

        if limit.usage_ratio.is_none() {
            if let Some(percent) = item.percent_used {
                let mut ratio = percent / 100.0;
                if ratio < 0.0 {
                    ratio = 0.0;
                }
                if ratio > 1.0 {
                    ratio = 1.0;
                }
                limit.usage_ratio = Some(ratio);
                meta.usable_fields += 1;
            }
        }

        if limit.reset_state.is_empty() || limit.reset_state == RESET_UNAVAILABLE || limit.reset_state == RESET_INVALID {
            let mut reset_state = item.reset_field_state;
            if reset_state.is_empty() {
                reset_state = if !item.resets_at.trim().is_empty() { RESET_ACTIVE } else { RESET_UNAVAILABLE };
            }
            if reset_state == RESET_INACTIVE && limit.usage_ratio.map_or(true, |r| r > 0.0) {
                reset_state = RESET_UNAVAILABLE;
            }

            match reset_state {
                RESET_ACTIVE => {
                    if let Some(reset_at) = parse_cline_time(&item.resets_at) {
                        limit.next_reset_at = Some(reset_at);
                        limit.reset_state = RESET_ACTIVE;
                        meta.usable_fields += 1;
                    } else {
                        limit.reset_state = RESET_INVALID;
                    }
                }
                RESET_INACTIVE => {
                    limit.reset_state = RESET_INACTIVE;
                    meta.usable_fields += 1;
                }
                RESET_INVALID => limit.reset_state = RESET_INVALID,
                _ => limit.reset_state = RESET_UNAVAILABLE,
            }
        }

        if limit.usage_ratio.is_some() || limit.reset_state != RESET_UNAVAILABLE {
            limits.insert(key.to_string(), limit.clone());
        }
    }

    meta.usable_windows = limits.len();
    meta.status = if meta.usable_fields == 0 {
        FETCH_UNUSABLE
    } else if meta.usable_windows == 3 && meta.usable_fields == 6 {
        FETCH_COMPLETE
    } else {
        FETCH_PARTIAL
    };

    (limits, meta)
}

#[allow(clippy::too_many_arguments)]
fn build_quota_data(
    now: DateTime<Utc>,
    scope: ModelScope,
    threshold: &InferenceCapThreshold,
    plans: &[Map<String, Value>],
    balance: Option<i64>,
    items: &[UsageItem],
    usage_fetch_meta: &UsageFetchMeta,
    official_limits: &HashMap<String, OfficialWindowLimit>,
    official_meta: &UsageLimitsFetchMeta,
) -> QuotaData {
    let cost_unavailable = usage_fetch_meta.truncated || usage_fetch_meta.ledger_unavailable;
    let empty = OfficialWindowLimit::default();
    let windows = vec![
        build_window(now, "last5h", Duration::hours(5), threshold.last5_hours, items, cost_unavailable, official_limits.get("last5h").unwrap_or(&empty)),
        build_window(now, "last7d", Duration::days(7), threshold.last7_days, items, cost_unavailable, official_limits.get("last7d").unwrap_or(&empty)),
        build_window(now, "last30d", Duration::days(30), threshold.last30_days, items, cost_unavailable, official_limits.get("last30d").unwrap_or(&empty)),
    ];

    let mut usage_fetch = json!({
        "pages": usage_fetch_meta.pages,
        "items_seen": usage_fetch_meta.items_seen,
        "cline_pass_items_seen": usage_fetch_meta.cline_pass_items_seen,
        "direct_items_seen": usage_fetch_meta.direct_items_seen,
        "unclassified_items_seen": usage_fetch_meta.unclassified_items_seen,
        "invalid_timestamp_items": usage_fetch_meta.invalid_timestamp_items,
        "truncated": usage_fetch_meta.truncated,
        "ledger_unavailable": usage_fetch_meta.ledger_unavailable,
    });
    if let Some(obj) = usage_fetch.as_object_mut() {
        if !usage_fetch_meta.ledger_error_code.is_empty() {
            obj.insert("ledger_error_code".into(), json!(usage_fetch_meta.ledger_error_code));
        }
    }

    let pass_status = worst_window_status(&windows);
    let mut status = pass_status;
    let mut status_basis = "cline_pass_windows";
    if scope != ModelScope::PassOnly && pass_status == "exhausted" {
        status = "warning";
        status_basis = "mixed_pool_pass_exhausted";
    }

    let mut data = QuotaData::new(PROVIDER_TYPE, status);
    data.ready = is_ready_status(&data.status);
    data.next_reset_at = earliest_window_reset(&windows);
    data.limits = limit_statuses(&windows, scope == ModelScope::PassOnly);
    data.raw_data = json!({
        "model_scope": scope.as_str(),
        "status_basis": status_basis,
        "pool": "cline_pass",
        "pool_note": "ClinePass is a separate provider; this quota applies to cline-pass/* models only.",
        "cost_scale": COST_UNITS_PER_USD,
        "balance": balance_raw_data(balance),
        "plans": plans,
        "windows": windows_raw_data(&windows),
        "usage_fetch": usage_fetch,
        "usage_limits_fetch": usage_limits_fetch_raw_data(official_meta),
    })
    .as_object()
    .unwrap()
    .clone();
    data
}

fn build_pass_unavailable_quota(
    scope: ModelScope,
    plans: &[Map<String, Value>],
    balance: Option<i64>,
    usage_limits_meta: &UsageLimitsFetchMeta,
) -> QuotaData {
    let mut status_basis = "cline_pass_unavailable";
    let status = match scope {
        ModelScope::PassOnly => "exhausted",
        _ => {
            if scope == ModelScope::Mixed {
                status_basis = "cline_pass_unavailable_mixed_pool";
            }
            "warning"
        }
    };

    let mut data = QuotaData::new(PROVIDER_TYPE, status);
    data.ready = is_ready_status(status);
    data.raw_data = json!({
        "model_scope": scope.as_str(),
        "status_basis": status_basis,
        "pool": "cline_pass",
        "pool_note": "ClinePass is a separate provider; this quota applies to cline-pass/* models only.",
        "pass_state": "unavailable",
        "balance": balance_raw_data(balance),
        "plans": plans,
        "usage_limits_fetch": usage_limits_fetch_raw_data(usage_limits_meta),
    })
    .as_object()
    .unwrap()
    .clone();
    data
}

fn build_direct_only_quota(balance: Option<i64>, plans: &[Map<String, Value>]) -> QuotaData {
    let mut data = QuotaData::new(PROVIDER_TYPE, "available");
    data.ready = true;
    data.raw_data = json!({
        "model_scope": ModelScope::Direct.as_str(),
        "status_basis": "direct_credit_balance_informational",
        "pool": "direct_credit",
        "pool_note": "Cline (usage-billing) credits balance is informational until exact pay-as-you-go exhaustion semantics are verified.",
        "balance": balance_raw_data(balance),
        "plans": plans,
    })
    .as_object()
    .unwrap()
    .clone();
    data
}

fn build_window(
    now: DateTime<Utc>,
    key: &'static str,
    duration: Duration,
    limit: i64,
    items: &[UsageItem],
    cost_unavailable: bool,
    official: &OfficialWindowLimit,
) -> Window {
    let mut window = Window {
        key,
        limit_units: limit,
        usage_source: SRC_UNAVAILABLE,
        cost_source: SRC_UNAVAILABLE,
        reset_source: SRC_UNAVAILABLE,
        state: STATE_UNAVAILABLE,
        usage_ratio: None,
        cost_usage_ratio: None,
        next_reset_at: None,
        window_start_at: None,
        cost_start_at: None,
        used_units: 0,
        credits_used: 0,
        items_count: 0,
        active: false,
        cost_available: false,
    };

    if let Some(ratio) = official.usage_ratio {
        window.usage_ratio = Some(ratio);
        window.usage_source = SRC_OFFICIAL_USAGE_LIMITS;
    }

    let mut reset_state = official.reset_state;
    if official.next_reset_at.is_some() {
        reset_state = RESET_ACTIVE;
    }
    if reset_state == RESET_INACTIVE && window.usage_ratio.map_or(false, |r| r > 0.0) {
        reset_state = RESET_UNAVAILABLE;
    }

    match reset_state {
        RESET_INACTIVE => {
            window.state = STATE_INACTIVE;
            window.cost_available = true;
            window.cost_source = SRC_OFFICIAL_NO_ACTIVE_WINDOW;
            window.reset_source = SRC_OFFICIAL_USAGE_LIMITS;
            if window.usage_ratio.is_none() {
                window.usage_ratio = Some(0.0);
                window.usage_source = SRC_OFFICIAL_USAGE_LIMITS;
            }
            window.cost_usage_ratio = Some(0.0);
            return window;
        }
        RESET_INVALID => {
            window.state = STATE_INVALID;
            return window;
        }
        RESET_UNAVAILABLE | "" => return window,
        _ => {}
    }

    let reset_at = match official.next_reset_at {
        Some(t) => t,
        None => {
            window.state = STATE_INVALID;
            return window;
        }
    };

    if reset_at <= now || reset_at > now + duration + WINDOW_BOUNDARY_TOLERANCE {
        window.state = STATE_INVALID;
        return window;
    }

    window.state = STATE_ACTIVE;
    window.active = true;
    window.next_reset_at = Some(reset_at);
    window.reset_source = SRC_OFFICIAL_USAGE_LIMITS;
    if cost_unavailable {
        return window;
    }

    let official_start = reset_at - duration;
    let cost_start = align_window_start(official_start, items);
    window.window_start_at = Some(official_start);
    window.cost_start_at = Some(cost_start);

    for item in items {
        let created_at = match parse_cline_time(&item.created_at) {
            Some(t) => t,
            None => {
                window.cost_available = false;
                window.cost_source = SRC_UNAVAILABLE;
                window.used_units = 0;
                window.credits_used = 0;
                window.items_count = 0;
                window.cost_usage_ratio = None;
                return window;
            }
        };
        if created_at < cost_start || created_at >= reset_at {
            continue;
        }

        match item.ai_model_type_name.trim() {
            "cline-pass" => {
                window.items_count += 1;
                window.used_units += item.cost_usd;
                window.credits_used += item.credits_used;
            }
            "" => {
                window.cost_available = false;
                window.cost_source = SRC_UNAVAILABLE;
                window.used_units = 0;
                window.credits_used = 0;
                window.items_count = 0;
                window.cost_usage_ratio = None;
                return window;
            }
            _ => {}
        }
    }

    window.cost_available = true;
    window.cost_source = SRC_OFFICIAL_WINDOW_LEDGER;
    if window.limit_units > 0 {
        let cost_ratio = window.used_units as f64 / window.limit_units as f64;
        window.cost_usage_ratio = Some(cost_ratio);
        if window.usage_ratio.is_none() {
            window.usage_ratio = Some(cost_ratio);
            window.usage_source = SRC_OFFICIAL_WINDOW_LEDGER;
        }
    }

    window
}

fn align_window_start(expected: DateTime<Utc>, items: &[UsageItem]) -> DateTime<Utc> {
    let mut aligned = expected;
    let mut best_distance = WINDOW_BOUNDARY_TOLERANCE + Duration::nanoseconds(1);

    for item in items {
        if item.ai_model_type_name.trim() != "cline-pass" {
            continue;
        }
        let created_at = match parse_cline_time(&item.created_at) {
            Some(t) => t,
            None => continue,
        };
        let distance = (created_at - expected).abs();
        if distance <= WINDOW_BOUNDARY_TOLERANCE && distance < best_distance {
            aligned = created_at;
            best_distance = distance;
        }
    }

    aligned
}

fn window_status(window: &Window) -> &'static str {
    match window.usage_ratio {
        None => "unknown",
        Some(ratio) if ratio >= 1.0 => "exhausted",
        Some(ratio) if ratio >= WARNING_THRESHOLD_RATIO => "warning",
        Some(_) => "available",
    }
}

fn worst_window_status(windows: &[Window]) -> &'static str {
    let mut status = "unknown";
    for window in windows {
        let s = window_status(window);
        if status_rank(s) > status_rank(status) {
            status = s;
        }
    }
    status
}

fn window_label(key: &str) -> &str {
    match key {
        "last5h" => WINDOW_5H,
        "last7d" => WINDOW_7D,
        "last30d" => WINDOW_30D,
        other => other,
    }
}

fn limit_statuses(windows: &[Window], allow_exhausted: bool) -> Vec<QuotaLimitStatus> {
    let mut limits = Vec::with_capacity(windows.len());
    for window in windows {
        let usage_ratio = window.usage_ratio.unwrap_or(0.0);
        let mut status = window_status(window);
        if !allow_exhausted && status == "exhausted" {
            status = "warning";
        }
        let mut l = QuotaLimitStatus::token(status, usage_ratio, window.next_reset_at);
        l.window = window_label(window.key).to_string();
        l.period_start = window.window_start_at;
        limits.push(l);
    }
    limits
}

fn earliest_window_reset(windows: &[Window]) -> Option<DateTime<Utc>> {
    windows.iter().filter_map(|w| w.next_reset_at).min()
}

fn windows_raw_data(windows: &[Window]) -> Map<String, Value> {
    let mut result = Map::new();
    for window in windows {
        let mut entry = json!({
            "window_state": window.state,
            "active_window": window.active,
            "limit_cost_units": window.limit_units,
            "usage_source": window.usage_source,
            "reset_source": window.reset_source,
            "cost_source": window.cost_source,
        });
        let obj = entry.as_object_mut().unwrap();
        if window.cost_available {
            obj.insert("items_count".into(), json!(window.items_count));
            obj.insert("used_cost_units".into(), json!(window.used_units));
            obj.insert("remaining_cost_units".into(), json!(window.limit_units - window.used_units));
            obj.insert("credits_used".into(), json!(window.credits_used));
        }
        if let Some(r) = window.usage_ratio {
            obj.insert("usage_ratio".into(), json!(r));
            obj.insert("usage_percent".into(), json!(r * 100.0));
        }
        if let Some(r) = window.cost_usage_ratio {
            obj.insert("cost_usage_ratio".into(), json!(r));
            obj.insert("cost_usage_percent".into(), json!(r * 100.0));
        }
        if let Some(t) = window.window_start_at {
            obj.insert("window_start_at".into(), json!(t.to_rfc3339_opts(SecondsFormat::AutoSi, true)));
        }
        if let (Some(cost), Some(win)) = (window.cost_start_at, window.window_start_at) {
            if cost != win {
                obj.insert("cost_start_at".into(), json!(cost.to_rfc3339_opts(SecondsFormat::AutoSi, true)));
            }
        }
        if let Some(t) = window.next_reset_at {
            obj.insert("next_reset_at".into(), json!(t.to_rfc3339_opts(SecondsFormat::AutoSi, true)));
        }
        result.insert(window.key.to_string(), entry);
    }
    result
}

fn usage_limits_fetch_raw_data(meta: &UsageLimitsFetchMeta) -> Map<String, Value> {
    json!({
        "status": meta.status,
        "entries_seen": meta.entries_seen,
        "recognized_entries": meta.recognized_entries,
        "usable_windows": meta.usable_windows,
        "usable_fields": meta.usable_fields,
    })
    .as_object()
    .unwrap()
    .clone()
}

fn balance_raw_data(balance: Option<i64>) -> Map<String, Value> {
    let mut result = json!({
        "unit_note": "Cline API response field name is balance; AxonHub displays it using Cline's Cline credits terminology."
    })
    .as_object()
    .unwrap()
    .clone();
    if let Some(b) = balance {
        result.insert("raw_balance".into(), json!(b));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(created_at: &str, cost_usd: i64, credits: i64, model: &str) -> UsageItem {
        UsageItem {
            created_at: created_at.to_string(),
            cost_usd,
            credits_used: credits,
            ai_model_type_name: model.to_string(),
        }
    }

    fn official(ratio: Option<f64>, reset: Option<DateTime<Utc>>, state: &'static str) -> OfficialWindowLimit {
        OfficialWindowLimit { usage_ratio: ratio, next_reset_at: reset, reset_state: state }
    }

    // ---- URL building ----

    #[test]
    fn builds_quota_urls() {
        assert_eq!(build_quota_url("", "/api/v1/plans", &[]), "https://api.cline.bot/api/v1/plans");
        assert_eq!(build_quota_url("http://api.cline.bot", "/x", &[]), "https://api.cline.bot/x");
        assert_eq!(build_quota_url("https://api.cline.bot/v1/whatever", "/x", &[]), "https://api.cline.bot/x");
        assert_eq!(
            build_quota_url("", "/u", &[("limit", "200".into()), ("cursor", "a b/c".into())]),
            "https://api.cline.bot/u?limit=200&cursor=a+b%2Fc"
        );
    }

    // ---- usage-limit parsing state machine ----

    #[test]
    fn parses_usage_limits_complete() {
        let items = vec![
            parse_usage_limit(&json!({"type": "five_hour", "percentUsed": 42.5, "resetsAt": "2026-09-29T18:00:00Z"})),
            parse_usage_limit(&json!({"type": "weekly", "percentUsed": 10, "resetsAt": "2026-10-02T00:00:00Z"})),
            parse_usage_limit(&json!({"type": "monthly", "percentUsed": 0, "resetsAt": "2026-10-20T00:00:00Z"})),
        ];
        let (limits, meta) = parse_usage_limits(items);
        assert_eq!(meta.status, FETCH_COMPLETE);
        assert_eq!(meta.usable_fields, 6);
        assert!((limits["last5h"].usage_ratio.unwrap() - 0.425).abs() < 1e-9);
        assert_eq!(limits["last5h"].reset_state, RESET_ACTIVE);
        assert!(limits["last5h"].next_reset_at.is_some());
    }

    #[test]
    fn inactive_null_reset_with_zero_ratio() {
        let items = vec![parse_usage_limit(&json!({"type": "five_hour", "percentUsed": 0, "resetsAt": null}))];
        let (limits, meta) = parse_usage_limits(items);
        assert_eq!(limits["last5h"].reset_state, RESET_INACTIVE);
        assert_eq!(meta.usable_fields, 2);
        assert_eq!(meta.status, FETCH_PARTIAL);
    }

    #[test]
    fn inactive_null_reset_with_positive_ratio_becomes_unavailable() {
        let items = vec![parse_usage_limit(&json!({"type": "five_hour", "percentUsed": 5, "resetsAt": null}))];
        let (limits, meta) = parse_usage_limits(items);
        // ratio kept (usable field), but reset demoted to unavailable
        assert_eq!(limits["last5h"].reset_state, RESET_UNAVAILABLE);
        assert!(limits["last5h"].usage_ratio.is_some());
        assert_eq!(meta.status, FETCH_PARTIAL);
    }

    #[test]
    fn missing_or_invalid_resets_at() {
        let missing = vec![parse_usage_limit(&json!({"type": "five_hour", "percentUsed": 10}))];
        let (l1, _) = parse_usage_limits(missing);
        assert_eq!(l1["last5h"].reset_state, RESET_UNAVAILABLE);

        let bad = vec![parse_usage_limit(&json!({"type": "five_hour", "percentUsed": 10, "resetsAt": "not-a-time"}))];
        let (l2, _) = parse_usage_limits(bad);
        assert_eq!(l2["last5h"].reset_state, RESET_INVALID);

        let nonstr = vec![parse_usage_limit(&json!({"type": "five_hour", "resetsAt": 123}))];
        let (l3, _) = parse_usage_limits(nonstr);
        assert_eq!(l3["last5h"].reset_state, RESET_INVALID);
    }

    #[test]
    fn no_usable_fields_is_unusable() {
        let items = vec![parse_usage_limit(&json!({"type": "unknown_window"}))];
        let (_, meta) = parse_usage_limits(items);
        assert_eq!(meta.status, FETCH_UNUSABLE);
    }

    // ---- window state machine ----

    #[test]
    fn active_window_uses_official_ratio_and_ledger_costs() {
        let now = DateTime::parse_from_rfc3339("2026-09-29T12:00:00Z").unwrap().with_timezone(&Utc);
        let reset = now + Duration::hours(1);
        let start = reset - Duration::hours(5);
        let items = vec![
            item(&start.to_rfc3339(), 50_000_000, 10, "cline-pass"),
            item(&(start + Duration::minutes(30)).to_rfc3339(), 25_000_000, 5, "cline-pass"),
            // outside window
            item(&(start - Duration::minutes(1)).to_rfc3339(), 99_000_000, 99, "cline-pass"),
            // direct model: ignored for pass costs
            item(&(start + Duration::minutes(45)).to_rfc3339(), 99_000_000, 99, "claude-sonnet"),
        ];
        let w = build_window(now, "last5h", Duration::hours(5), 100_000_000, &items, false, &official(Some(0.5), Some(reset), RESET_ACTIVE));
        assert_eq!(w.state, STATE_ACTIVE);
        assert_eq!(w.usage_ratio, Some(0.5));
        assert_eq!(w.used_units, 75_000_000);
        assert_eq!(w.items_count, 2);
        assert!(w.cost_available);
        assert!((w.cost_usage_ratio.unwrap() - 0.75).abs() < 1e-9);
    }

    #[test]
    fn inactive_window_is_available_with_zero_cost() {
        let now = Utc::now();
        let w = build_window(now, "last5h", Duration::hours(5), 100, &[], false, &official(None, None, RESET_INACTIVE));
        assert_eq!(w.state, STATE_INACTIVE);
        assert_eq!(w.usage_ratio, Some(0.0));
        assert_eq!(w.cost_usage_ratio, Some(0.0));
        assert!(w.cost_available);
        assert_eq!(w.cost_source, SRC_OFFICIAL_NO_ACTIVE_WINDOW);
    }

    #[test]
    fn invalid_reset_times_mark_window_invalid() {
        let now = Utc::now();
        // past reset
        let past = now - Duration::hours(1);
        let w = build_window(now, "last5h", Duration::hours(5), 100, &[], false, &official(Some(0.5), Some(past), RESET_ACTIVE));
        assert_eq!(w.state, STATE_INVALID);
        // reset beyond now + duration + 2s tolerance
        let far = now + Duration::hours(5) + Duration::seconds(3);
        let w2 = build_window(now, "last5h", Duration::hours(5), 100, &[], false, &official(Some(0.5), Some(far), RESET_ACTIVE));
        assert_eq!(w2.state, STATE_INVALID);
        // just inside the tolerance is still valid
        let near = now + Duration::hours(5) + Duration::seconds(1);
        let w3 = build_window(now, "last5h", Duration::hours(5), 100, &[], false, &official(Some(0.5), Some(near), RESET_ACTIVE));
        assert_eq!(w3.state, STATE_ACTIVE);
        // reset_state invalid
        let w4 = build_window(now, "last5h", Duration::hours(5), 100, &[], false, &official(None, None, RESET_INVALID));
        assert_eq!(w4.state, STATE_INVALID);
        // active state without a reset time
        let w5 = build_window(now, "last5h", Duration::hours(5), 100, &[], false, &official(Some(0.5), None, RESET_ACTIVE));
        assert_eq!(w5.state, STATE_INVALID);
    }

    #[test]
    fn cost_unavailable_skips_ledger() {
        let now = Utc::now();
        let reset = now + Duration::hours(1);
        let items = vec![item(&(now - Duration::minutes(10)).to_rfc3339(), 1, 1, "cline-pass")];
        let w = build_window(now, "last5h", Duration::hours(5), 100, &items, true, &official(Some(0.3), Some(reset), RESET_ACTIVE));
        assert_eq!(w.state, STATE_ACTIVE);
        assert!(!w.cost_available); // Go returns early without the ledger
        assert_eq!(w.used_units, 0);
        assert_eq!(w.cost_usage_ratio, None);
    }

    #[test]
    fn ledger_ratio_fills_missing_usage_ratio() {
        let now = Utc::now();
        let reset = now + Duration::hours(1);
        let start = reset - Duration::hours(5);
        let items = vec![item(&start.to_rfc3339(), 50, 0, "cline-pass")];
        let w = build_window(now, "last5h", Duration::hours(5), 100, &items, false, &official(None, Some(reset), RESET_ACTIVE));
        assert_eq!(w.usage_ratio, Some(0.5));
        assert_eq!(w.usage_source, SRC_OFFICIAL_WINDOW_LEDGER);
    }

    #[test]
    fn unclassified_ledger_item_disables_cost() {
        let now = Utc::now();
        let reset = now + Duration::hours(1);
        let start = reset - Duration::hours(5);
        let items = vec![
            item(&start.to_rfc3339(), 50, 0, "cline-pass"),
            item(&(start + Duration::minutes(1)).to_rfc3339(), 50, 0, ""),
        ];
        let w = build_window(now, "last5h", Duration::hours(5), 100, &items, false, &official(Some(0.2), Some(reset), RESET_ACTIVE));
        assert!(!w.cost_available);
        assert_eq!(w.used_units, 0);
        assert_eq!(w.cost_usage_ratio, None);
        assert_eq!(w.usage_ratio, Some(0.2)); // official ratio survives
    }

    // ---- overall assembly ----

    fn threshold() -> InferenceCapThreshold {
        InferenceCapThreshold { last5_hours: 100, last7_days: 200, last30_days: 300 }
    }

    #[test]
    fn mixed_scope_with_exhausted_pass_downgrades_to_warning() {
        let now = Utc::now();
        let reset = now + Duration::hours(1);
        let official = official(Some(1.0), Some(reset), RESET_ACTIVE);
        let mut limits = HashMap::new();
        limits.insert("last5h".to_string(), official.clone());
        limits.insert("last7d".to_string(), official.clone());
        limits.insert("last30d".to_string(), official);
        let meta = UsageLimitsFetchMeta { status: FETCH_COMPLETE, usable_windows: 3, usable_fields: 6, ..Default::default() };

        let data = build_quota_data(now, ModelScope::Mixed, &threshold(), &[], Some(1_000_000_000), &[], &UsageFetchMeta::default(), &limits, &meta);
        assert_eq!(data.status, "warning");
        assert_eq!(data.raw_data["status_basis"], json!("mixed_pool_pass_exhausted"));
        // pass-only would keep exhausted
        let data2 = build_quota_data(now, ModelScope::PassOnly, &threshold(), &[], None, &[], &UsageFetchMeta::default(), &limits, &meta);
        assert_eq!(data2.status, "exhausted");
        // mixed scope downgrades the per-window status too (Go allowExhausted=false)
        assert_eq!(data.limits[0].status, "warning");
        assert_eq!(data2.limits[0].status, "exhausted");
        assert_eq!(data.limits[0].window, "5h");
        assert_eq!(data.limits[2].window, "30d");
    }

    #[test]
    fn ledger_429_degrades_to_cost_unavailable() {
        // build_quota_data with ledger_unavailable keeps official ratios.
        let now = Utc::now();
        let reset = now + Duration::hours(1);
        let mut limits = HashMap::new();
        let official = official(Some(0.5), Some(reset), RESET_ACTIVE);
        limits.insert("last5h".to_string(), official.clone());
        limits.insert("last7d".to_string(), official.clone());
        limits.insert("last30d".to_string(), official);
        let meta = UsageLimitsFetchMeta { status: FETCH_COMPLETE, usable_windows: 3, usable_fields: 6, ..Default::default() };
        let fetch_meta = UsageFetchMeta {
            ledger_unavailable: true,
            ledger_error_code: LEDGER_ERROR_RATE_LIMITED.to_string(),
            ..Default::default()
        };

        let data = build_quota_data(now, ModelScope::PassOnly, &threshold(), &[], Some(5), &[], &fetch_meta, &limits, &meta);
        assert_eq!(data.status, "available"); // official ratio 0.5
        assert_eq!(data.raw_data["usage_fetch"]["ledger_unavailable"], json!(true));
        assert_eq!(data.raw_data["usage_fetch"]["ledger_error_code"], json!("rate_limited"));
        assert_eq!(data.raw_data["windows"]["last5h"]["cost_source"], json!("unavailable"));

        assert_eq!(ledger_error_code(&FetchError::Status(429)), "rate_limited");
        assert_eq!(ledger_error_code(&FetchError::Status(500)), "http_500");
        assert_eq!(ledger_error_code(&FetchError::Transport), "transport");
    }

    #[test]
    fn pass_unavailable_quota_by_scope() {
        let meta = UsageLimitsFetchMeta { status: FETCH_PASS_UNAVAILABLE, ..Default::default() };
        let data = build_pass_unavailable_quota(ModelScope::PassOnly, &[], Some(7), &meta);
        assert_eq!(data.status, "exhausted");
        assert_eq!(data.raw_data["pass_state"], json!("unavailable"));
        let mixed = build_pass_unavailable_quota(ModelScope::Mixed, &[], None, &meta);
        assert_eq!(mixed.status, "warning");
        assert_eq!(mixed.raw_data["status_basis"], json!("cline_pass_unavailable_mixed_pool"));
    }

    #[test]
    fn direct_only_quota_is_available() {
        let data = build_direct_only_quota(Some(100), &[]);
        assert_eq!(data.status, "available");
        assert!(data.ready);
        assert_eq!(data.raw_data["pool"], json!("direct_credit"));
        assert_eq!(data.raw_data["balance"]["raw_balance"], json!(100));
    }

    #[test]
    fn model_scope_classification() {
        let mut ch = crate::storage::Channel {
            id: "1".into(), name: "c".into(), channel_type: "cline".into(), base_url: String::new(),
            credentials: String::new(), supported_models: String::new(), model_mapping: String::new(),
            weight: 0, priority: 0, status: String::new(), settings: String::new(), created_at: String::new(), updated_at: String::new(),
            disabled_api_keys: "[]".into(),
        };
        ch.supported_models = r#"["cline-pass/claude", "claude-sonnet-4"]"#.into();
        assert_eq!(classify_model_scope(&ch), ModelScope::Mixed);
        ch.supported_models = r#"["cline-pass/claude"]"#.into();
        assert_eq!(classify_model_scope(&ch), ModelScope::PassOnly);
        ch.supported_models = r#"["claude-sonnet-4"]"#.into();
        assert_eq!(classify_model_scope(&ch), ModelScope::Direct);
        ch.supported_models = String::new();
        assert_eq!(classify_model_scope(&ch), ModelScope::Unknown);
    }

    #[test]
    fn threshold_selection_requires_active_enabled_pass() {
        let plans: Vec<Plan> = serde_json::from_value(json!([
            {"type": "free", "isActive": true, "entitlements": {}},
            {"type": "pro", "interval": "monthly", "isActive": true, "entitlements":
                {"cline_pass": {"enabled": true, "inferenceCapThreshold": {"last5HoursUsageCostUSDPerUser": 3}}}}
        ]))
        .unwrap();
        let (t, summaries, found) = select_pass_threshold(&plans);
        assert!(found);
        assert_eq!(t.last5_hours, 3);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0]["type"], json!("pro"));

        let inactive: Vec<Plan> = serde_json::from_value(json!([
            {"type": "pro", "isActive": false, "entitlements": {"cline_pass": {"enabled": true, "inferenceCapThreshold": {"last5HoursUsageCostUSDPerUser": 3}}}}
        ]))
        .unwrap();
        assert!(!select_pass_threshold(&inactive).2);
    }
}

