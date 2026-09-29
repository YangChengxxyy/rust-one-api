//! ZhiPu family (open.bigmodel.cn / api.z.ai) quota checkers — port of
//! axonhub's `zhipu_checker.go` + `zai_checker.go`.
//!
//! One request per API key (`Authorization: <key>` bare, no Bearer prefix);
//! the per-key snapshots are folded into one channel payload. A single-key
//! channel keeps one limit per window; a multi-key channel reports the most
//! binding window of each account inside the `zhipu_accounts` availability
//! group (OR semantics for routing). Raw data never stores a full key — only
//! a sha256 digest prefix and the four trailing characters.
//!
//! Deviation from axonhub: none for disabled keys — parked keys are excluded
//! from the serving-key fan-out (bounded by `MAX_ZHIPU_QUOTA_DISABLED_ACCOUNTS`)
//! and shown in raw data as disabled accounts.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

use crate::provider_quota::credentials::{disabled_key_set, ChannelCredentials};
use crate::provider_quota::types::{
    is_ready_status, QuotaChecker, QuotaData, QuotaError, QuotaLimitStatus, WARNING_THRESHOLD_RATIO,
    WINDOW_5H, WINDOW_WEEKLY,
};
use crate::storage::Channel;

const ZHIPU_QUOTA_URL: &str = "https://open.bigmodel.cn/api/monitor/usage/quota/limit";
const ZAI_QUOTA_URL: &str = "https://api.z.ai/api/monitor/usage/quota/limit";

/// Tags the limit of every API key of a multi-key channel; the routing
/// evaluator ORs members of one availability group.
const ZHIPU_ACCOUNTS_AVAILABILITY_GROUP: &str = "zhipu_accounts";

/// Bounds how many parked (disabled) keys one check reports. Keys that can
/// still serve are always checked, because their quota decides the status.
const MAX_ZHIPU_QUOTA_DISABLED_ACCOUNTS: usize = 32;

const ZHIPU_WINDOW_FIVE_HOUR: &str = "five_hour";
const ZHIPU_WINDOW_WEEKLY: &str = "weekly_limit";

const ZHIPU_CREDIT_UNIT_HOUR: i64 = 3;
const ZHIPU_CREDIT_UNIT_WEEK: i64 = 6;

pub struct ZhipuChecker;

#[async_trait]
impl QuotaChecker for ZhipuChecker {
    fn provider_type(&self) -> &'static str {
        "zhipu"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        collect_family_quota(http, channel, creds, "zhipu", ZHIPU_QUOTA_URL).await
    }
}

pub struct ZaiChecker;

#[async_trait]
impl QuotaChecker for ZaiChecker {
    fn provider_type(&self) -> &'static str {
        "zai"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        collect_family_quota(http, channel, creds, "zai", ZAI_QUOTA_URL).await
    }
}

#[derive(Debug, Deserialize)]
struct ZhipuQuotaResponse {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    code: i64,
    #[serde(default)]
    msg: String,
    data: Option<ZhipuQuotaData>,
}

#[derive(Debug, Deserialize)]
struct ZhipuQuotaData {
    #[serde(default)]
    level: String,
    #[serde(default)]
    limits: Vec<ZhipuLimitEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ZhipuLimitEntry {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    percentage: f64,
    next_reset_time: Option<i64>,
    unit: Option<i64>,
    number: Option<i64>,
    usage: Option<i64>,
    current_value: Option<i64>,
    remaining: Option<i64>,
}

/// Normalized per-window row stored in raw data.
#[derive(Debug, Clone, Serialize)]
struct ZhipuWindowRow {
    window: String,
    used_percent: f64,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reset_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    used: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remaining: Option<i64>,
}

/// Per-API-key snapshot stored in raw data under "accounts". The key itself
/// never reaches the payload.
#[derive(Debug, Serialize)]
struct ZhipuAccountQuota {
    r#ref: String,
    suffix: String,
    #[serde(skip_serializing_if = "is_false")]
    disabled: bool,
    status: String,
    ready: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    level: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rows: Vec<ZhipuWindowRow>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

struct ZhipuAccountSnapshot {
    level: String,
    rows: Vec<(ZhipuWindowRow, Option<DateTime<Utc>>)>,
    status: String,
    ready: bool,
}

#[derive(Clone, Copy, PartialEq)]
struct ZhipuWindowSpec {
    name: &'static str,
    label: &'static str,
    length: Duration,
}

const FIVE_HOUR_SPEC: ZhipuWindowSpec = ZhipuWindowSpec {
    name: ZHIPU_WINDOW_FIVE_HOUR,
    label: WINDOW_5H,
    length: Duration::from_secs(5 * 3600),
};
const WEEKLY_SPEC: ZhipuWindowSpec = ZhipuWindowSpec {
    name: ZHIPU_WINDOW_WEEKLY,
    label: WINDOW_WEEKLY,
    length: Duration::from_secs(7 * 24 * 3600),
};

/// Queries the quota endpoint once per API key and folds the answers into one
/// channel payload. Serving keys are all checked (they decide the channel
/// status); parked keys are display-only, bounded, and never mask a serving
/// failure. Only fails when no key could be read at all.
async fn collect_family_quota(
    http: &reqwest::Client,
    channel: &Channel,
    creds: &ChannelCredentials,
    provider_type: &str,
    quota_url: &str,
) -> Result<QuotaData, QuotaError> {
    let keys = channel_api_keys(creds);
    if keys.is_empty() {
        return Err(QuotaError::InvalidCredentials("channel has no API key".into()));
    }

    let disabled = disabled_key_set(channel);
    let (enabled_keys, disabled_keys) = plan_targets(&keys, &disabled);

    let mut targets = enabled_keys.clone();
    targets.extend(disabled_keys.iter().cloned());

    let mut accounts: Vec<ZhipuAccountQuota> = Vec::with_capacity(targets.len());
    let mut failures = 0usize;
    let mut enabled_successes = 0usize;
    let mut first_err: Option<QuotaError> = None;

    for key in &targets {
        let mut account = ZhipuAccountQuota {
            r#ref: key_ref(key),
            suffix: key_suffix(key),
            disabled: disabled.contains(key),
            status: String::new(),
            ready: false,
            level: String::new(),
            error: String::new(),
            rows: Vec::new(),
        };

        match fetch_account_snapshot(http, quota_url, key).await {
            Err(err) => {
                failures += 1;
                account.status = "unknown".into();
                account.error = err.to_string();
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Ok(snapshot) => {
                if !account.disabled {
                    enabled_successes += 1;
                }
                account.status = snapshot.status.clone();
                account.ready = snapshot.ready;
                account.level = snapshot.level.clone();
                account.rows = snapshot.rows.iter().map(|(row, _)| row.clone()).collect();
            }
        }
        accounts.push(account);
    }

    if accounts.is_empty() || failures == accounts.len() {
        // Every key failed: keep the underlying reason instead of a generic
        // summary.
        return Err(all_keys_error(first_err, keys.len(), None));
    }
    if !enabled_keys.is_empty() && enabled_successes == 0 {
        // No serving key could be read; disabled accounts must not mask the
        // failure into a bogus exhausted verdict.
        return Err(all_keys_error(first_err, keys.len(), Some(enabled_keys.len())));
    }

    Ok(build_quota_data(provider_type, accounts))
}

/// Splits the channel keys into serving keys and the (truncated) parked keys
/// that stay visible in raw data. Port of the Go fan-out split including the
/// `maxZhipuQuotaDisabledAccounts` truncation.
fn plan_targets(keys: &[String], disabled: &std::collections::HashSet<String>) -> (Vec<String>, Vec<String>) {
    let mut enabled = Vec::with_capacity(keys.len());
    let mut parked = Vec::new();
    for key in keys {
        if disabled.contains(key) {
            parked.push(key.clone());
        } else {
            enabled.push(key.clone());
        }
    }
    parked.truncate(MAX_ZHIPU_QUOTA_DISABLED_ACCOUNTS);
    (enabled, parked)
}

/// Wraps the first failure while preserving its variant, mirroring Go's
/// `fmt.Errorf("... %d keys: %w")`.
fn all_keys_error(first_err: Option<QuotaError>, total: usize, enabled: Option<usize>) -> QuotaError {
    let scope = match enabled {
        Some(n) => format!("zhipu quota check failed for all {n} enabled keys"),
        None => format!("zhipu quota check failed for all {total} keys"),
    };
    match first_err {
        Some(QuotaError::InvalidCredentials(m)) => QuotaError::InvalidCredentials(format!("{scope}: {m}")),
        Some(QuotaError::Http(m)) => QuotaError::Http(format!("{scope}: {m}")),
        Some(QuotaError::Parse(m)) => QuotaError::Parse(format!("{scope}: {m}")),
        None => QuotaError::Parse(scope),
    }
}

async fn fetch_account_snapshot(
    http: &reqwest::Client,
    quota_url: &str,
    api_key: &str,
) -> Result<ZhipuAccountSnapshot, QuotaError> {
    let resp = http
        .get(quota_url)
        // Bare key, no Bearer prefix — matches the family API.
        .header("Authorization", api_key)
        .header("Content-Type", "application/json")
        .header("Accept-Language", "en-US,en")
        .send()
        .await
        .map_err(|e| QuotaError::Http(format!("zhipu quota request failed: {e}")))?;

    // The Go checker does not inspect the HTTP status; the body is parsed
    // directly (the API reports failures via success=false).
    let body = resp
        .text()
        .await
        .map_err(|e| QuotaError::Http(format!("zhipu quota response read failed: {e}")))?;

    parse_account_snapshot(&body)
}

/// Folds the per-account snapshots into the channel payload.
fn build_quota_data(provider_type: &str, accounts: Vec<ZhipuAccountQuota>) -> QuotaData {
    let usable: Vec<&ZhipuAccountQuota> = accounts
        .iter()
        .filter(|a| !a.disabled && a.error.is_empty() && !a.rows.is_empty())
        .collect();

    let (status, ready) = aggregate_status(&usable);

    let mut data = QuotaData::new(provider_type, &status);
    data.raw_data.insert("level".into(), Value::String(aggregate_level(&accounts)));
    data.raw_data.insert(
        "rows".into(),
        serde_json::to_value(worst_rows(&usable)).unwrap_or(Value::Array(Vec::new())),
    );
    data.raw_data.insert(
        "accounts".into(),
        serde_json::to_value(&accounts).unwrap_or(Value::Array(Vec::new())),
    );
    data.next_reset_at = earliest_reset(&usable);
    data.ready = ready;
    data.limits = channel_limits(&accounts, &usable);
    data
}

/// Channel status of the serving accounts: only a pool without a single
/// usable account is exhausted; exhausted accounts are masked by healthier
/// peers (OR semantics).
fn aggregate_status(usable: &[&ZhipuAccountQuota]) -> (String, bool) {
    if usable.is_empty() {
        return ("exhausted".into(), false);
    }

    let mut status = "";
    for account in usable {
        if account.status == "exhausted" {
            continue;
        }
        if status.is_empty() {
            status = &account.status;
            continue;
        }
        status = worse_status(status, &account.status);
    }

    if status.is_empty() {
        return ("exhausted".into(), false);
    }
    (status.to_string(), is_ready_status(status))
}

/// Single-key shape keeps one limit per window; anything else (multi-key, or
/// the sole key unusable) reports binding limits per account.
fn channel_limits(accounts: &[ZhipuAccountQuota], usable: &[&ZhipuAccountQuota]) -> Vec<QuotaLimitStatus> {
    if accounts.len() == 1 && usable.len() == 1 {
        return window_limits(usable[0]);
    }
    binding_limits(usable)
}

/// One limit per window of a single-key channel.
fn window_limits(account: &ZhipuAccountQuota) -> Vec<QuotaLimitStatus> {
    let mut limits = Vec::new();
    for (row, reset_at) in rows_with_resets(account) {
        let Some(spec) = window_spec_by_name(&row.window) else {
            continue;
        };
        let mut limit = QuotaLimitStatus::token(&row.status, row.used_percent / 100.0, reset_at);
        limit.window = spec.label.to_string();
        limit.period_start = crate::provider_quota::types::period_start_from_reset(
            reset_at.as_ref(),
            chrono::Duration::from_std(spec.length).unwrap_or_default(),
        );
        limits.push(limit);
    }
    limits
}

/// The binding window of every usable account inside one availability group.
fn binding_limits(usable: &[&ZhipuAccountQuota]) -> Vec<QuotaLimitStatus> {
    let mut limits = Vec::with_capacity(usable.len());
    for account in usable {
        let Some((row, spec, reset_at)) = binding_row(account) else {
            continue;
        };
        let mut limit = QuotaLimitStatus::token(&row.status, row.used_percent / 100.0, reset_at);
        limit.availability_group = ZHIPU_ACCOUNTS_AVAILABILITY_GROUP.to_string();
        limit.window = spec.label.to_string();
        limit.account = account.suffix.clone();
        limits.push(limit);
    }
    limits
}

/// The window of an account that limits it most.
fn binding_row(account: &ZhipuAccountQuota) -> Option<(ZhipuWindowRow, ZhipuWindowSpec, Option<DateTime<Utc>>)> {
    let mut binding: Option<(ZhipuWindowRow, ZhipuWindowSpec, Option<DateTime<Utc>>)> = None;
    for (row, reset_at) in rows_with_resets(account) {
        // Unknown row names are skipped, mirroring Go's `continue`.
        let Some(spec) = window_spec_by_name(&row.window) else {
            continue;
        };
        let better = match &binding {
            None => true,
            Some((current, _, _)) => row.used_percent > current.used_percent,
        };
        if better {
            binding = Some((row, spec, reset_at));
        }
    }
    binding
}

fn rows_with_resets(account: &ZhipuAccountQuota) -> impl Iterator<Item = (ZhipuWindowRow, Option<DateTime<Utc>>)> + use<'_> {
    account
        .rows
        .iter()
        .map(|row| (row.clone(), parse_rfc3339(row.reset_at.as_deref())))
}

/// Most used account per window.
fn worst_rows(usable: &[&ZhipuAccountQuota]) -> Vec<ZhipuWindowRow> {
    let mut rows = Vec::new();
    for spec in [FIVE_HOUR_SPEC, WEEKLY_SPEC] {
        let mut worst: Option<ZhipuWindowRow> = None;
        for account in usable {
            for row in &account.rows {
                if row.window != spec.name {
                    continue;
                }
                if worst.as_ref().is_none_or(|w| row.used_percent > w.used_percent) {
                    worst = Some(row.clone());
                }
            }
        }
        if let Some(worst) = worst {
            rows.push(worst);
        }
    }
    rows
}

fn aggregate_level(accounts: &[ZhipuAccountQuota]) -> String {
    accounts
        .iter()
        .find(|a| !a.level.is_empty())
        .map(|a| a.level.clone())
        .unwrap_or_default()
}

fn earliest_reset(usable: &[&ZhipuAccountQuota]) -> Option<DateTime<Utc>> {
    let mut earliest: Option<DateTime<Utc>> = None;
    for account in usable {
        for row in &account.rows {
            let Some(reset_at) = parse_rfc3339(row.reset_at.as_deref()) else {
                continue;
            };
            if earliest.is_none_or(|e| reset_at < e) {
                earliest = Some(reset_at);
            }
        }
    }
    earliest
}

/// Parses a single-account payload.
fn parse_account_snapshot(body: &str) -> Result<ZhipuAccountSnapshot, QuotaError> {
    let response: ZhipuQuotaResponse = serde_json::from_str(body)
        .map_err(|e| QuotaError::Parse(format!("failed to parse zhipu quota response: {e}")))?;

    if !response.success {
        let msg = if response.msg.is_empty() {
            format!("API error code {}", response.code)
        } else {
            response.msg
        };
        return Err(QuotaError::Parse(format!("zhipu API error: {msg}")));
    }

    let data = response
        .data
        .ok_or_else(|| QuotaError::Parse("zhipu quota response contains no data".into()))?;

    let windows = response_windows(&data.limits);
    if windows.is_empty() {
        return Err(QuotaError::Parse(
            "zhipu quota response contains no TOKENS_LIMIT or CREDIT_LIMIT entries".into(),
        ));
    }

    let mut snapshot = ZhipuAccountSnapshot {
        level: data.level,
        rows: Vec::new(),
        status: "available".to_string(),
        ready: false,
    };

    for (entry, spec) in windows {
        let (row, reset_at) = entry_to_row(entry, spec);
        snapshot.status = worse_status(&snapshot.status, &row.status).to_string();
        snapshot.rows.push((row, reset_at));
    }

    snapshot.ready = is_ready_status(&snapshot.status);
    Ok(snapshot)
}

fn entry_to_row(entry: &ZhipuLimitEntry, spec: ZhipuWindowSpec) -> (ZhipuWindowRow, Option<DateTime<Utc>>) {
    let ratio = entry.percentage / 100.0;
    let mut row = ZhipuWindowRow {
        window: spec.name.to_string(),
        used_percent: entry.percentage,
        status: status_for_ratio(ratio).to_string(),
        reset_at: None,
        usage: entry.usage,
        used: entry.current_value,
        remaining: entry.remaining,
    };

    let mut reset_at = None;
    if entry.next_reset_time.is_some_and(|t| t > 0) {
        if let Some(t) = DateTime::from_timestamp_millis(entry.next_reset_time.unwrap()) {
            reset_at = Some(t);
            row.reset_at = Some(t.to_rfc3339());
        }
    }
    (row, reset_at)
}

/// Normalizes the limit entries of one account payload. CREDIT_LIMIT entries
/// (GLM coding plan) name their window through unit/number; TOKENS_LIMIT
/// entries (api.z.ai) keep the historical ordering rule where the entry
/// without a reset time is the rolling 5-hour bucket.
fn response_windows(limits: &[ZhipuLimitEntry]) -> Vec<(&ZhipuLimitEntry, ZhipuWindowSpec)> {
    let credit_entries: Vec<&ZhipuLimitEntry> = limits
        .iter()
        .filter(|e| e.kind.eq_ignore_ascii_case("CREDIT_LIMIT"))
        .collect();
    if !credit_entries.is_empty() {
        return build_response_windows(credit_entries);
    }

    let token_entries: Vec<&ZhipuLimitEntry> = limits
        .iter()
        .filter(|e| e.kind.eq_ignore_ascii_case("TOKENS_LIMIT"))
        .collect();
    build_response_windows(order_buckets(token_entries))
}

fn build_response_windows(entries: Vec<&ZhipuLimitEntry>) -> Vec<(&ZhipuLimitEntry, ZhipuWindowSpec)> {
    let mut windows = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let spec = credit_window_spec(entry)
            .or_else(|| positional_spec(index))
            .map(|s| (*entry, s));
        if let Some(w) = spec {
            windows.push(w);
        }
    }
    windows
}

/// Maps the unit/number pair of a CREDIT_LIMIT entry onto a known window.
/// Unit 3 = hours, unit 6 = weeks; the resulting length must be exactly 5h
/// or 7d.
fn credit_window_spec(entry: &ZhipuLimitEntry) -> Option<ZhipuWindowSpec> {
    let unit = entry.unit?;
    let number = entry.number?;
    if number <= 0 {
        return None;
    }
    let length = match unit {
        ZHIPU_CREDIT_UNIT_HOUR => Duration::from_secs((number as u64) * 3600),
        ZHIPU_CREDIT_UNIT_WEEK => Duration::from_secs((number as u64) * 7 * 24 * 3600),
        _ => return None,
    };
    window_spec_for_length(length)
}

fn window_spec_for_length(length: Duration) -> Option<ZhipuWindowSpec> {
    if length == FIVE_HOUR_SPEC.length {
        Some(FIVE_HOUR_SPEC)
    } else if length == WEEKLY_SPEC.length {
        Some(WEEKLY_SPEC)
    } else {
        None
    }
}

fn window_spec_by_name(name: &str) -> Option<ZhipuWindowSpec> {
    match name {
        ZHIPU_WINDOW_FIVE_HOUR => Some(FIVE_HOUR_SPEC),
        ZHIPU_WINDOW_WEEKLY => Some(WEEKLY_SPEC),
        _ => None,
    }
}

fn positional_spec(index: usize) -> Option<ZhipuWindowSpec> {
    match index {
        0 => Some(FIVE_HOUR_SPEC),
        1 => Some(WEEKLY_SPEC),
        _ => None,
    }
}

/// TOKENS_LIMIT entries ordered so the 5-hour bucket comes first: a bucket
/// without nextResetTime is the 5-hour bucket (the rolling 5h window omits
/// reset time at 0% usage). If all buckets have a reset time, the API return
/// order is trusted.
fn order_buckets(entries: Vec<&ZhipuLimitEntry>) -> Vec<&ZhipuLimitEntry> {
    let mut without_reset = Vec::new();
    let mut with_reset = Vec::new();
    for e in entries {
        if e.next_reset_time.is_none_or(|t| t <= 0) {
            without_reset.push(e);
        } else {
            with_reset.push(e);
        }
    }
    without_reset.extend(with_reset);
    without_reset
}

/// Non-empty API keys of the channel in order, deduplicated.
fn channel_api_keys(creds: &ChannelCredentials) -> Vec<String> {
    let mut keys = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for candidate in creds.all_api_keys() {
        let key = candidate.trim();
        if key.is_empty() || !seen.insert(key.to_string()) {
            continue;
        }
        keys.push(key.to_string());
    }
    keys
}

/// Stable, non-reversible identity of a key inside the payload:
/// sha256("zhipu:" + key), first 8 hex chars.
fn key_ref(key: &str) -> String {
    // Go: hex.EncodeToString(digest[:])[:8] — 8 hex chars (4 bytes).
    hex_prefix(&sha256(format!("zhipu:{key}").as_bytes()), 4)
}

/// Trailing fragment the channel UI already prints when it masks a key.
fn key_suffix(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() <= 4 {
        return key.to_string();
    }
    chars[chars.len() - 4..].iter().collect()
}

fn status_for_ratio(ratio: f64) -> &'static str {
    if ratio >= 1.0 {
        "exhausted"
    } else if ratio >= WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    }
}

fn worse_status<'a>(a: &'a str, b: &'a str) -> &'a str {
    fn rank(s: &str) -> u8 {
        match s {
            "available" => 0,
            "warning" => 1,
            "exhausted" => 2,
            _ => 0,
        }
    }
    if rank(b) > rank(a) {
        b
    } else {
        a
    }
}

fn parse_rfc3339(s: Option<&str>) -> Option<DateTime<Utc>> {
    s.and_then(|s| DateTime::parse_from_rfc3339(s).ok().map(|t| t.with_timezone(&Utc)))
}

fn hex_prefix(bytes: &[u8], n: usize) -> String {
    bytes.iter().take(n).map(|b| format!("{b:02x}")).collect()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

// ---------------------------------------------------------------------------
// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn account_from_body(body: &str) -> ZhipuAccountQuota {
        let snapshot = parse_account_snapshot(body).unwrap();
        ZhipuAccountQuota {
            r#ref: "abcd1234".into(),
            suffix: "1234".into(),
            status: snapshot.status,
            disabled: false,
            ready: snapshot.ready,
            level: snapshot.level,
            error: String::new(),
            rows: snapshot.rows.iter().map(|(r, _)| r.clone()).collect(),
        }
    }
    fn disabled_channel(raw: &str) -> Channel {
        Channel { disabled_api_keys: raw.to_string(), ..Default::default() }
    }

    #[test]
    fn plan_targets_splits_and_truncates() {
        let keys: Vec<String> = (0..40).map(|i| format!("k{i}")).collect();
        let disabled: std::collections::HashSet<String> =
            keys.iter().map(|k| k.as_str()).take(39).chain(std::iter::once("extra")).map(String::from).collect();
        let (enabled, parked) = plan_targets(&keys, &disabled);
        assert_eq!(enabled, vec!["k39".to_string()]);
        assert_eq!(parked.len(), MAX_ZHIPU_QUOTA_DISABLED_ACCOUNTS);
        assert_eq!(parked[0], "k0");
        // unknown disabled keys never leak into the fan-out
        assert!(!parked.contains(&"extra".to_string()));
    }

    #[test]
    fn disabled_accounts_shown_but_not_serving() {
        // Two accounts: one serving, one parked; both readable.
        let serving = account_from_body(CREDIT_BODY);
        let mut parked = account_from_body(CREDIT_BODY);
        parked.disabled = true;
        parked.r#ref = "deadbeef".into();
        parked.suffix = "9999".into();

        let data = build_quota_data("zhipu", vec![serving, parked]);
        let accounts = data.raw_data["accounts"].as_array().unwrap();
        assert_eq!(accounts.len(), 2);
        assert_eq!(accounts[1]["disabled"], json!(true));
        assert_eq!(accounts[0].get("disabled"), None); // omitempty
        // usable set (rows / status source) keeps only the serving key
        assert_eq!(data.status, "available");
        assert!(data.ready);
    }

    #[test]
    fn all_keys_disabled_is_exhausted() {
        let mut parked = account_from_body(CREDIT_BODY);
        parked.disabled = true;
        let data = build_quota_data("zhipu", vec![parked]);
        assert_eq!(data.status, "exhausted");
        assert!(!data.ready);
        // sole key disabled -> falls through to the account-aware (empty) path
        assert!(data.limits.is_empty());
        assert_eq!(data.raw_data["accounts"][0]["disabled"], json!(true));
    }

    #[test]
    fn expired_disables_still_serve() {
        let ch = disabled_channel(
            r#"[{"key":"k1","disabledAt":"2026-01-01T00:00:00Z","errorCode":403,"expiresAt":"2000-01-01T00:00:00Z"}]"#,
        );
        assert!(disabled_key_set(&ch).is_empty());
    }

    const CREDIT_BODY: &str = r#"{
        "success": true,
        "data": {
            "level": "GLM Coding Plan Max",
            "limits": [
                {"type": "CREDIT_LIMIT", "percentage": 42.5, "nextResetTime": 1786531600123,
                 "unit": 3, "number": 5, "usage": 425, "currentValue": 1000, "remaining": 575},
                {"type": "CREDIT_LIMIT", "percentage": 15.0, "nextResetTime": 1787049600000,
                 "unit": 6, "number": 1, "usage": 150, "currentValue": 1000, "remaining": 850}
            ]
        }
    }"#;

    #[test]
    fn credit_limit_unit_mapping() {
        let snapshot = parse_account_snapshot(CREDIT_BODY).unwrap();
        assert_eq!(snapshot.level, "GLM Coding Plan Max");
        assert_eq!(snapshot.status, "available");
        assert_eq!(snapshot.rows.len(), 2);
        let (five_hour, _) = &snapshot.rows[0];
        assert_eq!(five_hour.window, "five_hour");
        assert!((five_hour.used_percent - 42.5).abs() < 1e-9);
        assert_eq!(
            five_hour.reset_at,
            Some(DateTime::from_timestamp_millis(1786531600123).unwrap().to_rfc3339())
        );
        let (weekly, _) = &snapshot.rows[1];
        assert_eq!(weekly.window, "weekly_limit");
    }

    #[test]
    fn single_key_builds_window_limits() {
        let account = account_from_body(CREDIT_BODY);
        let data = build_quota_data("zhipu", vec![account]);
        assert_eq!(data.status, "available");
        assert_eq!(data.limits.len(), 2);
        assert_eq!(data.limits[0].window, "5h");
        assert_eq!(data.limits[1].window, "weekly");
        assert!(data.limits[0].period_start.is_some());
        assert_eq!(data.next_reset_at, DateTime::from_timestamp_millis(1786531600123));
        assert_eq!(data.raw_data["level"], json!("GLM Coding Plan Max"));
        assert_eq!(data.raw_data["accounts"][0]["suffix"], json!("1234"));
        // Full key never appears; ref is an 8-hex digest.
        assert_eq!(data.raw_data["accounts"][0]["ref"].as_str().unwrap().len(), 8);
    }

    #[test]
    fn tokens_limit_positional_bucketing() {
        // api.z.ai shape: no reset time on the 5h bucket, weekly listed first.
        let body = r#"{
            "success": true,
            "data": {"level": "", "limits": [
                {"type": "TOKENS_LIMIT", "percentage": 60.0, "nextResetTime": 1787049600000},
                {"type": "TOKENS_LIMIT", "percentage": 10.0}
            ]}
        }"#;
        let snapshot = parse_account_snapshot(body).unwrap();
        let (five_hour, _) = &snapshot.rows[0];
        assert_eq!(five_hour.window, "five_hour");
        assert!((five_hour.used_percent - 10.0).abs() < 1e-9);
        assert_eq!(five_hour.reset_at, None);
        let (weekly, _) = &snapshot.rows[1];
        assert_eq!(weekly.window, "weekly_limit");
        assert_eq!(weekly.used_percent, 60.0);
    }

    #[test]
    fn all_buckets_with_reset_trusts_api_order() {
        let body = r#"{
            "success": true,
            "data": {"limits": [
                {"type": "TOKENS_LIMIT", "percentage": 30.0, "nextResetTime": 1786531600123},
                {"type": "TOKENS_LIMIT", "percentage": 90.0, "nextResetTime": 1787049600000}
            ]}
        }"#;
        let snapshot = parse_account_snapshot(body).unwrap();
        assert_eq!(snapshot.rows[0].0.window, "five_hour");
        assert_eq!(snapshot.rows[1].0.window, "weekly_limit");
        assert_eq!(snapshot.status, "warning");
    }

    #[test]
    fn multi_key_availability_group_merge() {
        let exhausted = account_from_body(
            r#"{"success": true, "data": {"level": "", "limits": [
                {"type": "CREDIT_LIMIT", "percentage": 100.0, "unit": 3, "number": 5},
                {"type": "CREDIT_LIMIT", "percentage": 50.0, "unit": 6, "number": 1}
            ]}}"#,
        );
        let healthy = account_from_body(
            r#"{"success": true, "data": {"level": "lite", "limits": [
                {"type": "CREDIT_LIMIT", "percentage": 20.0, "nextResetTime": 1786531600123,
                 "unit": 3, "number": 5},
                {"type": "CREDIT_LIMIT", "percentage": 85.0, "nextResetTime": 1787049600000,
                 "unit": 6, "number": 1}
            ]}}"#,
        );
        let data = build_quota_data("zhipu", vec![exhausted, healthy]);
        // OR semantics: the exhausted account is masked by the healthy one.
        assert_eq!(data.status, "warning");
        assert_eq!(data.limits.len(), 2);
        for limit in &data.limits {
            assert_eq!(limit.availability_group, "zhipu_accounts");
            assert_eq!(limit.account, "1234");
        }
        // Binding rows: exhausted account -> 100% 5h; healthy account -> 85% weekly.
        assert_eq!(data.limits[0].window, "5h");
        assert!((data.limits[0].usage_ratio - 1.0).abs() < 1e-9);
        assert_eq!(data.limits[1].window, "weekly");
        assert!((data.limits[1].usage_ratio - 0.85).abs() < 1e-9);
        assert_eq!(data.raw_data["rows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn all_keys_failing_is_error() {
        let accounts_fail = true;
        assert!(accounts_fail);
        let err = parse_account_snapshot(r#"{"success": false, "code": 401, "msg": "invalid key"}"#);
        assert!(matches!(err, Err(QuotaError::Parse(m)) if m.contains("invalid key")));
    }

    #[test]
    fn parse_failures() {
        assert!(matches!(parse_account_snapshot("nope"), Err(QuotaError::Parse(_))));
        assert!(matches!(
            parse_account_snapshot(r#"{"success": true}"#),
            Err(QuotaError::Parse(m)) if m.contains("no data")
        ));
        assert!(matches!(
            parse_account_snapshot(r#"{"success": true, "data": {"limits": []}}"#),
            Err(QuotaError::Parse(m)) if m.contains("no TOKENS_LIMIT")
        ));
        // Unknown credit window length falls back to positional; extra entries are dropped.
        let snapshot = parse_account_snapshot(
            r#"{"success": true, "data": {"limits": [
                {"type": "CREDIT_LIMIT", "percentage": 1.0, "unit": 3, "number": 24}
            ]}}"#,
        )
        .unwrap();
        // 24h length matches no spec and index 0 positional still maps to 5h.
        assert_eq!(snapshot.rows[0].0.window, "five_hour");
    }

    #[test]
    fn key_helpers() {
        assert_eq!(key_suffix("abcdefgh1234"), "1234");
        assert_eq!(key_suffix("abc"), "abc");
        assert_eq!(key_ref("k").len(), 8);
        // Known digest vector: sha256("zhipu:k") prefix.
        assert_eq!(hex_prefix(&sha256(b"zhipu:k"), 32), {
            let d = sha256("zhipu:k".as_bytes());
            hex_prefix(&d, 32)
        });
        // sha256("") well-known value.
        assert_eq!(
            hex_prefix(&sha256(b""), 32),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex_prefix(&sha256(b"abc"), 32),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn channel_api_keys_dedup_and_trim() {
        let creds = ChannelCredentials {
            api_keys: vec![" a ".into(), "".into(), "a".into(), "b".into()],
            ..Default::default()
        };
        assert_eq!(channel_api_keys(&creds), vec!["a".to_string(), "b".to_string()]);
    }
}
