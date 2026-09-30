//! OAuth acquisition + refresh for the claudecode, codex and github_copilot
//! channels (port of axonhub's OAuth flows).
//!
//! Wire conventions (axonhub `OAuthCredentials`):
//! `channel.credentials` = `{"oauth": {"client_id"?, "access_token",
//! "refresh_token"?, "id_token"?, "expires_at"? (RFC3339), "token_type"?,
//! "scopes"?}}`; copilot instead stores the GitHub token as plain `api_key`.
//!
//! Process-local state (session store, refresh single-flight, copilot token
//! cache) follows the `keystate.rs` `LazyLock<Mutex<...>>` pattern.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use parking_lot::Mutex;
use anyhow::{anyhow, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::storage::{Channel, ChannelRepo, Db};

// ---------- provider constants (axonhub claudecode/codex constants.go, copilot.go) ----------

pub const CLAUDE_AUTHORIZE_URL: &str = "https://claude.ai/oauth/authorize";
pub const CLAUDE_TOKEN_URL: &str = "https://api.anthropic.com/v1/oauth/token";
pub const CLAUDE_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
pub const CLAUDE_REDIRECT_URI: &str = "http://localhost:54545/callback";
pub const CLAUDE_SCOPE: &str = "org:create_api_key user:profile user:inference";
pub const CLAUDE_USER_AGENT: &str = "claude-cli/2.1.170 (external, cli)";

pub const CODEX_AUTHORIZE_URL: &str = "https://auth.openai.com/oauth/authorize";
pub const CODEX_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
pub const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const CODEX_REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
pub const CODEX_SCOPE: &str = "openid profile email offline_access";

pub const GITHUB_DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
pub const GITHUB_DEVICE_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
pub const COPILOT_TOKEN_URL: &str = "https://api.github.com/copilot_internal/v2/token";

fn copilot_client_id() -> String {
    std::env::var("GITHUB_COPILOT_CLIENT_ID")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "Iv1.b507a08c87ecfe98".to_string())
}

// ---------- PKCE ----------

/// n crypto-random bytes assembled from `Uuid::new_v4` payloads (16 random
/// bytes each, v4 = 122 random bits per UUID; 4 UUIDs give the 64/32 bytes
/// PKCE needs). Good enough for code_verifier/state entropy.
fn random_bytes(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        out.extend_from_slice(Uuid::new_v4().as_bytes());
    }
    out.truncate(n);
    out
}

fn b64url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn pkce_pair() -> (String, String) {
    // verifier = base64url_nopad(64 random bytes)
    let verifier = b64url(&random_bytes(64));
    let challenge = pkce_challenge(&verifier);
    (verifier, challenge)
}

/// challenge = base64url_nopad(sha256(verifier_ascii))
pub fn pkce_challenge(verifier: &str) -> String {
    b64url(&Sha256::digest(verifier.as_bytes()))
}

pub fn new_state() -> String {
    // state = base64url_nopad(32 random bytes)
    b64url(&random_bytes(32))
}

/// Minimal percent-encoding of everything outside RFC 3986 unreserved.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn query(params: &[(&str, &str)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// claudecode authorize URL (PKCE S256).
pub fn claude_auth_url(state: &str, code_challenge: &str) -> String {
    format!(
        "{}?{}",
        CLAUDE_AUTHORIZE_URL,
        query(&[
            ("response_type", "code"),
            ("client_id", CLAUDE_CLIENT_ID),
            ("redirect_uri", CLAUDE_REDIRECT_URI),
            ("scope", CLAUDE_SCOPE),
            ("code_challenge", code_challenge),
            ("code_challenge_method", "S256"),
            ("state", state),
        ])
    )
}

/// codex authorize URL (PKCE S256 + codex-specific flags).
pub fn codex_auth_url(state: &str, code_challenge: &str) -> String {
    format!(
        "{}?{}",
        CODEX_AUTHORIZE_URL,
        query(&[
            ("response_type", "code"),
            ("client_id", CODEX_CLIENT_ID),
            ("redirect_uri", CODEX_REDIRECT_URI),
            ("scope", CODEX_SCOPE),
            ("code_challenge", code_challenge),
            ("code_challenge_method", "S256"),
            ("state", state),
            ("id_token_add_organizations", "true"),
            ("codex_cli_simplified_flow", "true"),
        ])
    )
}

// ---------- session store ----------

#[derive(Debug, Clone)]
pub struct DeviceSession {
    pub device_code: String,
    pub client_id: String,
    pub interval: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OAuthSession {
    pub code_verifier: Option<String>,
    pub device: Option<DeviceSession>,
    pub created_at: DateTime<Utc>,
}

static SESSIONS: LazyLock<Mutex<HashMap<String, OAuthSession>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn session_ttl(created_at: DateTime<Utc>, device_ttl: Option<i64>) -> DateTime<Utc> {
    match device_ttl {
        // copilot: min(expires_in, 15min)
        Some(secs) => created_at + Duration::seconds(secs.min(15 * 60)),
        None => created_at + Duration::minutes(10),
    }
}
fn session_deadline(s: &OAuthSession) -> DateTime<Utc> {
    session_ttl(
        s.created_at,
        s.device.as_ref().map(|d| (d.expires_at - s.created_at).num_seconds()),
    )
}

/// Lazily evicts expired sessions, then inserts the new one.
pub fn put_session(session_id: &str, session: OAuthSession) {
    let mut map = SESSIONS.lock();
    map.retain(|_, s| session_deadline(s) > Utc::now());
    map.insert(session_id.to_string(), session);
}

pub fn take_session(session_id: &str) -> Option<OAuthSession> {
    let mut map = SESSIONS.lock();
    map.retain(|_, s| session_deadline(s) > Utc::now());
    map.remove(session_id)
}

pub fn get_session(session_id: &str) -> Option<OAuthSession> {
    let mut map = SESSIONS.lock();
    map.retain(|_, s| session_deadline(s) > Utc::now());
    map.get(session_id).cloned()
}

pub fn delete_session(session_id: &str) {
    SESSIONS.lock().remove(session_id);
}

// ---------- callback parsing (pure) ----------

/// Extracts `code` + `state` params from a callback URL. `state` lives in the
/// fragment first (claude.ai appends `#state=...`), falling back to the query
/// (codex). Returns Err when `state` is missing or != `session_id`, or no code.
pub fn parse_callback_url(callback_url: &str, session_id: &str) -> Result<String, String> {
    let (base, fragment) = match callback_url.split_once('#') {
        Some((b, f)) => (b, Some(f)),
        None => (callback_url, None),
    };
    let query_part = base.split_once('?').map(|(_, q)| q).unwrap_or("");
    let get_param = |part: &str, key: &str| -> Option<String> {
        part.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == key).then(|| v.to_string())
        })
    };
    let state = fragment
        .and_then(|f| get_param(f, "state"))
        .or_else(|| get_param(query_part, "state"))
        .ok_or_else(|| "callback missing state".to_string())?;
    if state != session_id {
        return Err(format!("state mismatch"));
    }
    get_param(query_part, "code").ok_or_else(|| "callback missing code".to_string())
}

// ---------- token response parsing (pure) ----------

#[derive(Debug, Clone, PartialEq)]
pub struct TokenSet {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub id_token: Option<String>,
}

#[derive(Deserialize)]
struct RawTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    id_token: Option<String>,
}

/// axonhub `TokenResponse`: `access_token` required, `refresh_token` optional
/// (KEEP the previous one when omitted), `expires_in` -> absolute `expires_at`.
pub fn parse_token_response(v: &Value, old_refresh_token: Option<&str>, now: DateTime<Utc>) -> Result<TokenSet, String> {
    let raw: RawTokenResponse =
        serde_json::from_value(v.clone()).map_err(|e| format!("token response: {e}"))?;
    if raw.access_token.is_empty() {
        return Err("token response missing access_token".to_string());
    }
    Ok(TokenSet {
        access_token: raw.access_token,
        refresh_token: raw.refresh_token.or_else(|| old_refresh_token.map(str::to_string)),
        expires_at: raw.expires_in.map(|s| now + Duration::seconds(s)),
        id_token: raw.id_token,
    })
}

/// Serializes a TokenSet into the `credentials.oauth` JSON shape.
pub fn token_set_to_oauth_json(t: &TokenSet, client_id: Option<&str>) -> Value {
    let mut o = json!({"access_token": t.access_token});
    if let Some(r) = &t.refresh_token {
        o["refresh_token"] = json!(r);
    }
    if let Some(e) = t.expires_at {
        o["expires_at"] = json!(e.to_rfc3339());
    }
    if let Some(i) = &t.id_token {
        o["id_token"] = json!(i);
    }
    if let Some(c) = client_id {
        o["client_id"] = json!(c);
    }
    o
}

// ---------- codex auth.json decode (pure) ----------

/// Decodes the codex CLI `auth.json`:
/// `{"tokens": {"access_token", "refresh_token"?, "id_token"?}, "last_refresh": RFC3339}`.
/// `expires_at` = `last_refresh + 1h`, or `now + 1h` when `last_refresh` is
/// missing but a refresh token exists (the CLI refreshes hourly).
pub fn decode_codex_auth_json(s: &str, now: DateTime<Utc>) -> Result<TokenSet, String> {
    #[derive(Deserialize)]
    struct RawAuth {
        tokens: RawTokens,
        last_refresh: Option<String>,
    }
    #[derive(Deserialize)]
    struct RawTokens {
        access_token: String,
        refresh_token: Option<String>,
        id_token: Option<String>,
    }
    let raw: RawAuth = serde_json::from_str(s).map_err(|e| format!("auth.json: {e}"))?;
    let expires_at = raw
        .last_refresh
        .as_deref()
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc) + Duration::hours(1))
        .or_else(|| {
            raw.tokens
                .refresh_token
                .is_some()
                .then(|| now + Duration::hours(1))
        });
    Ok(TokenSet {
        access_token: raw.tokens.access_token,
        refresh_token: raw.tokens.refresh_token,
        expires_at,
        id_token: raw.tokens.id_token,
    })
}

// ---------- credentials merge into channel (pure) ----------

/// Sets/overwrites `credentials.oauth` on the channel's credentials JSON.
pub fn merge_oauth_into(channel: &mut Channel, oauth: &Value) {
    let mut creds: Value = serde_json::from_str(&channel.credentials).unwrap_or(json!({}));
    creds["oauth"] = oauth.clone();
    channel.credentials = creds.to_string();
}

/// Sets `credentials.api_key` (copilot attach: the GitHub token itself).
pub fn set_api_key_into(channel: &mut Channel, key: &str) {
    let mut creds: Value = serde_json::from_str(&channel.credentials).unwrap_or(json!({}));
    creds["api_key"] = json!(key);
    channel.credentials = creds.to_string();
}

// ---------- device flow (copilot) ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevicePollStatus {
    Pending,
    SlowDown,
    Expired,
    Denied,
}

/// GitHub device-flow grant_error -> poll status.
pub fn map_device_error(err: &str) -> DevicePollStatus {
    match err {
        "authorization_pending" => DevicePollStatus::Pending,
        "slow_down" => DevicePollStatus::SlowDown,
        "expired_token" => DevicePollStatus::Expired,
        "access_denied" => DevicePollStatus::Denied,
        _ => DevicePollStatus::Pending,
    }
}

/// Token endpoint bodies come back as JSON (Accept: application/json) or
/// form-encoded depending on GitHub's mood; parse both.
pub fn parse_token_payload(body: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        if v.is_object() {
            return Some(v);
        }
    }
    let mut map = serde_json::Map::new();
    for kv in body.split('&') {
        let (k, v) = kv.split_once('=')?;
        map.insert(k.to_string(), Value::String(v.to_string()));
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

/// Starts the GitHub device flow. No live call; errors propagate to the admin
/// endpoint.
pub async fn copilot_device_start(http: &reqwest::Client) -> Result<(String, DeviceSession, Value)> {
    let client_id = copilot_client_id();
    let resp = http
        .post(GITHUB_DEVICE_CODE_URL)
        .header("Accept", "application/json")
        .form(&[("client_id", client_id.as_str()), ("scope", "read:user")])
        .send()
        .await
        .map_err(|e| anyhow!("device code request: {e}"))?;
    let text = resp.text().await.unwrap_or_default();
    let v: Value = serde_json::from_str(&text).map_err(|e| anyhow!("device code response: {e}"))?;
    let device_code = v
        .get("device_code")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow!("device code response missing device_code"))?
        .to_string();
    let expires_in = v.get("expires_in").and_then(|x| x.as_i64()).unwrap_or(900);
    let interval = v.get("interval").and_then(|x| x.as_u64()).unwrap_or(5);
    let now = Utc::now();
    let session = DeviceSession {
        device_code,
        client_id,
        interval,
        expires_at: now + Duration::seconds(expires_in),
    };
    Ok((session.device_code.clone(), session, v))
}

/// One device-flow poll. Returns Ok(token) on grant, or the mapped status.
pub async fn copilot_device_poll(
    http: &reqwest::Client,
    device: &DeviceSession,
) -> Result<std::result::Result<String, DevicePollStatus>, anyhow::Error> {
    let resp = http
        .post(GITHUB_DEVICE_TOKEN_URL)
        .header("Accept", "application/json")
        .form(&[
            ("client_id", device.client_id.as_str()),
            ("device_code", device.device_code.as_str()),
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:device_code",
            ),
        ])
        .send()
        .await
        .map_err(|e| anyhow!("device token request: {e}"))?;
    let text = resp.text().await.unwrap_or_default();
    let v = parse_token_payload(&text).ok_or_else(|| anyhow!("unparseable token response"))?;
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Ok(Err(map_device_error(err)));
    }
    match v.get("access_token").and_then(|t| t.as_str()) {
        Some(t) => Ok(Ok(t.to_string())),
        None => Err(anyhow!("token response missing access_token")),
    }
}

// ---------- copilot relay token (github token -> copilot bearer) ----------

static COPILOT_TOKENS: LazyLock<Mutex<HashMap<String, (String, i64)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
/// Serializes the whole check+fetch (simple correctness over lock granularity).
static COPILOT_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn github_token_key(github_token: &str) -> String {
    b64url(&Sha256::digest(github_token.as_bytes()))
}

/// Pure cache validity: entry usable until `expires_at - 5min`.
pub fn copilot_cache_valid(entry: &(String, i64), now_unix: i64) -> bool {
    entry.1 - 5 * 60 > now_unix
}

pub fn cached_copilot_token(github_token: &str, now_unix: i64) -> Option<String> {
    let map = COPILOT_TOKENS.lock();
    let entry = map.get(&github_token_key(github_token))?;
    copilot_cache_valid(entry, now_unix).then(|| entry.0.clone())
}

pub fn cache_copilot_token(github_token: &str, token: &str, expires_at_unix: i64) {
    COPILOT_TOKENS
        .lock()
        .insert(github_token_key(github_token), (token.to_string(), expires_at_unix));
}

/// Exchanges a GitHub token for the short-lived copilot relay bearer
/// (`GET /copilot_internal/v2/token`), cached per token until expires_at-5min.
pub async fn copilot_token(http: &reqwest::Client, github_token: &str) -> Result<String> {
    let _guard = COPILOT_LOCK.lock().await;
    if let Some(t) = cached_copilot_token(github_token, Utc::now().timestamp()) {
        return Ok(t);
    }
    let resp = http
        .get(COPILOT_TOKEN_URL)
        .header("Authorization", format!("token {github_token}"))
        .header("Accept", "application/json")
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| anyhow!("copilot token request: {e}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!("copilot token exchange status {}", resp.status()));
    }
    let text = resp.text().await.unwrap_or_default();
    let v: Value =
        serde_json::from_str(&text).map_err(|e| anyhow!("copilot token response: {e}"))?;
    let token = v
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow!("copilot token response missing token"))?
        .to_string();
    let expires_at = v.get("expires_at").and_then(|t| t.as_i64()).unwrap_or(0);
    cache_copilot_token(github_token, &token, expires_at);
    Ok(token)
}

// ---------- refresh ----------

/// Channels whose refresh is currently in flight. A concurrent request for the
/// same channel returns the (possibly stale) channel unchanged rather than
/// waiting — simpler than a wait group, and the next request sees the fresh
/// token (documented deviation from a single-flight-with-wait design).
static REFRESH_IN_FLIGHT: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

fn token_url_formencoded(body: &str) -> Option<Value> {
    parse_token_payload(body)
}

async fn post_refresh(http: &reqwest::Client, channel_type: &str, client_id: &str, refresh_token: &str) -> Result<Value> {
    let resp = match channel_type {
        "claudecode" => http
            .post(CLAUDE_TOKEN_URL)
            .header("User-Agent", CLAUDE_USER_AGENT)
            .header("Content-Type", "application/json")
            .body(json!({
                "grant_type": "refresh_token",
                "client_id": client_id,
                "refresh_token": refresh_token,
            }).to_string())
            .send()
            .await
            .map_err(|e| anyhow!("claude refresh: {e}"))?,
        _ => http
            .post(CODEX_TOKEN_URL)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", client_id),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await
            .map_err(|e| anyhow!("codex refresh: {e}"))?,
    };
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("refresh status {status}: {text}"));
    }
    if channel_type == "codex" {
        token_url_formencoded(&text).ok_or_else(|| anyhow!("unparseable refresh response"))
    } else {
        serde_json::from_str(&text).map_err(|e| anyhow!("refresh response: {e}"))
    }
}

/// Refreshes `credentials.oauth` for claudecode/codex channels when the access
/// token is missing/expiring (< now+5min) and a refresh token exists; persists
/// via `ChannelRepo::update`. Any failure logs a warning and returns the
/// channel unchanged — refresh must never break the relay.
pub async fn maybe_refresh_oauth(pool: &Db, http: &reqwest::Client, channel: &Channel) -> Channel {
    if !matches!(channel.channel_type.as_str(), "claudecode" | "codex") {
        return channel.clone();
    }
    let creds = crate::provider_quota::credentials::ChannelCredentials::parse(&channel.credentials);
    let Some(oauth) = creds.oauth else {
        return channel.clone();
    };
    let Some(refresh_token) = oauth.refresh_token.clone() else {
        return channel.clone();
    };
    if let Some(expires_at) = oauth.expires_at {
        if expires_at > Utc::now() + Duration::minutes(5) {
            return channel.clone();
        }
    }
    {
        let mut inflight = REFRESH_IN_FLIGHT.lock();
        if inflight.contains(&channel.id) {
            return channel.clone();
        }
        inflight.insert(channel.id.clone());
    }
    let result = try_refresh(pool, http, channel, &refresh_token).await;
    REFRESH_IN_FLIGHT.lock().remove(&channel.id);
    match result {
        Ok(ch) => ch,
        Err(e) => {
            tracing::warn!(channel = %channel.id, "oauth refresh failed: {e}");
            channel.clone()
        }
    }
}

async fn try_refresh(
    pool: &Db,
    http: &reqwest::Client,
    channel: &Channel,
    refresh_token: &str,
) -> Result<Channel> {
    let creds = crate::provider_quota::credentials::ChannelCredentials::parse(&channel.credentials);
    let oauth = creds.oauth.ok_or_else(|| anyhow!("no oauth block"))?;
    let client_id = oauth
        .client_id
        .clone()
        .unwrap_or_else(|| match channel.channel_type.as_str() {
            "claudecode" => CLAUDE_CLIENT_ID.to_string(),
            _ => CODEX_CLIENT_ID.to_string(),
        });
    let v = post_refresh(http, &channel.channel_type, &client_id, refresh_token).await?;
    let set = parse_token_response(&v, Some(refresh_token), Utc::now())
        .map_err(|e| anyhow!("{e}"))?;
    let oauth_json = token_set_to_oauth_json(&set, Some(&client_id));
    let mut updated = channel.clone();
    merge_oauth_into(&mut updated, &oauth_json);
    ChannelRepo::update(pool, &updated).await?;
    tracing::info!(channel = %channel.id, "oauth token refreshed");
    Ok(updated)
}

// ---------- authorize-code exchange (admin flow) ----------

/// Exchanges an authorization code (claudecode: JSON body; codex: form).
pub async fn exchange_code(
    http: &reqwest::Client,
    channel_type: &str,
    code: &str,
    code_verifier: &str,
    state: &str,
) -> Result<TokenSet> {
    let (token_url, client_id, redirect_uri) = match channel_type {
        "claudecode" => (CLAUDE_TOKEN_URL, CLAUDE_CLIENT_ID, CLAUDE_REDIRECT_URI),
        _ => (CODEX_TOKEN_URL, CODEX_CLIENT_ID, CODEX_REDIRECT_URI),
    };
    let resp = if channel_type == "claudecode" {
        http.post(token_url)
            .header("User-Agent", CLAUDE_USER_AGENT)
            .header("Content-Type", "application/json")
            .body(json!({
                "grant_type": "authorization_code",
                "code": code,
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "code_verifier": code_verifier,
                "state": state,
            }).to_string())
            .send()
            .await
            .map_err(|e| anyhow!("claude exchange: {e}"))?
    } else {
        http.post(token_url)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", client_id),
                ("redirect_uri", redirect_uri),
                ("code_verifier", code_verifier),
                ("state", state),
            ])
            .send()
            .await
            .map_err(|e| anyhow!("codex exchange: {e}"))?
    };
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("exchange status {status}: {text}"));
    }
    let v = if channel_type == "codex" {
        parse_token_payload(&text).ok_or_else(|| anyhow!("unparseable exchange response"))?
    } else {
        serde_json::from_str(&text).map_err(|e| anyhow!("exchange response: {e}"))?
    };
    parse_token_response(&v, None, Utc::now()).map_err(|e| anyhow!("{e}"))
}

// ---------- tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_challenge_matches_rfc7636_vector() {
        // RFC 7636 appendix B
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assert_eq!(
            pkce_challenge(verifier),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn pkce_pair_shape() {
        let (verifier, challenge) = pkce_pair();
        // 64 bytes -> 86 base64 chars, no padding, url-safe alphabet
        assert_eq!(verifier.len(), 86);
        assert!(!verifier.contains('+') && !verifier.contains('/') && !verifier.contains('='));
        assert_eq!(challenge, pkce_challenge(&verifier));
        let state = new_state();
        assert_eq!(state.len(), 43); // 32 bytes
    }

    #[test]
    fn callback_state_fragment_first_then_query() {
        let sid = "sess123";
        // fragment state (claude.ai style)
        let url = "http://localhost:54545/callback?code=abc123&iss=https%3A%2F%2Fclaude.ai#state=sess123";
        assert_eq!(parse_callback_url(url, sid).unwrap(), "abc123");
        // query-only state (codex style)
        let url = "http://localhost:1455/auth/callback?code=c1&state=sess123";
        assert_eq!(parse_callback_url(url, sid).unwrap(), "c1");
        // fragment beats query on mismatch
        let url = "http://x/cb?code=c2&state=wrong#state=sess123";
        assert_eq!(parse_callback_url(url, sid).unwrap(), "c2");
        // mismatch -> error
        let url = "http://x/cb?code=c3&state=wrong";
        assert!(parse_callback_url(url, sid).is_err());
        // missing state / code
        assert!(parse_callback_url("http://x/cb?code=c4", sid).is_err());
        assert!(parse_callback_url("http://x/cb?state=sess123", sid).is_err());
    }

    #[test]
    fn token_response_keeps_old_refresh_token() {
        let now = Utc::now();
        // response omits refresh_token -> old one preserved
        let set = parse_token_response(
            &json!({"access_token": "at2", "expires_in": 3600}),
            Some("rt1"),
            now,
        )
        .unwrap();
        assert_eq!(set.refresh_token.as_deref(), Some("rt1"));
        assert_eq!(set.expires_at, Some(now + Duration::seconds(3600)));
        // response carries a new one -> wins
        let set = parse_token_response(
            &json!({"access_token": "at2", "refresh_token": "rt2"}),
            Some("rt1"),
            now,
        )
        .unwrap();
        assert_eq!(set.refresh_token.as_deref(), Some("rt2"));
        // missing access_token -> error
        assert!(parse_token_response(&json!({}), None, now).is_err());
    }

    #[test]
    fn codex_auth_json_decode_both_expiry_branches() {
        let now = Utc::now();
        // last_refresh present -> last_refresh + 1h
        let auth = json!({
            "tokens": {"access_token": "at", "refresh_token": "rt", "id_token": "idt"},
            "last_refresh": "2026-09-29T00:00:00Z",
        })
        .to_string();
        let set = decode_codex_auth_json(&auth, now).unwrap();
        assert_eq!(
            set.expires_at,
            Some("2026-09-29T01:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        // last_refresh missing but refresh_token present -> now + 1h
        let auth = json!({"tokens": {"access_token": "at", "refresh_token": "rt"}}).to_string();
        let set = decode_codex_auth_json(&auth, now).unwrap();
        assert_eq!(set.expires_at, Some(now + Duration::hours(1)));
        // neither -> no expiry
        let auth = json!({"tokens": {"access_token": "at"}}).to_string();
        let set = decode_codex_auth_json(&auth, now).unwrap();
        assert_eq!(set.expires_at, None);
    }

    #[test]
    fn device_error_mapping_table() {
        assert_eq!(map_device_error("authorization_pending"), DevicePollStatus::Pending);
        assert_eq!(map_device_error("slow_down"), DevicePollStatus::SlowDown);
        assert_eq!(map_device_error("expired_token"), DevicePollStatus::Expired);
        assert_eq!(map_device_error("access_denied"), DevicePollStatus::Denied);
        assert_eq!(map_device_error("whatever"), DevicePollStatus::Pending);
    }

    #[test]
    fn token_payload_json_and_form() {
        assert_eq!(
            parse_token_payload(r#"{"access_token":"t"}"#).unwrap()["access_token"],
            "t"
        );
        assert_eq!(
            parse_token_payload("access_token=t&error=").unwrap()["access_token"],
            "t"
        );
    }

    #[test]
    fn session_ttl_eviction() {
        let old = OAuthSession {
            code_verifier: Some("v".into()),
            device: None,
            created_at: Utc::now() - Duration::minutes(11),
        };
        SESSIONS.lock().insert("old".into(), old);
        put_session(
            "new",
            OAuthSession { code_verifier: None, device: None, created_at: Utc::now() },
        );
        assert!(get_session("old").is_none());
        assert!(get_session("new").is_some());
        // device session capped at 15min
        let dev = OAuthSession {
            code_verifier: None,
            device: Some(DeviceSession {
                device_code: "d".into(),
                client_id: "c".into(),
                interval: 5,
                expires_at: Utc::now() + Duration::hours(2),
            }),
            created_at: Utc::now() - Duration::minutes(16),
        };
        SESSIONS.lock().insert("dev".into(), dev);
        assert!(get_session("dev").is_none());
        assert!(take_session("new").is_some());
        assert!(get_session("new").is_none());
    }

    #[test]
    fn copilot_cache_validity() {
        let entry = ("tok".to_string(), 1000i64);
        // valid until expires_at - 5min
        assert!(copilot_cache_valid(&entry, 1000 - 5 * 60 - 1));
        assert!(!copilot_cache_valid(&entry, 1000 - 5 * 60));
        assert!(!copilot_cache_valid(&entry, 999));
    }

    #[test]
    fn copilot_cache_hit_and_purge() {
        let gh = "gh_test_token";
        cache_copilot_token(gh, "cop1", Utc::now().timestamp() + 3600);
        assert_eq!(
            cached_copilot_token(gh, Utc::now().timestamp()).as_deref(),
            Some("cop1")
        );
        // expired entry not served
        cache_copilot_token(gh, "cop2", Utc::now().timestamp());
        assert!(cached_copilot_token(gh, Utc::now().timestamp()).is_none());
        COPILOT_TOKENS.lock().remove(&github_token_key(gh));
    }

    #[test]
    fn credentials_merge_helpers() {
        let mut ch = Channel {
            id: "c".into(),
            name: "n".into(),
            channel_type: "claudecode".into(),
            base_url: String::new(),
            credentials: json!({"api_key": "keep"}).to_string(),
            disabled_api_keys: String::new(),
            supported_models: String::new(),
            model_mapping: String::new(),
            weight: 1,
            priority: 0,
            status: String::new(),
            settings: String::new(),
            created_at: String::new(),
            updated_at: String::new(),
        };
        merge_oauth_into(&mut ch, &json!({"access_token": "at"}));
        let v: Value = serde_json::from_str(&ch.credentials).unwrap();
        assert_eq!(v["oauth"]["access_token"], "at");
        assert_eq!(v["api_key"], "keep");
        set_api_key_into(&mut ch, "gh");
        let v: Value = serde_json::from_str(&ch.credentials).unwrap();
        assert_eq!(v["api_key"], "gh");
    }

    #[test]
    fn token_set_json_roundtrip() {
        let set = TokenSet {
            access_token: "at".into(),
            refresh_token: Some("rt".into()),
            expires_at: Some(Utc::now()),
            id_token: Some("id".into()),
        };
        let v = token_set_to_oauth_json(&set, Some("cid"));
        assert_eq!(v["client_id"], "cid");
        assert_eq!(v["access_token"], "at");
        // parses back through the credentials layer
        let creds = json!({"oauth": v}).to_string();
        let parsed = crate::provider_quota::credentials::ChannelCredentials::parse(&creds);
        assert_eq!(parsed.oauth.unwrap().access_token, "at");
    }
}
