//! Ollama Cloud quota checker — port of axonhub's `ollama_checker.go`.
//!
//! There is no official usage API; quota is scraped from the logged-in
//! `https://ollama.com/settings` HTML page using the browser session cookie
//! stored on `channel.settings.provider_quota.ollama.auth_cookie`. Only the
//! `__Secure-session` cookie is forwarded.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::provider_quota::credentials::{auth_cookie, ChannelCredentials};
use crate::provider_quota::types::{
    is_ready_status, period_start_from_reset, status_rank, QuotaChecker, QuotaData, QuotaError,
    QuotaLimitStatus, WINDOW_5H, WINDOW_WEEKLY,
};
use crate::storage::Channel;

pub struct OllamaChecker;

const SETTINGS_URL: &str = "https://ollama.com/settings";
const QUOTA_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const COOKIE_NAME: &str = "__Secure-session";
/// Guards against a bare name or a trivially short placeholder: only the
/// prefix (16 chars) plus a plausible token payload passes.
const MIN_COOKIE_LEN: usize = 30;
/// Bounded scan window after each usage header keyword (Go: idx+4000).
const SCAN_WINDOW: usize = 4000;

#[async_trait]
impl QuotaChecker for OllamaChecker {
    fn provider_type(&self) -> &'static str {
        "ollama"
    }

    async fn check_quota(
        &self,
        http: &reqwest::Client,
        channel: &Channel,
        _creds: &ChannelCredentials,
    ) -> Result<QuotaData, QuotaError> {
        let raw = auth_cookie(&channel.settings, "ollama")
            .ok_or_else(|| QuotaError::InvalidCredentials("channel has no Ollama quota cookie".into()))?;
        let cookie = normalize_ollama_cookie(&raw)
            .map_err(|e| QuotaError::InvalidCredentials(format!("invalid Ollama auth cookie: {}", e)))?;

        let resp = http
            .get(SETTINGS_URL)
            .header("Cookie", &cookie)
            .header("Accept", "text/html,application/xhtml+xml")
            .header("User-Agent", QUOTA_UA)
            .send()
            .await
            .map_err(|e| QuotaError::Http(format!("Ollama settings request failed: {}", e)))?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| QuotaError::Http(format!("Ollama settings request failed: {}", e)))?;
        if !(200..300).contains(&status) {
            if status == 401 || status == 403 {
                return Err(QuotaError::InvalidCredentials(format!(
                    "Ollama settings page returned {} (expired session cookie?)",
                    status
                )));
            }
            return Err(QuotaError::Http(format!("HTTP {}: {}", status, truncate(&body, 200))));
        }

        parse_response(&body)
    }
}

/// Canonicalizes a raw browser cookie capture into the exact allowlisted
/// Cookie header. Only the `__Secure-session` cookie survives; every other
/// cookie is dropped. The token must be long enough to be a plausible
/// encrypted session.
pub fn normalize_ollama_cookie(raw: &str) -> Result<String, String> {
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

    if cleaned.contains(['\r', '\n']) {
        return Err("cookie contains line breaks".into());
    }

    for part in cleaned.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, value) = match part.split_once('=') {
            Some((n, v)) => (n.trim(), v),
            None => return Err("invalid cookie segment: expected name=value".into()),
        };
        if name.is_empty() || value.is_empty() {
            return Err("invalid cookie segment: expected name=value".into());
        }

        // Only the __Secure-session cookie is forwarded.
        if !name.eq_ignore_ascii_case(COOKIE_NAME) {
            continue;
        }

        let value = value.trim();
        if value.len() < MIN_COOKIE_LEN {
            return Err("Ollama cookie is too short to be a valid session".into());
        }
        return Ok(format!("{}={}", COOKIE_NAME, value));
    }

    Err("no __Secure-session Ollama cookie found".into())
}

struct UsageWindow {
    key: &'static str,
    percent: f64,
    reset_at: Option<DateTime<Utc>>,
}

/// Parses the settings page HTML. Each meter block is a
/// "label ... data-time" pair; the parser locates the header keyword, then
/// captures the nearest `aria-label="...X% used..."` percent and
/// `data-time="RFC3339"` reset within a bounded window after it.
pub fn parse_response(html: &str) -> Result<QuotaData, QuotaError> {
    let mut windows: Vec<UsageWindow> = Vec::new();
    extract_window(html, "Session usage", WINDOW_5H, &mut windows);
    extract_window(html, "Weekly usage", WINDOW_WEEKLY, &mut windows);

    if windows.is_empty() {
        // A sign-in page means the session credential is no longer valid;
        // anything else is a genuine parse failure (markup change).
        if looks_like_login_page(html) {
            return Err(QuotaError::InvalidCredentials(
                "ollama.com/settings returned the sign-in page (expired session cookie?)".into(),
            ));
        }
        return Err(QuotaError::Parse(
            "no Ollama usage windows found in settings page (expired cookie or markup change)".into(),
        ));
    }

    let mut overall = "available";
    let mut next_reset_at: Option<DateTime<Utc>> = None;
    let mut limits = Vec::with_capacity(windows.len());
    let mut raw_windows = serde_json::Map::new();

    for w in windows {
        let usage_ratio = w.percent / 100.0;
        let status = window_status(usage_ratio);
        if status_rank(status) > status_rank(overall) {
            overall = status;
        }

        if let Some(rc) = w.reset_at {
            if next_reset_at.map_or(true, |cur| rc < cur) {
                next_reset_at = Some(rc);
            }
        }

        let mut raw = serde_json::json!({
            "usage_percent": w.percent,
            "status": status,
            "percent_remaining": 100.0 - w.percent,
        });
        if let Some(t) = &w.reset_at {
            raw["reset_time"] = serde_json::json!(t.to_rfc3339());
        }
        raw_windows.insert(w.key.to_string(), raw);

        let mut limit = QuotaLimitStatus::token(status, usage_ratio, w.reset_at);
        limit.window = w.key.to_string();
        limit.period_start = period_start_from_reset(w.reset_at.as_ref(), window_duration(w.key));
        limits.push(limit);
    }

    let mut data = QuotaData::new("ollama", overall);
    data.ready = is_ready_status(overall);
    data.next_reset_at = next_reset_at;
    data.limits = limits;
    data.raw_data.insert("windows".into(), serde_json::Value::Object(raw_windows));
    Ok(data)
}

fn extract_window(html: &str, keyword: &str, key: &'static str, out: &mut Vec<UsageWindow>) {
    let idx = match html.find(keyword) {
        Some(i) => i,
        None => return,
    };
    let end = (idx + SCAN_WINDOW).min(html.len());
    let chunk = &html[idx..end];

    // aria-label="([^"]+)"
    let label = match find_aria_label(chunk) {
        Some(l) if !l.is_empty() => l,
        _ => return,
    };
    let pct_str = match find_percent_used(label) {
        Some(p) => p,
        None => return,
    };
    let percent: f64 = match pct_str.parse() {
        Ok(v) => v,
        Err(_) => return,
    };

    // data-time="([^"]+)"
    let mut reset_at = None;
    if let Some(t) = find_data_time(chunk) {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(t) {
            reset_at = Some(parsed.with_timezone(&Utc));
        }
    }

    out.push(UsageWindow { key, percent, reset_at });
}

/// Leftmost `aria-label="..."` capture (Go regex `aria-label="([^"]+)"`).
fn find_aria_label(chunk: &str) -> Option<&str> {
    const MARKER: &str = "aria-label=\"";
    let mut search = 0;
    while let Some(rel) = chunk[search..].find(MARKER) {
        let start = search + rel + MARKER.len();
        let end = chunk[start..].find('"').map(|e| start + e);
        match end {
            Some(e) if e > start => return Some(&chunk[start..e]),
            // Unbalanced quote: continue scanning after this marker.
            _ => search = start,
        }
    }
    None
}

/// Leftmost `([0-9.]+)\s*%\s*used` capture.
fn find_percent_used(label: &str) -> Option<&str> {
    let bytes = label.as_bytes();
    let is_digit = |b: u8| b.is_ascii_digit() || b == b'.';
    let mut i = 0;
    while i < bytes.len() {
        if !is_digit(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_digit(bytes[i]) {
            i += 1;
        }
        let run = &label[start..i];
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'%' {
            j += 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if label[j..].starts_with("used") {
                return Some(run);
            }
        }
    }
    None
}

/// Leftmost `data-time="..."` capture.
fn find_data_time(chunk: &str) -> Option<&str> {
    const MARKER: &str = "data-time=\"";
    let start = chunk.find(MARKER)? + MARKER.len();
    let end = chunk[start..].find('"')? + start;
    Some(&chunk[start..end])
}

fn window_status(usage_ratio: f64) -> &'static str {
    if usage_ratio >= 1.0 {
        "exhausted"
    } else if usage_ratio >= crate::provider_quota::types::WARNING_THRESHOLD_RATIO {
        "warning"
    } else {
        "available"
    }
}

/// Fixed window length used to derive period start; 0 means informational.
fn window_duration(key: &str) -> Duration {
    match key {
        WINDOW_5H => Duration::hours(5),
        WINDOW_WEEKLY => Duration::days(7),
        _ => Duration::zero(),
    }
}

/// Detects the AuthKit sign-in page: an expired cookie may return HTTP 200
/// with a login page instead of 401/403.
fn looks_like_login_page(html: &str) -> bool {
    let lower = html.to_lowercase();
    const MARKERS: [&str; 6] = [
        "<title>sign in",
        "sign-in",
        "signin",
        "wos-session", // session cookie reference implies a redirect/auth flow
        "log in to your account",
        "/signin",
    ];
    MARKERS.iter().any(|m| lower.contains(m))
}

fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_quota::types::QuotaLimitType;

    const SESSION: &str = "__Secure-session=eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig";

    #[test]
    fn normalizes_cookie_dropping_others() {
        let raw = "Cookie: cf_clearance=abc; aid=xyz; __Secure-session=eyJ0b2tlbi4uLn0.sig.padding-to-thirty-chars; _ga=GA1.2.3";
        let out = normalize_ollama_cookie(raw).unwrap();
        assert!(out.starts_with("__Secure-session=eyJ0b2tlbi4uLn0.sig.padding-to-thirty-chars"));
        assert_eq!(out.matches(';').count(), 0);
    }

    #[test]
    fn rejects_short_or_missing_session() {
        assert!(normalize_ollama_cookie("__Secure-session=short").is_err());
        assert!(normalize_ollama_cookie("cf_clearance=abc; other=1").is_err());
        assert!(normalize_ollama_cookie("").is_err());
        assert!(normalize_ollama_cookie("multi\r\nline").is_err());
        assert!(normalize_ollama_cookie("novalue").is_err());
    }

    #[test]
    fn accepts_bare_name_value_without_label() {
        assert_eq!(normalize_ollama_cookie(SESSION).unwrap(), SESSION);
    }

    const SETTINGS_HTML: &str = r#"<html><body>
<div class="settings">
  <section>
    <h3>Session usage</h3>
    <div role="progressbar" aria-label="42.5 % used" data-time="2026-09-29T14:00:00Z"></div>
  </section>
  <section>
    <h3>Weekly usage</h3>
    <div role="progressbar" aria-label="81 % used" data-time="2026-10-02T00:00:00Z"></div>
  </section>
</div></body></html>"#;

    #[test]
    fn parses_settings_page_windows() {
        let data = parse_response(SETTINGS_HTML).unwrap();
        assert_eq!(data.status, "warning"); // 81% weekly is the worst window
        assert_eq!(data.limits.len(), 2);
        let five = data.limits.iter().find(|l| l.window == "5h").unwrap();
        assert_eq!(five.status, "available");
        assert!((five.usage_ratio - 0.425).abs() < 1e-9);
        assert_eq!(five.next_reset_at.unwrap().to_rfc3339(), "2026-09-29T14:00:00+00:00");
        let weekly = data.limits.iter().find(|l| l.window == "weekly").unwrap();
        assert_eq!(weekly.status, "warning");
        // next_reset_at = earliest reset (session window)
        assert_eq!(data.next_reset_at.unwrap().to_rfc3339(), "2026-09-29T14:00:00+00:00");
        assert_eq!(data.limits[0].kind, QuotaLimitType::Token);
    }

    #[test]
    fn exhausted_percent_maps_to_exhausted() {
        let html = r#"<h3>Session usage</h3><div aria-label="100 % used" data-time="2026-09-29T14:00:00Z"></div>"#;
        let data = parse_response(html).unwrap();
        assert_eq!(data.status, "exhausted");
        assert!(!data.ready);
    }

    #[test]
    fn missing_reset_time_still_yields_window() {
        let html = r#"<h3>Session usage</h3><div aria-label="10%used"></div>"#;
        let data = parse_response(html).unwrap();
        assert_eq!(data.status, "available");
        assert!(data.limits[0].next_reset_at.is_none());
        assert!(data.limits[0].period_start.is_none());
    }

    #[test]
    fn login_page_maps_to_invalid_credentials() {
        let html = r#"<html><head><title>Sign in - Ollama</title></head><body>log in to your account</body></html>"#;
        let err = parse_response(html).unwrap_err();
        assert!(matches!(err, QuotaError::InvalidCredentials(_)));
    }

    #[test]
    fn unrelated_markup_is_a_parse_error() {
        let err = parse_response("<html><body>maintenance</body></html>").unwrap_err();
        assert!(matches!(err, QuotaError::Parse(_)));
    }

    #[test]
    fn meter_outside_scan_window_is_ignored() {
        // The weekly meter sits further than 4000 chars after its header.
        let filler = "x".repeat(4100);
        let html = format!(
            "<h3>Session usage</h3><div aria-label=\"5 % used\" data-time=\"2026-09-29T14:00:00Z\"></div><h3>Weekly usage</h3>{}<div aria-label=\"90 % used\"></div>",
            filler
        );
        let data = parse_response(&html).unwrap();
        assert_eq!(data.limits.len(), 1);
        assert_eq!(data.limits[0].window, "5h");
    }
}
