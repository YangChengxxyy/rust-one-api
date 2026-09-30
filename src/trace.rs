//! Request trace collection (Phase 2): one `requests` summary row plus one
//! `request_executions` row per channel attempt, written asynchronously after
//! the response completes so the respond path never blocks on trace I/O.
//!
//! Levels (`trace.level` in config):
//! - `off`: no rows, near-zero overhead (all recorder calls are no-ops).
//! - `meta` (default): summary + per-attempt metadata (status/error/latency).
//! - `full`: additionally headers and bodies, sanitized (`authorization`,
//!   `x-api-key`) and truncated to [`MAX_BODY_BYTES`].

use std::time::{Duration, Instant};

use serde_json::{json, Value};
use uuid::Uuid;

use llm::Usage;

use crate::storage::{Db, TraceExecution, TraceRepo, TraceRequest};

/// Headers/bodies are capped at 16 KiB before hitting the database
/// (`full` level only) to keep the sqlite file from ballooning.
pub const MAX_BODY_BYTES: usize = 16 * 1024;

/// Header names whose values are replaced with `***` before persisting.
const SENSITIVE_HEADERS: [&str; 2] = ["authorization", "x-api-key"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceLevel {
    Off,
    Meta,
    Full,
}

impl TraceLevel {
    /// Unknown values fall back to `meta` with a warning (safe default:
    /// tracing stays on, bodies stay out).
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Self::Off,
            "full" => Self::Full,
            "meta" => Self::Meta,
            other => {
                tracing::warn!("unknown trace.level {other:?}, falling back to \"meta\"");
                Self::Meta
            }
        }
    }

    fn captures_bodies(self) -> bool {
        self == Self::Full
    }
}

/// Header pairs -> JSON object string with sensitive values masked.
pub fn sanitize_headers(headers: &[(String, String)]) -> String {
    let map: serde_json::Map<String, Value> = headers
        .iter()
        .map(|(k, v)| {
            let masked = SENSITIVE_HEADERS.contains(&k.to_ascii_lowercase().as_str());
            (k.clone(), Value::String(if masked { "***".into() } else { v.clone() }))
        })
        .collect();
    Value::Object(map).to_string()
}

/// Lossy UTF-8 decode capped at `MAX_BODY_BYTES` on a char boundary.
pub fn truncate_body(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= MAX_BODY_BYTES {
        return text.into_owned();
    }
    let mut end = MAX_BODY_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = text[..end].to_string();
    out.push_str("…[truncated]");
    out
}

/// In-progress attempt recorder. Cheap to create; capture methods are no-ops
/// below the `full` level.
pub struct PendingAttempt {
    attempt: i64,
    channel_id: String,
    started: Instant,
    full: bool,
    request_headers: Option<String>,
    request_body: Option<String>,
    response_headers: Option<String>,
    response_body: Option<String>,
}

impl PendingAttempt {
    pub fn capture_request(&mut self, headers: &[(String, String)], body: &[u8]) {
        if self.full {
            self.request_headers = Some(sanitize_headers(headers));
            self.request_body = Some(truncate_body(body));
        }
    }

    pub fn capture_response_headers(&mut self, headers: &reqwest::header::HeaderMap) {
        if self.full {
            let pairs: Vec<(String, String)> = headers
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            self.response_headers = Some(sanitize_headers(&pairs));
        }
    }

    /// Whole-body capture for non-stream responses.
    pub fn set_response_body(&mut self, body: &[u8]) {
        if self.full {
            self.response_body = Some(truncate_body(body));
        }
    }

    /// Incremental capture for stream responses; stops appending past the cap.
    pub fn feed_response_bytes(&mut self, bytes: &[u8]) {
        if !self.full {
            return;
        }
        let buf = self.response_body.get_or_insert_with(String::new);
        if buf.len() >= MAX_BODY_BYTES {
            return;
        }
        let text = String::from_utf8_lossy(bytes);
        let remaining = MAX_BODY_BYTES - buf.len();
        if text.len() <= remaining {
            buf.push_str(&text);
        } else {
            let mut end = remaining;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            buf.push_str(&text[..end]);
            buf.push_str("…[truncated]");
        }
    }
}

/// Per-request trace collector owned by the relay loop. `Off` level keeps the
/// request_id (so usage_logs still correlate) but records nothing.
pub struct RequestTracer {
    request_id: String,
    inner: Option<TracerInner>,
}

/// Empty tracer used as the `mem::replace` placeholder when ownership moves
/// into the detached stream task.
impl Default for RequestTracer {
    fn default() -> Self {
        Self { request_id: Uuid::new_v4().to_string(), inner: None }
    }
}

struct TracerInner {
    level: TraceLevel,
    api_key_id: Option<String>,
    model: String,
    stream: bool,
    started: Instant,
    ttft: Option<Duration>,
    executions: Vec<TraceExecution>,
    next_attempt: i64,
}

/// Terminal state for the request row, filled by the path that completes the
/// response (billing writer).
pub struct TraceOutcome<'a> {
    pub status: &'a str,
    pub error: Option<String>,
    /// Final successful channel (None when every candidate failed).
    pub channel_id: Option<String>,
    pub usage: &'a Usage,
    pub cost: String,
}

impl RequestTracer {
    pub fn new(level: TraceLevel, api_key_id: Option<String>, model: String, stream: bool) -> Self {
        let inner = (level != TraceLevel::Off).then(|| TracerInner {
            level,
            api_key_id,
            model,
            stream,
            started: Instant::now(),
            ttft: None,
            executions: Vec::new(),
            next_attempt: 1,
        });
        Self { request_id: Uuid::new_v4().to_string(), inner }
    }

    /// Shared with usage_logs.request_id so billing rows join to traces.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn begin_attempt(&mut self, channel_id: &str) -> PendingAttempt {
        let (attempt, full) = match &mut self.inner {
            Some(inner) => {
                let n = inner.next_attempt;
                inner.next_attempt += 1;
                (n, inner.level.captures_bodies())
            }
            None => (0, false),
        };
        PendingAttempt {
            attempt,
            channel_id: channel_id.to_string(),
            started: Instant::now(),
            full,
            request_headers: None,
            request_body: None,
            response_headers: None,
            response_body: None,
        }
    }

    /// Close an attempt: `status` is "success" or "failed", `error` carries the
    /// upstream failure message when failed.
    pub fn record_attempt(&mut self, pending: PendingAttempt, status: &str, error: Option<String>) {
        let Some(inner) = &mut self.inner else { return };
        inner.executions.push(TraceExecution {
            id: Uuid::new_v4().to_string(),
            request_id: self.request_id.clone(),
            attempt: pending.attempt,
            channel_id: pending.channel_id,
            status: status.to_string(),
            error,
            latency_ms: pending.started.elapsed().as_millis() as i64,
            request_headers: pending.request_headers,
            request_body: pending.request_body,
            response_headers: pending.response_headers,
            response_body: pending.response_body,
            created_at: chrono::Utc::now().to_rfc3339(),
        });
    }

    /// Time to first forwarded stream event; only the first call sticks.
    pub fn record_ttft(&mut self) {
        if let Some(inner) = &mut self.inner {
            if inner.ttft.is_none() {
                inner.ttft = Some(inner.started.elapsed());
            }
        }
    }

    /// Detached write: spawns a task so the respond path never awaits trace
    /// I/O. No-op at `off` level. Insert failures are logged, never fatal.
    pub fn submit(self, pool: &Db, outcome: TraceOutcome<'_>) {
        let Some(inner) = self.inner else { return };
        let pool = pool.clone();
        let request_id = self.request_id.clone();
        let status = outcome.status.to_string();
        let error = outcome.error;
        let channel_id = outcome.channel_id;
        let usage = json!({
            "prompt_tokens": outcome.usage.prompt_tokens,
            "completion_tokens": outcome.usage.completion_tokens,
            "cached_tokens": outcome.usage.cached_tokens.unwrap_or(0),
            "reasoning_tokens": outcome.usage.reasoning_tokens.unwrap_or(0),
            "total_tokens": outcome.usage.total_tokens,
        })
        .to_string();
        let cost = outcome.cost;
        tokio::spawn(async move {
            let row = TraceRequest {
                id: request_id,
                api_key_id: inner.api_key_id,
                channel_id,
                model: inner.model,
                stream: inner.stream,
                status,
                error,
                ttft_ms: inner.ttft.map(|d| d.as_millis() as i64),
                latency_ms: inner.started.elapsed().as_millis() as i64,
                usage,
                cost,
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            if let Err(e) = TraceRepo::insert_request(&pool, &row).await {
                tracing::error!("trace request insert failed: {e}");
                return;
            }
            if let Err(e) = TraceRepo::insert_executions(&pool, &inner.executions).await {
                tracing::error!("trace executions insert failed: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse() {
        assert_eq!(TraceLevel::parse("off"), TraceLevel::Off);
        assert_eq!(TraceLevel::parse("meta"), TraceLevel::Meta);
        assert_eq!(TraceLevel::parse("FULL"), TraceLevel::Full);
        assert_eq!(TraceLevel::parse("bogus"), TraceLevel::Meta);
    }

    #[test]
    fn headers_are_sanitized() {
        let h = vec![
            ("Authorization".to_string(), "Bearer sk-secret".to_string()),
            ("x-api-key".to_string(), "sk-secret".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ];
        let v: Value = serde_json::from_str(&sanitize_headers(&h)).unwrap();
        assert_eq!(v["Authorization"], "***");
        assert_eq!(v["x-api-key"], "***");
        assert_eq!(v["content-type"], "application/json");
    }

    #[test]
    fn body_truncates_on_char_boundary() {
        let body = vec![b'a'; MAX_BODY_BYTES + 100];
        let out = truncate_body(&body);
        assert!(out.len() <= MAX_BODY_BYTES + "…[truncated]".len());
        assert!(out.ends_with("…[truncated]"));

        // multibyte char straddling the cap must not panic
        let mut body = vec![b'a'; MAX_BODY_BYTES - 1];
        body.extend_from_slice("汉".as_bytes());
        let out = truncate_body(&body);
        assert!(out.ends_with("…[truncated]"));
        assert!(out.len() <= MAX_BODY_BYTES + "…[truncated]".len());

        let short = truncate_body(b"hello");
        assert_eq!(short, "hello");
    }

    #[test]
    fn off_level_records_nothing() {
        let mut t = RequestTracer::new(TraceLevel::Off, None, "m".into(), false);
        let mut p = t.begin_attempt("ch1");
        p.capture_request(&[("authorization".into(), "x".into())], b"{}");
        t.record_attempt(p, "failed", Some("boom".into()));
        t.record_ttft();
        assert!(t.inner.is_none());
        // id still exists for usage_log correlation
        assert!(!t.request_id().is_empty());
    }

    #[test]
    fn meta_level_skips_bodies() {
        let mut t = RequestTracer::new(TraceLevel::Meta, None, "m".into(), false);
        let mut p = t.begin_attempt("ch1");
        p.capture_request(&[("a".into(), "b".into())], b"body");
        p.set_response_body(b"resp");
        t.record_attempt(p, "success", None);
        let inner = t.inner.as_ref().unwrap();
        assert_eq!(inner.executions.len(), 1);
        assert!(inner.executions[0].request_body.is_none());
        assert!(inner.executions[0].response_body.is_none());
        assert_eq!(inner.executions[0].attempt, 1);
    }

    #[test]
    fn stream_feed_stops_at_cap() {
        let mut t = RequestTracer::new(TraceLevel::Full, None, "m".into(), true);
        let mut p = t.begin_attempt("ch1");
        let chunk = vec![b'x'; 8 * 1024];
        p.feed_response_bytes(&chunk);
        p.feed_response_bytes(&chunk);
        p.feed_response_bytes(&chunk); // exceeds cap, ignored past it
        let body = p.response_body.as_ref().unwrap();
        assert!(body.len() <= MAX_BODY_BYTES + "…[truncated]".len());
    }
}
