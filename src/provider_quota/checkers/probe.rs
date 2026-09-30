//! Generic fallback probe for channels without a specialized checker:
//! a lightweight "list models" GET classified into quota status.

use std::time::Duration;

use crate::provider_quota::credentials::ChannelCredentials;
use crate::provider_quota::types::QuotaData;
use crate::storage::Channel;

/// Returns (status, detail) — status in available/warning/exhausted/unknown.
pub async fn probe(http: &reqwest::Client, channel: &Channel, creds: &ChannelCredentials) -> (String, QuotaData) {
    let api_key = creds.api_key.as_deref().unwrap_or_default();
    let base = channel.base_url.trim_end_matches('/');
    let req = match channel.channel_type.as_str() {
        "openai/chat_completions" | "openai/responses" | "openai" | "openai_responses" => {
            http.get(format!("{base}/models")).bearer_auth(api_key)
        }
        "claude/messages" => http
            .get(format!("{base}/v1/models"))
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01"),
        "gemini/models" => http.get(base).header("x-goog-api-key", api_key),
        _ => http.get(base).bearer_auth(api_key),
    };

    let (http_status, detail) = match req.timeout(Duration::from_secs(15)).send().await {
        Ok(resp) => {
            let code = resp.status().as_u16();
            let body = resp.bytes().await.unwrap_or_default();
            let detail = if (200..300).contains(&code) {
                String::new()
            } else {
                String::from_utf8_lossy(&body).chars().take(200).collect()
            };
            (code, detail)
        }
        Err(e) => (0u16, format!("network error: {e}")),
    };

    let status = match http_status {
        200..=299 => "available",
        402 => "exhausted",
        429 => "warning",
        _ => "unknown",
    };

    let mut data = QuotaData::new("generic_probe", status);
    data.raw_data.insert("http_status".into(), http_status.into());
    data.raw_data.insert("checked_at".into(), chrono::Utc::now().to_rfc3339().into());
    if !detail.is_empty() {
        data.raw_data.insert("detail".into(), detail.into());
    }
    (status.to_string(), data)
}
