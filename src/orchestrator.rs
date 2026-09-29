//! Relay core: client-format inbound -> unified -> provider outbound, with
//! channel failover (weight desc, up to 3 candidates), quota precheck, and
//! usage/billing logging.
//!
//! Frozen JSON conventions (see project context):
//! - channel.credentials: `{"api_key": "..."}`
//! - channel.model_mapping: `{"requested_model": "upstream_model"}`
//! - api_key.quota: `{"max_requests_per_day": n?, "max_tokens_per_day": n?,
//!    "max_cost_per_day": "decimal"?}` — absent fields are unlimited.

use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use llm::sse::{SseEvent, SseParser};
use llm::transformer::{Credentials, InboundTransformer, OutboundTransformer};
use llm::transformers::{create_inbound, create_outbound};
use llm::{Request, Usage, StreamChunk};
use rust_decimal::Decimal;
use std::str::FromStr;
use serde_json::json;
use uuid::Uuid;

use crate::error::AppError;
use crate::pricing::{self, ModelPrice};
use chrono::Utc;
use crate::keystate;
use crate::token_estimate;
use crate::provider_quota::credentials::{
    disable_key, serving_api_keys, ChannelCredentials,
};
use crate::provider_quota::routing;
use crate::provider_quota::types::{QuotaData, QuotaLimitType};
use crate::storage::{
    ApiKey, Channel, ChannelRepo, Db, ModelPriceRepo, ProviderQuotaRepo, UsageLog,
    UsageLogRepo,
};

/// Result of a relay dispatch.
pub enum RelayOutcome {
    /// Complete JSON response (status + body already in client wire format).
    Json { status: u16, body: Vec<u8> },
    /// SSE stream in the client's wire format. The pipeline runs in a spawned
    /// task that owns the upstream stream, so billing (usage_log insert) is
    /// written even if the client disconnects mid-stream.
    Stream(futures::stream::BoxStream<'static, Result<Bytes, std::io::Error>>),
}

/// Retryable upstream failure carrying the HTTP status when one was received,
/// so the relay loop can distinguish auth failures (401/403 -> disable the
/// key, retry same channel) from 5xx/network errors (move to next candidate).
struct UpstreamFailure {
    status: Option<u16>,
    msg: String,
}

impl UpstreamFailure {
    fn network(msg: impl Into<String>) -> Self {
        Self { status: None, msg: msg.into() }
    }

    fn is_auth_failure(&self) -> bool {
        matches!(self.status, Some(401 | 403))
    }
}

impl UpstreamFailure {
    /// Channel-level failures (keystate auto-disable) are 5xx and network
    /// errors; 4xx client errors are request problems, not channel health.
    fn counts_as_channel_failure(&self) -> bool {
        match self.status {
            Some(status) => status >= 500,
            None => true,
        }
    }
}

pub struct Relay {
    pool: Db,
    http: reqwest::Client,
}

impl Relay {
    pub fn new(pool: Db) -> Self {
        Self {
            pool,
            http: reqwest::Client::new(),
        }
    }

    /// `model_hint` fills the unified request's model when the client body
    /// doesn't carry one (Gemini: the model lives in the URL path).
    /// `force_stream` forces `stream = true` (Gemini `:streamGenerateContent`).
    pub async fn relay(
        &self,
        inbound_format: &str,
        model_hint: Option<String>,
        force_stream: bool,
        api_key: &ApiKey,
        body: &[u8],
    ) -> Result<RelayOutcome, AppError> {
        let inbound = create_inbound(inbound_format)
            .ok_or_else(|| AppError::bad_request(format!("unknown inbound format {inbound_format}")))?;
        let mut req: Request = inbound
            .transform_request(body)
            .map_err(|e| AppError::bad_request(format!("invalid request: {e}")))?;
        if req.model.is_empty() {
            req.model = model_hint.unwrap_or_default();
        }
        if force_stream {
            req.stream = true;
        }
        let requested_model = req.model.clone();

        self.quota_precheck(api_key).await?;

        // Candidates: enabled + supporting the model, tiered by quota routing
        // evaluation (Open first, then no-data/unknown, sticky-only last).
        let mut candidates =
            ChannelRepo::list_enabled_for_model(&self.pool, &requested_model).await?;
        let mut tiered: Vec<(u8, Channel)> = Vec::with_capacity(candidates.len());
        for ch in candidates.drain(..) {
            let rows = ProviderQuotaRepo::list_for_channel(&self.pool, &ch.id).await?;
            let Some(row) = rows.iter().find(|r| r.account_key.is_empty()) else {
                tiered.push((1, ch));
                continue;
            };
            let data: Option<QuotaData> =
                serde_json::from_str(&row.quota_data).ok();
            let (state, reason) = match data {
                Some(d) => routing::evaluate_quota_routing(
                    &d.limits,
                    &row.status,
                    QuotaLimitType::Token,
                    Utc::now(),
                ),
                None => (routing::RoutingState::Unknown, None),
            };
            match state {
                routing::RoutingState::Exhausted => continue,
                routing::RoutingState::Open => tiered.push((0, ch)),
                routing::RoutingState::Unknown => tiered.push((1, ch)),
                routing::RoutingState::StickyOnly => {
                    if let Some(r) = reason {
                        tracing::info!(
                            channel_id = %ch.id,
                            reason = r,
                            "channel deprioritized to sticky-only"
                        );
                    }
                    tiered.push((2, ch));
                }
            }
        }
        if tiered.is_empty() {
            return Err(AppError::not_found(format!(
                "no channel available for model {requested_model}"
            )));
        }
        tiered.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.weight.cmp(&a.1.weight)));
        let mut candidates: Vec<Channel> = tiered.into_iter().map(|(_, ch)| ch).collect();
        candidates.truncate(3);

        let started = Instant::now();
        let mut last_err = String::from("unknown error");
        let mut skipped_disabled: Vec<String> = Vec::new();
        for channel in &candidates {
            // Process-local auto-disable layer (keystate); durable status
            // filtering already happened in list_enabled_for_model.
            if let Some(until) = keystate::channel_disabled_until(&channel.id) {
                tracing::debug!(channel = %channel.id, until = %until, "skipping auto-disabled channel");
                skipped_disabled.push(format!("{} (until {until})", channel.name));
                continue;
            }
            let mut ch = channel.clone();
            // OAuth channels: refresh first, then prefer the OAuth access
            // token over api_key/multi-key rotation (copilot exchanges its
            // GitHub token for a short-lived relay bearer).
            ch = crate::oauth::maybe_refresh_oauth(&self.pool, &self.http, &ch).await;
            let creds_typed = ChannelCredentials::parse(&ch.credentials);
            let mut serving = match ch.channel_type.as_str() {
                "claudecode" | "codex" => creds_typed
                    .oauth_access_token()
                    .map(|t| vec![t])
                    .unwrap_or_default(),
                "github_copilot" => {
                    let github_token = creds_typed
                        .api_key
                        .clone()
                        .or_else(|| creds_typed.all_api_keys().first().map(|s| s.to_string()));
                    let exchanged = match github_token {
                        Some(t) => crate::oauth::copilot_token(&self.http, &t).await,
                        None => Err(anyhow::anyhow!("copilot channel has no github token")),
                    };
                    match exchanged {
                        Ok(copilot) => vec![copilot],
                        Err(e) => {
                            tracing::warn!(channel = %ch.id, "copilot token exchange failed: {e}");
                            last_err = format!("copilot token exchange failed: {e}");
                            continue;
                        }
                    }
                }
                _ => Vec::new(),
            };
            if serving.is_empty() {
                // No OAuth token: rotate the api_keys array as before.
                serving = serving_api_keys(&creds_typed, &ch);
            }
            if serving.is_empty() {
                // Fallback for channels without an api_keys array: the raw
                // single api_key (possibly an OAuth blob) verbatim.
                let creds: serde_json::Value =
                    serde_json::from_str(&ch.credentials).unwrap_or(json!({}));
                if let Some(k) = creds.get("api_key").and_then(|v| v.as_str()) {
                    if !k.trim().is_empty() {
                        serving.push(k.trim().to_string());
                    }
                }
            }
            // Key-level retry (axonhub ChannelRetryable semantics): a 401/403
            // parks the offending key (persisted via ChannelRepo::update) and
            // retries the SAME channel with the next serving key. Key retries
            // do NOT consume the 3-candidate budget above; each distinct key
            // is tried at most once per request (`tried`). 5xx/network errors
            // and exhausted keys move to the next candidate channel.
            let mut tried = std::collections::HashSet::new();
            while let Some(key) = keystate::next_key(&serving, &ch.id) {
                if !tried.insert(key.clone()) {
                    break;
                }
                match self
                    .try_channel(&ch, &key, &req, &requested_model, api_key, inbound_format)
                    .await
                {
                    Ok(outcome) => {
                        keystate::record_channel_success(&ch.id);
                        keystate::record_key_success(&ch.id, &key);
                        return Ok(outcome);
                    }
                    Err(f) => {
                        if f.is_auth_failure() {
                            let streak = keystate::record_key_failure(&ch.id, &key);
                            let expires_at = keystate::disable_expires_at(streak);
                            let status = f.status.unwrap_or(0);
                            disable_key(&mut ch, &key, status as i64, &f.msg, expires_at);
                            if let Err(e) = ChannelRepo::update(&self.pool, &ch).await {
                                tracing::warn!(
                                    channel = %ch.id,
                                    "failed to persist disabled_api_keys: {e}"
                                );
                            }
                            tracing::warn!(
                                channel = %ch.id,
                                key = %key,
                                "api key disabled after upstream {status}, retrying channel"
                            );
                            serving.retain(|k| k != &key);
                            last_err = f.msg;
                            if serving.is_empty() {
                                break;
                            }
                            continue;
                        }
                        tracing::warn!(channel = %ch.id, "upstream attempt failed: {}", f.msg);
                        let counts = f.counts_as_channel_failure();
                        last_err = f.msg;
                        if counts {
                            let streak = keystate::record_channel_failure(&ch.id);
                            if streak >= keystate::CHANNEL_DISABLE_THRESHOLD {
                                let until = keystate::disable_channel(&ch.id);
                                tracing::warn!(
                                    channel = %ch.id,
                                    name = %ch.name,
                                    until = %until,
                                    "channel auto-disabled after {streak} consecutive failures"
                                );
                            }
                        }
                        break;
                    }
                }
            }
        }
        // All candidates failed: log once (last channel), then surface upstream error.
        write_billing(
            &self.pool,
            Some(api_key.id.as_str()),
            candidates.last(),
            &requested_model,
            req.stream,
            &Usage::default(),
            "failed",
            started,
            &Uuid::new_v4().to_string(),
        )
        .await;
        if !skipped_disabled.is_empty() && skipped_disabled.len() == candidates.len() {
            return Err(AppError::not_found(format!(
                "no channel available for model {requested_model}: all {} candidate(s) auto-disabled: {}",
                candidates.len(),
                skipped_disabled.join(", ")
            )));
        }
        Err(AppError::upstream(format!(
            "all upstream channels failed for model {requested_model}: {last_err}"
        )))
    }

    async fn quota_precheck(&self, api_key: &ApiKey) -> Result<(), AppError> {
        let quota: serde_json::Value = serde_json::from_str(&api_key.quota).unwrap_or(json!({}));
        let max_req = quota.get("max_requests_per_day").and_then(|v| v.as_i64());
        let max_tok = quota.get("max_tokens_per_day").and_then(|v| v.as_i64());
        let max_cost = quota
            .get("max_cost_per_day")
            .and_then(|v| v.as_str())
            .and_then(|s| Decimal::from_str(s).ok());
        if max_req.is_none() && max_tok.is_none() && max_cost.is_none() {
            return Ok(());
        }
        let since = chrono::Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .to_rfc3339();
        let (count, tokens, cost) =
            UsageLogRepo::aggregate_for_key(&self.pool, &api_key.id, &since).await?;
        if let Some(m) = max_req {
            if count >= m {
                return Err(AppError::quota_exceeded(format!(
                    "daily request quota exceeded ({count}/{m})"
                )));
            }
        }
        if let Some(m) = max_tok {
            if tokens >= m {
                return Err(AppError::quota_exceeded(format!(
                    "daily token quota exceeded ({tokens}/{m})"
                )));
            }
        }
        if let Some(m) = max_cost {
            if let Ok(spent) = Decimal::from_str(&cost) {
                if spent >= m {
                    return Err(AppError::quota_exceeded(format!(
                        "daily cost quota exceeded ({spent}/{m})"
                    )));
                }
            }
        }
        Ok(())
    }

    /// One attempt against one channel with a specific upstream key.
    /// `Err(UpstreamFailure)` = retryable failure; `Ok(RelayOutcome::Json{4xx})`
    /// = upstream client error surfaced in the client's own wire format
    /// (already logged, no retry). 401/403 come back as `Err` with the status
    /// set so the relay loop can disable the key and rotate.
    async fn try_channel(
        &self,
        channel: &Channel,
        upstream_key: &str,
        req: &Request,
        requested_model: &str,
        api_key: &ApiKey,
        inbound_format: &str,
    ) -> Result<RelayOutcome, UpstreamFailure> {
        // Fresh inbound instance per attempt: instances are per-request and
        // may be stateful (e.g. Anthropic content_block lifecycle).
        let inbound = create_inbound_used(inbound_format).ok_or_else(|| {
            UpstreamFailure::network(format!("unknown inbound format {inbound_format}"))
        })?;
        // Apply model_mapping: requested -> upstream model name.
        let mapping: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&channel.model_mapping).unwrap_or_default();
        let mut upstream_req = req.clone();
        if let Some(mapped) = mapping.get(&req.model).and_then(|v| v.as_str()) {
            upstream_req.model = mapped.to_string();
        }

        let outbound = create_outbound(outbound_format_for_channel_type(&channel.channel_type))
            .ok_or_else(|| {
                UpstreamFailure::network(format!(
                    "unknown channel type {}",
                    channel.channel_type
                ))
            })?;
        let out_req = outbound
            .build_request(&upstream_req, &Credentials { api_key: upstream_key.to_string() })
            .map_err(|e| UpstreamFailure::network(format!("build upstream request: {e}")))?;


        let url = format!("{}{}", channel.base_url.trim_end_matches('/'), out_req.path);
        let mut http_req = self
            .http
            .post(&url)
            .timeout(std::time::Duration::from_secs(120));
        for (k, v) in &out_req.headers {
            http_req = http_req.header(k, v);
        }
        let resp = http_req
            .body(out_req.body)
            .send()
            .await
            .map_err(|e| UpstreamFailure::network(format!("network: {e}")))?;

        let status = resp.status().as_u16();
        if status >= 500 {
            let body = resp.bytes().await.unwrap_or_default();
            return Err(UpstreamFailure::network(format!(
                "upstream {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        if status == 401 || status == 403 {
            // Auth failure for THIS key: the relay loop disables it and
            // retries the channel with the next key (axonhub disables on
            // 401/403; 429 and other 4xx never disable a key).
            let body = resp.bytes().await.unwrap_or_default();
            return Err(UpstreamFailure {
                status: Some(status),
                msg: format!("upstream {status}: {}", String::from_utf8_lossy(&body)),
            });
        }
        if status >= 400 {
            // No retry on 4xx; log a failed usage row and surface the error
            // re-encoded in the client's own wire format.
            let body = resp.bytes().await.unwrap_or_default();
            let err = outbound.extract_error(status, &body);
            let client_body = inbound.transform_error(&err);
            write_billing(
                &self.pool,
                Some(api_key.id.as_str()),
                Some(channel),
                requested_model,
                upstream_req.stream,
                &Usage::default(),
                "failed",
                Instant::now(),
                &Uuid::new_v4().to_string(),
            )
            .await;
            return Ok(RelayOutcome::Json { status, body: client_body });
        }

        if upstream_req.stream {
            self.stream_response(resp, inbound, outbound, channel, requested_model, api_key, req)
                .await
                .map_err(UpstreamFailure::network)
        } else {
            self.json_response(resp, inbound, outbound, channel, requested_model, api_key, req)
                .await
                .map_err(UpstreamFailure::network)
        }
    }

    async fn json_response(
        &self,
        resp: reqwest::Response,
        inbound: Box<dyn InboundTransformer>,
        outbound: Box<dyn OutboundTransformer>,
        channel: &Channel,
        requested_model: &str,
        api_key: &ApiKey,
        req: &Request,
    ) -> Result<RelayOutcome, String> {
        let started = Instant::now();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("read upstream body: {e}"))?;
        let unified = outbound
            .transform_response(&body)
            .map_err(|e| format!("bad upstream response: {e}"))?;
        let usage = if unified.usage.is_some() {
            unified.usage.clone().unwrap_or_default()
        } else {
            // Non-stream fallback: estimate from request prompt + response text.
            let mut completion = String::new();
            for choice in &unified.choices {
                if let Some(content) = &choice.message.content {
                    token_estimate::push_capped(&mut completion, &content.text());
                }
                if let Some(calls) = &choice.message.tool_calls {
                    for c in calls {
                        token_estimate::push_capped(&mut completion, &c.function.arguments);
                    }
                }
            }
            let usage = token_estimate::final_usage(None, req, &completion);
            tracing::warn!(
                request_id = %Uuid::new_v4().to_string(),
                model = %requested_model,
                "non-stream response carried no usage; billing on estimated tokens (prompt={}, completion={})",
                usage.prompt_tokens,
                usage.completion_tokens
            );
            usage
        };
        let client_body = inbound
            .transform_response(&unified)
            .map_err(|e| format!("encode client response: {e}"))?;
        write_billing(
            &self.pool,
            Some(api_key.id.as_str()),
            Some(channel),
            requested_model,
            false,
            &usage,
            "success",
            started,
            &Uuid::new_v4().to_string(),
        )
        .await;
        Ok(RelayOutcome::Json {
            status: 200,
            body: client_body,
        })
    }

    /// Builds the client SSE stream. A detached task fully consumes the
    /// upstream stream, re-encodes events for the client, and writes the
    /// usage_log row afterwards. Because the task owns the upstream stream
    /// (not the client body), billing happens even when the client
    /// disconnects; channel sends then fail and are ignored.
    async fn stream_response(
        &self,
        resp: reqwest::Response,
        inbound: Box<dyn InboundTransformer>,
        outbound: Box<dyn OutboundTransformer>,
        channel: &Channel,
        requested_model: &str,
        api_key: &ApiKey,
        req: &Request,
    ) -> Result<RelayOutcome, String> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
        let pool = self.pool.clone();
        let channel = channel.clone();
        let api_key_id = api_key.id.clone();
        let requested_model = requested_model.to_string();
        let req = req.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut parser = SseParser::new();
            let mut last_usage: Option<Usage> = None;
            let mut completion_text = String::new();
            let mut ok = true;
            let mut upstream = resp.bytes_stream();

            while let Some(item) = upstream.next().await {
                match item {
                    Ok(bytes) => {
                        for ev in parser.feed(&bytes) {
                            if let Err(e) = forward_event(
                                &tx,
                                &ev,
                                inbound.as_ref(),
                                outbound.as_ref(),
                                &mut last_usage,
                                &mut completion_text,
                            )
                            .await
                            {
                                tracing::warn!("{e}");
                                ok = false;
                            }
                        }
                        if !ok {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("upstream stream error: {e}");
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                for ev in parser.finish() {
                    if let Err(e) = forward_event(
                        &tx,
                        &ev,
                        inbound.as_ref(),
                        outbound.as_ref(),
                        &mut last_usage,
                        &mut completion_text,
                    )
                    .await
                    {
                        tracing::warn!("{e}");
                        ok = false;
                    }
                }
            }
            if ok {
                for se in inbound.stream_end() {
                    if tx.send(Ok(Bytes::from(se.encode()))).await.is_err() {
                        break;
                    }
                }
            }

            let request_id = Uuid::new_v4().to_string();
            let usage = token_estimate::final_usage(
                last_usage.as_ref(),
                &req,
                &completion_text,
            );
            if last_usage.is_none() {
                tracing::warn!(
                    request_id = %request_id,
                    model = %requested_model,
                    "upstream stream carried no usage; billing on estimated tokens (prompt={}, completion={})",
                    usage.prompt_tokens,
                    usage.completion_tokens
                );
            }
            write_billing(
                &pool,
                Some(api_key_id.as_str()),
                Some(&channel),
                &requested_model,
                true,
                &usage,
                if ok { "success" } else { "failed" },
                started,
                &request_id,
            )
            .await;
        });

        let client_stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .boxed();
        Ok(RelayOutcome::Stream(client_stream))
    }
}

fn create_inbound_used(format: &str) -> Option<Box<dyn InboundTransformer>> {
    create_inbound(format)
}

/// OAuth-backed channel types relay through a different outbound wire format
/// than their channel_type name suggests (axonhub transformer mapping).
pub fn outbound_format_for_channel_type(ct: &str) -> &str {
    match ct {
        "claudecode" => "claude/messages",
        "codex" | "github_copilot" => "openai/chat_completions",
        other => other,
    }
}

/// Feed one upstream SSE event through both transformers; returns false-y Err
/// when the stream should abort (decode error). If the receiving client is
/// gone the send fails silently and we keep draining for billing.
async fn forward_event(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    ev: &SseEvent,
    inbound: &dyn InboundTransformer,
    outbound: &dyn OutboundTransformer,
    usage: &mut Option<Usage>,
    completion_text: &mut String,
) -> Result<(), String> {
    match outbound.transform_stream_event(ev) {
        Err(e) => Err(format!("upstream stream decode: {e}")),
        Ok(chunks) => {
            for chunk in chunks {
                if let Some(u) = &chunk.usage {
                    *usage = Some(u.clone());
                }
                accumulate_completion(&chunk, completion_text);
                let events = inbound
                    .transform_stream_chunk(&chunk)
                    .map_err(|e| format!("client stream encode: {e}"))?;
                for se in events {
                    if tx.send(Ok(Bytes::from(se.encode()))).await.is_err() {
                        // Client gone; keep consuming to finish billing.
                        return Ok(());
                    }
                }
            }
            Ok(())
        }
    }
}

/// Accumulates streamed delta text (content + tool-call arguments) for the
/// usage fallback, bounded by the 256KB cap.
fn accumulate_completion(chunk: &StreamChunk, buf: &mut String) {
    for ch in &chunk.choices {
        if let Some(t) = &ch.delta.content {
            crate::token_estimate::push_capped(buf, t);
        }
        if let Some(calls) = &ch.delta.tool_calls {
            for c in calls {
                crate::token_estimate::push_capped(buf, &c.function.arguments);
            }
        }
    }
}

/// Shared billing writer. Price lookup uses the model as the client requested
/// it (pre-mapping), channel-specific row first, then global (repo behavior).
/// If no price row (or unparseable price) exists, the log is still written
/// with cost 0.
async fn write_billing(
    pool: &Db,
    api_key_id: Option<&str>,
    channel: Option<&Channel>,
    requested_model: &str,
    stream: bool,
    usage: &Usage,
    status: &str,
    started: Instant,
    request_id: &str,
) {
    let mut cost = Decimal::ZERO;
    let mut cost_items: serde_json::Value = serde_json::json!([]);
    if let Some(channel) = channel {
        if let Ok(Some(price_row)) =
            ModelPriceRepo::find(pool, Some(&channel.id), requested_model).await
        {
            if let Ok(price) = serde_json::from_str::<ModelPrice>(&price_row.price) {
                let breakdown = pricing::compute_cost(usage, &price);
                cost = breakdown.total;
                cost_items = serde_json::to_value(
                    breakdown
                        .items
                        .iter()
                        .map(|i| {
                            json!({
                                "code": i.code,
                                "quantity": i.quantity,
                                "amount": pricing::format_cost(&i.amount),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap_or_default();
            }
        }
    }
    let log = UsageLog {
        id: Uuid::new_v4().to_string(),
        request_id: request_id.to_string(),
        api_key_id: api_key_id.map(str::to_string),
        channel_id: channel.map(|c| c.id.clone()),
        model: requested_model.to_string(),
        stream,
        prompt_tokens: usage.prompt_tokens as i64,
        completion_tokens: usage.completion_tokens as i64,
        cached_tokens: usage.cached_tokens.unwrap_or(0) as i64,
        reasoning_tokens: usage.reasoning_tokens.unwrap_or(0) as i64,
        total_tokens: usage.total_tokens as i64,
        cost: pricing::format_cost(&cost),
        cost_items: cost_items.to_string(),
        status: status.to_string(),
        latency_ms: started.elapsed().as_millis() as i64,
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    if let Err(e) = UsageLogRepo::insert(pool, &log).await {
        tracing::error!("usage log insert failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::outbound_format_for_channel_type;

    #[test]
    fn outbound_format_mapping() {
        assert_eq!(outbound_format_for_channel_type("claudecode"), "claude/messages");
        assert_eq!(outbound_format_for_channel_type("codex"), "openai/chat_completions");
        assert_eq!(outbound_format_for_channel_type("github_copilot"), "openai/chat_completions");
        // identity passthrough for every other channel type
        assert_eq!(outbound_format_for_channel_type("openai"), "openai");
        assert_eq!(outbound_format_for_channel_type("gemini/gemini-pro"), "gemini/gemini-pro");
    }

    #[test]
    fn channel_failure_counts_5xx_and_network_only() {
        use super::UpstreamFailure;
        let f = |status| UpstreamFailure { status, msg: String::new() };
        assert!(UpstreamFailure::network("boom").counts_as_channel_failure());
        assert!(f(Some(500)).counts_as_channel_failure());
        assert!(f(Some(529)).counts_as_channel_failure());
        // 4xx client errors are request problems, not channel health.
        assert!(!f(Some(400)).counts_as_channel_failure());
        assert!(!f(Some(401)).counts_as_channel_failure());
        assert!(!f(Some(404)).counts_as_channel_failure());
        assert!(!f(Some(429)).counts_as_channel_failure());
    }
}
