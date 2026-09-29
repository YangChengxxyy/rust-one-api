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
use llm::{Request, Usage};
use rust_decimal::Decimal;
use std::str::FromStr;
use serde_json::json;
use uuid::Uuid;

use crate::error::AppError;
use crate::pricing::{self, ModelPrice};
use chrono::Utc;
use crate::provider_quota::credentials::ChannelCredentials;
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
        for channel in &candidates {
            match self
                .try_channel(channel, &req, &requested_model, api_key, inbound_format)
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(msg) => {
                    tracing::warn!(channel = %channel.id, "upstream attempt failed: {msg}");
                    last_err = msg;
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
        )
        .await;
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

    /// One attempt against one channel. `Err` = retryable failure message;
    /// `Ok(RelayOutcome::Json{4xx})` = upstream client error surfaced in the
    /// client's own wire format (already logged, no retry).
    async fn try_channel(
        &self,
        channel: &Channel,
        req: &Request,
        requested_model: &str,
        api_key: &ApiKey,
        inbound_format: &str,
    ) -> Result<RelayOutcome, String> {
        // Fresh inbound instance per attempt: instances are per-request and
        // may be stateful (e.g. Anthropic content_block lifecycle).
        let inbound = create_inbound_used(inbound_format)
            .ok_or_else(|| format!("unknown inbound format {inbound_format}"))?;
        // Apply model_mapping: requested -> upstream model name.
        let mapping: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&channel.model_mapping).unwrap_or_default();
        let mut upstream_req = req.clone();
        if let Some(mapped) = mapping.get(&req.model).and_then(|v| v.as_str()) {
            upstream_req.model = mapped.to_string();
        }

        let creds: serde_json::Value =
            serde_json::from_str(&channel.credentials).unwrap_or(json!({}));
        // Prefer the first still-serving key when multiple api_keys rotate.
        let creds_typed = ChannelCredentials::parse(&channel.credentials);
        let upstream_key = crate::provider_quota::credentials::serving_api_keys(&creds_typed, channel)
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                creds
                    .get("api_key")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            });

        let outbound = create_outbound(&channel.channel_type)
            .ok_or_else(|| format!("unknown channel type {}", channel.channel_type))?;
        let out_req = outbound
            .build_request(&upstream_req, &Credentials { api_key: upstream_key })
            .map_err(|e| format!("build upstream request: {e}"))?;

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
            .map_err(|e| format!("network: {e}"))?;

        let status = resp.status().as_u16();
        if status >= 500 {
            let body = resp.bytes().await.unwrap_or_default();
            return Err(format!("upstream {status}: {}", String::from_utf8_lossy(&body)));
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
            )
            .await;
            return Ok(RelayOutcome::Json { status, body: client_body });
        }

        if upstream_req.stream {
            self.stream_response(resp, inbound, outbound, channel, requested_model, api_key)
                .await
        } else {
            self.json_response(resp, inbound, outbound, channel, requested_model, api_key)
                .await
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
    ) -> Result<RelayOutcome, String> {
        let started = Instant::now();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("read upstream body: {e}"))?;
        let unified = outbound
            .transform_response(&body)
            .map_err(|e| format!("bad upstream response: {e}"))?;
        let usage = unified.usage.clone().unwrap_or_default();
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
    ) -> Result<RelayOutcome, String> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
        let pool = self.pool.clone();
        let channel = channel.clone();
        let api_key_id = api_key.id.clone();
        let requested_model = requested_model.to_string();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut parser = SseParser::new();
            let mut last_usage = Usage::default();
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
            write_billing(
                &pool,
                Some(api_key_id.as_str()),
                Some(&channel),
                &requested_model,
                true,
                &last_usage,
                if ok { "success" } else { "failed" },
                started,
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

/// Feed one upstream SSE event through both transformers; returns false-y Err
/// when the stream should abort (decode error). If the receiving client is
/// gone the send fails silently and we keep draining for billing.
async fn forward_event(
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    ev: &SseEvent,
    inbound: &dyn InboundTransformer,
    outbound: &dyn OutboundTransformer,
    usage: &mut Usage,
) -> Result<(), String> {
    match outbound.transform_stream_event(ev) {
        Err(e) => Err(format!("upstream stream decode: {e}")),
        Ok(chunks) => {
            for chunk in chunks {
                if let Some(ref u) = chunk.usage {
                    *usage = u.clone();
                }
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
        request_id: Uuid::new_v4().to_string(),
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
