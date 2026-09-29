//! Axum HTTP server: relay endpoints, admin API, health check.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::config::Config;
use crate::error::AppError;
use crate::oauth;
use crate::orchestrator::{Relay, RelayOutcome};
use crate::provider_quota::check_channel;
use crate::storage::{
    ApiKey, ApiKeyRepo, Channel, ChannelRepo, Db, ModelPriceRepo, ProviderQuotaRepo,
    UsageLogRepo,
};

struct AppState {
    relay: Relay,
    pool: Db,
    admin_token: Option<String>,
    http: reqwest::Client,
}

pub async fn run(cfg: Config, pool: Db) -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    let state = Arc::new(AppState {
        relay: Relay::new(pool.clone()),
        pool,
        admin_token: cfg.admin_token.clone(),
        http,
    });

    let relay_routes = Router::new()
        .route("/v1/chat/completions", post(relay_openai))
        .route("/v1/messages", post(relay_claude))
        .route("/anthropic/v1/messages", post(relay_claude))
        .route("/gemini/{ver}/models/{model_action}", post(relay_gemini))
        .route("/v1/models", get(list_models));

    let admin_routes = Router::new()
        .route("/channels", post(admin_create_channel).get(admin_list_channels))
        .route(
            "/channels/{id}",
            get(admin_get_channel).put(admin_update_channel).delete(admin_delete_channel),
        )
        .route("/keys", post(admin_create_key).get(admin_list_keys))
        .route("/keys/{id}", delete(admin_delete_key))
        .route("/prices", post(admin_upsert_price).get(admin_list_prices))
        .route("/prices/{id}", delete(admin_delete_price))
        .route("/usage", get(admin_usage))
        .route("/quota", get(admin_quota))
        .route("/quota/check", post(admin_quota_check))
        .route("/channels/{id}/quota/resets", get(admin_channel_quota_resets))
        .route("/channels/{id}/quota/reset", post(admin_channel_quota_reset))
        .route("/oauth/claudecode/start", post(admin_oauth_claude_start))
        .route("/oauth/claudecode/exchange", post(admin_oauth_claude_exchange))
        .route("/oauth/codex/start", post(admin_oauth_codex_start))
        .route("/oauth/codex/exchange", post(admin_oauth_codex_exchange))
        .route("/oauth/codex/decode", post(admin_oauth_codex_decode))
        .route("/oauth/copilot/start", post(admin_oauth_copilot_start))
        .route("/oauth/copilot/poll", post(admin_oauth_copilot_poll))
        .layer(middleware::from_fn_with_state(state.clone(), admin_guard));

    let app = Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .merge(relay_routes)
        .nest("/admin", admin_routes)
        .layer(axum::extract::DefaultBodyLimit::max(32 * 1024 * 1024))
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    tracing::info!("listening on {}", cfg.listen);
    axum::serve(listener, app).await?;
    Ok(())
}

// ---------- relay ----------

/// API key from `Authorization: Bearer`, then `x-api-key`, then `?key=`.
fn extract_api_key(headers: &HeaderMap, query_key: Option<&String>) -> Option<String> {
    if let Some(auth) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return Some(token.trim().to_string());
        }
    }
    if let Some(k) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(k.to_string());
    }
    query_key.map(|k| k.trim().to_string())
}

async fn authenticate(state: &AppState, headers: &HeaderMap, query_key: Option<&String>) -> Result<ApiKey, AppError> {
    let key = extract_api_key(headers, query_key)
        .ok_or_else(|| AppError::unauthorized("missing api key"))?;
    let api_key = ApiKeyRepo::get_by_key(&state.pool, &key)
        .await?
        .ok_or_else(|| AppError::unauthorized("invalid api key"))?;
    if api_key.status != "enabled" {
        return Err(AppError::unauthorized("api key disabled"));
    }
    if let Some(exp) = &api_key.expired_at {
        if exp.as_str() < chrono::Utc::now().to_rfc3339().as_str() {
            return Err(AppError::unauthorized("api key expired"));
        }
    }
    Ok(api_key)
}

fn outcome_to_response(outcome: RelayOutcome) -> Response {
    match outcome {
        RelayOutcome::Json { status, body } => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        RelayOutcome::Stream(stream) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(axum::body::Body::from_stream(stream))
            .unwrap(),
    }
}

async fn relay_openai(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: Request,
) -> Result<Response, AppError> {
    let body = axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024)
        .await
        .map_err(|e| AppError::bad_request(format!("read body: {e}")))?;
    let api_key = authenticate(&state, &headers, None).await?;
    let outcome = state
        .relay
        .relay("openai/chat_completions", None, false, &api_key, &body)
        .await?;
    Ok(outcome_to_response(outcome))
}

/// Pure model aggregation: union of supported_models JSON arrays and
/// model_mapping JSON keys across the given channels (callers pass enabled
/// channels only), deduped and sorted.
fn aggregate_models(channels: &[Channel]) -> Vec<String> {
    let mut models = std::collections::BTreeSet::new();
    for ch in channels {
        if let Ok(list) = serde_json::from_str::<Vec<String>>(&ch.supported_models) {
            models.extend(list);
        }
        if let Ok(map) =
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&ch.model_mapping)
        {
            models.extend(map.keys().cloned());
        }
    }
    models.into_iter().collect()
}

async fn list_models(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    authenticate(&state, &headers, None).await?;
    let channels: Vec<Channel> = ChannelRepo::list(&state.pool)
        .await?
        .into_iter()
        .filter(|ch| ch.status == "enabled")
        .collect();
    let data: Vec<Value> = aggregate_models(&channels)
        .into_iter()
        .map(|id| json!({"id": id, "object": "model", "created": 0, "owned_by": "rust-one-api"}))
        .collect();
    Ok(Json(json!({"object": "list", "data": data})).into_response())
}

async fn relay_claude(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: Request,
) -> Result<Response, AppError> {
    let body = axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024)
        .await
        .map_err(|e| AppError::bad_request(format!("read body: {e}")))?;
    let api_key = authenticate(&state, &headers, None).await?;
    let outcome = state
        .relay
        .relay("claude/messages", None, false, &api_key, &body)
        .await?;
    Ok(outcome_to_response(outcome))
}

async fn relay_gemini(
    State(state): State<Arc<AppState>>,
    Path((_ver, model_action)): Path<(String, String)>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    req: Request,
) -> Result<Response, AppError> {
    // model_action is `{model}` or `{model}:streamGenerateContent` / `:generateContent`.
    let (model, action) = match model_action.split_once(':') {
        Some((m, a)) => (m.to_string(), a.to_string()),
        None => (model_action.clone(), String::new()),
    };
    let force_stream = action == "streamGenerateContent";
    let body = axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024)
        .await
        .map_err(|e| AppError::bad_request(format!("read body: {e}")))?;
    let api_key = authenticate(&state, &headers, query.get("key")).await?;
    let outcome = state
        .relay
        .relay("gemini/models", Some(model), force_stream, &api_key, &body)
        .await?;
    Ok(outcome_to_response(outcome))
}

// ---------- admin ----------

async fn admin_guard(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let expected = state
        .admin_token
        .as_ref()
        .ok_or_else(|| AppError::forbidden("admin api disabled"))?;
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if provided != expected {
        return Err(AppError::forbidden("invalid admin token"));
    }
    Ok(next.run(req).await)
}

fn require<'a>(body: &'a Value, field: &str) -> Result<&'a str, AppError> {
    body.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::bad_request(format!("missing field {field}")))
}

fn channel_to_json(ch: &Channel) -> Value {
    json!({
        "id": ch.id, "name": ch.name, "channel_type": ch.channel_type,
        "base_url": ch.base_url, "credentials": credentials_masked(ch),
        "supported_models": ch.supported_models, "model_mapping": ch.model_mapping,
        "weight": ch.weight, "status": ch.status, "settings": ch.settings,
        "created_at": ch.created_at, "updated_at": ch.updated_at,
    })
}

fn credentials_masked(ch: &Channel) -> Value {
    let mut v: Value = serde_json::from_str(&ch.credentials).unwrap_or(json!({}));
    if let Some(obj) = v.get_mut("api_key") {
        if let Some(s) = obj.as_str().map(str::to_string) {
            *obj = Value::String(if s.len() > 6 { format!("{}…", &s[..6]) } else { "…".into() });
        }
    }
    v
}

async fn admin_create_channel(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let name = require(&body, "name")?.to_string();
    let channel_type = require(&body, "channel_type")?.to_string();
    let base_url = require(&body, "base_url")?.to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let ch = Channel {
        id: Uuid::new_v4().to_string(),
        name,
        channel_type,
        base_url,
        credentials: body.get("credentials").map(|v| v.to_string()).unwrap_or_else(|| "{}".into()),
        disabled_api_keys: body.get("disabled_api_keys").map(|v| v.to_string()).unwrap_or_else(|| "[]".into()),
        supported_models: body
            .get("supported_models")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "[]".into()),
        model_mapping: body.get("model_mapping").map(|v| v.to_string()).unwrap_or_else(|| "{}".into()),
        weight: body.get("weight").and_then(|v| v.as_i64()).unwrap_or(0),
        status: "enabled".into(),
        settings: "{}".into(),
        created_at: now.clone(),
        updated_at: now,
    };
    ChannelRepo::insert(&state.pool, &ch).await?;
    Ok((StatusCode::CREATED, Json(json!({"id": ch.id}))).into_response())
}

async fn admin_list_channels(State(state): State<Arc<AppState>>) -> Result<Response, AppError> {
    let rows = ChannelRepo::list(&state.pool).await?;
    Ok(Json(json!(rows.iter().map(channel_to_json).collect::<Vec<_>>())).into_response())
}

async fn admin_get_channel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, AppError> {
    match ChannelRepo::get(&state.pool, &id).await? {
        Some(ch) => Ok(Json(channel_to_json(&ch)).into_response()),
        None => Err(AppError::not_found("channel not found")),
    }
}

async fn admin_update_channel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let mut ch = ChannelRepo::get(&state.pool, &id)
        .await?
        .ok_or_else(|| AppError::not_found("channel not found"))?;
    for field in ["name", "channel_type", "base_url", "credentials", "disabled_api_keys", "supported_models", "model_mapping", "status", "settings"] {
        if let Some(v) = body.get(field) {
            let s = match v.as_str() {
                Some(s) => s.to_string(),
                None => v.to_string(),
            };
            match field {
                "name" => ch.name = s,
                "channel_type" => ch.channel_type = s,
                "base_url" => ch.base_url = s,
                "credentials" => ch.credentials = s,
                "disabled_api_keys" => ch.disabled_api_keys = s,
                "supported_models" => ch.supported_models = s,
                "model_mapping" => ch.model_mapping = s,
                "status" => ch.status = s,
                "settings" => ch.settings = s,
                _ => unreachable!(),
            }
        }
    }
    if let Some(w) = body.get("weight").and_then(|v| v.as_i64()) {
        ch.weight = w;
    }
    ch.updated_at = chrono::Utc::now().to_rfc3339();
    ChannelRepo::update(&state.pool, &ch).await?;
    Ok(Json(channel_to_json(&ch)).into_response())
}

async fn admin_delete_channel(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, AppError> {
    ChannelRepo::delete(&state.pool, &id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admin_create_key(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let key = body
        .get("key")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("sk-{}", Uuid::new_v4().simple()));
    let now = chrono::Utc::now().to_rfc3339();
    let k = ApiKey {
        id: Uuid::new_v4().to_string(),
        key: key.clone(),
        name: body
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        status: "enabled".into(),
        quota: body.get("quota").map(|v| v.to_string()).unwrap_or_else(|| "{}".into()),
        expired_at: body
            .get("expired_at")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        created_at: now.clone(),
        updated_at: now,
    };
    ApiKeyRepo::insert(&state.pool, &k).await?;
    Ok((StatusCode::CREATED, Json(json!({"id": k.id, "key": key}))).into_response())
}

async fn admin_list_keys(State(state): State<Arc<AppState>>) -> Result<Response, AppError> {
    let rows = ApiKeyRepo::list(&state.pool).await?;
    Ok(Json(json!(rows
        .iter()
        .map(|k| json!({
            "id": k.id, "key": k.key, "name": k.name, "status": k.status,
            "quota": k.quota, "expired_at": k.expired_at,
            "created_at": k.created_at, "updated_at": k.updated_at,
        }))
        .collect::<Vec<_>>()))
    .into_response())
}

async fn admin_delete_key(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, AppError> {
    ApiKeyRepo::delete(&state.pool, &id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admin_upsert_price(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let model = require(&body, "model")?;
    let price = match body.get("price") {
        Some(Value::String(s)) => s.clone(),
        Some(v @ Value::Object(_)) => v.to_string(),
        _ => return Err(AppError::bad_request("missing field price")),
    };
    let channel_id = body.get("channel_id").and_then(|v| v.as_str());
    let reference_id = ModelPriceRepo::upsert(&state.pool, channel_id, model, &price).await?;
    Ok((StatusCode::CREATED, Json(json!({"reference_id": reference_id}))).into_response())
}

async fn admin_list_prices(State(state): State<Arc<AppState>>) -> Result<Response, AppError> {
    let rows = ModelPriceRepo::list(&state.pool).await?;
    Ok(Json(json!(rows
        .iter()
        .map(|p| json!({
            "id": p.id, "channel_id": p.channel_id, "model": p.model,
            "price": p.price, "reference_id": p.reference_id,
            "created_at": p.created_at, "updated_at": p.updated_at,
        }))
        .collect::<Vec<_>>()))
    .into_response())
}

async fn admin_delete_price(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, AppError> {
    ModelPriceRepo::delete(&state.pool, &id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admin_usage(
    State(state): State<Arc<AppState>>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, AppError> {
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(100);
    let logs = UsageLogRepo::list_filtered(
        &state.pool,
        q.get("api_key_id").map(|s| s.as_str()),
        q.get("channel_id").map(|s| s.as_str()),
        q.get("since").map(|s| s.as_str()),
        limit,
    )
    .await?;
    Ok(Json(json!(logs
        .iter()
        .map(|l| json!({
            "id": l.id, "request_id": l.request_id, "api_key_id": l.api_key_id,
            "channel_id": l.channel_id, "model": l.model, "stream": l.stream,
            "prompt_tokens": l.prompt_tokens, "completion_tokens": l.completion_tokens,
            "cached_tokens": l.cached_tokens, "reasoning_tokens": l.reasoning_tokens,
            "total_tokens": l.total_tokens, "cost": l.cost,
            "cost_items": l.cost_items, "status": l.status,
            "latency_ms": l.latency_ms, "created_at": l.created_at,
        }))
        .collect::<Vec<_>>()))
    .into_response())
}

async fn admin_quota(
    State(state): State<Arc<AppState>>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, AppError> {
    let rows = match q.get("channel_id") {
        Some(cid) => ProviderQuotaRepo::list_for_channel(&state.pool, cid).await?,
        None => {
            let mut all = Vec::new();
            for ch in ChannelRepo::list(&state.pool).await? {
                all.extend(ProviderQuotaRepo::list_for_channel(&state.pool, &ch.id).await?);
            }
            all
        }
    };
    Ok(Json(json!(rows
        .iter()
        .map(|r| json!({
            "id": r.id, "channel_id": r.channel_id, "provider_type": r.provider_type,
            "account_key": r.account_key, "status": r.status, "quota_data": r.quota_data,
            "next_check_at": r.next_check_at,
            "created_at": r.created_at, "updated_at": r.updated_at,
        }))
        .collect::<Vec<_>>()))
    .into_response())
}

async fn admin_quota_check(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let channels = match body.get("channel_id").and_then(|v| v.as_str()) {
        Some(cid) => vec![ChannelRepo::get(&state.pool, cid)
            .await?
            .ok_or_else(|| AppError::not_found("channel not found"))?],
        None => ChannelRepo::list(&state.pool)
            .await?
            .into_iter()
            .filter(|ch| ch.status == "enabled")
            .collect(),
    };
    let mut results = Vec::new();
    for ch in channels {
        let res = check_channel(&state.pool, &state.http, &ch).await;
        let status = ProviderQuotaRepo::list_for_channel(&state.pool, &ch.id)
            .await?
            .into_iter()
            .find(|r| r.account_key.is_empty());
        results.push(json!({
            "channel_id": ch.id,
            "ok": res.is_ok(),
            "error": res.err().map(|e| e.to_string()),
            "status": status.map(|r| r.status),
        }));
    }
    Ok(Json(json!(results)).into_response())
}

async fn admin_channel_quota_resets(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<String>,
) -> Result<Response, AppError> {
    let channel = ChannelRepo::get(&state.pool, &channel_id)
        .await?
        .ok_or_else(|| AppError::not_found("channel not found"))?;
    let checker = crate::provider_quota::checkers::checker_for_channel(&channel)
        .ok_or_else(|| AppError::bad_request("provider quota reset is not supported"))?;
    let resetter = checker
        .as_resetter()
        .ok_or_else(|| AppError::bad_request("provider quota reset is not supported"))?;

    let creds = crate::provider_quota::credentials::ChannelCredentials::parse(&channel.credentials);
    let list = resetter
        .list_resets(&state.http, &channel, &creds)
        .await
        .map_err(|e| AppError::upstream(e.to_string()))?;
    Ok(Json(serde_json::to_value(&list).unwrap_or(Value::Null)).into_response())
}

async fn admin_channel_quota_reset(
    State(state): State<Arc<AppState>>,
    Path(channel_id): Path<String>,
) -> Result<Response, AppError> {
    let channel = ChannelRepo::get(&state.pool, &channel_id)
        .await?
        .ok_or_else(|| AppError::not_found("channel not found"))?;
    let checker = crate::provider_quota::checkers::checker_for_channel(&channel)
        .ok_or_else(|| AppError::bad_request("provider quota reset is not supported"))?;
    let resetter = checker
        .as_resetter()
        .ok_or_else(|| AppError::bad_request("provider quota reset is not supported"))?;

    let creds = crate::provider_quota::credentials::ChannelCredentials::parse(&channel.credentials);
    resetter.reset(&state.http, &channel, &creds).await.map_err(|e| AppError::upstream(e.to_string()))?;
    Ok(Json(json!({"ok": true})).into_response())
}

// ---------- admin oauth ----------

fn bad(msg: impl Into<String>) -> AppError {
    AppError::bad_request(msg.into())
}

/// Attaches the OAuth result to `channel_id` (merge `credentials.oauth` /
/// set `credentials.api_key`) and persists via ChannelRepo::update.
async fn attach_oauth(
    state: &AppState,
    channel_id: &str,
    f: impl FnOnce(&mut Channel),
) -> Result<(), AppError> {
    let mut ch = ChannelRepo::get(&state.pool, channel_id)
        .await?
        .ok_or_else(|| bad(format!("channel {channel_id} not found")))?;
    f(&mut ch);
    ChannelRepo::update(&state.pool, &ch).await?;
    Ok(())
}

async fn admin_oauth_claude_start(
    State(_state): State<Arc<AppState>>,
) -> Result<Response, AppError> {
    let session_id = oauth::new_state();
    let (verifier, challenge) = oauth::pkce_pair();
    oauth::put_session(
        &session_id,
        oauth::OAuthSession {
            code_verifier: Some(verifier),
            device: None,
            created_at: chrono::Utc::now(),
        },
    );
    Ok(Json(json!({
        "session_id": session_id,
        "auth_url": oauth::claude_auth_url(&session_id, &challenge),
    }))
    .into_response())
}

async fn admin_oauth_claude_exchange(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let session_id = require(&body, "session_id")?;
    let callback_url = require(&body, "callback_url")?;
    let session = oauth::get_session(session_id)
        .ok_or_else(|| bad("unknown or expired session"))?;
    let verifier = session
        .code_verifier
        .ok_or_else(|| bad("session has no code verifier"))?;
    let code = oauth::parse_callback_url(callback_url, session_id).map_err(bad)?;
    let set = oauth::exchange_code(&state.http, "claudecode", &code, &verifier, session_id)
        .await
        .map_err(|e| bad(format!("token exchange failed: {e}")))?;
    oauth::delete_session(session_id);
    let credentials = oauth::token_set_to_oauth_json(&set, Some(oauth::CLAUDE_CLIENT_ID));
    if let Some(cid) = body.get("channel_id").and_then(|v| v.as_str()) {
        let cred = credentials.clone();
        attach_oauth(&state, cid, move |ch| oauth::merge_oauth_into(ch, &cred)).await?;
    }
    Ok(Json(json!({"credentials": credentials})).into_response())
}

async fn admin_oauth_codex_start(
    State(_state): State<Arc<AppState>>,
) -> Result<Response, AppError> {
    let session_id = oauth::new_state();
    let (verifier, challenge) = oauth::pkce_pair();
    oauth::put_session(
        &session_id,
        oauth::OAuthSession {
            code_verifier: Some(verifier),
            device: None,
            created_at: chrono::Utc::now(),
        },
    );
    Ok(Json(json!({
        "session_id": session_id,
        "auth_url": oauth::codex_auth_url(&session_id, &challenge),
    }))
    .into_response())
}

async fn admin_oauth_codex_exchange(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let session_id = require(&body, "session_id")?;
    let callback_url = require(&body, "callback_url")?;
    let session = oauth::get_session(session_id)
        .ok_or_else(|| bad("unknown or expired session"))?;
    let verifier = session
        .code_verifier
        .ok_or_else(|| bad("session has no code verifier"))?;
    // codex: code + state both in the query string
    let code = oauth::parse_callback_url(callback_url, session_id).map_err(bad)?;
    let set = oauth::exchange_code(&state.http, "codex", &code, &verifier, session_id)
        .await
        .map_err(|e| bad(format!("token exchange failed: {e}")))?;
    oauth::delete_session(session_id);
    let credentials = oauth::token_set_to_oauth_json(&set, Some(oauth::CODEX_CLIENT_ID));
    if let Some(cid) = body.get("channel_id").and_then(|v| v.as_str()) {
        let cred = credentials.clone();
        attach_oauth(&state, cid, move |ch| oauth::merge_oauth_into(ch, &cred)).await?;
    }
    Ok(Json(json!({"credentials": credentials})).into_response())
}

/// Decodes a codex CLI `auth.json` pasted by the operator.
async fn admin_oauth_codex_decode(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let auth_json = require(&body, "auth_json")?;
    let set = oauth::decode_codex_auth_json(auth_json, chrono::Utc::now()).map_err(bad)?;
    let credentials = oauth::token_set_to_oauth_json(&set, Some(oauth::CODEX_CLIENT_ID));
    if let Some(cid) = body.get("channel_id").and_then(|v| v.as_str()) {
        let cred = credentials.clone();
        attach_oauth(&state, cid, move |ch| oauth::merge_oauth_into(ch, &cred)).await?;
    }
    Ok(Json(json!({"credentials": credentials})).into_response())
}

async fn admin_oauth_copilot_start(
    State(state): State<Arc<AppState>>,
) -> Result<Response, AppError> {
    let session_id = oauth::new_state();
    let (_device_code, device, resp) = oauth::copilot_device_start(&state.http)
        .await
        .map_err(|e| bad(format!("device flow start failed: {e}")))?;
    oauth::put_session(
        &session_id,
        oauth::OAuthSession {
            code_verifier: None,
            device: Some(device),
            created_at: chrono::Utc::now(),
        },
    );
    Ok(Json(json!({
        "session_id": session_id,
        "user_code": resp.get("user_code").and_then(|v| v.as_str()),
        "verification_uri": resp.get("verification_uri").and_then(|v| v.as_str()),
        "expires_in": resp.get("expires_in").and_then(|v| v.as_i64()),
        "interval": resp.get("interval").and_then(|v| v.as_i64()),
    }))
    .into_response())
}

async fn admin_oauth_copilot_poll(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let session_id = require(&body, "session_id")?;
    let session = oauth::get_session(session_id)
        .ok_or_else(|| bad("unknown or expired session"))?;
    let device = session
        .device
        .ok_or_else(|| bad("session is not a device flow"))?;
    let result = oauth::copilot_device_poll(&state.http, &device)
        .await
        .map_err(|e| bad(format!("device poll failed: {e}")))?;
    match result {
        Ok(token) => {
            oauth::delete_session(session_id);
            if let Some(cid) = body.get("channel_id").and_then(|v| v.as_str()) {
                let t = token.clone();
                attach_oauth(&state, cid, move |ch| oauth::set_api_key_into(ch, &t)).await?;
            }
            Ok(Json(json!({"status": "complete", "access_token": token})).into_response())
        }
        Err(oauth::DevicePollStatus::Pending) => {
            Ok(Json(json!({"status": "pending"})).into_response())
        }
        Err(oauth::DevicePollStatus::SlowDown) => {
            Ok(Json(json!({"status": "slow_down"})).into_response())
        }
        Err(oauth::DevicePollStatus::Expired) => {
            oauth::delete_session(session_id);
            Ok(Json(json!({"status": "pending", "message": "device code expired; restart the flow"})).into_response())
        }
        Err(oauth::DevicePollStatus::Denied) => {
            oauth::delete_session(session_id);
            Ok(Json(json!({"status": "pending", "message": "access denied; restart the flow"})).into_response())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::aggregate_models;
    use crate::storage::Channel;

    fn ch(supported: &str, mapping: &str) -> Channel {
        Channel {
            id: "c".into(),
            name: "c".into(),
            channel_type: "openai".into(),
            base_url: "http://x".into(),
            credentials: "{}".into(),
            disabled_api_keys: "[]".into(),
            supported_models: supported.into(),
            model_mapping: mapping.into(),
            weight: 1,
            status: "enabled".into(),
            settings: "{}".into(),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[test]
    fn aggregate_dedupes_and_sorts() {
        let channels = vec![
            ch(r#"["gpt-4o","claude-3"]"#, r#"{"alias-a": "up"}"#),
            ch(r#"["gpt-4o"]"#, r#"{"alias-b": "up", "alias-a": "up2"}"#),
        ];
        assert_eq!(
            aggregate_models(&channels),
            vec!["alias-a", "alias-b", "claude-3", "gpt-4o"]
        );
    }

    #[test]
    fn aggregate_handles_mapping_only_and_garbage() {
        // Mapping keys count even with an empty supported list; malformed
        // JSON contributes nothing instead of failing.
        let channels = vec![
            ch("[]", r#"{"mapped": "up"}"#),
            ch("not json", "also not json"),
        ];
        assert_eq!(aggregate_models(&channels), vec!["mapped"]);
    }

    #[test]
    fn aggregate_excludes_channels_not_passed_in() {
        // Disabled-channel exclusion is the caller's filter; anything not
        // passed simply does not contribute.
        let enabled = vec![ch(r#"["m1"]"#, "{}")];
        let all = vec![ch(r#"["m1"]"#, "{}"), ch(r#"["m2"]"#, "{}")];
        assert_eq!(aggregate_models(&enabled), vec!["m1"]);
        assert_eq!(aggregate_models(&all), vec!["m1", "m2"]);
    }
}
