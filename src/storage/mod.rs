//! Storage layer: connection setup, migrations, row structs, and repos.
//! All SQL uses `?` placeholders (sqlx Any rewrites them for postgres).

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use sqlx::any::Any;
use sqlx::pool::PoolOptions;
use sqlx::{FromRow, Pool, Row};
use std::path::PathBuf;
use uuid::Uuid;

pub type AnyPool = Pool<Any>;
pub type Db = AnyPool;

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn new_id() -> String {
    Uuid::new_v4().to_string()
}

/// Connect to the database described by `database_url`.
/// sqlite URLs get create-if-missing plus WAL/foreign_keys pragmas.
pub async fn connect(database_url: &str) -> Result<Db> {
    sqlx::any::install_default_drivers();

    if database_url.starts_with("sqlite:") {
        // sqlite://path -> sqlite:path?mode=rwc (create if missing)
        let url = if let Some(rest) = database_url.strip_prefix("sqlite://") {
            let rest = rest.trim_start_matches("//");
            format!("sqlite:{rest}?mode=rwc")
        } else {
            database_url.to_string()
        };
        let pool = PoolOptions::<Any>::new()
            .max_connections(1)
            .connect(&url)
            .await
            .context("connect sqlite")?;
        // Per-connection pragmas; pool is capped at 1 connection so they apply globally.
        sqlx::query("PRAGMA journal_mode=WAL;").execute(&pool).await?;
        sqlx::query("PRAGMA foreign_keys=ON;").execute(&pool).await?;
        Ok(pool)
    } else {
        let opts = if database_url.starts_with("postgres") {
            PoolOptions::<Any>::new().max_connections(10)
        } else {
            PoolOptions::<Any>::new()
        };
        Ok(opts.connect(database_url).await.context("connect database")?)
    }
}

/// Run migrations from migrations/{sqlite|postgres}, resolved relative to CARGO_MANIFEST_DIR.
pub async fn migrate(pool: &AnyPool) -> Result<()> {
    let scheme = pool.connect_options().database_url.scheme().to_string();
    let subdir = if scheme == "postgres" { "postgres" } else { "sqlite" };
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations").join(subdir);
    let migrator = sqlx::migrate::Migrator::new(dir).await?;
    // Any pool: run migrations on each connection's level via the migrate extension
    migrator.run(pool).await?;
    Ok(())
}

// ---------- Row structs ----------

#[derive(Debug, Clone, Default)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub channel_type: String,
    pub base_url: String,
    pub credentials: String,
    pub disabled_api_keys: String,
    pub supported_models: String,
    pub model_mapping: String,
    pub weight: i64,
    pub status: String,
    pub settings: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ApiKey {
    pub id: String,
    pub key: String,
    pub name: String,
    pub status: String,
    pub quota: String,
    pub expired_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ModelPrice {
    pub id: String,
    pub channel_id: Option<String>,
    pub model: String,
    pub price: String,
    pub reference_id: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UsageLog {
    pub id: String,
    pub request_id: String,
    pub api_key_id: Option<String>,
    pub channel_id: Option<String>,
    pub model: String,
    pub stream: bool,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub cost: String,
    pub cost_items: String,
    pub status: String,
    pub latency_ms: i64,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct ProviderQuotaStatus {
    pub id: String,
    pub channel_id: String,
    pub provider_type: String,
    pub account_key: String,
    pub status: String,
    pub quota_data: String,
    pub next_check_at: Option<String>,
    pub next_reset_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

// sqlx's FromRow derive requires the (disabled) "macros" feature, so we
// implement FromRow for AnyRow manually.
macro_rules! impl_from_row {
    ($t:ident { $($f:ident),+ $(,)? }) => {
        impl<'r> FromRow<'r, sqlx::any::AnyRow> for $t {
            fn from_row(row: &'r sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
                Ok($t { $($f: row.try_get(stringify!($f))?),+ })
            }
        }
    };
}

impl_from_row!(Channel { id, name, channel_type, base_url, credentials, disabled_api_keys, supported_models, model_mapping, weight, status, settings, created_at, updated_at });
impl_from_row!(ApiKey { id, key, name, status, quota, expired_at, created_at, updated_at });
impl_from_row!(ModelPrice { id, channel_id, model, price, reference_id, created_at, updated_at });
// UsageLog: sqlite stores stream as INTEGER (BIGINT kind to the Any driver),
// postgres as BOOLEAN; decode bool or int accordingly.
impl<'r> FromRow<'r, sqlx::any::AnyRow> for UsageLog {
    fn from_row(row: &'r sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        let stream = row
            .try_get::<bool, _>("stream")
            .or_else(|_| row.try_get::<i64, _>("stream").map(|v| v != 0))?;
        Ok(UsageLog {
            id: row.try_get("id")?,
            request_id: row.try_get("request_id")?,
            api_key_id: row.try_get("api_key_id")?,
            channel_id: row.try_get("channel_id")?,
            model: row.try_get("model")?,
            stream,
            prompt_tokens: row.try_get("prompt_tokens")?,
            completion_tokens: row.try_get("completion_tokens")?,
            cached_tokens: row.try_get("cached_tokens")?,
            reasoning_tokens: row.try_get("reasoning_tokens")?,
            total_tokens: row.try_get("total_tokens")?,
            cost: row.try_get("cost")?,
            cost_items: row.try_get("cost_items")?,
            status: row.try_get("status")?,
            latency_ms: row.try_get("latency_ms")?,
            created_at: row.try_get("created_at")?,
        })
    }
}
impl_from_row!(ProviderQuotaStatus { id, channel_id, provider_type, account_key, status, quota_data, next_check_at, next_reset_at, created_at, updated_at });

// ---------- ChannelRepo ----------

pub struct ChannelRepo;

impl ChannelRepo {
    pub async fn insert(pool: &AnyPool, ch: &Channel) -> Result<()> {
        sqlx::query(
            "INSERT INTO channels (id, name, channel_type, base_url, credentials, disabled_api_keys, supported_models, model_mapping, weight, status, settings, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(&ch.id)
        .bind(&ch.name)
        .bind(&ch.channel_type)
        .bind(&ch.base_url)
        .bind(&ch.credentials)
        .bind(&ch.disabled_api_keys)
        .bind(&ch.supported_models)
        .bind(&ch.model_mapping)
        .bind(ch.weight)
        .bind(&ch.status)
        .bind(&ch.settings)
        .bind(&ch.created_at)
        .bind(&ch.updated_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update(pool: &AnyPool, ch: &Channel) -> Result<()> {
        sqlx::query(
            "UPDATE channels SET name = $1, channel_type = $2, base_url = $3, credentials = $4, disabled_api_keys = $5, supported_models = $6, model_mapping = $7, weight = $8, status = $9, settings = $10, updated_at = $11 WHERE id = $12",
        )
        .bind(&ch.name)
        .bind(&ch.channel_type)
        .bind(&ch.base_url)
        .bind(&ch.credentials)
        .bind(&ch.disabled_api_keys)
        .bind(&ch.supported_models)
        .bind(&ch.model_mapping)
        .bind(ch.weight)
        .bind(&ch.status)
        .bind(&ch.settings)
        .bind(&ch.updated_at)
        .bind(&ch.id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn delete(pool: &AnyPool, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM channels WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn get(pool: &AnyPool, id: &str) -> Result<Option<Channel>> {
        sqlx::query_as::<_, Channel>("SELECT * FROM channels WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(Into::into)
    }

    pub async fn list(pool: &AnyPool) -> Result<Vec<Channel>> {
        sqlx::query_as::<_, Channel>("SELECT * FROM channels ORDER BY created_at")
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    /// Enabled channels supporting `model`: listed in supported_models JSON array,
    /// OR a key of model_mapping JSON equals model.
    /// JSON matching is done in Rust: neither sqlite nor postgres can query a
    /// JSON *object's keys* portably through the sqlx Any abstraction.
    pub async fn list_enabled_for_model(pool: &AnyPool, model: &str) -> Result<Vec<Channel>> {
        let enabled = sqlx::query_as::<_, Channel>(
            "SELECT * FROM channels WHERE status = 'enabled' ORDER BY created_at",
        )
        .fetch_all(pool)
        .await?;
        Ok(enabled
            .into_iter()
            .filter(|ch| {
                let supported: Vec<String> =
                    serde_json::from_str(&ch.supported_models).unwrap_or_default();
                if supported.iter().any(|m| m == model) {
                    return true;
                }
                let mapping: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(&ch.model_mapping).unwrap_or_default();
                mapping.contains_key(model)
            })
            .collect())
    }
}

// ---------- ApiKeyRepo ----------

pub struct ApiKeyRepo;

impl ApiKeyRepo {
    pub async fn insert(pool: &AnyPool, k: &ApiKey) -> Result<()> {
        sqlx::query(
            "INSERT INTO api_keys (id, key, name, status, quota, expired_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&k.id)
        .bind(&k.key)
        .bind(&k.name)
        .bind(&k.status)
        .bind(&k.quota)
        .bind(&k.expired_at)
        .bind(&k.created_at)
        .bind(&k.updated_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn get_by_key(pool: &AnyPool, key: &str) -> Result<Option<ApiKey>> {
        sqlx::query_as::<_, ApiKey>("SELECT * FROM api_keys WHERE key = $1")
            .bind(key)
            .fetch_optional(pool)
            .await
            .map_err(Into::into)
    }

    pub async fn update_status(pool: &AnyPool, id: &str, status: &str) -> Result<()> {
        sqlx::query("UPDATE api_keys SET status = $1, updated_at = $2 WHERE id = $3")
            .bind(status)
            .bind(now())
            .bind(id)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn list(pool: &AnyPool) -> Result<Vec<ApiKey>> {
        sqlx::query_as::<_, ApiKey>("SELECT * FROM api_keys ORDER BY created_at")
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    pub async fn delete(pool: &AnyPool, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM api_keys WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(())
    }
}

// ---------- ModelPriceRepo ----------

pub struct ModelPriceRepo;

impl ModelPriceRepo {
    /// Insert or update by (channel_id, model); on update bump reference_id to a new uuid.
    pub async fn upsert(pool: &AnyPool, channel_id: Option<&str>, model: &str, price: &str) -> Result<String> {
        let ts = now();
        let id = new_id();
        let reference_id = new_id();
        // COALESCE normalizes NULL channel_id so the unique index catches conflicts on both backends.
        let res = sqlx::query(
            "INSERT INTO model_prices (id, channel_id, model, price, reference_id, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (COALESCE(channel_id, ''), model) DO UPDATE SET \
             price = excluded.price, reference_id = excluded.reference_id, updated_at = excluded.updated_at",
        )
        .bind(&id)
        .bind(channel_id)
        .bind(model)
        .bind(price)
        .bind(&reference_id)
        .bind(&ts)
        .bind(&ts)
        .execute(pool)
        .await?;
        let _ = res;
        Ok(reference_id)
    }

    /// Channel-specific row first, then global (channel_id IS NULL).
    pub async fn find(pool: &AnyPool, channel_id: Option<&str>, model: &str) -> Result<Option<ModelPrice>> {
        if let Some(cid) = channel_id {
            let row = sqlx::query_as::<_, ModelPrice>(
                "SELECT * FROM model_prices WHERE channel_id = $1 AND model = $2",
            )
            .bind(cid)
            .bind(model)
            .fetch_optional(pool)
            .await?;
            if row.is_some() {
                return Ok(row);
            }
        }
        sqlx::query_as::<_, ModelPrice>(
            "SELECT * FROM model_prices WHERE channel_id IS NULL AND model = $1",
        )
        .bind(model)
        .fetch_optional(pool)
        .await
        .map_err(Into::into)
    }

    pub async fn list(pool: &AnyPool) -> Result<Vec<ModelPrice>> {
        sqlx::query_as::<_, ModelPrice>("SELECT * FROM model_prices ORDER BY model")
            .fetch_all(pool)
            .await
            .map_err(Into::into)
    }

    pub async fn delete(pool: &AnyPool, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM model_prices WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(())
    }
}

// ---------- UsageLogRepo ----------

pub struct UsageLogRepo;

impl UsageLogRepo {
    pub async fn insert(pool: &AnyPool, log: &UsageLog) -> Result<()> {
        sqlx::query(
            "INSERT INTO usage_logs (id, request_id, api_key_id, channel_id, model, stream, prompt_tokens, completion_tokens, cached_tokens, reasoning_tokens, total_tokens, cost, cost_items, status, latency_ms, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
        )
        .bind(&log.id)
        .bind(&log.request_id)
        .bind(&log.api_key_id)
        .bind(&log.channel_id)
        .bind(&log.model)
        .bind(log.stream)
        .bind(log.prompt_tokens)
        .bind(log.completion_tokens)
        .bind(log.cached_tokens)
        .bind(log.reasoning_tokens)
        .bind(log.total_tokens)
        .bind(&log.cost)
        .bind(&log.cost_items)
        .bind(&log.status)
        .bind(log.latency_ms)
        .bind(&log.created_at)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn list_filtered(
        pool: &AnyPool,
        api_key_id: Option<&str>,
        channel_id: Option<&str>,
        since: Option<&str>,
        limit: u32,
    ) -> Result<Vec<UsageLog>> {
        // $n placeholders: portable across postgres (native) and sqlite ($name form).
        let mut sql = String::from("SELECT * FROM usage_logs WHERE 1=1");
        let mut n = 0;
        if api_key_id.is_some() {
            n += 1;
            sql.push_str(&format!(" AND api_key_id = ${n}"));
        }
        if channel_id.is_some() {
            n += 1;
            sql.push_str(&format!(" AND channel_id = ${n}"));
        }
        if since.is_some() {
            n += 1;
            sql.push_str(&format!(" AND created_at >= ${n}"));
        }
        sql.push_str(&format!(" ORDER BY created_at DESC LIMIT ${}", n + 1));

        let mut q = sqlx::query_as::<Any, UsageLog>(&sql);
        if let Some(v) = api_key_id {
            q = q.bind(v);
        }
        if let Some(v) = channel_id {
            q = q.bind(v);
        }
        if let Some(v) = since {
            q = q.bind(v);
        }
        q = q.bind(limit as i64);
        q.fetch_all(pool).await.map_err(Into::into)
    }

    pub async fn aggregate_for_key(
        pool: &AnyPool,
        api_key_id: &str,
        since_rfc3339: &str,
    ) -> Result<(i64, i64, String)> {
        // SUM over TEXT cost would need a CAST that differs between backends;
        // instead fetch the cost column and sum in Rust with rust_decimal.
        let row: Vec<(i64, i64, String)> = sqlx::query_as(
            "SELECT total_tokens, 0, cost FROM usage_logs WHERE api_key_id = $1 AND created_at >= $2",
        )
        .bind(api_key_id)
        .bind(since_rfc3339)
        .fetch_all(pool)
        .await?;
        let mut count: i64 = 0;
        let mut tokens: i64 = 0;
        let mut cost = Decimal::ZERO;
        for (total, _, cost_text) in row {
            count += 1;
            tokens += total;
            if let Ok(d) = Decimal::from_str(&cost_text) {
                cost += d;
            }
        }
        Ok((count, tokens, cost.to_string()))
    }

    /// Total cost on one channel since a point in time (quota period_cost fill).
    pub async fn cost_for_channel_since(pool: &AnyPool, channel_id: &str, since_rfc3339: &str) -> Result<f64> {
        // Same portability reasoning as aggregate_for_key: sum TEXT costs in Rust.
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT cost FROM usage_logs WHERE channel_id = $1 AND created_at >= $2",
        )
        .bind(channel_id)
        .bind(since_rfc3339)
        .fetch_all(pool)
        .await?;
        let mut cost = Decimal::ZERO;
        for (text,) in rows {
            if let Ok(d) = Decimal::from_str(&text) {
                cost += d;
            }
        }
        Ok(cost.to_string().parse::<f64>().unwrap_or(0.0))
    }
}

// ---------- ProviderQuotaRepo ----------

pub struct ProviderQuotaRepo;

impl ProviderQuotaRepo {
    pub async fn upsert_status(
        pool: &AnyPool,
        channel_id: &str,
        provider_type: &str,
        account_key: &str,
        status: &str,
        quota_data: &str,
        next_check_at: Option<&str>,
        next_reset_at: Option<&str>,
    ) -> Result<()> {
        let ts = now();
        sqlx::query(
            "INSERT INTO provider_quota_status (id, channel_id, provider_type, account_key, status, quota_data, next_check_at, next_reset_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (channel_id, provider_type, account_key) DO UPDATE SET \
             status = excluded.status, quota_data = excluded.quota_data, next_check_at = excluded.next_check_at, next_reset_at = excluded.next_reset_at, updated_at = excluded.updated_at",
        )
        .bind(new_id())
        .bind(channel_id)
        .bind(provider_type)
        .bind(account_key)
        .bind(status)
        .bind(quota_data)
        .bind(next_check_at)
        .bind(next_reset_at)
        .bind(&ts)
        .bind(&ts)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Rows with next_check_at NULL or <= now (RFC3339 strings compare lexicographically).
    pub async fn list_due(pool: &AnyPool, now_rfc3339: &str) -> Result<Vec<ProviderQuotaStatus>> {
        sqlx::query_as::<_, ProviderQuotaStatus>(
            "SELECT * FROM provider_quota_status WHERE next_check_at IS NULL OR next_check_at <= $1",
        )
        .bind(now_rfc3339)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    }

    pub async fn list_for_channel(pool: &AnyPool, channel_id: &str) -> Result<Vec<ProviderQuotaStatus>> {
        sqlx::query_as::<_, ProviderQuotaStatus>(
            "SELECT * FROM provider_quota_status WHERE channel_id = $1",
        )
        .bind(channel_id)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
    }
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> Db {
        let pool = connect("sqlite::memory:").await.expect("connect");
        migrate(&pool).await.expect("migrate");
        pool
    }

    fn channel(id: &str, name: &str, supported: &str, mapping: &str, status: &str) -> Channel {
        let ts = now();
        Channel {
            id: id.to_string(),
            name: name.to_string(),
            channel_type: "openai".into(),
            base_url: "https://api.example.com".into(),
            credentials: "{}".into(),
            disabled_api_keys: "[]".into(),
            supported_models: supported.into(),
            model_mapping: mapping.into(),
            weight: 1,
            status: status.into(),
            settings: "{}".into(),
            created_at: ts.clone(),
            updated_at: ts,
        }
    }

    #[tokio::test]
    async fn test_channels() {
        let db = test_db().await;
        ChannelRepo::insert(&db, &channel("c1", "one", r#"["gpt-4o"]"#, "{}", "enabled")).await.unwrap();
        ChannelRepo::insert(&db, &channel("c2", "two", "[]", r#"{"claude-x": "claude-3"}"#, "enabled")).await.unwrap();
        ChannelRepo::insert(&db, &channel("c3", "three", r#"["gpt-4o"]"#, "{}", "disabled")).await.unwrap();

        assert_eq!(ChannelRepo::list(&db).await.unwrap().len(), 3);
        let got = ChannelRepo::get(&db, "c1").await.unwrap().unwrap();
        assert_eq!(got.name, "one");

        // supported_models match only for enabled
        let m = ChannelRepo::list_enabled_for_model(&db, "gpt-4o").await.unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].id, "c1");
        let mut dk = got.clone();
        dk.disabled_api_keys = r#"[{"key":"sk-x","disabledAt":"2026-01-01T00:00:00Z","errorCode":403}]"#.into();
        ChannelRepo::update(&db, &dk).await.unwrap();
        assert_eq!(
            ChannelRepo::get(&db, "c1").await.unwrap().unwrap().disabled_api_keys,
            dk.disabled_api_keys
        );
        let m = ChannelRepo::list_enabled_for_model(&db, "claude-x").await.unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].id, "c2");

        let mut upd = got.clone();
        upd.name = "renamed".into();
        ChannelRepo::update(&db, &upd).await.unwrap();
        assert_eq!(ChannelRepo::get(&db, "c1").await.unwrap().unwrap().name, "renamed");
        ChannelRepo::delete(&db, "c1").await.unwrap();
        assert!(ChannelRepo::get(&db, "c1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_api_keys() {
        let db = test_db().await;
        let ts = now();
        ApiKeyRepo::insert(&db, &ApiKey {
            id: "k1".into(),
            key: "sk-test-1".into(),
            name: "test".into(),
            status: "enabled".into(),
            quota: "{}".into(),
            expired_at: None,
            created_at: ts.clone(),
            updated_at: ts,
        })
        .await
        .unwrap();

        let k = ApiKeyRepo::get_by_key(&db, "sk-test-1").await.unwrap().unwrap();
        assert_eq!(k.id, "k1");
        assert!(ApiKeyRepo::get_by_key(&db, "nope").await.unwrap().is_none());

        ApiKeyRepo::update_status(&db, "k1", "disabled").await.unwrap();
        assert_eq!(ApiKeyRepo::get_by_key(&db, "sk-test-1").await.unwrap().unwrap().status, "disabled");

        assert_eq!(ApiKeyRepo::list(&db).await.unwrap().len(), 1);
        ApiKeyRepo::delete(&db, "k1").await.unwrap();
        assert!(ApiKeyRepo::list(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_model_prices() {
        let db = test_db().await;
        let r1 = ModelPriceRepo::upsert(&db, None, "gpt-4o", "1.00").await.unwrap();
        // same (channel, model) -> update: still one row, new reference_id
        let r2 = ModelPriceRepo::upsert(&db, None, "gpt-4o", "2.00").await.unwrap();
        assert_ne!(r1, r2);
        assert_eq!(ModelPriceRepo::list(&db).await.unwrap().len(), 1);

        // channel-specific wins over global
        ModelPriceRepo::upsert(&db, Some("ch1"), "gpt-4o", "3.00").await.unwrap();
        let g = ModelPriceRepo::find(&db, None, "gpt-4o").await.unwrap().unwrap();
        assert_eq!(g.price, "2.00");
        assert!(g.channel_id.is_none());
        let s = ModelPriceRepo::find(&db, Some("ch1"), "gpt-4o").await.unwrap().unwrap();
        assert_eq!(s.price, "3.00");
        assert_eq!(s.channel_id.as_deref(), Some("ch1"));
        // unknown channel falls back to global
        let f = ModelPriceRepo::find(&db, Some("chX"), "gpt-4o").await.unwrap().unwrap();
        assert_eq!(f.price, "2.00");
        // delete
        ModelPriceRepo::delete(&db, &s.id).await.unwrap();
        // channel row deleted -> falls back to the global row
        let f = ModelPriceRepo::find(&db, Some("ch1"), "gpt-4o").await.unwrap().unwrap();
        assert_eq!(f.price, "2.00");
        assert!(ModelPriceRepo::list(&db).await.unwrap().iter().all(|p| p.channel_id.is_none()));
    }

    fn usage(id: &str, key: &str, tokens: i64, cost: &str) -> UsageLog {
        UsageLog {
            id: id.into(),
            request_id: format!("req-{id}"),
            api_key_id: Some(key.into()),
            channel_id: Some("ch1".into()),
            model: "gpt-4o".into(),
            stream: true,
            prompt_tokens: 10,
            completion_tokens: tokens - 10,
            cached_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: tokens,
            cost: cost.into(),
            cost_items: "[]".into(),
            status: "success".into(),
            latency_ms: 100,
            created_at: format!("2026-01-0{}T00:00:00+00:00", &id[id.len() - 1..]),
        }
    }

    #[tokio::test]
    async fn test_usage_logs() {
        let db = test_db().await;
        UsageLogRepo::insert(&db, &usage("a1", "k1", 100, "0.015")).await.unwrap();
        UsageLogRepo::insert(&db, &usage("a2", "k1", 50, "0.005")).await.unwrap();
        UsageLogRepo::insert(&db, &usage("a3", "k2", 7, "1.0")).await.unwrap();

        let logs = UsageLogRepo::list_filtered(&db, Some("k1"), None, None, 10).await.unwrap();
        assert_eq!(logs.len(), 2);
        assert!(logs.iter().all(|l| l.stream));

        let (count, tokens, cost) =
            UsageLogRepo::aggregate_for_key(&db, "k1", "2000-01-01T00:00:00+00:00").await.unwrap();
        assert_eq!((count, tokens), (2, 150));
        assert_eq!(cost, "0.020");

        // since filters rows out
        let (count, _, _) =
            UsageLogRepo::aggregate_for_key(&db, "k1", "2026-01-02T00:00:00+00:00").await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_provider_quota() {
        let db = test_db().await;
        ProviderQuotaRepo::upsert_status(&db, "ch1", "openai", "acct1", "ok", "{}", None, None).await.unwrap();
        ProviderQuotaRepo::upsert_status(&db, "ch1", "openai", "acct1", "error", r#"{"x":1}"#, Some("2030-01-01T00:00:00+00:00"), None).await.unwrap();

        let rows = ProviderQuotaRepo::list_for_channel(&db, "ch1").await.unwrap();
        assert_eq!(rows.len(), 1); // upsert kept one row
        assert_eq!(rows[0].status, "error");

        // next_check_at in future -> not due
        assert!(ProviderQuotaRepo::list_due(&db, "2026-01-01T00:00:00+00:00").await.unwrap().is_empty());
        // due when now passes next_check_at, or NULL
        ProviderQuotaRepo::upsert_status(&db, "ch2", "gemini", "", "unknown", "{}", None, None).await.unwrap();
        let due = ProviderQuotaRepo::list_due(&db, "2031-01-01T00:00:00+00:00").await.unwrap();
        assert_eq!(due.len(), 2);
    }
}
