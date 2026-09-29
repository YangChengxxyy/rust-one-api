//! Provider quota: per-provider checkers (ported from axonhub
//! `biz/provider_quota`), unified `QuotaData` normalization, storage, and the
//! background scheduler feeding routing decisions.

pub mod checkers;
pub mod credentials;
pub mod normalize;
pub mod routing;
pub mod timeparse;
pub mod types;

use anyhow::Result;
 use chrono::{Duration as ChronoDuration, Utc};
 use futures::StreamExt;
use crate::keystate;

use crate::storage::{Channel, ChannelRepo, Db, ProviderQuotaRepo};
use checkers::checker_for_channel;
use credentials::ChannelCredentials;
use normalize::normalize_quota_data;
use types::QuotaError;

/// Check one channel's quota and upsert its status row.
///
/// Specialized checker when one matches the channel, else the generic probe.
/// InvalidCredentials marks the row unknown with a short retry; other errors
/// keep status unknown and back off 5 minutes; success re-checks in 30.
pub async fn check_channel(pool: &Db, http: &reqwest::Client, channel: &Channel) -> Result<()> {
    // OAuth channels (claudecode/codex) may need a token refresh before the
    // checker probes upstream; refresh failure falls back to the stale token.
    let channel = crate::oauth::maybe_refresh_oauth(pool, http, channel).await;
    let creds = ChannelCredentials::parse(&channel.credentials);
    let now = Utc::now();

    let outcome: std::result::Result<(String, types::QuotaData), QuotaError> = match checker_for_channel(&channel) {
        Some(checker) => checker
            .check_quota(http, &channel, &creds)
            .await
            .map(|d| normalize_quota_data(d))
            .map(|d| (d.status.clone(), d)),
        None => Ok(checkers::probe::probe(http, &channel, &creds).await),
    };

    // Fill period_cost from our usage logs, then derive period_quota
    // (axonhub does this in the quota service, not in checkers).
    let outcome = match outcome {
        Ok((status, mut data)) => {
            for limit in &mut data.limits {
                if let Some(ps) = limit.period_start {
                    if let Ok(cost) =
                        crate::storage::UsageLogRepo::cost_for_channel_since(pool, &channel.id, &ps.to_rfc3339()).await
                    {
                        limit.period_cost = Some(cost);
                        limit.fill_period_quota();
                    }
                }
            }
            Ok((status, data))
        }
        Err(e) => Err(e),
    };

    let (status, quota_data, provider_type, backoff_min) = match outcome {
        Ok((status, data)) => {
            let pt = data.provider_type.clone();
            (status, serde_json::to_string(&data)?, pt, 30)
        }
        Err(e) => {
            tracing::warn!(channel = %channel.name, error = %e, "quota check failed");
            let mut data = types::QuotaData::new("error", "unknown");
            data.raw_data.insert("error".into(), e.to_string().into());
            ("unknown".to_string(), serde_json::to_string(&data)?, "error".to_string(), 5)
        }
    };

    let data: types::QuotaData = serde_json::from_str(&quota_data)?;
    ProviderQuotaRepo::upsert_status(
        pool,
        &channel.id,
        &provider_type,
        "",
        &status,
        &quota_data,
        Some(&(now + ChronoDuration::minutes(backoff_min)).to_rfc3339()),
        data.next_reset_at.map(|t| t.to_rfc3339()).as_deref(),
    )
    .await?;
    Ok(())
}

/// Background scheduler: every 60s, probe due channels concurrently (max 8).
pub fn spawn_scheduler(pool: Db, http: reqwest::Client) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            ticker.tick().await;
            let now = Utc::now().to_rfc3339();
            let due = match ProviderQuotaRepo::list_due(&pool, &now).await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, "quota scheduler: list_due failed");
                    continue;
                }
            };
            futures::stream::iter(due)
                .map(|row| {
                    let pool = pool.clone();
                    let http = http.clone();
                    async move {
                        match ChannelRepo::get(&pool, &row.channel_id).await {
                            Ok(Some(ch)) if ch.status == "enabled" => {
                                if let Err(e) = check_channel(&pool, &http, &ch).await {
                                    tracing::warn!(channel = %ch.name, error = %e, "quota scheduler: check failed");
                                }
                            }
                            Ok(_) => {} // channel gone or disabled
                            Err(e) => tracing::warn!(error = %e, "quota scheduler: channel fetch failed"),
                        }
                    }
                })
                .buffer_unordered(8)
                .collect::<Vec<_>>()
                 .await;
            // Recovery sweep: channels whose process-local auto-disable window
            // (keystate) expired get one probe; a healthy quota status
            // (available|warning) re-enables them, anything else re-arms the
            // backoff with the next doubling window.
            for channel_id in keystate::disabled_channels_expired() {
                let ch = match ChannelRepo::get(&pool, &channel_id).await {
                    Ok(Some(ch)) => ch,
                    Ok(None) => {
                        keystate::enable_channel(&channel_id);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(channel = %channel_id, error = %e, "recovery sweep: channel fetch failed");
                        continue;
                    }
                };
                if ch.status != "enabled" {
                    // Durable admin disable wins; drop the local disable.
                    keystate::enable_channel(&channel_id);
                    continue;
                }
                let healthy = match check_channel(&pool, &http, &ch).await {
                    Err(e) => {
                        tracing::warn!(channel = %ch.name, error = %e, "recovery sweep: probe failed");
                        false
                    }
                    Ok(()) => match ProviderQuotaRepo::list_for_channel(&pool, &channel_id).await {
                        Ok(rows) => rows.first().map(|r| r.status == "available" || r.status == "warning").unwrap_or(false),
                        Err(e) => {
                            tracing::warn!(channel = %ch.name, error = %e, "recovery sweep: status read failed");
                            false
                        }
                    },
                };
                if healthy {
                    keystate::enable_channel(&channel_id);
                    tracing::info!(channel = %ch.name, "channel recovered; auto-disable cleared");
                } else {
                    keystate::record_channel_failure(&channel_id);
                    let until = keystate::disable_channel(&channel_id);
                    tracing::warn!(channel = %ch.name, until = %until, "channel recovery failed; auto-disable re-armed");
                }
            }
         }
    });
}
