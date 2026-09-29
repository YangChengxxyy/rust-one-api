//! Process-local relay key state: round-robin rotation cursor per channel and
//! consecutive-failure counters per (channel, key) driving the exponential
//! disable expiry.
//!
//! axonhub keeps rotation/disable decisions in its channel service
//! (`internal/server/biz/channel_apikey.go`); rust-one-api has no channel
//! cache layer, so this module holds the process-local half (cursor +
//! failure streak) while the durable half lives in
//! `channels.disabled_api_keys` (see `provider_quota::credentials`).

use std::collections::HashMap;
use parking_lot::Mutex;
use std::sync::LazyLock;

use chrono::{DateTime, Duration, Utc};

/// First temporary disable lasts 5 minutes; each consecutive failure of the
/// same key doubles it, capped at 24 hours. axonhub has no fixed policy (its
/// durations are operator-configured per rule, see
/// `channel_auto_disable.go` `executeAPIKeyRuleAction`), so this default is
/// documented here rather than mirrored.
const BASE_DISABLE_SECS: i64 = 5 * 60;
const MAX_DISABLE_SECS: i64 = 24 * 3600;

static CURSORS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static FAILURES: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn failure_id(channel_id: &str, key: &str) -> String {
    format!("{channel_id}\0{key}")
}

/// Round-robin over `keys`, scoped to the channel so concurrent channels
/// rotate independently. Returns `None` for an empty list. Wraps around; the
/// cursor advances on every call.
pub fn next_key(keys: &[String], channel_id: &str) -> Option<String> {
    if keys.is_empty() {
        return None;
    }
    let mut map = CURSORS.lock();
    let cur = map.entry(channel_id.to_string()).or_insert(0);
    let idx = *cur % keys.len();
    *cur = (idx + 1) % keys.len();
    Some(keys[idx].clone())
}

/// Records another failure of the key and returns the new consecutive count
/// (1 for the first failure). Process-local: a restart resets streaks, which
/// only shortens the next disable window.
pub fn record_key_failure(channel_id: &str, key: &str) -> u32 {
    let mut map = FAILURES.lock();
    let c = map.entry(failure_id(channel_id, key)).or_insert(0);
    *c += 1;
    *c
}

/// A successful request through the key resets its failure streak.
pub fn record_key_success(channel_id: &str, key: &str) {
    FAILURES.lock().remove(&failure_id(channel_id, key));
}

/// Exponential disable expiry: 5min * 2^(count-1), capped at 24h from `now`.
pub fn disable_expires_at(count: u32) -> Option<DateTime<Utc>> {
    let shifts = u32::from(count.saturating_sub(1)).min(31);
    let secs = BASE_DISABLE_SECS
        .saturating_mul(1i64 << shifts)
        .min(MAX_DISABLE_SECS);
    Some(Utc::now() + Duration::seconds(secs))
}

// ---------------------------------------------------------------------------
// Channel-level auto-disable + recovery (axonhub `channel_auto_disable.go`
// semantics with opinionated defaults instead of its rule engine).
// ---------------------------------------------------------------------------

/// Consecutive channel failures before auto-disable. axonhub makes this
/// operator-configured per rule; we fix it.
pub const CHANNEL_DISABLE_THRESHOLD: u32 = 3;
/// First channel disable lasts 1 minute; each further failure-driven disable
/// doubles it, capped at 30 minutes.
const CHANNEL_BASE_DISABLE_SECS: i64 = 60;
const CHANNEL_MAX_DISABLE_SECS: i64 = 30 * 60;

static CHANNEL_FAILURES: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static CHANNEL_DISABLED_UNTIL: LazyLock<Mutex<HashMap<String, DateTime<Utc>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Records a failed channel attempt and returns the new consecutive streak
/// (reset by [`record_channel_success`]). Process-local, like key streaks: a
/// restart clears streaks and disables, re-enabling every channel.
pub fn record_channel_failure(channel_id: &str) -> u32 {
    let mut map = CHANNEL_FAILURES.lock();
    let c = map.entry(channel_id.to_string()).or_insert(0);
    *c += 1;
    *c
}

/// A successful request through the channel resets its failure streak.
pub fn record_channel_success(channel_id: &str) {
    CHANNEL_FAILURES.lock().remove(channel_id);
}

/// Exponential channel disable window: 1min * 2^(streak-3), capped at 30min
/// from `now`. Streaks below the threshold still yield the base 1 minute.
pub fn channel_disable_expires_at(streak: u32) -> DateTime<Utc> {
    let shifts = streak.saturating_sub(CHANNEL_DISABLE_THRESHOLD).min(31);
    let secs = CHANNEL_BASE_DISABLE_SECS
        .saturating_mul(1i64 << shifts)
        .min(CHANNEL_MAX_DISABLE_SECS);
    Utc::now() + Duration::seconds(secs)
}

/// Disable expiry for a channel, or `None` if it is not (or no longer)
/// disabled. Entries already in the past are drained here.
pub fn channel_disabled_until(channel_id: &str) -> Option<DateTime<Utc>> {
    let mut map = CHANNEL_DISABLED_UNTIL.lock();
    match map.get(channel_id) {
        Some(until) if *until > Utc::now() => Some(*until),
        _ => {
            map.remove(channel_id);
            None
        }
    }
}

/// Auto-disables the channel for the next backoff window (see
/// [`channel_disable_expires_at`]) and returns the expiry. Callers re-arming
/// after a failed recovery should bump the streak first
/// ([`record_channel_failure`]) so the window doubles.
pub fn disable_channel(channel_id: &str) -> DateTime<Utc> {
    let streak = CHANNEL_FAILURES
        .lock()
        .get(channel_id)
        .copied()
        .unwrap_or(CHANNEL_DISABLE_THRESHOLD);
    let until = channel_disable_expires_at(streak.max(CHANNEL_DISABLE_THRESHOLD));
    CHANNEL_DISABLED_UNTIL
        .lock()
        .insert(channel_id.to_string(), until);
    until
}

/// Clears the disable and the failure streak (used by the recovery sweep and
/// on manual re-enable).
pub fn enable_channel(channel_id: &str) {
    CHANNEL_DISABLED_UNTIL.lock().remove(channel_id);
    CHANNEL_FAILURES.lock().remove(channel_id);
}

/// Drains and returns channels whose disable window has expired; the recovery
/// sweep probes exactly these.
pub fn disabled_channels_expired() -> Vec<String> {
    let now = Utc::now();
    let mut map = CHANNEL_DISABLED_UNTIL.lock();
    let (expired, active) = map.drain().partition(|(_, until)| *until <= now);
    *map = active;
    expired.into_keys().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_key_empty_is_none() {
        assert!(next_key(&[], "ch").is_none());
    }

    #[test]
    fn next_key_wraps() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let id = format!("wrap-{}", line!());
        let seq: Vec<_> = (0..5).filter_map(|_| next_key(&keys, &id)).collect();
        assert_eq!(seq, vec!["a", "b", "c", "a", "b"]);
    }

    #[test]
    fn next_key_single_key_always_same() {
        let keys = vec!["k".to_string()];
        let id = format!("single-{}", line!());
        assert_eq!(next_key(&keys, &id).as_deref(), Some("k"));
        assert_eq!(next_key(&keys, &id).as_deref(), Some("k"));
    }

    #[test]
    fn next_key_skips_disabled_by_callers_pruning() {
        // Callers remove disabled keys from the serving list before calling;
        // rotation must still return a live key and keep wrapping.
        let keys = vec!["b".to_string(), "c".to_string()];
        let id = format!("prune-{}", line!());
        assert_eq!(next_key(&keys, &id).as_deref(), Some("b"));
        assert_eq!(next_key(&keys, &id).as_deref(), Some("c"));
        assert_eq!(next_key(&keys, &id).as_deref(), Some("b"));
    }

    #[test]
    fn failure_streak_increments_and_resets() {
        let id = format!("streak-{}", line!());
        assert_eq!(record_key_failure(&id, "k"), 1);
        assert_eq!(record_key_failure(&id, "k"), 2);
        record_key_success(&id, "k");
        assert_eq!(record_key_failure(&id, "k"), 1);
    }

    #[test]
    fn disable_expiry_doubles_then_caps() {
        let now = Utc::now();
        let d1 = disable_expires_at(1).unwrap();
        assert!(d1 - now >= Duration::minutes(5) - Duration::seconds(5));
        let d5 = disable_expires_at(5).unwrap();
        assert!(d5 - now >= Duration::minutes(80) - Duration::seconds(5));
        let d30 = disable_expires_at(30).unwrap();
        assert!(d30 - now <= Duration::hours(24) + Duration::minutes(1));
        let d99 = disable_expires_at(99).unwrap();
        assert!(d99 - now <= Duration::hours(24) + Duration::minutes(1));
    }

    #[test]
    fn channel_streak_increments_and_resets() {
        let id = format!("chstreak-{}", line!());
        assert_eq!(record_channel_failure(&id), 1);
        assert_eq!(record_channel_failure(&id), 2);
        record_channel_success(&id);
        assert_eq!(record_channel_failure(&id), 1);
        record_channel_success(&id);
    }

    #[test]
    fn channel_disable_at_threshold_with_doubling_backoff() {
        let id = format!("chdisable-{}", line!());
        // Two failures stay below the threshold and never disable.
        assert_eq!(record_channel_failure(&id), 1);
        assert_eq!(record_channel_failure(&id), 2);
        assert!(channel_disabled_until(&id).is_none());
        // Third consecutive failure hits the threshold and triggers disable.
        let streak = record_channel_failure(&id);
        assert_eq!(streak, CHANNEL_DISABLE_THRESHOLD);
        let u1 = disable_channel(&id);
        let now = Utc::now();
        assert!(u1 - now <= Duration::minutes(1));
        assert!(channel_disabled_until(&id).is_some());

        // Re-arm after a failed recovery: streak bump doubles the window.
        record_channel_failure(&id);
        let u2 = disable_channel(&id);
        assert!(u2 - now > Duration::minutes(1));
        assert!(u2 - now <= Duration::minutes(2) + Duration::seconds(5));

        // Cap at 30 minutes even for huge streaks.
        let u99 = channel_disable_expires_at(99);
        assert!(u99 - now <= Duration::minutes(30) + Duration::seconds(5));
    }

    #[test]
    fn channel_enable_clears_state() {
        let id = format!("chenable-{}", line!());
        record_channel_failure(&id);
        disable_channel(&id);
        enable_channel(&id);
        assert!(channel_disabled_until(&id).is_none());
        assert_eq!(record_channel_failure(&id), 1);
    }

    #[test]
    fn disabled_channels_expired_drains_only_past_entries() {
        let past = format!("chpast-{}", line!());
        let future = format!("chfuture-{}", line!());
        let mut map = CHANNEL_DISABLED_UNTIL.lock();
        map.insert(past.clone(), Utc::now() - Duration::seconds(1));
        map.insert(future.clone(), Utc::now() + Duration::minutes(5));
        drop(map);
        let mut expired = disabled_channels_expired();
        expired.sort();
        assert_eq!(expired, vec![past]);
        assert!(channel_disabled_until(&future).is_some());
        assert!(disabled_channels_expired().is_empty());
        enable_channel(&future);
    }
}
