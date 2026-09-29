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
use std::sync::{LazyLock, Mutex};

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
    let mut map = CURSORS.lock().unwrap();
    let cur = map.entry(channel_id.to_string()).or_insert(0);
    let idx = *cur % keys.len();
    *cur = (idx + 1) % keys.len();
    Some(keys[idx].clone())
}

/// Records another failure of the key and returns the new consecutive count
/// (1 for the first failure). Process-local: a restart resets streaks, which
/// only shortens the next disable window.
pub fn record_key_failure(channel_id: &str, key: &str) -> u32 {
    let mut map = FAILURES.lock().unwrap();
    let c = map.entry(failure_id(channel_id, key)).or_insert(0);
    *c += 1;
    *c
}

/// A successful request through the key resets its failure streak.
pub fn record_key_success(channel_id: &str, key: &str) {
    FAILURES.lock().unwrap().remove(&failure_id(channel_id, key));
}

/// Exponential disable expiry: 5min * 2^(count-1), capped at 24h from `now`.
pub fn disable_expires_at(count: u32) -> Option<DateTime<Utc>> {
    let shifts = u32::from(count.saturating_sub(1)).min(31);
    let secs = BASE_DISABLE_SECS
        .saturating_mul(1i64 << shifts)
        .min(MAX_DISABLE_SECS);
    Some(Utc::now() + Duration::seconds(secs))
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
}
