//! Channel credential extraction for quota checkers.
//!
//! `channel.credentials` JSON shape (superset of the relay convention):
//! ```json
//! {
//!   "api_key": "...",                 // single key; OAuth checkers also accept an OAuth JSON blob here
//!   "api_keys": ["...", "..."],       // multi-key channels (zhipu family)
//!   "oauth": {"access_token": "...", "refresh_token": "...", "expired": "RFC3339"},
//!   "management_api_key": "..."       // zenmux
//! }
//! ```
//! Cookie-based checkers (ollama, commandcode fallback) read
//! `channel.settings.provider_quota.<provider>.auth_cookie`.

use serde::{Deserialize, Deserializer};

use chrono::{DateTime, Utc};
use std::collections::HashSet;

use crate::storage::Channel;
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct ChannelCredentials {
    pub api_key: Option<String>,
    pub api_keys: Vec<String>,
    pub oauth: Option<OAuthCredentials>,
    pub management_api_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OAuthCredentials {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct RawCredentials {
    api_key: Option<String>,
    #[serde(default)]
    api_keys: Vec<String>,
    oauth: Option<RawOAuth>,
    management_api_key: Option<String>,
}

#[derive(Deserialize)]
struct RawOAuth {
    access_token: String,
    refresh_token: Option<String>,
}

impl ChannelCredentials {
    pub fn parse(channel_credentials_json: &str) -> Self {
        let raw: RawCredentials = serde_json::from_str(channel_credentials_json).unwrap_or(RawCredentials {
            api_key: None,
            api_keys: Vec::new(),
            oauth: None,
            management_api_key: None,
        });
        Self {
            api_key: raw.api_key,
            api_keys: raw.api_keys,
            oauth: raw.oauth.map(|o| OAuthCredentials { access_token: o.access_token, refresh_token: o.refresh_token }),
            management_api_key: raw.management_api_key,
        }
    }

    /// All usable API keys: api_keys if present, else api_key alone.
    pub fn all_api_keys(&self) -> Vec<&str> {
        if !self.api_keys.is_empty() {
            self.api_keys.iter().map(|s| s.as_str()).collect()
        } else {
            self.api_key.as_deref().into_iter().collect()
        }
    }

    /// OAuth access token: explicit oauth block first, else api_key parsed as
    /// an OAuth JSON blob (axonhub convention for claudecode/codex/copilot).
    pub fn oauth_access_token(&self) -> Option<String> {
        if let Some(o) = &self.oauth {
            if !o.access_token.is_empty() {
                return Some(o.access_token.clone());
            }
        }
        let key = self.api_key.as_deref()?;
        if key.starts_with('{') {
            let v: serde_json::Value = serde_json::from_str(key).ok()?;
            return v.get("access_token").and_then(|t| t.as_str()).map(str::to_string);
        }
        None
    }
}

/// Reads a provider-specific auth cookie from channel.settings JSON:
/// `settings.provider_quota.<provider>.auth_cookie`.
pub fn auth_cookie(settings_json: &str, provider: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(settings_json).ok()?;
    v.get("provider_quota")?
        .get(provider)?
        .get("auth_cookie")?
        .as_str()
        .map(str::to_string)
}

/// A parked (disabled) API key record, port of axonhub's `DisabledAPIKey`.
/// Persisted on the channel as a JSON array; the disable is keyed by the key
/// plaintext. `expiresAt` marks a temporary disable that lapses on its own.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisabledAPIKey {
    pub key: String,
    #[serde(default = "Utc::now", deserialize_with = "deserialize_ts")]
    pub disabled_at: DateTime<Utc>,
    pub error_code: i64,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default, deserialize_with = "deserialize_opt_ts")]
    pub expires_at: Option<DateTime<Utc>>,
}

impl DisabledAPIKey {
    /// A temporary disable that has elapsed no longer parks the key.
    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|t| Utc::now() > t).unwrap_or(false)
    }
}

/// RFC3339 with tolerance: numeric unix seconds and a few string layouts are
/// also accepted so historical rows never fail to parse.
fn parse_ts(v: &serde_json::Value) -> Option<DateTime<Utc>> {
    match v {
        Value::Number(n) => n
            .as_i64()
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .or_else(|| n.as_f64().and_then(|ms| DateTime::from_timestamp_millis(ms as i64))),
        Value::String(s) => s.parse::<DateTime<Utc>>()
            .ok()
            .or_else(|| DateTime::parse_from_rfc3339(s).ok().map(|t| t.with_timezone(&Utc)))
            .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok().map(|t| t.and_utc())),
        _ => None,
    }
}

fn deserialize_ts<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    parse_ts(&v).ok_or_else(|| serde::de::Error::custom("invalid timestamp"))
}

fn deserialize_opt_ts<'de, D: Deserializer<'de>>(d: D) -> Result<Option<DateTime<Utc>>, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    if v.is_null() {
        return Ok(None);
    }
    parse_ts(&v).map(Some).ok_or_else(|| serde::de::Error::custom("invalid timestamp"))
}

/// Keys carrying an active (non-expired) disable record, trimmed. Mirrors
/// axonhub's `zhipuDisabledKeySet` / `filterDisabled` set construction.
pub fn disabled_key_set(channel: &Channel) -> HashSet<String> {
    let entries: Vec<DisabledAPIKey> = serde_json::from_str(&channel.disabled_api_keys).unwrap_or_default();
    let mut set = HashSet::with_capacity(entries.len());
    for entry in entries {
        let key = entry.key.trim();
        if key.is_empty() || entry.is_expired() {
            continue;
        }
        set.insert(key.to_string());
    }
    set
}

/// Non-empty, trimmed API keys minus the disabled set, order preserved.
/// Port of axonhub's `GetEnabledAPIKeys`.
pub fn serving_api_keys(creds: &ChannelCredentials, channel: &Channel) -> Vec<String> {
    let disabled = disabled_key_set(channel);
    creds
        .all_api_keys()
        .into_iter()
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty() && !disabled.contains(k))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_shapes() {
        let c = ChannelCredentials::parse(r#"{"api_key":"k1","api_keys":["a","b"],"oauth":{"access_token":"tok"},"management_api_key":"m"}"#);
        assert_eq!(c.all_api_keys(), vec!["a", "b"]);
        assert_eq!(c.oauth_access_token().as_deref(), Some("tok"));
        assert_eq!(c.management_api_key.as_deref(), Some("m"));
    }

    #[test]
    fn oauth_json_in_api_key() {
        let c = ChannelCredentials::parse(r#"{"api_key":"{\"access_token\":\"nested\"}"}"#);
        assert_eq!(c.oauth_access_token().as_deref(), Some("nested"));
        let plain = ChannelCredentials::parse(r#"{"api_key":"sk-plain"}"#);
        assert_eq!(plain.oauth_access_token(), None);
        assert_eq!(plain.all_api_keys(), vec!["sk-plain"]);
    }

    #[test]
    fn cookie_from_settings() {
        let s = r#"{"provider_quota":{"ollama":{"auth_cookie":"__Secure-session=abc"}}}"#;
        assert_eq!(auth_cookie(s, "ollama").as_deref(), Some("__Secure-session=abc"));
        assert_eq!(auth_cookie("{}", "ollama"), None);
    }

    #[test]
    fn disabled_key_set_ignores_expired_and_blank() {
        let ch = |raw: &str| crate::storage::Channel {
            disabled_api_keys: raw.to_string(),
            ..Default::default()
        };
        let past = Utc::now() - chrono::Duration::hours(1);
        let future = Utc::now() + chrono::Duration::hours(1);
        let c = ch(&format!(
            r#"[{{"key":"k1","disabledAt":"2026-01-01T00:00:00Z","errorCode":403}},
                {{"key":"  k2  ","disabledAt":"2026-01-01T00:00:00Z","errorCode":429,"expiresAt":"{future}"}},
                {{"key":"k3","disabledAt":"2026-01-01T00:00:00Z","errorCode":429,"expiresAt":"{past}"}},
                {{"key":"  ","disabledAt":"2026-01-01T00:00:00Z","errorCode":500}}]"#
        ));
        let set = disabled_key_set(&c);
        assert_eq!(set, HashSet::from(["k1".to_string(), "k2".to_string()]));

        // invalid JSON degrades to an empty set
        assert!(disabled_key_set(&ch("not json")).is_empty());
    }

    #[test]
    fn serving_api_keys_drop_disabled_preserve_order() {
        let creds = ChannelCredentials::parse(r#"{"api_key":"a","api_keys":["a","b"," c ","","d"]}"#);
        // d's disable is expired (2000), so d still serves; b stays parked.
        let ch = crate::storage::Channel {
            disabled_api_keys: r#"[{"key":"b","disabledAt":"2026-01-01T00:00:00Z","errorCode":403},{"key":"d","disabledAt":"2026-01-01T00:00:00Z","errorCode":429,"expiresAt":"2000-01-01T00:00:00Z"}]"#.into(),
            ..Default::default()
        };
        assert_eq!(serving_api_keys(&creds, &ch), vec!["a", "c", "d"]);
    }
}
