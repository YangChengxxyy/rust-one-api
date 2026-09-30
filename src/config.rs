use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_db")]
    pub database_url: String,
    #[serde(default = "default_log")]
    pub log: String,
    /// Admin API bearer token for management endpoints.
    pub admin_token: Option<String>,
    #[serde(default)]
    pub trace: TraceConfig,
    #[serde(default)]
    pub lb: LbConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LbConfig {
    /// Instance-level default LB strategy: priority | round_robin |
    /// weighted_shuffle | error_aware | sticky. Channel
    /// `settings.lb_strategy` overrides per channel.
    #[serde(default = "default_lb_strategy")]
    pub default: String,
}

impl Default for LbConfig {
    fn default() -> Self {
        Self { default: default_lb_strategy() }
    }
}

fn default_lb_strategy() -> String {
    "weighted_shuffle".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TraceConfig {
    /// off | meta | full. `meta` writes request summary + per-attempt
    /// metadata only; `full` additionally stores sanitized/truncated
    /// headers and bodies.
    #[serde(default = "default_trace_level")]
    pub level: String,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self { level: default_trace_level() }
    }
}

fn default_trace_level() -> String {
    "meta".to_string()
}

fn default_listen() -> String {
    "0.0.0.0:3000".to_string()
}
fn default_db() -> String {
    "sqlite://rust-one-api.db".to_string()
}
fn default_log() -> String {
    "info".to_string()
}

impl Config {
    /// Load from `config.yaml` (or path in `CONFIG_FILE`), with `ROA_`-prefixed
    /// env overrides for the flat fields.
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yaml".to_string());
        let mut cfg: Config = match std::fs::read_to_string(&path) {
            Ok(text) => serde_yaml::from_str(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("{path} not found, using defaults");
                serde_yaml::from_str("{}")?
            }
            Err(e) => return Err(e.into()),
        };
        if let Ok(v) = std::env::var("ROA_LISTEN") {
            cfg.listen = v;
        }
        if let Ok(v) = std::env::var("ROA_DATABASE_URL") {
            cfg.database_url = v;
        }
        if let Ok(v) = std::env::var("ROA_ADMIN_TOKEN") {
            cfg.admin_token = Some(v);
        }
        if let Ok(v) = std::env::var("ROA_TRACE_LEVEL") {
            cfg.trace.level = v;
        }
        if let Ok(v) = std::env::var("ROA_LB_DEFAULT") {
            cfg.lb.default = v;
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply_on_empty_doc() {
        let cfg: Config = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:3000");
        assert_eq!(cfg.database_url, "sqlite://rust-one-api.db");
        assert_eq!(cfg.log, "info");
        assert!(cfg.admin_token.is_none());
        assert_eq!(cfg.trace.level, "meta");
    }

    #[test]
    fn trace_level_parses_from_yaml() {
        let cfg: Config = serde_yaml::from_str("trace:\n  level: full").unwrap();
        assert_eq!(cfg.trace.level, "full");
    }

    #[test]
    fn lb_default_falls_back_and_parses() {
        let cfg: Config = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.lb.default, "weighted_shuffle");
        let cfg: Config = serde_yaml::from_str("lb:\n  default: round_robin").unwrap();
        assert_eq!(cfg.lb.default, "round_robin");
    }
}
