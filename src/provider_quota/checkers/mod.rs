//! Checker registry: channel_type mapping + URL sniffing, mirroring axonhub
//! `provider_quota.go:getProviderType` + `url_detection.go`.

pub mod apertis;
pub mod charm_hyper;
pub mod claudecode;
pub mod cline;
pub mod codex;
pub mod commandcode;
pub mod github_copilot;
pub mod kimi_code;
pub mod minimax;
pub mod nanogpt;
pub mod neuralwatt;
pub mod ollama;
pub mod opencode_go;
pub mod probe;
pub mod synthetic;
pub mod wafer;
pub mod zenmux;
pub mod zhipu;

use crate::provider_quota::types::QuotaChecker;
use crate::storage::Channel;

/// Picks the quota checker for a channel. Returns None when no specialized
/// checker applies — the caller falls back to the generic probe.
pub fn checker_for_channel(channel: &Channel) -> Option<Box<dyn QuotaChecker>> {
    let checker: Box<dyn QuotaChecker> = match channel.channel_type.as_str() {
        "claudecode" => Box::new(claudecode::ClaudeCodeChecker),
        "codex" => Box::new(codex::CodexChecker),
        "github_copilot" => Box::new(github_copilot::GithubCopilotChecker),
        "cline" => Box::new(cline::ClineChecker),
        "opencode_go" | "opencode_go_anthropic" => Box::new(opencode_go::OpencodeGoChecker),
        "kimi_code" | "moonshot_coding" => Box::new(kimi_code::KimiCodeChecker),
        "minimax" | "minimax_anthropic" => Box::new(minimax::MinimaxChecker),
        "zhipu" | "zhipu_anthropic" => Box::new(zhipu::ZhipuChecker),
        "zai" | "zai_anthropic" => Box::new(zhipu::ZaiChecker),
        "commandcode" | "commandcode_anthropic" => Box::new(commandcode::CommandcodeChecker),
        "ollama" | "ollama_anthropic" => Box::new(ollama::OllamaChecker),
        t if t.starts_with("zenmux") => Box::new(zenmux::ZenmuxChecker),
        "nanogpt" | "nanogpt_responses" => Box::new(nanogpt::NanogptChecker),
        // OpenAI-type channels: sniff provider from base_url host.
        "openai/chat_completions" | "openai" | "openai_responses" => {
            match detect_provider_from_url(&channel.base_url)? {
                "wafer" => Box::new(wafer::WaferChecker),
                "synthetic" => Box::new(synthetic::SyntheticChecker),
                "neuralwatt" => Box::new(neuralwatt::NeuralwattChecker),
                "apertis" => Box::new(apertis::ApertisChecker),
                "charm_hyper" => Box::new(charm_hyper::CharmHyperChecker),
                _ => return None,
            }
        }
        _ => return None,
    };
    Some(checker)
}

/// Host-suffix detection for OpenAI-compatible specialty providers.
pub fn detect_provider_from_url(base_url: &str) -> Option<&'static str> {
    let host = base_url
        .split("://")
        .nth(1)
        .unwrap_or(base_url)
        .split(['/', ':', '?'])
        .next()
        .unwrap_or("");
    const TABLE: &[(&str, &str)] = &[
        ("wafer.ai", "wafer"),
        ("api.synthetic.new", "synthetic"),
        ("api.neuralwatt.com", "neuralwatt"),
        ("api.apertis.ai", "apertis"),
        ("hyper.charm.land", "charm_hyper"),
    ];
    TABLE.iter().find(|(suffix, _)| host == *suffix || host.ends_with(&format!(".{suffix}"))).map(|(_, name)| *name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_sniffing() {
        assert_eq!(detect_provider_from_url("https://wafer.ai/v1"), Some("wafer"));
        assert_eq!(detect_provider_from_url("https://sub.wafer.ai"), Some("wafer"));
        assert_eq!(detect_provider_from_url("https://api.synthetic.new"), Some("synthetic"));
        assert_eq!(detect_provider_from_url("https://api.openai.com/v1"), None);
        assert_eq!(detect_provider_from_url("http://127.0.0.1:9100"), None);
    }

    #[test]
    fn channel_type_mapping() {
        let mut ch = Channel::default();
        ch.channel_type = "claudecode".into();
        assert_eq!(checker_for_channel(&ch).unwrap().provider_type(), "claudecode");
        ch.channel_type = "minimax_anthropic".into();
        assert_eq!(checker_for_channel(&ch).unwrap().provider_type(), "minimax");
        ch.channel_type = "openai/chat_completions".into();
        ch.base_url = "https://api.neuralwatt.com".into();
        assert_eq!(checker_for_channel(&ch).unwrap().provider_type(), "neuralwatt");
        ch.base_url = "https://api.openai.com".into();
        assert!(checker_for_channel(&ch).is_none());
    }
}
