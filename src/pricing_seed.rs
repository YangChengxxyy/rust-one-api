//! Boot-time seeding of the global model price catalog from an embedded YAML snapshot.

use crate::pricing::ModelPrice;
use crate::storage::{Db, ModelPriceRepo};
use serde::Deserialize;

const SEED_YAML: &str = include_str!("../data/model_prices.seed.yaml");

#[derive(Debug, Deserialize)]
struct SeedEntry {
    model: String,
    price: ModelPrice,
}

/// Parse the embedded seed file into (model, price) pairs.
pub fn parse_seed() -> anyhow::Result<Vec<(String, ModelPrice)>> {
    let entries: Vec<SeedEntry> = serde_yaml::from_str(SEED_YAML)?;
    Ok(entries.into_iter().map(|e| (e.model, e.price)).collect())
}

/// Seed the global (channel-less) model price catalog at boot.
///
/// Skips (returns 0) if any model prices already exist, so admin edits are
/// never overwritten. Seeding failures must not block boot; callers should
/// warn and continue.
pub async fn seed_model_prices(pool: &Db) -> anyhow::Result<usize> {
    if !ModelPriceRepo::list(pool).await?.is_empty() {
        return Ok(0);
    }
    let entries = parse_seed()?;
    let count = entries.len();
    for (model, price) in entries {
        let price_json = serde_json::to_string(&price)?;
        ModelPriceRepo::upsert(pool, None, &model, &price_json).await?;
    }
    tracing::info!("seeded {count} model prices from catalog snapshot");
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::{compute_cost, Pricing};
    use rust_decimal::Decimal;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn seed_for(model: &str) -> ModelPrice {
        parse_seed()
            .unwrap()
            .into_iter()
            .find(|(m, _)| m == model)
            .unwrap_or_else(|| panic!("model {model} missing from seed"))
            .1
    }

    #[test]
    fn seed_parses_all_entries() {
        let entries = parse_seed().unwrap();
        // 10 OpenAI + 8 Anthropic + 3 Gemini
        assert_eq!(entries.len(), 21);
        // Anthropic entries all carry a cache_write item.
        for (m, p) in &entries {
            if m.starts_with("claude") {
                assert!(
                    p.items.iter().any(|i| i.code == crate::pricing::PriceItemCode::CacheWriteTokens),
                    "{m} missing cache_write_tokens"
                );
            }
        }
    }

    #[test]
    fn gpt_4o_spot_check() {
        let price = seed_for("gpt-4o");
        let usage = llm::Usage {
            prompt_tokens: 1_000_000,
            completion_tokens: 1_000_000,
            total_tokens: 2_000_000,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        assert_eq!(c.total, dec("12.50"));
    }

    #[test]
    fn claude_3_5_haiku_spot_check() {
        let price = seed_for("claude-3-5-haiku");
        let usage = llm::Usage {
            prompt_tokens: 1_000_000,
            completion_tokens: 1_000_000,
            cache_write_tokens: Some(1_000_000),
            total_tokens: 3_000_000,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        // 1M prompt * 0.80 + 1M completion * 4.00 + 1M cache write * 1.00
        assert_eq!(c.total, dec("5.80"));
    }

    #[test]
    fn gemini_2_5_pro_tier_boundaries() {
        let price = seed_for("gemini-2.5-pro");
        let prompt_item = price
            .items
            .iter()
            .find(|i| i.code == crate::pricing::PriceItemCode::PromptTokens)
            .unwrap();
        assert!(matches!(prompt_item.pricing, Pricing::UsageTiered { .. }));

        // 100k prompt: fully in tier 1 -> 100000/1M * 1.25 = 0.125
        let usage = llm::Usage {
            prompt_tokens: 100_000,
            total_tokens: 100_000,
            ..Default::default()
        };
        assert_eq!(compute_cost(&usage, &price).total, dec("0.125"));

        // 300k prompt: progressive tiers -> 200k*1.25/1M + 100k*2.50/1M = 0.25 + 0.25
        let usage = llm::Usage {
            prompt_tokens: 300_000,
            total_tokens: 300_000,
            ..Default::default()
        };
        assert_eq!(compute_cost(&usage, &price).total, dec("0.5"));
    }
}
