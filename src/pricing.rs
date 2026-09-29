//! Pricing / billing: compute cost from `llm::Usage` + a `ModelPrice` document.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelPrice {
    #[serde(default)]
    pub items: Vec<PriceItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceItem {
    pub code: PriceItemCode,
    pub pricing: Pricing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceItemCode {
    PromptTokens,
    CompletionTokens,
    CachedTokens,
    CacheWriteTokens,
    ReasoningTokens,
    RequestCount,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Pricing {
    FlatFee { amount: Decimal },
    UsagePerUnit { unit_price: Decimal, unit_size: u64 },
    /// Progressive/marginal brackets.
    UsageTiered { unit_size: u64, tiers: Vec<Tier> },
    /// Whole quantity billed at the matched bracket rate.
    UsageVolume { unit_size: u64, tiers: Vec<Tier> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier {
    /// None = unbounded; tiers sorted ascending.
    pub up_to: Option<u64>,
    pub unit_price: Decimal,
}

pub struct CostItem {
    pub code: PriceItemCode,
    pub quantity: u64,
    pub amount: Decimal,
}

pub struct CostBreakdown {
    pub items: Vec<CostItem>,
    pub total: Decimal,
}

fn find(price: &ModelPrice, code: PriceItemCode) -> Option<&Pricing> {
    price.items.iter().find(|i| i.code == code).map(|i| &i.pricing)
}

/// amount = quantity / unit_size * unit_price, no intermediate rounding.
fn per_unit(quantity: u64, unit_size: u64, unit_price: Decimal) -> Decimal {
    Decimal::from(quantity) / Decimal::from(unit_size) * unit_price
}

fn tiered(quantity: u64, unit_size: u64, tiers: &[Tier]) -> Decimal {
    let mut amount = Decimal::ZERO;
    let mut prev: u64 = 0;
    for t in tiers {
        let cap = t.up_to.unwrap_or(u64::MAX);
        if cap <= prev {
            continue;
        }
        if quantity <= prev {
            break;
        }
        let portion = quantity.min(cap) - prev;
        amount += per_unit(portion, unit_size, t.unit_price);
        prev = cap;
        if quantity <= cap {
            break;
        }
    }
    amount
}

fn volume(quantity: u64, unit_size: u64, tiers: &[Tier]) -> Decimal {
    for t in tiers {
        match t.up_to {
            Some(up_to) if quantity > up_to => continue,
            _ => return per_unit(quantity, unit_size, t.unit_price),
        }
    }
    Decimal::ZERO
}

fn evaluate(quantity: u64, pricing: &Pricing) -> Decimal {
    match pricing {
        Pricing::FlatFee { amount } => *amount,
        Pricing::UsagePerUnit { unit_price, unit_size } => per_unit(quantity, *unit_size, *unit_price),
        Pricing::UsageTiered { unit_size, tiers } => tiered(quantity, *unit_size, tiers),
        Pricing::UsageVolume { unit_size, tiers } => volume(quantity, *unit_size, tiers),
    }
}

fn cost(price: &ModelPrice, code: PriceItemCode, quantity: u64) -> Option<Decimal> {
    if quantity == 0 {
        return None;
    }
    find(price, code).map(|p| evaluate(quantity, p))
}

pub fn compute_cost(usage: &llm::Usage, price: &ModelPrice) -> CostBreakdown {
    let mut items = Vec::new();
    let mut push = |code: PriceItemCode, quantity: u64, amount: Decimal| {
        items.push(CostItem { code, quantity, amount });
    };

    let cached = usage.cached_tokens.unwrap_or(0);
    let cache_write = usage.cache_write_tokens.unwrap_or(0);
    let reasoning = usage.reasoning_tokens.unwrap_or(0).min(usage.completion_tokens);

    // Prompt: cached tokens billed separately if a CachedTokens item exists.
    let cached_price = find(price, PriceItemCode::CachedTokens);
    let billable_prompt = usage.prompt_tokens.saturating_sub(
        if cached_price.is_some() { cached } else { 0 },
    );
    if let Some(a) = cost(price, PriceItemCode::PromptTokens, billable_prompt) {
        push(PriceItemCode::PromptTokens, billable_prompt, a);
    }
    if let (Some(_), Some(a)) = (cached_price, cost(price, PriceItemCode::CachedTokens, cached)) {
        push(PriceItemCode::CachedTokens, cached, a);
    }

    // Completion: reasoning split out if a ReasoningTokens item exists.
    let reasoning_price = find(price, PriceItemCode::ReasoningTokens);
    let billable_completion = if reasoning_price.is_some() {
        usage.completion_tokens - reasoning
    } else {
        usage.completion_tokens
    };
    if let Some(a) = cost(price, PriceItemCode::CompletionTokens, billable_completion) {
        push(PriceItemCode::CompletionTokens, billable_completion, a);
    }
    if let (Some(_), Some(a)) = (reasoning_price, cost(price, PriceItemCode::ReasoningTokens, reasoning)) {
        push(PriceItemCode::ReasoningTokens, reasoning, a);
    }

    // Cache write: fallback to PromptTokens price.
    let cw = match find(price, PriceItemCode::CacheWriteTokens) {
        Some(_) => cost(price, PriceItemCode::CacheWriteTokens, cache_write),
        None => cost(price, PriceItemCode::PromptTokens, cache_write),
    };
    if let Some(a) = cw {
        push(PriceItemCode::CacheWriteTokens, cache_write, a);
    }

    // Request count: flat fee, quantity 1.
    if let Some(p) = find(price, PriceItemCode::RequestCount) {
        if let Pricing::FlatFee { amount } = p {
            if *amount != Decimal::ZERO {
                push(PriceItemCode::RequestCount, 1, *amount);
            }
        }
    }

    let total = items.iter().map(|i| i.amount).sum();
    CostBreakdown { items, total }
}

/// Normalize, max 8 decimal places, strip trailing zeros.
pub fn format_cost(d: &Decimal) -> String {
    let d = d.normalize(); // strips trailing zeros (and normalizes exponent)
    // Cap to 8 decimal places.
    let s = d.round_dp(8).normalize().to_string();
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use serde_json::json;

    fn per_unit_item(code: PriceItemCode, unit_price: Decimal) -> PriceItem {
        PriceItem {
            code,
            pricing: Pricing::UsagePerUnit { unit_price, unit_size: 1_000_000 },
        }
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn per_unit_math() {
        let price = ModelPrice {
            items: vec![
                per_unit_item(PriceItemCode::PromptTokens, dec("1.5")),
                per_unit_item(PriceItemCode::CompletionTokens, dec("3")),
            ],
        };
        let usage = llm::Usage {
            prompt_tokens: 250_000,
            completion_tokens: 0,
            total_tokens: 250_000,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        assert_eq!(c.total, dec("0.375"));
    }

    #[test]
    fn tiered_progressive() {
        let price = ModelPrice {
            items: vec![PriceItem {
                code: PriceItemCode::PromptTokens,
                pricing: Pricing::UsageTiered {
                    unit_size: 1_000_000,
                    tiers: vec![
                        Tier { up_to: Some(100_000), unit_price: dec("1") },
                        Tier { up_to: None, unit_price: dec("2") },
                    ],
                },
            }],
        };
        let usage = llm::Usage { prompt_tokens: 250_000, total_tokens: 250_000, ..Default::default() };
        let c = compute_cost(&usage, &price);
        // 100k@1 + 150k@2 = 0.1 + 0.3
        assert_eq!(c.total, dec("0.4"));
        assert_eq!(c.items[0].quantity, 250_000);
    }

    #[test]
    fn volume_whole_bracket() {
        let price = ModelPrice {
            items: vec![PriceItem {
                code: PriceItemCode::PromptTokens,
                pricing: Pricing::UsageVolume {
                    unit_size: 1_000_000,
                    tiers: vec![
                        Tier { up_to: Some(100_000), unit_price: dec("1") },
                        Tier { up_to: None, unit_price: dec("2") },
                    ],
                },
            }],
        };
        let usage = llm::Usage { prompt_tokens: 250_000, total_tokens: 250_000, ..Default::default() };
        let c = compute_cost(&usage, &price);
        // all 250k @2 = 0.5
        assert_eq!(c.total, dec("0.5"));
    }

    #[test]
    fn cached_fallback_and_split() {
        // With CachedTokens item: cached billed separately, prompt reduced.
        let price = ModelPrice {
            items: vec![
                per_unit_item(PriceItemCode::PromptTokens, dec("2")),
                per_unit_item(PriceItemCode::CachedTokens, dec("0.2")),
            ],
        };
        let usage = llm::Usage {
            prompt_tokens: 1000,
            cached_tokens: Some(400),
            total_tokens: 1000,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        assert_eq!(c.items.len(), 2);
        assert_eq!(c.items[0].code, PriceItemCode::PromptTokens);
        assert_eq!(c.items[0].quantity, 600);
        assert_eq!(c.items[1].code, PriceItemCode::CachedTokens);
        assert_eq!(c.items[1].quantity, 400);
        // 600*2 + 400*0.2 per 1M = 0.00128
        assert_eq!(c.total, dec("0.00128"));

        // Without CachedTokens item: cached stays inside billable prompt.
        let price2 = ModelPrice { items: vec![per_unit_item(PriceItemCode::PromptTokens, dec("2"))] };
        let c2 = compute_cost(&usage, &price2);
        assert_eq!(c2.items.len(), 1);
        assert_eq!(c2.items[0].quantity, 1000);
        assert_eq!(c2.total, dec("0.002"));
    }

    #[test]
    fn reasoning_split() {
        let price = ModelPrice {
            items: vec![
                per_unit_item(PriceItemCode::CompletionTokens, dec("3")),
                per_unit_item(PriceItemCode::ReasoningTokens, dec("6")),
            ],
        };
        let usage = llm::Usage {
            prompt_tokens: 0,
            completion_tokens: 1000,
            reasoning_tokens: Some(300),
            total_tokens: 1000,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        assert_eq!(c.items.len(), 2);
        assert_eq!(c.items[0].code, PriceItemCode::CompletionTokens);
        assert_eq!(c.items[0].quantity, 700);
        assert_eq!(c.items[1].code, PriceItemCode::ReasoningTokens);
        assert_eq!(c.items[1].quantity, 300);
        // 700*3 + 300*6 per 1M
        assert_eq!(c.total, dec("0.0039"));

        // Without ReasoningTokens item: all completion at CompletionTokens.
        let price2 = ModelPrice { items: vec![per_unit_item(PriceItemCode::CompletionTokens, dec("3"))] };
        let c2 = compute_cost(&usage, &price2);
        assert_eq!(c2.items.len(), 1);
        assert_eq!(c2.items[0].quantity, 1000);
        assert_eq!(c2.total, dec("0.003"));
    }

    #[test]
    fn cache_write_fallback() {
        let price = ModelPrice { items: vec![per_unit_item(PriceItemCode::PromptTokens, dec("2"))] };
        let usage = llm::Usage {
            prompt_tokens: 1000,
            cache_write_tokens: Some(500),
            total_tokens: 1500,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        // prompt 1000 + cache_write 500 fallback @ PromptTokens price
        assert_eq!(c.total, dec("0.003"));
        assert_eq!(c.items[1].code, PriceItemCode::CacheWriteTokens);
        assert_eq!(c.items[1].quantity, 500);

        let price2 = ModelPrice {
            items: vec![
                per_unit_item(PriceItemCode::CacheWriteTokens, dec("2.5")),
            ],
        };
        let c2 = compute_cost(&usage, &price2);
        // only cache_write item present; prompt has no price -> 0
        assert_eq!(c2.items.len(), 1);
        assert_eq!(c2.total, dec("0.00125"));
    }

    #[test]
    fn flat_request_count() {
        let price = ModelPrice {
            items: vec![
                per_unit_item(PriceItemCode::PromptTokens, dec("1")),
                PriceItem {
                    code: PriceItemCode::RequestCount,
                    pricing: Pricing::FlatFee { amount: dec("0.005") },
                },
            ],
        };
        let usage = llm::Usage { prompt_tokens: 1000, total_tokens: 1000, ..Default::default() };
        let c = compute_cost(&usage, &price);
        assert_eq!(c.items.last().unwrap().code, PriceItemCode::RequestCount);
        assert_eq!(c.items.last().unwrap().quantity, 1);
        assert_eq!(c.total, dec("0.006"));
    }

    #[test]
    fn missing_item_contributes_zero() {
        let price = ModelPrice::default();
        let usage = llm::Usage {
            prompt_tokens: 1000,
            completion_tokens: 500,
            cached_tokens: Some(200),
            cache_write_tokens: Some(100),
            reasoning_tokens: Some(50),
            total_tokens: 1500,
            ..Default::default()
        };
        let c = compute_cost(&usage, &price);
        assert!(c.items.is_empty());
        assert_eq!(c.total, Decimal::ZERO);
    }

    #[test]
    fn zero_amount_flat_fee_skipped() {
        let price = ModelPrice {
            items: vec![PriceItem {
                code: PriceItemCode::RequestCount,
                pricing: Pricing::FlatFee { amount: Decimal::ZERO },
            }],
        };
        let usage = llm::Usage::default();
        let c = compute_cost(&usage, &price);
        assert!(c.items.is_empty());
    }

    #[test]
    fn json_roundtrip() {
        let v = json!({
            "items": [
                {"code": "prompt_tokens", "pricing": {"mode": "usage_per_unit", "unit_price": "1.5", "unit_size": 1000000}},
                {"code": "request_count", "pricing": {"mode": "flat_fee", "amount": "0.005"}},
                {"code": "cached_tokens", "pricing": {"mode": "usage_tiered", "unit_size": 1000, "tiers": [{"up_to": 100, "unit_price": "0.1"}, {"up_to": null, "unit_price": "0.2"}]}},
                {"code": "weird_new_code", "pricing": {"mode": "flat_fee", "amount": "1"}}
            ]
        });
        let p: ModelPrice = serde_json::from_value(v).unwrap();
        assert_eq!(p.items.len(), 4);
        assert_eq!(p.items[0].code, PriceItemCode::PromptTokens);
        assert_eq!(p.items[3].code, PriceItemCode::Unknown);
        assert!(matches!(p.items[2].pricing, Pricing::UsageTiered { .. }));
        let usage = llm::Usage { prompt_tokens: 500, cached_tokens: Some(150), total_tokens: 500, ..Default::default() };
        let c = compute_cost(&usage, &p);
        // billable prompt 350 * 1.5/1M = 0.000525
        // cached tiered unit_size 1000: 100@0.1 + 50@0.2 = 0.01+0.01 = 0.02 per... per unit: 100/1000*0.1 + 50/1000*0.2 = 0.01+0.01=0.02
        // flat 0.005
        assert_eq!(c.total, dec("0.025525"));
    }

    #[test]
    fn format_cost_trims() {
        assert_eq!(format_cost(&dec("0.37500000")), "0.375");
        assert_eq!(format_cost(&dec("1.23456789012")), "1.23456789");
        assert_eq!(format_cost(&dec("2.00000000")), "2");
        assert_eq!(format_cost(&Decimal::ZERO), "0");
    }

    #[test]
    fn tiered_boundary_exact() {
        let price = ModelPrice {
            items: vec![PriceItem {
                code: PriceItemCode::PromptTokens,
                pricing: Pricing::UsageTiered {
                    unit_size: 1,
                    tiers: vec![
                        Tier { up_to: Some(100), unit_price: dec("1") },
                        Tier { up_to: None, unit_price: dec("2") },
                    ],
                },
            }],
        };
        // exactly at boundary -> only first tier
        let c = compute_cost(&llm::Usage { prompt_tokens: 100, ..Default::default() }, &price);
        assert_eq!(c.total, dec("100"));
        // one over -> 100@1 + 1@2
        let c2 = compute_cost(&llm::Usage { prompt_tokens: 101, ..Default::default() }, &price);
        assert_eq!(c2.total, dec("102"));
    }

}
