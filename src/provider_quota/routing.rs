//! Quota-aware routing — direct port of axonhub's
//! `biz/provider_quota/routing.go` (EvaluateQuotaRouting and helpers).
//! `EffectiveStatus` is not ported: it is unused outside axonhub's status
//! aggregation, which rust-one-api does not perform.

use chrono::{DateTime, Utc};

use super::types::{QuotaLimitStatus, QuotaLimitType, WINDOW_CREDITS, WINDOW_PAY_AS_YOU_GO};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingState {
    Open,
    StickyOnly,
    Exhausted,
    Unknown,
}

const ROUTING_STATUS_EXHAUSTED: &str = "exhausted";
const ROUTING_STATUS_AVAILABLE: &str = "available";
const ROUTING_STATUS_WARNING: &str = "warning";

pub fn is_balance_limit(l: &QuotaLimitStatus) -> bool {
    l.window == WINDOW_PAY_AS_YOU_GO || l.window == WINDOW_CREDITS
}

/// Fraction of [period_start, next_reset_at) elapsed at `now`. Requires an
/// active window (start < now < reset, reset > start); clamps to [0, 1].
pub fn elapsed_ratio(l: &QuotaLimitStatus, now: DateTime<Utc>) -> Option<f64> {
    let (start, reset) = (l.period_start?, l.next_reset_at?);
    if !(start < now) || !(now < reset) || !(reset > start) {
        return None;
    }
    let ratio =
        (now - start).num_nanoseconds().unwrap_or(0) as f64 / (reset - start).num_nanoseconds().unwrap_or(1) as f64;
    Some(ratio.clamp(0.0, 1.0))
}

fn routing_limit_exhausted(limit: &QuotaLimitStatus) -> bool {
    limit.status == ROUTING_STATUS_EXHAUSTED || limit.usage_ratio >= 1.0
}

/// OR-grouping for routing: named availability groups count when any limit of
/// `limit_type` carries the name (cross-type limits in such a group are
/// included too). Ungrouped windows and balances each form one group.
fn routing_groups(limits: &[QuotaLimitStatus], limit_type: QuotaLimitType) -> Vec<Vec<&QuotaLimitStatus>> {
    let group_names: std::collections::HashSet<&str> = limits
        .iter()
        .filter(|l| l.kind == limit_type && !l.availability_group.is_empty())
        .map(|l| l.availability_group.as_str())
        .collect();

    let mut grouped: std::collections::HashMap<&str, Vec<&QuotaLimitStatus>> = std::collections::HashMap::new();
    let mut ungrouped_windows = Vec::new();
    let mut ungrouped_balances = Vec::new();
    for l in limits {
        let in_named_group = group_names.contains(l.availability_group.as_str());
        if l.kind != limit_type && !in_named_group {
            continue;
        }
        if in_named_group {
            grouped.entry(l.availability_group.as_str()).or_default().push(l);
            continue;
        }
        if is_balance_limit(l) {
            ungrouped_balances.push(l);
        } else {
            ungrouped_windows.push(l);
        }
    }

    let mut groups: Vec<Vec<&QuotaLimitStatus>> = grouped.into_values().collect();
    if !ungrouped_windows.is_empty() {
        groups.push(ungrouped_windows);
    }
    if !ungrouped_balances.is_empty() {
        groups.push(ungrouped_balances);
    }
    groups
}

/// Evaluates whether a channel may accept new (non-sticky) traffic and why it
/// was deprioritized: "window_exhausted_balance_fallback" | "window_pressure".
pub fn evaluate_quota_routing(
    limits: &[QuotaLimitStatus],
    overall_status: &str,
    limit_type: QuotaLimitType,
    now: DateTime<Utc>,
) -> (RoutingState, Option<&'static str>) {
    if overall_status == ROUTING_STATUS_EXHAUSTED {
        return (RoutingState::Exhausted, None);
    }

    let groups = routing_groups(limits, limit_type);
    if groups.is_empty() {
        if overall_status == ROUTING_STATUS_AVAILABLE || overall_status == ROUTING_STATUS_WARNING {
            return (RoutingState::Open, None);
        }
        return (RoutingState::Unknown, None);
    }

    let mut all_groups_exhausted = true;
    let mut has_window = false;
    let mut all_windows_exhausted = true;
    let mut balance_available = false;
    for group in &groups {
        // Ungrouped limits AND together; a named availability group ORs.
        let mut group_available = !group.is_empty() && group[0].availability_group.is_empty();
        for limit in group {
            if group[0].availability_group.is_empty() {
                if routing_limit_exhausted(limit) {
                    group_available = false;
                }
            } else if !routing_limit_exhausted(limit) {
                group_available = true;
            }

            if is_balance_limit(limit) {
                if !routing_limit_exhausted(limit) {
                    balance_available = true;
                }
                continue;
            }
            has_window = true;
            if !routing_limit_exhausted(limit) {
                all_windows_exhausted = false;
            }
        }
        if group_available {
            all_groups_exhausted = false;
        }
    }

    if all_groups_exhausted {
        return (RoutingState::Exhausted, None);
    }

    if has_window && all_windows_exhausted && balance_available {
        return (RoutingState::StickyOnly, Some("window_exhausted_balance_fallback"));
    }

    for group in &groups {
        for limit in group {
            if is_balance_limit(limit) || routing_limit_exhausted(limit) {
                continue;
            }
            if let Some(elapsed) = elapsed_ratio(limit, now) {
                if limit.usage_ratio > elapsed {
                    return (RoutingState::StickyOnly, Some("window_pressure"));
                }
            }
        }
    }

    (RoutingState::Open, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::collections::HashMap;

    fn limit(kind: QuotaLimitType, status: &str, usage_ratio: f64, window: &str) -> QuotaLimitStatus {
        QuotaLimitStatus {
            kind,
            status: status.to_string(),
            usage_ratio,
            ready: false,
            next_reset_at: None,
            availability_group: String::new(),
            window: window.to_string(),
            account: String::new(),
            period_start: None,
            period_cost: None,
            period_quota: None,
        }
    }
    use chrono::TimeZone;

    fn now_fixture() -> (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) {
        let now = Utc.with_ymd_and_hms(2026, 9, 8, 12, 0, 0).unwrap();
        (now, now - Duration::minutes(30), now + Duration::minutes(30))
    }

    #[test]
    fn is_balance_limit_uses_window_label() {
        for (window, want) in [
            (WINDOW_PAY_AS_YOU_GO, true),
            (WINDOW_CREDITS, true),
            ("payg", false),
            ("", false),
            ("5h", false),
        ] {
            assert_eq!(is_balance_limit(&limit(QuotaLimitType::Token, "available", 0.0, window)), want, "{window}");
        }
    }

    #[test]
    fn elapsed_ratio_requires_an_active_window() {
        let (now, start, reset) = now_fixture();
        for (name, s, r, want_ratio, want_ok) in [
            ("active", Some(start), Some(reset), Some(0.5), true),
            ("missing start", None, Some(reset), None, false),
            ("missing reset", Some(start), None, None, false),
            ("start at now", Some(now), Some(reset), None, false),
            ("reset at now", Some(start), Some(now), None, false),
        ] {
            let mut l = limit(QuotaLimitType::Token, "available", 0.0, "5h");
            l.period_start = s;
            l.next_reset_at = r;
            let got = elapsed_ratio(&l, now);
            assert_eq!(got.is_some(), want_ok, "{name}");
            if let Some(ratio) = got {
                assert_eq!(ratio, want_ratio.unwrap(), "{name}");
            }
        }
    }

    #[test]
    fn evaluate_quota_routing_decision_table() {
        let (now, window_start, window_reset) = now_fixture();

        let mut timed = limit(QuotaLimitType::Token, "available", 0.4, "5h");
        timed.period_start = Some(window_start);
        timed.next_reset_at = Some(window_reset);

        let mut pressured = limit(QuotaLimitType::Token, "available", 0.8, "5h");
        pressured.period_start = Some(window_start);
        pressured.next_reset_at = Some(window_reset);
        pressured.availability_group = "funding".into();
        let mut payg = limit(QuotaLimitType::Token, "available", 0.0, WINDOW_PAY_AS_YOU_GO);
        payg.availability_group = "funding".into();

        let mut ratio_one = limit(QuotaLimitType::Token, "available", 1.0, "5h");
        ratio_one.period_start = Some(window_start);
        ratio_one.next_reset_at = Some(window_reset);

        let group = |name: &str, mut l: QuotaLimitStatus| {
            l.availability_group = name.into();
            l
        };

        let cases: Vec<(&str, Vec<QuotaLimitStatus>, &str, QuotaLimitType, RoutingState, Option<&str>)> = vec![
            ("open window under pace", vec![timed.clone()], "available", QuotaLimitType::Token, RoutingState::Open, None),
            (
                "window over pace and balance positive",
                vec![pressured.clone(), payg.clone()],
                "available",
                QuotaLimitType::Token,
                RoutingState::StickyOnly,
                Some("window_pressure"),
            ),
            (
                "window exhausted and payg available in same group",
                vec![
                    group("funding", limit(QuotaLimitType::Token, "exhausted", 1.0, "5h")),
                    group("funding", limit(QuotaLimitType::Token, "available", 0.0, WINDOW_PAY_AS_YOU_GO)),
                ],
                "available",
                QuotaLimitType::Token,
                RoutingState::StickyOnly,
                Some("window_exhausted_balance_fallback"),
            ),
            (
                "window exhausted and another window available in same group",
                vec![
                    group("funding", limit(QuotaLimitType::Token, "exhausted", 1.0, "5h")),
                    group("funding", limit(QuotaLimitType::Token, "available", 0.2, "7d")),
                ],
                "available",
                QuotaLimitType::Token,
                RoutingState::Open,
                None,
            ),
            (
                "window group exhausted and separate balance group available",
                vec![
                    group("window", limit(QuotaLimitType::Token, "exhausted", 1.0, "5h")),
                    group("balance", limit(QuotaLimitType::Token, "available", 0.0, WINDOW_CREDITS)),
                ],
                "available",
                QuotaLimitType::Token,
                RoutingState::StickyOnly,
                Some("window_exhausted_balance_fallback"),
            ),
            (
                "window exhausted and balance exhausted",
                vec![
                    group("funding", limit(QuotaLimitType::Token, "exhausted", 1.0, "5h")),
                    group("funding", limit(QuotaLimitType::Token, "exhausted", 1.0, WINDOW_PAY_AS_YOU_GO)),
                ],
                "available",
                QuotaLimitType::Token,
                RoutingState::Exhausted,
                None,
            ),
            (
                "balance-only available",
                vec![limit(QuotaLimitType::Token, "available", 0.0, WINDOW_CREDITS)],
                "available",
                QuotaLimitType::Token,
                RoutingState::Open,
                None,
            ),
            (
                "balance-only exhausted",
                vec![limit(QuotaLimitType::Token, "exhausted", 1.0, WINDOW_CREDITS)],
                "available",
                QuotaLimitType::Token,
                RoutingState::Exhausted,
                None,
            ),
            (
                "window without time information",
                vec![limit(QuotaLimitType::Token, "available", 0.9, "5h")],
                "available",
                QuotaLimitType::Token,
                RoutingState::Open,
                None,
            ),
            (
                "stale window is not balance",
                vec![limit(QuotaLimitType::Token, "exhausted", 0.0, "5h")],
                "available",
                QuotaLimitType::Token,
                RoutingState::Exhausted,
                None,
            ),
            (
                "ratio one is exhausted",
                vec![ratio_one],
                "available",
                QuotaLimitType::Token,
                RoutingState::Exhausted,
                None,
            ),
            (
                "ungrouped windows require all windows",
                vec![
                    limit(QuotaLimitType::Token, "available", 0.2, "5h"),
                    limit(QuotaLimitType::Token, "exhausted", 1.0, "7d"),
                ],
                "available",
                QuotaLimitType::Token,
                RoutingState::Exhausted,
                None,
            ),
            (
                "channel exhausted short-circuits limits",
                vec![limit(QuotaLimitType::Token, "available", 0.0, "5h")],
                "exhausted",
                QuotaLimitType::Token,
                RoutingState::Exhausted,
                None,
            ),
            (
                "limit type isolation without shared group",
                vec![
                    limit(QuotaLimitType::Image, "exhausted", 1.0, "5h"),
                    limit(QuotaLimitType::Token, "available", 0.0, "5h"),
                ],
                "available",
                QuotaLimitType::Image,
                RoutingState::Exhausted,
                None,
            ),
            (
                "cross type or group inclusion",
                vec![
                    group("funding", limit(QuotaLimitType::Token, "exhausted", 1.0, "5h")),
                    group("funding", limit(QuotaLimitType::SubscriptionCycle, "available", 0.0, "cycle")),
                ],
                "available",
                QuotaLimitType::Token,
                RoutingState::Open,
                None,
            ),
            ("unknown and no data is neutral", vec![], "unknown", QuotaLimitType::Token, RoutingState::Unknown, None),
        ];

        for (name, limits, overall, lt, want_state, want_reason) in cases {
            let (state, reason) = evaluate_quota_routing(&limits, overall, lt, now);
            assert_eq!(state, want_state, "{name} (reason {reason:?})");
            assert_eq!(reason, want_reason, "{name}");
        }
    }

    #[test]
    fn routing_groups_availability_or_semantics() {
        // Group name counts when any TOKEN limit carries it; cross-type limits
        // in that group are included; ungrouped other-type limits are dropped.
        let limits = vec![
            group("funding", limit(QuotaLimitType::Token, "exhausted", 1.0, "5h")),
            group("funding", limit(QuotaLimitType::SubscriptionCycle, "available", 0.0, "cycle")),
            limit(QuotaLimitType::Image, "available", 0.0, "5h"),
            limit(QuotaLimitType::Token, "available", 0.0, "7d"),
            limit(QuotaLimitType::Token, "available", 0.0, WINDOW_CREDITS),
        ];
        let groups = routing_groups(&limits, QuotaLimitType::Token);

        let by_group: HashMap<&str, usize> = groups
            .iter()
            .map(|g| (g[0].availability_group.as_str(), g.len()))
            .collect();
        assert_eq!(by_group["funding"], 2);
        // ungrouped windows and balances form two separate groups
        let empty_groups: Vec<&Vec<&QuotaLimitStatus>> =
            groups.iter().filter(|g| g[0].availability_group.is_empty()).collect();
        assert_eq!(empty_groups.len(), 2);
        let window_group = empty_groups.iter().find(|g| g[0].window == "7d").unwrap();
        assert_eq!(window_group.len(), 1);
        let balance_group = empty_groups.iter().find(|g| g[0].window == WINDOW_CREDITS).unwrap();
        assert_eq!(balance_group.len(), 1);
        // image limit without a named group is excluded entirely
        assert!(!groups.iter().any(|g| g.iter().any(|l| l.kind == QuotaLimitType::Image)));
    }

    fn group(name: &str, mut l: QuotaLimitStatus) -> QuotaLimitStatus {
        l.availability_group = name.into();
        l
    }
}
