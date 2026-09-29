//! Port of axonhub `normalization.go`: every checker result passes through
//! `normalize_quota_data` before being stored/used.

use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::types::*;

pub fn normalize_quota_data(data: QuotaData) -> QuotaData {
    normalize_quota_data_at(data, Utc::now())
}

pub fn normalize_quota_data_at(mut data: QuotaData, now: DateTime<Utc>) -> QuotaData {
    data.limits = normalize_quota_limits(std::mem::take(&mut data.limits), now);
    let status = normalize_overall_quota_status(&data.status, &data.limits);

    // Earliest future reset across the top level and all limits.
    let mut next_reset_at = data.next_reset_at.filter(|t| *t > now);
    for l in &data.limits {
        if let Some(t) = l.next_reset_at {
            if t > now && next_reset_at.map_or(true, |cur| t < cur) {
                next_reset_at = Some(t);
            }
        }
    }

    data.status = status.to_string();
    data.next_reset_at = next_reset_at;
    data.ready = is_ready_status(&data.status);
    data
}

fn normalize_overall_quota_status(explicit: &str, limits: &[QuotaLimitStatus]) -> &'static str {
    let status = normalize_quota_status(explicit);
    if status == "exhausted" {
        return "exhausted";
    }
    let mut best_ready: Option<&'static str> = if is_ready_status(status) { Some(match status { "available" => "available", _ => "warning" }) } else { None };
    let mut has_exhausted = false;
    for l in limits {
        if is_ready_status(&l.status) {
            let s: &'static str = if l.status == "available" { "available" } else { "warning" };
            if best_ready.map_or(true, |b| status_rank(s) > status_rank(b)) {
                best_ready = Some(s);
            }
        } else if l.status == "exhausted" {
            has_exhausted = true;
        }
    }
    if let Some(s) = best_ready {
        return s;
    }
    if has_exhausted {
        return "exhausted";
    }
    "unknown"
}

fn normalize_quota_limits(limits: Vec<QuotaLimitStatus>, now: DateTime<Utc>) -> Vec<QuotaLimitStatus> {
    let mut out: Vec<QuotaLimitStatus> = Vec::with_capacity(limits.len());
    let mut indexes: HashMap<(QuotaLimitType, String, String, String), usize> = HashMap::new();
    for mut limit in limits {
        if limit.window.is_empty() || !limit.usage_ratio.is_finite() || limit.usage_ratio < 0.0 {
            continue;
        }
        limit.usage_ratio = limit.usage_ratio.min(1.0);
        limit.status = normalize_quota_status(&limit.status).to_string();
        if limit.usage_ratio >= 1.0 {
            limit.status = "exhausted".into();
        }
        limit.ready = is_ready_status(&limit.status);
        if limit.next_reset_at.map_or(false, |t| t <= now) {
            limit.next_reset_at = None;
        }
        if limit.next_reset_at.is_none() {
            limit.period_start = None;
        } else if let (Some(ps), Some(nr)) = (limit.period_start, limit.next_reset_at) {
            if ps >= nr {
                limit.period_start = None;
            }
        }

        let key = (limit.kind, limit.window.clone(), limit.availability_group.clone(), limit.account.clone());
        if let Some(&idx) = indexes.get(&key) {
            merge_quota_limit(&mut out[idx], limit);
        } else {
            indexes.insert(key, out.len());
            out.push(limit);
        }
    }
    out
}

fn merge_quota_limit(existing: &mut QuotaLimitStatus, incoming: QuotaLimitStatus) {
    if incoming.usage_ratio > existing.usage_ratio {
        existing.usage_ratio = incoming.usage_ratio;
    }
    if status_rank(&incoming.status) > status_rank(&existing.status) {
        existing.status = incoming.status;
    }
    if incoming.next_reset_at.map_or(false, |t| existing.next_reset_at.map_or(true, |e| t < e)) {
        existing.next_reset_at = incoming.next_reset_at;
    }
    if incoming.period_start.map_or(false, |t| existing.period_start.map_or(true, |e| t < e)) {
        existing.period_start = incoming.period_start;
    }
    if existing.next_reset_at.is_none()
        || existing.period_start.is_none()
        || existing.period_start >= existing.next_reset_at
    {
        existing.period_start = None;
    }
    existing.ready = is_ready_status(&existing.status);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn t(sec: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(sec, 0).unwrap()
    }

    #[test]
    fn drops_invalid_limits_and_clamps_ratio() {
        let now = t(1_000_000);
        let limits = vec![
            QuotaLimitStatus::token("available", 0.5, Some(now + Duration::hours(1))).with_window("5h", Duration::hours(5)),
            QuotaLimitStatus::token("available", 1.7, None).with_window("7d", Duration::days(7)), // clamp -> exhausted
            QuotaLimitStatus::token("available", f64::NAN, None).with_window("daily", Duration::hours(24)), // dropped
            QuotaLimitStatus::token("available", 0.1, None), // dropped: no window
        ];
        let mut d = QuotaData::new("test", "available");
        d.limits = limits;
        let out = normalize_quota_data_at(d, now);
        assert_eq!(out.limits.len(), 2);
        assert_eq!(out.limits[1].status, "exhausted");
        // Overall: best ready among limits wins over exhausted presence.
        assert_eq!(out.status, "available");
        assert_eq!(out.next_reset_at, Some(now + Duration::hours(1)));
    }

    #[test]
    fn explicit_exhausted_is_kept() {
        let mut d = QuotaData::new("test", "exhausted");
        d.limits = vec![QuotaLimitStatus::token("available", 0.1, None).with_window("5h", Duration::hours(5))];
        let out = normalize_quota_data_at(d, Utc::now());
        assert_eq!(out.status, "exhausted");
        assert!(!out.ready);
    }

    #[test]
    fn merges_duplicate_windows_taking_worse() {
        let now = t(1_000_000);
        let a = QuotaLimitStatus::token("available", 0.3, Some(now + Duration::hours(2))).with_window("5h", Duration::hours(5));
        let mut b = QuotaLimitStatus::token("warning", 0.9, Some(now + Duration::hours(3))).with_window("5h", Duration::hours(5));
        b.account = "acct".into();
        let mut d = QuotaData::new("test", "available");
        d.limits = vec![a, b.clone(), {
            let mut c = b.clone();
            c.usage_ratio = 0.95; // same identity -> merges
            c
        }];
        let out = normalize_quota_data_at(d, now);
        assert_eq!(out.limits.len(), 2);
        let merged = out.limits.iter().find(|l| l.account == "acct").unwrap();
        assert_eq!(merged.usage_ratio, 0.95);
        assert_eq!(merged.status, "warning");
        assert_eq!(out.status, "warning");
    }

    #[test]
    fn past_reset_cleared() {
        let now = t(1_000_000);
        let l = QuotaLimitStatus::token("warning", 0.9, Some(now - Duration::hours(1))).with_window("5h", Duration::hours(5));
        let mut d = QuotaData::new("test", "available");
        d.limits = vec![l];
        let out = normalize_quota_data_at(d, now);
        assert_eq!(out.limits[0].next_reset_at, None);
        assert_eq!(out.limits[0].period_start, None);
        assert_eq!(out.next_reset_at, None);
    }
}
