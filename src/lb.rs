//! Channel load balancing (Phase 3), mirroring axonhub's ChannelBalancer
//! split: [`sort_candidates`] is the pure `ChannelPool` half (candidates +
//! strategy + context -> ordered candidates), [`LbSelector`] is the stateful
//! half (per-request `next`/`commit`/`rollback` fused with the process-local
//! breaker state in `keystate`).
//!
//! Strategies (`lb_strategy`):
//! - `priority`: priority desc, then weight desc — high-priority tiers are
//!   exhausted before degrading (axonhub ordering-policy semantics).
//! - `round_robin`: rotates each quota tier by a compressed cursor (one
//!   process-global counter); key-level rotation already lives in keystate.
//! - `weighted_shuffle`: Efraimidis–Spirakis keys `u^(1/weight)` — first pick
//!   is proportional to weight (generalizes the old weight-desc order).
//! - `error_aware`: scores channels by success/latency EMA lazily aggregated
//!   from the Phase 2 `requests` table (in-memory cache, periodic refresh;
//!   no real-time stats component; with `trace.level = off` there is no data
//!   and all channels score neutral).
//! - `sticky`: pins a request session (`x-session-id` header, else the
//!   unified `user` field) to a channel; pins carry a TTL with lazy cleanup
//!   so selector state cannot grow unbounded (axonhub has the same hazard).
//!
//! Quota tiers dominate every strategy: `Candidate.tier` ascends first
//! (Open before Unknown before StickyOnly), the strategy only orders within
//! a tier.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::keystate;
use crate::storage::{Channel, Db, TraceRepo};

/// Channel-level `settings.lb_strategy` overrides the instance `lb.default`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LbStrategy {
    Priority,
    RoundRobin,
    WeightedShuffle,
    ErrorAware,
    Sticky,
}

impl LbStrategy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "priority" => Some(Self::Priority),
            "round_robin" | "round-robin" => Some(Self::RoundRobin),
            "weighted_shuffle" | "weighted-shuffle" => Some(Self::WeightedShuffle),
            "error_aware" | "error-aware" => Some(Self::ErrorAware),
            "sticky" => Some(Self::Sticky),
            _ => None,
        }
    }

    /// Unknown values fall back to `weighted_shuffle` with a warning (the
    /// pre-Phase-3 behavior, so a typo never breaks routing).
    pub fn parse_or_default(s: &str) -> Self {
        Self::parse(s).unwrap_or_else(|| {
            tracing::warn!("unknown lb strategy {s:?}, falling back to \"weighted_shuffle\"");
            Self::WeightedShuffle
        })
    }
}

/// A candidate channel in its quota-routing tier; tier dominates ordering.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub tier: u8,
    pub channel: Channel,
}

/// Channel-level strategy override from `settings.lb_strategy`; invalid
/// values warn and are ignored (instance default applies).
fn channel_strategy(ch: &Channel) -> Option<LbStrategy> {
    let v: serde_json::Value = serde_json::from_str(&ch.settings).ok()?;
    let raw = v.get("lb_strategy")?.as_str()?;
    match LbStrategy::parse(raw) {
        Some(s) => Some(s),
        None => {
            tracing::warn!(channel_id = %ch.id, "ignoring invalid settings.lb_strategy {raw:?}");
            None
        }
    }
}

/// Governing strategy for one request: the highest-priority candidate's
/// `settings.lb_strategy` (ties by created_at, then id), else `fallback`.
pub fn resolve_strategy(candidates: &[Candidate], fallback: LbStrategy) -> LbStrategy {
    let Some(first) = candidates.iter().min_by(|a, b| {
        b.channel
            .priority
            .cmp(&a.channel.priority)
            .then_with(|| a.channel.created_at.cmp(&b.channel.created_at))
            .then_with(|| a.channel.id.cmp(&b.channel.id))
    }) else {
        return fallback;
    };
    channel_strategy(&first.channel).unwrap_or(fallback)
}

/// Per-request ordering context; every input is explicit so the pool
/// function stays deterministic and unit-testable.
pub struct RouteCtx<'a> {
    pub strategy: LbStrategy,
    /// Error-aware scores; ignored by other strategies.
    pub metrics: Option<&'a HashMap<String, ChannelMetrics>>,
    /// Round-robin cursor value for this request (from [`next_rr_offset`]).
    pub rr_offset: u64,
    /// Uniforms in (0, 1) for weighted_shuffle; one draw per candidate.
    /// `Send` so the routing context never blocks the relay future's Send.
    pub rand: &'a mut (dyn FnMut() -> f64 + Send),
}

/// Pure pool half: orders `candidates` in place by tier, then by strategy
/// within each tier (rotation/shuffle never lifts a channel above a better
/// quota tier).
pub fn sort_candidates(candidates: &mut Vec<Candidate>, ctx: &mut RouteCtx) {
    candidates.sort_by(|a, b| a.tier.cmp(&b.tier));
    let mut start = 0;
    while start < candidates.len() {
        let tier = candidates[start].tier;
        let mut end = start + 1;
        while end < candidates.len() && candidates[end].tier == tier {
            end += 1;
        }
        sort_tier(&mut candidates[start..end], ctx);
        start = end;
    }
}

fn sort_tier(cands: &mut [Candidate], ctx: &mut RouteCtx) {
    match ctx.strategy {
        LbStrategy::Priority | LbStrategy::Sticky => cands.sort_by(|a, b| {
            // Sticky's unpinned base order is the priority/weight order; the
            // session pin itself is applied by the selector at `next`.
            b.channel
                .priority
                .cmp(&a.channel.priority)
                .then_with(|| b.channel.weight.cmp(&a.channel.weight))
                .then_with(|| a.channel.id.cmp(&b.channel.id))
        }),
        LbStrategy::WeightedShuffle => {
            // Efraimidis–Spirakis: key = u^(1/w) sorts a weighted sample
            // without replacement; u in (0,1), non-positive weights sink.
            let mut keyed: Vec<(f64, Candidate)> = cands
                .iter()
                .cloned()
                .map(|c| {
                    let u = (ctx.rand)().clamp(1e-12, 1.0);
                    let w = c.channel.weight.max(0) as f64;
                    let key = if w > 0.0 { u.powf(1.0 / w) } else { 0.0 };
                    (key, c)
                })
                .collect();
            keyed.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.1.channel.id.cmp(&b.1.channel.id))
            });
            let ordered: Vec<Candidate> = keyed.into_iter().map(|(_, c)| c).collect();
            cands.clone_from_slice(&ordered);
        }
        LbStrategy::RoundRobin => {
            cands.sort_by(|a, b| a.channel.id.cmp(&b.channel.id));
            if !cands.is_empty() {
                let n = cands.len();
                cands.rotate_left((ctx.rr_offset % n as u64) as usize);
            }
        }
        LbStrategy::ErrorAware => {
            let empty = HashMap::new();
            let metrics = ctx.metrics.unwrap_or(&empty);
            let mut scored: Vec<(f64, Candidate)> = cands
                .iter()
                .cloned()
                .map(|c| {
                    let score = metrics.get(&c.channel.id).map(score_channel).unwrap_or(0.0);
                    (score, c)
                })
                .collect();
            scored.sort_by(|a, b| {
                b.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.1.channel.id.cmp(&b.1.channel.id))
            });
            let ordered: Vec<Candidate> = scored.into_iter().map(|(_, c)| c).collect();
            cands.clone_from_slice(&ordered);
        }
    }
}

/// Error-aware score: success EMA dominates, latency EMA penalizes
/// logarithmically (a 10s channel is bad but not infinitely worse than 5s).
/// Range roughly (-25*ln(1+s), 100]; channels without data score 0 (neutral:
/// above degraded channels, below healthy ones).
fn score_channel(m: &ChannelMetrics) -> f64 {
    m.success_ema * 100.0 - 25.0 * (1.0 + m.latency_ema_ms / 1000.0).ln()
}

// ---------------------------------------------------------------------------
// Round-robin cursor: one process-global counter per decision ("channel+key
// double enumeration" — keys rotate inside keystate already, so channel side
// stays a single compressed count).
// ---------------------------------------------------------------------------

static RR_CURSOR: AtomicU64 = AtomicU64::new(0);

/// Next round-robin cursor value; callers pass it into [`RouteCtx`].
pub fn next_rr_offset() -> u64 {
    RR_CURSOR.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Sticky sessions: session -> channel pin with TTL and lazy cleanup. The TTL
// exists precisely because this map would otherwise grow with every new
// session key forever (roadmap-flagged hazard, same as axonhub).
// ---------------------------------------------------------------------------

/// Pin lifetime; refreshed on every successful request through the pin.
const STICKY_TTL: Duration = Duration::from_secs(30 * 60);
/// Hard cap on live pins; at capacity expired entries are purged first, then
/// the soonest-to-expire entry is evicted.
const STICKY_MAX: usize = 10_000;

struct StickyEntry {
    channel_id: String,
    expires: Instant,
}

static STICKY: LazyLock<Mutex<HashMap<String, StickyEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn purge_expired(map: &mut HashMap<String, StickyEntry>, now: Instant) {
    map.retain(|_, e| e.expires > now);
}

/// Pinned channel for `session`, or `None` when unpinned/expired.
pub fn sticky_get(session: &str) -> Option<String> {
    let mut map = STICKY.lock();
    let now = Instant::now();
    if map.len() >= STICKY_MAX {
        purge_expired(&mut map, now);
    }
    match map.get(session) {
        Some(e) if e.expires > now => Some(e.channel_id.clone()),
        Some(_) => {
            map.remove(session);
            None
        }
        None => None,
    }
}

fn sticky_pin_with_ttl(session: &str, channel_id: &str, ttl: Duration) {
    let mut map = STICKY.lock();
    let now = Instant::now();
    if map.len() >= STICKY_MAX && !map.contains_key(session) {
        purge_expired(&mut map, now);
        if map.len() >= STICKY_MAX {
            // evict the soonest-to-expire pin
            if let Some(oldest) = map
                .iter()
                .min_by_key(|(_, e)| e.expires)
                .map(|(k, _)| k.clone())
            {
                map.remove(&oldest);
            }
        }
    }
    map.insert(
        session.to_string(),
        StickyEntry {
            channel_id: channel_id.to_string(),
            expires: now + ttl,
        },
    );
}

fn sticky_unpin_if(session: &str, channel_id: &str) {
    let mut map = STICKY.lock();
    if map.get(session).map(|e| e.channel_id.as_str()) == Some(channel_id) {
        map.remove(session);
    }
}

// ---------------------------------------------------------------------------
// Error-aware metrics: EMA over the Phase 2 `requests` table, lazily
// refreshed (memory cache + periodic refresh, no live stats component).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct ChannelMetrics {
    /// EMA of per-request success in [0, 1]; new channels start at 1.0.
    pub success_ema: f64,
    /// EMA of request latency in ms; new channels start at 0.
    pub latency_ema_ms: f64,
    /// Last refresh (unix seconds) in which this channel had window data.
    last_seen_unix: i64,
}

/// Window of `requests` rows aggregated per refresh.
const METRICS_WINDOW: Duration = Duration::from_secs(10 * 60);
/// Minimum interval between DB refreshes.
const METRICS_REFRESH: Duration = Duration::from_secs(30);
/// Channels idle longer than this are dropped (their EMA has gone stale).
const METRICS_KEEP: Duration = Duration::from_secs(30 * 60);
/// Blend factor between the previous EMA and the new window aggregate.
const EMA_ALPHA: f64 = 0.3;

struct MetricsCache {
    last_refresh: Option<Instant>,
    map: HashMap<String, ChannelMetrics>,
}

static METRICS: LazyLock<Mutex<MetricsCache>> = LazyLock::new(|| {
    Mutex::new(MetricsCache {
        last_refresh: None,
        map: HashMap::new(),
    })
});

/// Refreshes the metrics cache when stale; cheap no-op within
/// [`METRICS_REFRESH`]. Failures keep the previous cache (callers log).
pub async fn maybe_refresh_metrics(pool: &Db) {
    let stale = {
        let cache = METRICS.lock();
        cache
            .last_refresh
            .map(|t| t.elapsed() >= METRICS_REFRESH)
            .unwrap_or(true)
    };
    if stale {
        refresh_metrics(pool).await;
    }
}

/// Unconditional refresh; production goes through [`maybe_refresh_metrics`],
/// tests force cycles after inserting rows.
pub async fn refresh_metrics(pool: &Db) {
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default();
    let since = (chrono::Utc::now()
        - chrono::Duration::from_std(METRICS_WINDOW).unwrap_or(chrono::Duration::seconds(600)))
    .to_rfc3339();
    let rows = match TraceRepo::channel_activity_since(pool, &since).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!("error-aware metrics refresh failed: {e}");
            METRICS.lock().last_refresh = Some(Instant::now());
            return;
        }
    };
    // Window aggregates: (successes, total, latency sum).
    let mut windows: HashMap<String, (u64, u64, u64)> = HashMap::new();
    for (channel_id, status, latency_ms) in rows {
        let w = windows.entry(channel_id).or_default();
        w.1 += 1;
        if status == "success" {
            w.0 += 1;
        }
        w.2 += latency_ms.max(0) as u64;
    }
    let mut cache = METRICS.lock();
    let keep_after = now_unix - METRICS_KEEP.as_secs() as i64;
    cache.map.retain(|_, m| m.last_seen_unix >= keep_after);
    for (channel_id, (successes, total, latency_sum)) in windows {
        if total == 0 {
            continue;
        }
        let win_success = successes as f64 / total as f64;
        let win_latency = latency_sum as f64 / total as f64;
        let entry = cache
            .map
            .entry(channel_id)
            .or_insert(ChannelMetrics {
                success_ema: 1.0,
                latency_ema_ms: 0.0,
                last_seen_unix: now_unix,
            });
        entry.success_ema = EMA_ALPHA * win_success + (1.0 - EMA_ALPHA) * entry.success_ema;
        entry.latency_ema_ms = EMA_ALPHA * win_latency + (1.0 - EMA_ALPHA) * entry.latency_ema_ms;
        entry.last_seen_unix = now_unix;
    }
    cache.last_refresh = Some(Instant::now());
}

/// Point-in-time copy of the metrics map for a routing decision.
pub fn metrics_snapshot() -> HashMap<String, ChannelMetrics> {
    METRICS.lock().map.clone()
}

// ---------------------------------------------------------------------------
// Entropy for weighted_shuffle: xorshift64* (LB only, not crypto). Each
// request gets a fresh seed so requests don't all replay the same shuffle.
// ---------------------------------------------------------------------------

static RNG_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn thread_rand() -> impl FnMut() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e3779b97f4a7c15);
    let mut state = nanos ^ RNG_COUNTER.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
    move || {
        // xorshift64*
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let v = state.wrapping_mul(0x2545F4914F6CDD1D);
        // [0, 1); clamped into (0, 1) at the shuffle call site
        ((v >> 11) as f64) * (1.0 / (1u64 << 53) as f64)
    }
}

// ---------------------------------------------------------------------------
// Selector: per-request state machine over the ordered candidates.
// ---------------------------------------------------------------------------

pub struct LbSelector {
    pending: VecDeque<Candidate>,
    session_key: Option<String>,
    current: Option<String>,
    skipped_disabled: Vec<String>,
    pin_checked: bool,
}

impl LbSelector {
    /// `candidates` must already be ordered by [`sort_candidates`]. Session
    /// pins (`sticky`) move the pinned channel to the front on the first
    /// `next`; pins pointing outside the candidate set are ignored.
    pub fn new(candidates: Vec<Candidate>, session_key: Option<String>) -> Self {
        Self {
            pending: candidates.into(),
            session_key,
            current: None,
            skipped_disabled: Vec::new(),
            pin_checked: false,
        }
    }

    /// Moves a live session pin to the front (once per request) and returns
    /// the next non-auto-disabled candidate. Skipped disabled channels are
    /// recorded for the all-candidates-failed error message.
    pub fn next(&mut self) -> Option<Candidate> {
        if !self.pin_checked {
            self.pin_checked = true;
            if let Some(session) = self.session_key.clone() {
                if let Some(pinned) = sticky_get(&session) {
                    if let Some(pos) = self
                        .pending
                        .iter()
                        .position(|c| c.channel.id == pinned)
                    {
                        if pos > 0 {
                            if let Some(c) = self.pending.remove(pos) {
                                self.pending.push_front(c);
                            }
                        }
                    }
                }
            }
        }
        while let Some(cand) = self.pending.pop_front() {
            if let Some(until) = keystate::channel_disabled_until(&cand.channel.id) {
                tracing::debug!(channel = %cand.channel.id, until = %until, "skipping auto-disabled channel");
                self.skipped_disabled
                    .push(format!("{} (until {until})", cand.channel.name));
                continue;
            }
            self.current = Some(cand.channel.id.clone());
            return Some(cand);
        }
        self.current = None;
        None
    }

    /// Channels skipped because of the process-local auto-disable breaker.
    pub fn skipped_disabled(&self) -> &[String] {
        &self.skipped_disabled
    }

    /// Success through the current channel: (re)pins the session to it.
    pub fn commit(&mut self) {
        if let (Some(session), Some(current)) = (&self.session_key, &self.current) {
            sticky_pin_with_ttl(session, current, STICKY_TTL);
        }
    }

    /// Failure through the current channel: a session pinned to it is
    /// released so the next request re-picks instead of hammering a broken
    /// channel until TTL expiry.
    pub fn rollback(&mut self) {
        if let (Some(session), Some(current)) = (&self.session_key, &self.current) {
            sticky_unpin_if(session, current);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate, ChannelRepo, TraceRequest, TraceRepo};

    fn mkch(id: &str, weight: i64, priority: i64) -> Channel {
        Channel {
            id: id.into(),
            name: format!("ch-{id}"),
            channel_type: "openai".into(),
            base_url: "http://x".into(),
            credentials: "{}".into(),
            disabled_api_keys: "[]".into(),
            supported_models: "[]".into(),
            model_mapping: "{}".into(),
            weight,
            priority,
            status: "enabled".into(),
            settings: "{}".into(),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    fn cands(list: Vec<(u8, &str, i64, i64)>) -> Vec<Candidate> {
        list.into_iter()
            .map(|(tier, id, w, p)| Candidate { tier, channel: mkch(id, w, p) })
            .collect()
    }

    fn ids(c: &[Candidate]) -> Vec<String> {
        c.iter().map(|c| c.channel.id.clone()).collect()
    }

    fn ctx<'a>(
        strategy: LbStrategy,
        metrics: Option<&'a HashMap<String, ChannelMetrics>>,
        rr_offset: u64,
        rand: &'a mut (dyn FnMut() -> f64 + Send),
    ) -> RouteCtx<'a> {
        RouteCtx { strategy, metrics, rr_offset, rand }
    }

    fn seq_rand(vals: Vec<f64>) -> impl FnMut() -> f64 + Send {
        let mut i = 0usize;
        move || {
            let v = vals[i % vals.len()];
            i += 1;
            v
        }
    }

    fn metrics(success_ema: f64, latency_ema_ms: f64) -> ChannelMetrics {
        ChannelMetrics { success_ema, latency_ema_ms, last_seen_unix: 0 }
    }

    #[test]
    fn strategy_parses_all_variants() {
        assert_eq!(LbStrategy::parse("priority"), Some(LbStrategy::Priority));
        assert_eq!(LbStrategy::parse("round_robin"), Some(LbStrategy::RoundRobin));
        assert_eq!(LbStrategy::parse("round-robin"), Some(LbStrategy::RoundRobin));
        assert_eq!(LbStrategy::parse("weighted_shuffle"), Some(LbStrategy::WeightedShuffle));
        assert_eq!(LbStrategy::parse("error_aware"), Some(LbStrategy::ErrorAware));
        assert_eq!(LbStrategy::parse("sticky"), Some(LbStrategy::Sticky));
        assert_eq!(LbStrategy::parse("nope"), None);
        assert_eq!(LbStrategy::parse_or_default("nope"), LbStrategy::WeightedShuffle);
    }

    #[test]
    fn resolve_strategy_prefers_highest_priority_setting() {
        let mut a = mkch("a", 1, 5);
        a.settings = r#"{"lb_strategy":"round_robin"}"#.into();
        let mut b = mkch("b", 1, 9);
        b.settings = r#"{"lb_strategy":"sticky"}"#.into();
        let list = vec![
            Candidate { tier: 0, channel: a },
            Candidate { tier: 0, channel: b },
        ];
        // b has the higher priority, so its sticky setting governs.
        assert_eq!(resolve_strategy(&list, LbStrategy::Priority), LbStrategy::Sticky);
        // Without any channel override, the instance fallback wins.
        let plain = cands(vec![(0, "x", 1, 9), (0, "y", 1, 1)]);
        assert_eq!(resolve_strategy(&plain, LbStrategy::RoundRobin), LbStrategy::RoundRobin);
        // Invalid channel value is ignored, fallback applies.
        let mut z = mkch("z", 1, 9);
        z.settings = r#"{"lb_strategy":"bogus"}"#.into();
        let bad = vec![Candidate { tier: 0, channel: z }];
        assert_eq!(resolve_strategy(&bad, LbStrategy::Priority), LbStrategy::Priority);
    }

    #[test]
    fn priority_orders_tier_then_priority_then_weight() {
        let mut list = cands(vec![
            (0, "z", 99, 3),   // low priority, high weight
            (2, "w", 99, 99),  // worst tier wins nothing
            (0, "x", 1, 10),   // priority tie with y, lighter
            (0, "y", 5, 10),
        ]);
        let mut r = seq_rand(vec![0.5]);
        let mut c = ctx(LbStrategy::Priority, None, 0, &mut r);
        sort_candidates(&mut list, &mut c);
        assert_eq!(ids(&list), vec!["y", "x", "z", "w"]);
    }

    #[test]
    fn priority_degrade_sequence_through_selector() {
        // Integration: 3 priority tiers degrade 10 -> 5 -> 1 through the
        // selector (the failover order the relay actually walks).
        let mut list = cands(vec![(0, "low", 1, 1), (0, "high", 1, 10), (0, "mid", 1, 5)]);
        let mut r = seq_rand(vec![0.5]);
        let mut c = ctx(LbStrategy::Priority, None, 0, &mut r);
        sort_candidates(&mut list, &mut c);
        let mut sel = LbSelector::new(list, None);
        let mut seq = Vec::new();
        while let Some(cand) = sel.next() {
            seq.push(cand.channel.id.clone());
            sel.rollback();
        }
        assert_eq!(seq, vec!["high", "mid", "low"]);
    }

    #[test]
    fn weighted_shuffle_ranks_by_es_keys() {
        // u^(1/w): a(w10,u0.5)=0.9330, b(w1,u0.9)=0.9, c(w1,u0.8)=0.8
        let mut list = cands(vec![(0, "a", 10, 0), (0, "b", 1, 0), (0, "c", 1, 0)]);
        let mut r = seq_rand(vec![0.5, 0.9, 0.8]);
        let mut c = ctx(LbStrategy::WeightedShuffle, None, 0, &mut r);
        sort_candidates(&mut list, &mut c);
        assert_eq!(ids(&list), vec!["a", "b", "c"]);

        // a(w10,u0.1)=0.7943, b(w1,u0.99)=0.99, c(w1,u0.5)=0.5 -> b first
        let mut list2 = cands(vec![(0, "a", 10, 0), (0, "b", 1, 0), (0, "c", 1, 0)]);
        let mut r2 = seq_rand(vec![0.1, 0.99, 0.5]);
        let mut c2 = ctx(LbStrategy::WeightedShuffle, None, 0, &mut r2);
        sort_candidates(&mut list2, &mut c2);
        assert_eq!(ids(&list2), vec!["b", "a", "c"]);

        // Deterministic: same draws, same order.
        let mut list3 = cands(vec![(0, "a", 10, 0), (0, "b", 1, 0), (0, "c", 1, 0)]);
        let mut r3 = seq_rand(vec![0.1, 0.99, 0.5]);
        let mut c3 = ctx(LbStrategy::WeightedShuffle, None, 0, &mut r3);
        sort_candidates(&mut list3, &mut c3);
        assert_eq!(ids(&list3), vec!["b", "a", "c"]);
    }

    #[test]
    fn weighted_shuffle_sinks_nonpositive_weights() {
        let mut list = cands(vec![(0, "zero", 0, 0), (0, "neg", -2, 0), (0, "one", 1, 0)]);
        let mut r = seq_rand(vec![0.5, 0.5, 0.5]);
        let mut c = ctx(LbStrategy::WeightedShuffle, None, 0, &mut r);
        sort_candidates(&mut list, &mut c);
        // Positive weight first; zero/neg sink, ties broken by id.
        assert_eq!(ids(&list), vec!["one", "neg", "zero"]);
        // Quota tier still dominates the shuffle keys.
        let mut tiered = cands(vec![(0, "t0", 1, 0), (2, "t2", 100, 0)]);
        let mut r2 = seq_rand(vec![0.5, 0.99]);
        let mut c2 = ctx(LbStrategy::WeightedShuffle, None, 0, &mut r2);
        sort_candidates(&mut tiered, &mut c2);
        assert_eq!(ids(&tiered), vec!["t0", "t2"]);
    }

    #[test]
    fn round_robin_rotates_within_quota_tiers() {
        let base = || cands(vec![(0, "b", 1, 0), (0, "a", 1, 0), (0, "c", 1, 0), (1, "e", 1, 0), (1, "d", 1, 0)]);
        let mut r = seq_rand(vec![0.5]);
        let rotate = |offset: u64, r: &mut (dyn FnMut() -> f64 + Send)| {
            let mut list = base();
            let mut c = ctx(LbStrategy::RoundRobin, None, offset, r);
            sort_candidates(&mut list, &mut c);
            ids(&list)
        };
        assert_eq!(rotate(0, &mut r), vec!["a", "b", "c", "d", "e"]);
        assert_eq!(rotate(1, &mut r), vec!["b", "c", "a", "e", "d"]);
        assert_eq!(rotate(2, &mut r), vec!["c", "a", "b", "d", "e"]);
        // Offset 3 wraps the tier0 group (len 3) and rotates tier1 (len 2).
        assert_eq!(rotate(3, &mut r), vec!["a", "b", "c", "e", "d"]);
    }

    #[test]
    fn error_aware_punishes_latency_and_failures() {
        let mut map: HashMap<String, ChannelMetrics> = HashMap::new();
        map.insert("fast".into(), metrics(1.0, 100.0));   // ~97.6
        map.insert("slow".into(), metrics(1.0, 20_000.0)); // ~23.9
        map.insert("flaky".into(), metrics(0.2, 100.0));   // ~17.6
        // "fresh" has no data: neutral 0.0, above degraded but below healthy.
        let mut list = cands(vec![
            (0, "flaky", 1, 0),
            (0, "fresh", 1, 0),
            (0, "fast", 1, 0),
            (0, "slow", 1, 0),
        ]);
        let mut r = seq_rand(vec![0.5]);
        let mut c = ctx(LbStrategy::ErrorAware, Some(&map), 0, &mut r);
        sort_candidates(&mut list, &mut c);
        assert_eq!(ids(&list), vec!["fast", "slow", "flaky", "fresh"]);
    }

    #[test]
    fn sticky_commit_pins_and_rollback_releases() {
        let session = "ut-sticky-pin";
        // Unpinned: candidates come out in the order given.
        let mut sel = LbSelector::new(cands(vec![(0, "s1", 1, 0), (0, "s2", 1, 0)]), Some(session.into()));
        let first = sel.next().unwrap();
        assert_eq!(first.channel.id, "s1");
        sel.commit();
        assert_eq!(sticky_get(session).as_deref(), Some("s1"));

        // Pinned channel jumps the queue in a later request.
        let mut sel2 = LbSelector::new(cands(vec![(0, "s2", 1, 0), (0, "s1", 1, 0)]), Some(session.into()));
        let promoted = sel2.next().unwrap();
        assert_eq!(promoted.channel.id, "s1");
        // Failing the pinned channel releases the pin for the next request.
        sel2.rollback();
        assert_eq!(sticky_get(session), None);

        let mut sel3 = LbSelector::new(cands(vec![(0, "s2", 1, 0), (0, "s1", 1, 0)]), Some(session.into()));
        assert_eq!(sel3.next().unwrap().channel.id, "s2");
        sel3.rollback();
        assert_eq!(sticky_get(session), None);
    }

    #[test]
    fn sticky_pins_expire_with_ttl() {
        let session = "ut-sticky-ttl";
        sticky_pin_with_ttl(session, "c1", Duration::from_millis(30));
        assert_eq!(sticky_get(session).as_deref(), Some("c1"));
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(sticky_get(session), None);
    }

    #[test]
    fn sticky_store_purges_expired_and_caps() {
        // Short TTL (60s) on all filler entries: cap eviction always picks
        // these over any real 30-min pins from parallel tests, and expired
        // entries are purged first.
        for i in 0..50 {
            sticky_pin_with_ttl(&format!("ut-cap-dead-{i}"), "c", Duration::ZERO);
        }
        for i in 0..STICKY_MAX {
            sticky_pin_with_ttl(&format!("ut-cap-{i}"), "c", Duration::from_secs(60));
        }
        sticky_pin_with_ttl("ut-cap-final", "c9", Duration::from_secs(60));
        assert_eq!(sticky_get("ut-cap-final").as_deref(), Some("c9"));
        let live = STICKY.lock().values().filter(|e| e.expires > Instant::now()).count();
        assert!(live <= STICKY_MAX, "live pins {live} exceeded cap {STICKY_MAX}");
    }

    #[test]
    fn selector_skips_auto_disabled_channels() {
        let mut sel = LbSelector::new(
            cands(vec![(0, "ut-skip-a", 1, 0), (0, "ut-skip-b", 1, 0)]),
            None,
        );
        keystate::disable_channel("ut-skip-a");
        let picked = sel.next().unwrap();
        assert_eq!(picked.channel.id, "ut-skip-b");
        assert_eq!(sel.skipped_disabled().len(), 1);
        assert!(sel.skipped_disabled()[0].contains("ut-skip-a"));
        assert!(sel.next().is_none());
        keystate::enable_channel("ut-skip-a");
    }

    #[tokio::test]
    async fn error_aware_refresh_flips_preference_after_latency_injection() {
        // Integration against a real DB: equal channels, then high-latency
        // rows for ea-a flip the preference to ea-b (roadmap acceptance).
        let pool = connect("sqlite::memory:").await.unwrap();
        migrate(&pool).await.unwrap();
        for id in ["ea-a", "ea-b"] {
            ChannelRepo::insert(&pool, &mkch(id, 1, 0)).await.unwrap();
        }
        let now = || chrono::Utc::now().to_rfc3339();
        let row = |id: &str, ch: &str, latency_ms: i64| TraceRequest {
            id: id.into(),
            api_key_id: None,
            channel_id: Some(ch.into()),
            model: "m".into(),
            stream: false,
            status: "success".into(),
            error: None,
            ttft_ms: None,
            latency_ms,
            usage: "{}".into(),
            cost: "0".into(),
            created_at: now(),
        };
        TraceRepo::insert_request(&pool, &row("r1", "ea-a", 100)).await.unwrap();
        TraceRepo::insert_request(&pool, &row("r2", "ea-b", 100)).await.unwrap();
        refresh_metrics(&pool).await;

        let sort = || {
            let snap = metrics_snapshot();
            let mut list = cands(vec![(0, "ea-b", 1, 0), (0, "ea-a", 1, 0)]);
            let mut r = seq_rand(vec![0.5]);
            let mut c = ctx(LbStrategy::ErrorAware, Some(&snap), 0, &mut r);
            sort_candidates(&mut list, &mut c);
            ids(&list)
        };
        // Equal health: id-ascending tiebreak (ea-a). Input order must NOT
        // leak through (ea-b was first in the input).
        assert_eq!(sort(), vec!["ea-a", "ea-b"]);

        for i in 0..3 {
            TraceRepo::insert_request(&pool, &row(&format!("r3-{i}"), "ea-a", 50_000))
                .await
                .unwrap();
        }
        refresh_metrics(&pool).await;
        assert_eq!(sort(), vec!["ea-b", "ea-a"]);
    }

    #[tokio::test]
    async fn channel_priority_column_roundtrips() {
        let pool = connect("sqlite::memory:").await.unwrap();
        migrate(&pool).await.unwrap();
        let mut ch = mkch("prio", 1, 7);
        ChannelRepo::insert(&pool, &ch).await.unwrap();
        assert_eq!(ChannelRepo::get(&pool, "prio").await.unwrap().unwrap().priority, 7);
        ch.priority = -3;
        ChannelRepo::update(&pool, &ch).await.unwrap();
        assert_eq!(ChannelRepo::get(&pool, "prio").await.unwrap().unwrap().priority, -3);
    }
}
