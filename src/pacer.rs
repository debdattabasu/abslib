//! Per-key **rate** limiting — a token bucket for each request kind.
//!
//! The sibling of [`crate::limits`], and the two are constantly confused because both make callers wait:
//!
//! | | bounds | right when the server… |
//! |---|---|---|
//! | [`limits`](crate::limits) | how many are **in flight** at once | refuses concurrency and fails the excess |
//! | this module | how many per **second** | throttles a sustained rate |
//!
//! They are not interchangeable, and picking the wrong one is a real failure mode. A semaphore cannot
//! enforce a rate, because rate ≈ concurrency ÷ latency and latency is not yours to control — a cap of 2
//! permits 20/s against a 100 ms server and 200/s against a 10 ms one. Equally, a pacer cannot enforce a
//! concurrency ceiling: any number of requests may be in flight simultaneously as long as they *started*
//! far enough apart.
//!
//! # Why a bucket rather than a minimum interval
//!
//! The obvious implementation is "space every send by `1/rate`", which is a token bucket with a burst of
//! one. It is simple and it is usually wrong, for two reasons found by measurement rather than argument:
//!
//! - Servers commonly tolerate a **burst** and only object to the sustained rate. Forcing even spacing
//!   throws away that headroom: a caller doing a four-way fan-out pays three artificial delays for
//!   something the server would have accepted at once.
//! - It taxes every request even when nothing is near the limit. At 45/s that is ~22 ms of enforced
//!   spacing on each call, so a burst of ten costs ≥220 ms of pure waiting.
//!
//! So: rate *and* burst, per key. Set `burst: 1` to get strict spacing where that is genuinely what the
//! server wants.
//!
//! # Where to put the wait
//!
//! In the **caller's** task, before the request is built or enqueued, and inside the caller's own deadline —
//! exactly as for [`limits`](crate::limits). That keeps the driver out of it: pacing inside a driver's write
//! path means either sleeping in a `select!` handler (which stalls reads, timers and shutdown together) or
//! parking the command in a queue that something else must remember to retry, which is a second scheduler.
//!
//! Where a driver *does* need to pace its own traffic, gate on [`ready_at`](Pacer::ready_at) from the loop
//! and disable the command arms until then — a gate on accepting work, never a sleep in a handler.
//!
//! # Fairness
//!
//! Waiters on one key queue on a `tokio::sync::Mutex`, which is FIFO, and each holds it only until its own
//! token is due. So a fan-out drains in the order it was issued and a late arrival cannot overtake the tail.
//! Because the wait is inside the caller's deadline, a caller that spends its whole budget queued gets a
//! timeout and never reaches the wire — bounded, not indefinite.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

/// A sustained rate plus the burst allowed above it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    /// Sustained requests per second. Must be `> 0`.
    pub per_second: f64,
    /// How many may go at once from idle. `1` gives strict `1/per_second` spacing.
    pub burst: u32,
}

impl Rate {
    /// `per_second` with a burst of `burst`, clamped to values a bucket can honour.
    pub fn new(per_second: f64, burst: u32) -> Self {
        Self {
            per_second: per_second.max(f64::MIN_POSITIVE),
            burst: burst.max(1),
        }
    }

    /// Strict even spacing: no burst above the sustained rate.
    pub fn spaced(per_second: f64) -> Self {
        Self::new(per_second, 1)
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
    rate: Rate,
}

impl Bucket {
    fn new(rate: Rate, now: Instant) -> Self {
        // Start full: a connection that has just opened has not spent anything.
        Self {
            tokens: rate.burst as f64,
            last: now,
            rate,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens =
                (self.tokens + elapsed * self.rate.per_second).min(self.rate.burst as f64);
            self.last = now;
        }
    }

    /// How long until one token is available. Zero when one is available now.
    fn wait_for_one(&self, now: Instant) -> Duration {
        let mut probe = Bucket {
            tokens: self.tokens,
            last: self.last,
            rate: self.rate,
        };
        probe.refill(now);
        if probe.tokens >= 1.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64((1.0 - probe.tokens) / self.rate.per_second)
        }
    }
}

/// Per-key token buckets, resolved once at construction.
///
/// A key absent from the map is **unlimited**, which is the common case — pace only what the server
/// actually throttles.
#[derive(Debug, Default)]
pub struct Pacer {
    buckets: HashMap<u32, Arc<Mutex<Bucket>>>,
}

impl Pacer {
    /// Build from a `key -> rate` map. A non-positive rate is treated as unlimited rather than as a
    /// deadlock: it is what a caller reaches for to mean "no limit".
    pub fn new(rates: &HashMap<u32, Rate>) -> Self {
        let now = Instant::now();
        Self {
            buckets: rates
                .iter()
                .filter(|(_, r)| r.per_second.is_finite() && r.per_second > 0.0)
                .map(|(&k, &r)| (k, Arc::new(Mutex::new(Bucket::new(r, now)))))
                .collect(),
        }
    }

    /// Whether `key` is paced at all — for callers that want to skip the await entirely.
    pub fn is_limited(&self, key: u32) -> bool {
        self.buckets.contains_key(&key)
    }

    /// Wait until `key` may send, then consume one token. Returns immediately when `key` is unpaced.
    ///
    /// Call this in the caller's task, *before* building or enqueueing the request, and inside whatever
    /// deadline the caller asked for — see the module docs.
    pub async fn acquire(&self, key: u32) {
        let Some(bucket) = self.buckets.get(&key).cloned() else {
            return;
        };
        // FIFO: waiters queue on the mutex in arrival order, and each holds it only until its own token is
        // due. Holding it across the sleep is deliberate — it is what makes the queue ordered instead of a
        // thundering retry loop where a late arrival can win the next token.
        let mut b = bucket.lock().await;
        let wait = b.wait_for_one(Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        b.refill(Instant::now());
        b.tokens = (b.tokens - 1.0).max(0.0);
    }

    /// When `key` will next be able to send, or `None` if unpaced or ready now.
    ///
    /// For a driver that must gate its own loop rather than await: arm a `sleep_until` on this and keep the
    /// command arms disabled until it fires.
    pub async fn ready_at(&self, key: u32) -> Option<Instant> {
        let bucket = self.buckets.get(&key)?;
        let b = bucket.lock().await;
        let now = Instant::now();
        let wait = b.wait_for_one(now);
        (!wait.is_zero()).then(|| now + wait)
    }

    /// Tokens currently available for `key`, or `None` if unpaced. Test/diagnostic use.
    pub async fn available(&self, key: u32) -> Option<f64> {
        let bucket = self.buckets.get(&key)?;
        let mut b = bucket.lock().await;
        b.refill(Instant::now());
        Some(b.tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rates(pairs: &[(u32, Rate)]) -> HashMap<u32, Rate> {
        pairs.iter().copied().collect()
    }

    #[tokio::test(start_paused = true)]
    async fn an_unpaced_key_never_waits() {
        let p = Pacer::new(&rates(&[(11, Rate::spaced(1.0))]));
        assert!(!p.is_limited(22));
        let t0 = Instant::now();
        p.acquire(22).await;
        assert_eq!(
            t0.elapsed(),
            Duration::ZERO,
            "an unpaced key must not sleep"
        );
        assert_eq!(p.available(22).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_goes_at_once_then_the_rate_binds() {
        // The reason this is a bucket and not a minimum interval: 4 may go immediately.
        let p = Pacer::new(&rates(&[(11, Rate::new(10.0, 4))]));
        let t0 = Instant::now();
        for _ in 0..4 {
            p.acquire(11).await;
        }
        assert_eq!(
            t0.elapsed(),
            Duration::ZERO,
            "the whole burst goes without waiting"
        );

        // The bucket is now empty, so the fifth waits ~1/10 s for a token.
        p.acquire(11).await;
        assert_eq!(t0.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_of_one_is_strict_spacing() {
        let p = Pacer::new(&rates(&[(11, Rate::spaced(20.0))]));
        let t0 = Instant::now();
        for _ in 0..3 {
            p.acquire(11).await;
        }
        // First is free (bucket starts full), then two 50ms gaps.
        assert_eq!(t0.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn the_bucket_refills_while_idle_but_never_past_the_burst() {
        let p = Pacer::new(&rates(&[(11, Rate::new(10.0, 3))]));
        for _ in 0..3 {
            p.acquire(11).await;
        }
        assert_eq!(p.available(11).await, Some(0.0));

        // 10/s for 200ms => 2 tokens back.
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(p.available(11).await, Some(2.0));

        // Idle for an hour: still capped at the burst, not an unbounded credit to spend at once.
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(
            p.available(11).await,
            Some(3.0),
            "credit must not accumulate past the burst"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn keys_are_paced_independently() {
        let p = Pacer::new(&rates(&[
            (11, Rate::spaced(1.0)),
            (22, Rate::new(100.0, 10)),
        ]));
        p.acquire(11).await; // drains key 11's single token
        let t0 = Instant::now();
        p.acquire(22).await;
        assert_eq!(t0.elapsed(), Duration::ZERO, "key 22 has its own budget");
        assert_eq!(p.available(11).await, Some(0.0));
    }

    #[tokio::test(start_paused = true)]
    async fn waiters_are_served_in_arrival_order() {
        // A fan-out must drain in the order it was issued; otherwise the tail of a burst can be starved
        // indefinitely by later arrivals, which is the failure a naive retry loop has.
        let p = Arc::new(Pacer::new(&rates(&[(11, Rate::spaced(10.0))])));
        p.acquire(11).await; // empty the bucket so everyone below has to wait

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for i in 0..5 {
            let (p, ord) = (p.clone(), order.clone());
            // Serialize *enqueue* order, or the spawns race and there is no arrival order to assert.
            let started = Arc::new(tokio::sync::Notify::new());
            let s2 = started.clone();
            handles.push(tokio::spawn(async move {
                s2.notify_one();
                p.acquire(11).await;
                ord.lock().unwrap().push(i);
            }));
            started.notified().await;
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            vec![0, 1, 2, 3, 4],
            "the pacer is not FIFO"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn ready_at_reports_the_gate_for_a_driver_that_cannot_await() {
        let p = Pacer::new(&rates(&[(11, Rate::spaced(4.0))]));
        assert_eq!(p.ready_at(11).await, None, "a full bucket is ready now");
        p.acquire(11).await;
        let at = p.ready_at(11).await.expect("now paced out");
        assert_eq!(at - Instant::now(), Duration::from_millis(250));
        assert_eq!(p.ready_at(99).await, None, "an unpaced key never gates");
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_or_negative_rate_means_unlimited_not_deadlock() {
        let p = Pacer::new(&rates(&[
            (
                11,
                Rate {
                    per_second: 0.0,
                    burst: 1,
                },
            ),
            (
                12,
                Rate {
                    per_second: -5.0,
                    burst: 1,
                },
            ),
        ]));
        assert!(!p.is_limited(11) && !p.is_limited(12));
        let t0 = Instant::now();
        p.acquire(11).await;
        p.acquire(12).await;
        assert_eq!(t0.elapsed(), Duration::ZERO);
    }
}
