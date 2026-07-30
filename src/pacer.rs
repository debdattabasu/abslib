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
//! # Per-kind, or connection-wide?
//!
//! Worth settling before choosing a control, because the two look identical from a single-kind probe and
//! call for opposite designs. Push **two different** request kinds at once, each at a rate that is clean
//! on its own. Both still clean ⇒ the budget is per kind, and one shared bucket would be actively wrong:
//! it makes one kind's traffic delay another that has spent nothing, and taxes every request for a
//! ceiling it is nowhere near. Something trips ⇒ the budget really is connection-wide, and belongs in a
//! single bucket outside this type.
//!
//! Where the budget is per kind but applies to *every* kind, [`Pacer::with_default`] is the shape: list
//! the kinds you have measured, and let the rest mint their own bucket at a default rate.
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

/// Per-key token buckets, resolved at construction — plus, optionally, a **default** rate that gives
/// every other key a bucket of its own.
///
/// With [`new`](Self::new), a key absent from the map is unlimited: pace only what the server actually
/// throttles. With [`with_default`](Self::with_default), an absent key instead gets its own bucket at
/// the default rate, created on first use.
///
/// The distinction matters more than it looks, and it is the difference between two limits that are
/// easy to confuse:
///
/// - A **connection-wide** budget is one bucket shared by every kind of request. Model it outside this
///   type — one rate, one queue — because that is what it is.
/// - A **per-kind** budget that happens to apply to *every* kind is N independent buckets. That is
///   [`with_default`](Self::with_default), and modelling it as one shared bucket is strictly wrong: it
///   makes traffic on one kind delay an unrelated kind that has spent nothing, and it charges every
///   request for a ceiling it is nowhere near.
///
/// Measuring which one you have takes two *different* request kinds sent at once, each at a rate that
/// is clean alone. If both stay clean, the budget is per kind.
#[derive(Debug, Default)]
pub struct Pacer {
    buckets: HashMap<u32, Arc<Mutex<Bucket>>>,
    /// Applied to any key not in `buckets`. `None` leaves unlisted keys unpaced.
    default_rate: Option<Rate>,
    /// Buckets minted on demand for keys covered by `default_rate`. A plain `std::sync::Mutex`: it is
    /// held only across a map lookup and never across an await.
    ///
    /// Unbounded in principle. In practice the key space is a protocol's set of message types — small,
    /// closed, and known at compile time — so this settles at a fixed size. Do not hand it keys drawn
    /// from user input.
    minted: std::sync::Mutex<HashMap<u32, Arc<Mutex<Bucket>>>>,
}

impl Pacer {
    /// Build from a `key -> rate` map. Keys not in the map are **unlimited**. A non-positive rate is
    /// treated as unlimited rather than as a deadlock: it is what a caller reaches for to mean "no
    /// limit".
    pub fn new(rates: &HashMap<u32, Rate>) -> Self {
        Self {
            buckets: Self::explicit(rates),
            default_rate: None,
            minted: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// As [`new`](Self::new), but every key *not* in the map gets its **own** bucket at `default`.
    ///
    /// For the common shape where a server budgets each request kind separately: a handful of kinds are
    /// measured and listed, and the rest share a rate but not a queue.
    pub fn with_default(rates: &HashMap<u32, Rate>, default: Rate) -> Self {
        Self {
            buckets: Self::explicit(rates),
            default_rate: Some(default).filter(|r| r.per_second.is_finite() && r.per_second > 0.0),
            minted: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn explicit(rates: &HashMap<u32, Rate>) -> HashMap<u32, Arc<Mutex<Bucket>>> {
        let now = Instant::now();
        rates
            .iter()
            .filter(|(_, r)| r.per_second.is_finite() && r.per_second > 0.0)
            .map(|(&k, &r)| (k, Arc::new(Mutex::new(Bucket::new(r, now)))))
            .collect()
    }

    /// This key's bucket: the explicit one, else a minted default, else `None` when unpaced.
    fn bucket(&self, key: u32) -> Option<Arc<Mutex<Bucket>>> {
        if let Some(b) = self.buckets.get(&key) {
            return Some(b.clone());
        }
        let rate = self.default_rate?;
        let mut minted = self.minted.lock().unwrap_or_else(|e| e.into_inner());
        Some(
            minted
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(Bucket::new(rate, Instant::now()))))
                .clone(),
        )
    }

    /// Whether `key` is paced at all — for callers that want to skip the await entirely.
    ///
    /// Always true under [`with_default`](Self::with_default), since every key is then paced.
    pub fn is_limited(&self, key: u32) -> bool {
        self.buckets.contains_key(&key) || self.default_rate.is_some()
    }

    /// Wait until `key` may send, then consume one token. Returns immediately when `key` is unpaced.
    ///
    /// Call this in the caller's task, *before* building or enqueueing the request, and inside whatever
    /// deadline the caller asked for — see the module docs.
    pub async fn acquire(&self, key: u32) {
        let Some(bucket) = self.bucket(key) else {
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
        let bucket = self.bucket(key)?;
        let b = bucket.lock().await;
        let now = Instant::now();
        let wait = b.wait_for_one(now);
        (!wait.is_zero()).then(|| now + wait)
    }

    /// Tokens currently available for `key`, or `None` if unpaced. Test/diagnostic use.
    pub async fn available(&self, key: u32) -> Option<f64> {
        let bucket = self.bucket(key)?;
        let mut b = bucket.lock().await;
        b.refill(Instant::now());
        Some(b.tokens)
    }
}

#[cfg(test)]
mod default_rate_tests {
    use super::*;

    fn rates(pairs: &[(u32, Rate)]) -> HashMap<u32, Rate> {
        pairs.iter().copied().collect()
    }

    /// A default gives an unlisted key its **own** bucket, not a share of someone else's.
    ///
    /// This is the whole point of the feature and the thing a "one global bucket" implementation gets
    /// wrong: spending key A's budget must leave key B untouched, because the server budgets them
    /// separately. Modelled as one shared bucket, a busy A would delay a B that has spent nothing.
    #[tokio::test(start_paused = true)]
    async fn a_default_gives_each_unlisted_key_its_own_budget() {
        let pacer = Pacer::with_default(&rates(&[]), Rate::new(10.0, 2));
        // Drain key 1's burst entirely.
        pacer.acquire(1).await;
        pacer.acquire(1).await;
        assert!(
            pacer.available(1).await.unwrap() < 1.0,
            "key 1's burst is spent"
        );
        // Key 2 has spent nothing and must be untouched.
        assert_eq!(
            pacer.available(2).await,
            Some(2.0),
            "an unrelated key shared key 1's bucket"
        );

        let start = Instant::now();
        pacer.acquire(2).await;
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "key 2 waited on key 1's spending"
        );
    }

    /// An explicit entry still wins over the default — that is what makes the default a *floor* for
    /// the kinds nobody measured, rather than an override of the ones somebody did.
    #[tokio::test(start_paused = true)]
    async fn an_explicit_rate_beats_the_default() {
        let pacer = Pacer::with_default(&rates(&[(7, Rate::new(1.0, 1))]), Rate::new(100.0, 100));
        assert_eq!(
            pacer.available(7).await,
            Some(1.0),
            "key 7 keeps its measured burst of 1"
        );
        assert_eq!(
            pacer.available(8).await,
            Some(100.0),
            "key 8 takes the default"
        );

        pacer.acquire(7).await;
        let start = Instant::now();
        pacer.acquire(7).await; // 1/s, burst spent => must wait ~1s
        assert!(
            start.elapsed() >= Duration::from_millis(900),
            "the explicit rate was not applied"
        );
    }

    /// `new` must stay exactly as it was: no default, unlisted keys unpaced.
    #[tokio::test(start_paused = true)]
    async fn without_a_default_an_unlisted_key_is_still_unpaced() {
        let pacer = Pacer::new(&rates(&[(7, Rate::new(1.0, 1))]));
        assert!(!pacer.is_limited(8));
        assert_eq!(pacer.available(8).await, None);
        let start = Instant::now();
        for _ in 0..1_000 {
            pacer.acquire(8).await;
        }
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "an unlisted key was paced without a default"
        );
    }

    /// A minted bucket is minted **once**: the second caller must see the first one's spending, or the
    /// default would be no limit at all.
    #[tokio::test(start_paused = true)]
    async fn a_minted_bucket_is_reused_not_recreated() {
        let pacer = Pacer::with_default(&rates(&[]), Rate::new(1.0, 2));
        pacer.acquire(42).await;
        pacer.acquire(42).await;
        let start = Instant::now();
        pacer.acquire(42).await; // burst spent; a fresh bucket would let this through instantly
        assert!(
            start.elapsed() >= Duration::from_millis(900),
            "the bucket was re-minted, not reused"
        );
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
