//! Per-request-kind ceilings on **concurrent in-flight requests** — the client half of a server-side cap.
//!
//! # What this is for
//!
//! Some protocols cap how many of a given request kind a session may have outstanding, and — this is the
//! part that makes a client-side gate necessary — the excess is **failed rather than queued or throttled**.
//! There is no `Retry-After`, no backpressure signal, often not even a distinct error code: the surplus
//! request comes back as a generic internal error, indistinguishable at the call site from the server
//! having a genuine problem.
//!
//! Bulk reads are where this bites. A caller fanning out a backfill across instruments or date ranges
//! issues exactly the traffic shape that trips the cap, and gets back failures that look nothing like the
//! limit they actually are. That is what this prevents.
//!
//! Only some request kinds are usually capped, and the cap differs per kind, which is why this is a map
//! from request kind to ceiling rather than one global limit. Note also that different kinds fan out along
//! different axes: a per-instrument request is naturally N-wide in instruments, whereas a whole-account
//! request is only as wide as the caller chooses to split a span into windows — so equal ceilings cost
//! very unequal amounts in practice.
//!
//! # Server limit vs client gate — they fail oppositely
//!
//! This is the whole reason the client-side gate is worth having:
//!
//! - **Server limit exceeded → failed, immediately.** No queue, no throttle. The error is generic, so a
//!   caller cannot tell it apart from a real fault, and cannot tell whether retrying is safe.
//! - **Client gate exceeded → queued**, in the caller's own task, on a fair semaphore. Nothing is sent and
//!   nothing is enqueued to the driver until a slot frees, so the excess never reaches the server to be
//!   refused.
//!
//! The failure does not disappear — it *changes into a better failure*. A `Timeout` after waiting your turn
//! is honest about what happened and is safe to retry; a generic error from a refused concurrent request is
//! neither.
//!
//! # Queued is not unbounded
//!
//! Arm the request's own deadline **before** the acquire, so it covers the queue wait as well as the round
//! trip. A caller that spends its whole budget waiting gets a timeout and never reaches the wire. That also
//! sets the practical ceiling on a single fan-out: with a 15 s budget and ~150 ms per serialized request,
//! roughly 75 calls before the tail starts timing out. Past that, chunk the work, raise the budget, or raise
//! the cap.
//!
//! The queue is **FIFO** — tokio's `Semaphore` hands out permits in request order — so a fan-out drains in
//! the order it was issued and the tail cannot be overtaken by later arrivals. Nothing is dropped silently,
//! and a queued caller that goes away releases its place: an abandoned future removes its waiter, and the
//! permit is returned by `Drop` however the request ends.
//!
//! # Choosing the ceilings
//!
//! **Measure, do not guess, and do not assume the measured allowance is usable.** Fan out N identical
//! requests on one session and count the answers. Two things learned the hard way doing exactly that:
//!
//! - A measured allowance of 2 is not the same as a *reliable* allowance of 2. A depth-2 pipeline can lose
//!   requests intermittently — varying by access pattern and between sessions on the same server, which is
//!   consistent with server-side load rather than any rule a client could schedule around. Depth 1 is often
//!   the only depth that never fails. Serializing costs ~150–200 ms per request, which is unremarkable for
//!   bulk history reads, and it never hands a caller an error it cannot diagnose.
//! - A cap may be worth setting on a kind the server does **not** limit, purely so "bulk reads serialize" is
//!   one rule rather than a per-kind exception a caller has to look up. That is a real cost (roughly 2× on
//!   the affected kind) traded for uniformity — a legitimate choice, but make it deliberately and write down
//!   which entries are wire-derived and which are preference.
//!
//! Keep a live test that asserts what the *server* still does underneath, so that if a limit appears or
//! disappears, the test says so instead of the gate quietly becoming wrong.
//!
//! # Why a semaphore on the handle, not a gate in the driver
//!
//! Where these caps are per **session**, per-connection state is exactly the right granularity — no shared
//! or fleet-wide machinery is needed.
//!
//! Waiting *before* the command is enqueued keeps the driver untouched. Deferring inside the driver's write
//! path instead would mean a parked command sitting in a queue until something thought to retry it, which is
//! a second scheduler. Here the caller's own task waits, the permit is released by `Drop` however the request
//! ends, and the request's deadline covers the wait because it was armed before the acquire.
//!
//! # What this gate does *not* hold up
//!
//! **Latency-sensitive request kinds should not touch it.** If your driver separates command paths by kind —
//! so that a trade, say, never queues behind a backlog of reads — then that separation is a *different*
//! mechanism from this one, and the two are easy to conflate because both are about one caller's work not
//! blocking another's. This module gates concurrency within a kind; command-path separation gates
//! head-of-line blocking between kinds. You generally want both.
//!
//! The gate does help the separation, though, as a side effect worth knowing. Because a caller waits here
//! **before** enqueueing, a gated fan-out of N parks N−1 callers in their own tasks and puts at most `cap`
//! requests into the driver's channel. Clearing a limit gives that up: 300 concurrent reads then really do
//! offer 300 messages to the channel, and the surplus waits on the channel instead — still not on the
//! latency-sensitive path, but on a queue whose depth is not a knob.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Per-request-kind in-flight ceilings, resolved into semaphores once at connect.
///
/// Keyed by `u32` so it fits both small command-byte protocols and protocols whose message type is a
/// wider enum. A kind absent from the map is unlimited.
///
/// ```
/// # use abslib::limits::RequestLimits;
/// # use std::collections::HashMap;
/// # #[tokio::main(flavor = "current_thread")] async fn main() {
/// // Kind 11 may have one request outstanding; everything else is unlimited.
/// let limits = RequestLimits::new(&HashMap::from([(11, 1)]));
///
/// // Acquire BEFORE building or enqueueing the request, inside the caller's own deadline, so the
/// // wait counts against the budget the caller asked for.
/// let permit = limits.acquire(11).await;
/// assert_eq!(limits.available(11), Some(0));
///
/// // An uncapped kind never waits and yields no permit, so the fast path costs one hash lookup.
/// assert!(limits.acquire(22).await.is_none());
///
/// // The permit is owned: it returns on drop, however the request ends — reply, timeout, or a
/// // caller that dropped its future. That last case is the one a hand-rolled counter gets wrong.
/// drop(permit);
/// assert_eq!(limits.available(11), Some(1));
/// # }
/// ```
///
/// A kind that is absent is unlimited, which is the common case — cap only what the server caps.
#[derive(Debug, Default)]
pub struct RequestLimits {
    gates: HashMap<u32, Arc<Semaphore>>,
}

impl RequestLimits {
    /// Build from a `code -> max concurrent` map. A limit of `0` is treated as unlimited rather than
    /// as a deadlock: it is what a caller reaches for to mean "no cap".
    pub fn new(caps: &HashMap<u32, usize>) -> Self {
        Self {
            gates: caps
                .iter()
                .filter(|(_, &n)| n > 0)
                .map(|(&code, &n)| (code, Arc::new(Semaphore::new(n))))
                .collect(),
        }
    }

    /// Whether `code` is capped at all — for callers that want to skip the acquire entirely.
    pub fn is_limited(&self, code: u32) -> bool {
        self.gates.contains_key(&code)
    }

    /// Wait for a slot on `code`. `None` when the command is uncapped, so a caller holds
    /// `Option<permit>` either way and the uncapped path costs one hash lookup.
    ///
    /// The permit is **owned**, so it lives as long as the caller keeps it and is returned by `Drop` —
    /// including when the caller's future is dropped mid-request, which is the case a hand-rolled
    /// counter gets wrong.
    pub async fn acquire(&self, code: u32) -> Option<OwnedSemaphorePermit> {
        let gate = self.gates.get(&code)?.clone();
        // The semaphore is never closed, so this cannot fail.
        gate.acquire_owned().await.ok()
    }

    /// Cap several kinds against **one shared** ceiling, for a server that budgets a *class* of
    /// request rather than each kind separately.
    ///
    /// Builder-style so it composes with [`new`](Self::new): per-kind ceilings first, then any groups.
    /// A key given here replaces whatever per-kind gate it had.
    ///
    /// ```
    /// # use abslib::RequestLimits;
    /// # use std::collections::HashMap;
    /// # #[tokio::main(flavor = "current_thread")] async fn main() {
    /// // Three kinds of bulk read that the server will only answer one at a time.
    /// let limits = RequestLimits::new(&HashMap::new()).with_group(&[10, 11, 12], 1);
    /// let held = limits.acquire(10).await.expect("a slot");
    /// // ...and a DIFFERENT kind in the same group now waits, which is the whole point.
    /// assert_eq!(limits.available(11), Some(0));
    /// drop(held);
    /// assert_eq!(limits.available(11), Some(1));
    /// # }
    /// ```
    ///
    /// **Reach for this only when a measurement says the kinds interact**, and be careful about which
    /// measurement: a shared *concurrency* ceiling looks exactly like a per-kind *rate* budget from
    /// the outside, because `rate ≈ concurrency ÷ latency`. A ceiling of 1 against a 250 ms server
    /// presents as a clean "4/s per kind" right up until two kinds are run at once. The discriminator
    /// is to issue the kinds **strictly serially with no delay at all**: that is the fastest a ceiling
    /// of 1 permits, so a real rate budget trips on it and a concurrency ceiling does not.
    pub fn with_group(mut self, keys: &[u32], max: usize) -> Self {
        if max > 0 {
            let gate = Arc::new(Semaphore::new(max));
            for &k in keys {
                self.gates.insert(k, gate.clone());
            }
        }
        self
    }

    /// Slots currently free for `code`, or `None` if uncapped. Test/diagnostic use.
    pub fn available(&self, code: u32) -> Option<usize> {
        self.gates.get(&code).map(|g| g.available_permits())
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;
    use std::time::Duration;

    /// A group is **one** ceiling shared by every kind in it, not one each.
    ///
    /// The distinction is the entire reason the method exists: a server that answers one bulk read at
    /// a time does not care which bulk read it is, and per-kind gates that are each individually
    /// correct still let two through together.
    #[tokio::test]
    async fn a_group_shares_one_ceiling_across_its_kinds() {
        let limits = Arc::new(RequestLimits::new(&HashMap::new()).with_group(&[1, 2, 3], 1));
        let held = limits.acquire(1).await.expect("slot");
        assert_eq!(
            limits.available(2),
            Some(0),
            "a sibling kind sees the slot as taken"
        );
        assert_eq!(limits.available(3), Some(0));

        let l = limits.clone();
        let other = tokio::spawn(async move { l.acquire(2).await.is_some() });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !other.is_finished(),
            "a different kind in the group was admitted alongside"
        );

        drop(held);
        assert!(tokio::time::timeout(Duration::from_secs(1), other)
            .await
            .expect("released")
            .unwrap());
    }

    /// A group replaces the per-kind gate for its keys, and leaves every other key alone.
    #[tokio::test]
    async fn a_group_overrides_per_kind_gates_and_touches_nothing_else() {
        let caps: HashMap<u32, usize> = [(1, 5), (9, 2)].into_iter().collect();
        let limits = RequestLimits::new(&caps).with_group(&[1, 2], 1);
        assert_eq!(
            limits.available(1),
            Some(1),
            "key 1 took the group's ceiling, not its old 5"
        );
        assert_eq!(limits.available(2), Some(1));
        assert_eq!(
            limits.available(9),
            Some(2),
            "an ungrouped key keeps its own gate"
        );
        assert!(
            !limits.is_limited(77),
            "and an unlisted key is still unlimited"
        );
    }

    /// `0` means unlimited here too, matching `new` rather than deadlocking the whole group.
    #[tokio::test]
    async fn a_group_ceiling_of_zero_is_unlimited() {
        let limits = RequestLimits::new(&HashMap::new()).with_group(&[1, 2], 0);
        assert!(!limits.is_limited(1));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), limits.acquire(1))
                .await
                .expect("must not block")
                .is_none()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn caps(pairs: &[(u32, usize)]) -> HashMap<u32, usize> {
        pairs.iter().copied().collect()
    }

    #[tokio::test]
    async fn an_uncapped_command_never_waits_and_yields_no_permit() {
        let limits = RequestLimits::new(&caps(&[(11, 1)]));
        assert!(!limits.is_limited(3));
        assert!(
            limits.acquire(3).await.is_none(),
            "an uncapped code hands out no permit"
        );
        assert_eq!(limits.available(3), None);
        // ...and taking the capped one does not affect it.
        let _held = limits.acquire(11).await.expect("capped");
        assert!(limits.acquire(3).await.is_none());
    }

    #[tokio::test]
    async fn a_capped_command_serializes_and_the_permit_is_returned_on_drop() {
        let limits = Arc::new(RequestLimits::new(&caps(&[(11, 1)])));
        let first = limits.acquire(11).await.expect("first slot");
        assert_eq!(limits.available(11), Some(0));

        // A second caller must wait, not fail and not proceed.
        let l = limits.clone();
        let second = tokio::spawn(async move { l.acquire(11).await.is_some() });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !second.is_finished(),
            "the second caller was let through while the first held the slot"
        );

        drop(first);
        assert!(tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("released")
            .unwrap());
    }

    #[tokio::test]
    async fn a_dropped_caller_returns_its_slot() {
        // The case a hand-rolled in-flight counter gets wrong: the caller's future is dropped
        // mid-request (a timeout, a `select!` that lost) and the slot must come back anyway.
        let limits = Arc::new(RequestLimits::new(&caps(&[(5, 1)])));
        {
            let held = limits.acquire(5).await.expect("slot");
            assert_eq!(limits.available(5), Some(0));
            drop(held);
        }
        assert_eq!(
            limits.available(5),
            Some(1),
            "the slot returned without anyone releasing it"
        );

        // And a future abandoned while *waiting* must not leave the gate wedged.
        let held = limits.acquire(5).await.expect("slot");
        let l = limits.clone();
        let waiter = tokio::spawn(async move { l.acquire(5).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        waiter.abort();
        let _ = waiter.await;
        drop(held);
        assert_eq!(
            limits.available(5),
            Some(1),
            "an abandoned waiter consumed the slot"
        );
    }

    #[tokio::test]
    async fn a_limit_of_zero_means_unlimited_not_deadlocked() {
        // `0` is what a caller writes to mean "no cap"; taking it literally would hang every request.
        let limits = RequestLimits::new(&caps(&[(11, 0)]));
        assert!(!limits.is_limited(11));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), limits.acquire(11))
                .await
                .expect("must not block")
                .is_none()
        );
    }

    #[tokio::test]
    async fn waiters_are_served_in_arrival_order_so_a_fan_out_cannot_starve() {
        // The queueing discipline is part of the contract, not an implementation detail: a fan-out of
        // N calls on a gated command is N-deep on this semaphore, and LIFO or arbitrary wake order
        // would let the tail wait past `request_timeout` while later arrivals overtook it. Tokio's
        // Semaphore documents itself as fair; this pins that we depend on it.
        let limits = Arc::new(RequestLimits::new(&caps(&[(11, 1)])));
        let held = limits.acquire(11).await.expect("slot");

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut waiters = Vec::new();
        for i in 0..5u32 {
            let (l, ord) = (limits.clone(), order.clone());
            waiters.push(tokio::spawn(async move {
                let _p = l.acquire(11).await;
                ord.lock().unwrap().push(i);
            }));
            // Serialize *enqueue* order; without this the spawns race and there is no arrival order
            // to assert against.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        drop(held);
        for w in waiters {
            tokio::time::timeout(Duration::from_secs(1), w)
                .await
                .expect("drained")
                .unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            vec![0, 1, 2, 3, 4],
            "the gate is not FIFO"
        );
    }

    #[tokio::test]
    async fn a_cap_above_one_admits_exactly_that_many() {
        let limits = Arc::new(RequestLimits::new(&caps(&[(11, 2)])));
        let _a = limits.acquire(11).await.expect("1");
        let _b = limits.acquire(11).await.expect("2");
        assert_eq!(limits.available(11), Some(0));
        let l = limits.clone();
        let third = tokio::spawn(async move { l.acquire(11).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !third.is_finished(),
            "a third caller was admitted to a cap of 2"
        );
        third.abort();
    }
}
